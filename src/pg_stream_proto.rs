use std::io::Write;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::messages::frontend;

/// A low level Postgres protocol stream with buffered message building.
///
/// Provides methods to construct Postgres frontend protocol messages
/// and flush them to the underlying stream.
pub struct PgStreamProto<S> {
    pub(crate) stream: S,
    pub(crate) buf: BytesMut,
}

impl<S> PgStreamProto<S> {
    /// Creates a new protocol stream from an underlying stream.
    pub fn from_stream(stream: S) -> Self {
        PgStreamProto {
            stream,
            buf: BytesMut::new(),
        }
    }

    /// Consumes the stream and returns the underlying stream and buffered data.
    pub fn into_parts(self) -> (S, Vec<u8>) {
        (self.stream, self.buf.to_vec())
    }

    /// Writes raw bytes directly to the buffer without framing.
    pub fn put_bytes(&mut self, src: &[u8]) -> &mut Self {
        self.buf.put(src);
        self
    }

    /// Adds a simple query message to the buffer.
    ///
    /// # Arguments
    ///
    /// * `stmt` - SQL statement to execute
    pub fn put_query(&mut self, stmt: &[u8]) -> &mut Self {
        frontend::MessageCode::QUERY.frame(&mut self.buf, |b| {
            put_cstring(b, stmt);
        });
        self
    }

    /// Adds a Parse message to the buffer for prepared statement creation.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for the prepared statement (empty for unnamed)
    /// * `stmt` - SQL statement text
    /// * `param_types` - OIDs of parameter data types (empty to infer)
    pub fn put_parse(&mut self, name: &[u8], stmt: &[u8], param_types: &[u32]) -> &mut Self {
        frontend::MessageCode::PARSE.frame(&mut self.buf, |b| {
            put_cstring(b, name);
            put_cstring(b, stmt);

            b.put_u16(param_types.len() as u16);
            for param_type in param_types {
                b.put_u32(*param_type);
            }
        });
        self
    }

    /// Adds a Describe message to the buffer.
    ///
    /// # Arguments
    ///
    /// * `describe_kind` - 'S' for statement or 'P' for portal
    /// * `name` - Name of the statement or portal to describe
    pub fn put_describe(&mut self, describe_kind: u8, name: &[u8]) -> &mut Self {
        frontend::MessageCode::DESCRIBE.frame(&mut self.buf, |b| {
            b.put_u8(describe_kind);
            put_cstring(b, name);
        });
        self
    }

    /// Adds a Bind message to the buffer for binding parameters to a prepared statement.
    ///
    /// # Arguments
    ///
    /// * `portal_name` - Name for the portal (empty for unnamed)
    /// * `stmt_name` - Name of the prepared statement to bind
    /// * `format_codes` - Format codes for parameters (0=text, 1=binary)
    /// * `params` - Parameter values (None for NULL)
    /// * `result_codes` - Format codes for result columns
    pub fn put_bind(
        &mut self,
        portal_name: &[u8],
        stmt_name: &[u8],
        format_codes: &[u16],
        params: &[Option<&[u8]>],
        result_codes: &[u16],
    ) -> &mut Self {
        frontend::MessageCode::BIND.frame(&mut self.buf, |b| {
            put_cstring(b, portal_name);
            put_cstring(b, stmt_name);

            b.put_u16(format_codes.len() as u16);
            for code in format_codes {
                b.put_u16(*code);
            }

            b.put_u16(params.len() as u16);
            for param in params {
                match param {
                    Some(param) => {
                        b.put_u32(param.len() as u32);
                        b.put_slice(param);
                    }
                    None => {
                        b.put_i32(-1);
                    }
                }
            }

            b.put_u16(result_codes.len() as u16);
            for code in result_codes {
                b.put_u16(*code);
            }
        });
        self
    }

    /// Adds an Execute message to the buffer for executing a bound portal.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the portal to execute
    /// * `max_rows` - Maximum number of rows to return (0 for unlimited)
    pub fn put_execute(&mut self, name: &[u8], max_rows: u32) -> &mut Self {
        frontend::MessageCode::EXECUTE.frame(&mut self.buf, |b| {
            put_cstring(b, name);
            b.put_u32(max_rows);
        });
        self
    }

    /// Adds a Close message to the buffer.
    ///
    /// # Arguments
    ///
    /// * `close_kind` - 'S' for statement or 'P' for portal
    /// * `name` - Name of the statement or portal to close
    pub fn put_close(&mut self, close_kind: u8, name: &[u8]) -> &mut Self {
        frontend::MessageCode::CLOSE.frame(&mut self.buf, |b| {
            b.put_u8(close_kind);
            put_cstring(b, name);
        });
        self
    }

    /// Adds a Flush message to the buffer to force sending buffered messages.
    pub fn put_flush(&mut self) -> &mut Self {
        frontend::MessageCode::FLUSH.frame(&mut self.buf, |_| {});
        self
    }

    /// Adds a Sync message to the buffer to end an extended query protocol sequence.
    pub fn put_sync(&mut self) -> &mut Self {
        frontend::MessageCode::SYNC.frame(&mut self.buf, |_| {});
        self
    }

    /// Adds a function call message to the buffer.
    ///
    /// # Arguments
    ///
    /// * `obj_id` - OID of the function to call
    /// * `format_codes` - Format codes for arguments (0=text, 1=binary)
    /// * `args` - Argument values (None for NULL)
    /// * `result_code` - Format code for the result (0=text, 1=binary)
    pub fn put_fn_call(
        &mut self,
        obj_id: u32,
        format_codes: &[u16],
        args: &[Option<&[u8]>],
        result_code: u16,
    ) -> &mut Self {
        frontend::MessageCode::FUNCTION_CALL.frame(&mut self.buf, |b| {
            b.put_u32(obj_id);

            b.put_u16(format_codes.len() as u16);
            for code in format_codes {
                b.put_u16(*code);
            }

            b.put_u16(args.len() as u16);
            for arg in args {
                match arg {
                    Some(arg) => {
                        b.put_u32(arg.len() as u32);
                        b.put_slice(arg);
                    }
                    None => {
                        b.put_i32(-1);
                    }
                }
            }

            b.put_u16(result_code);
        });
        self
    }
}

impl<S: Write> PgStreamProto<S> {
    /// Flushes the buffered messages to the stream (blocking).
    pub fn flush_blocking(&mut self) -> std::io::Result<()> {
        self.stream.write_all(&self.buf)?;
        self.buf.clear();
        self.stream.flush()
    }
}

impl<S: AsyncWrite + Unpin> PgStreamProto<S> {
    /// Flushes the buffered messages to the stream (async).
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.stream.write_all_buf(&mut self.buf).await?;
        self.stream.flush().await
    }
}

#[inline]
pub(crate) fn put_cstring(b: &mut impl bytes::BufMut, src: &[u8]) {
    b.put_slice(src);
    b.put_u8(0);
}

#[cfg(test)]
mod tests {
    use crate::{PgStreamProto, messages::frontend::ParameterKind};
    use bytes::Buf;

    /// Helper macro for asserting a slice or string from the buffer.
    /// Usage: `assert_buf_eq!(pg_stream, b"STMT\0");`
    macro_rules! assert_buf_eq {
        ($pg_stream:expr, $expected:expr) => {{
            let len = $expected.len();
            let got = $pg_stream.buf.copy_to_bytes(len);
            assert_eq!(&$expected[..], &got[..]);
        }};
    }

    #[test]
    fn test_put_query() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_query(b"SELECT 1");

        assert_eq!(b'Q', pg_stream.buf.get_u8());
        assert_eq!(13, pg_stream.buf.get_u32());
        assert_buf_eq!(pg_stream, b"SELECT 1\0");
    }

    #[test]
    fn test_put_parse() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_parse(b"STMT", b"SELECT 1", &[ParameterKind::Unspecified as u32]);

        assert_eq!(b'P', pg_stream.buf.get_u8());
        assert_eq!(24, pg_stream.buf.get_u32());
        assert_buf_eq!(pg_stream, b"STMT\0");
        assert_buf_eq!(pg_stream, b"SELECT 1\0");
        assert_eq!(1, pg_stream.buf.get_u16());
        assert_eq!(0, pg_stream.buf.get_u32());
    }

    #[test]
    fn test_put_describe_stmt() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_describe(b'S', b"STMT");

        assert_eq!(b'D', pg_stream.buf.get_u8());
        assert_eq!(10, pg_stream.buf.get_u32());
        assert_eq!(b'S', pg_stream.buf.get_u8());
        assert_buf_eq!(pg_stream, b"STMT\0");
    }

    #[test]
    fn test_put_describe_portal() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_describe(b'P', b"PORTAL");

        assert_eq!(b'D', pg_stream.buf.get_u8());
        assert_eq!(12, pg_stream.buf.get_u32());
        assert_eq!(b'P', pg_stream.buf.get_u8());
        assert_buf_eq!(pg_stream, b"PORTAL\0");
    }

    #[test]
    fn test_put_bind() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_bind(b"PORTAL", b"STMT", &[1, 1], &[Some(&[0]), None], &[1]);

        assert_eq!(b'B', pg_stream.buf.get_u8());
        assert_eq!(37, pg_stream.buf.get_u32());

        assert_buf_eq!(pg_stream, b"PORTAL\0");
        assert_buf_eq!(pg_stream, b"STMT\0");

        assert_eq!(2, pg_stream.buf.get_u16());
        assert_eq!(1, pg_stream.buf.get_u16());
        assert_eq!(1, pg_stream.buf.get_u16());

        assert_eq!(2, pg_stream.buf.get_u16());
        assert_eq!(1, pg_stream.buf.get_u32());
        assert_eq!(0, pg_stream.buf.get_u8());
        assert_eq!(-1, pg_stream.buf.get_i32());

        assert_eq!(1, pg_stream.buf.get_u16());
        assert_eq!(1, pg_stream.buf.get_u16());
    }

    #[test]
    fn test_put_execute() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_execute(b"PORTAL", 0);

        assert_eq!(b'E', pg_stream.buf.get_u8());
        assert_eq!(15, pg_stream.buf.get_u32());
        assert_buf_eq!(pg_stream, b"PORTAL\0");
        assert_eq!(0, pg_stream.buf.get_u32());
    }

    #[test]
    fn test_put_close_stmt() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_close(b'S', b"STMT");

        assert_eq!(b'C', pg_stream.buf.get_u8());
        assert_eq!(10, pg_stream.buf.get_u32());
        assert_eq!(b'S', pg_stream.buf.get_u8());
        assert_buf_eq!(pg_stream, b"STMT\0");
    }

    #[test]
    fn test_put_close_portal() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_close(b'P', b"PORTAL");

        assert_eq!(b'C', pg_stream.buf.get_u8());
        assert_eq!(12, pg_stream.buf.get_u32());
        assert_eq!(b'P', pg_stream.buf.get_u8());
        assert_buf_eq!(pg_stream, b"PORTAL\0");
    }

    #[test]
    fn test_put_flush() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_flush();

        assert_eq!(b'H', pg_stream.buf.get_u8());
        assert_eq!(4, pg_stream.buf.get_u32());
    }

    #[test]
    fn test_put_sync() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_sync();

        assert_eq!(b'S', pg_stream.buf.get_u8());
        assert_eq!(4, pg_stream.buf.get_u32());
    }

    #[test]
    fn test_put_fn_call() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_fn_call(101, &[1, 1], &[Some(&[0]), None], 1);

        assert_eq!(b'F', pg_stream.buf.get_u8());
        assert_eq!(27, pg_stream.buf.get_u32());

        assert_eq!(101, pg_stream.buf.get_u32());

        assert_eq!(2, pg_stream.buf.get_u16());
        assert_eq!(1, pg_stream.buf.get_u16());
        assert_eq!(1, pg_stream.buf.get_u16());

        assert_eq!(2, pg_stream.buf.get_u16());
        assert_eq!(1, pg_stream.buf.get_u32());
        assert_eq!(0, pg_stream.buf.get_u8());
        assert_eq!(-1, pg_stream.buf.get_i32());

        assert_eq!(1, pg_stream.buf.get_u16());
    }

    #[test]
    fn test_flush_blocking() {
        let stream = Vec::<u8>::new();
        let mut pg_stream = PgStreamProto::from_stream(stream);
        pg_stream.put_sync();
        pg_stream.flush_blocking().unwrap();

        let (stream, _) = pg_stream.into_parts();
        assert_eq!(b'S', stream[0]);
        assert_eq!(4, u32::from_be_bytes(stream[1..5].try_into().unwrap()));
    }
}
