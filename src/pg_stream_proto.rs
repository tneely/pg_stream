use std::io::Write;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::messages::frontend;

pub struct PgStreamProto<S> {
    pub(crate) stream: S,
    pub(crate) buf: BytesMut,
}

impl<S> PgStreamProto<S> {
    pub fn from_stream(stream: S) -> Self {
        PgStreamProto {
            stream,
            buf: BytesMut::new(),
        }
    }

    pub fn into_parts(self) -> (S, Vec<u8>) {
        (self.stream, self.buf.to_vec())
    }

    pub fn put_bytes(&mut self, src: &[u8]) -> &mut Self {
        self.buf.put(src);
        self
    }

    pub fn put_query(&mut self, stmt: &[u8]) -> &mut Self {
        frontend::MessageCode::QUERY.frame(&mut self.buf, |b| {
            put_cstring(b, stmt);
        });
        self
    }

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

    pub fn put_describe(&mut self, describe_kind: u8, name: &[u8]) -> &mut Self {
        frontend::MessageCode::DESCRIBE.frame(&mut self.buf, |b| {
            b.put_u8(describe_kind);
            put_cstring(b, name);
        });
        self
    }

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

    pub fn put_execute(&mut self, name: &[u8], max_rows: u32) -> &mut Self {
        frontend::MessageCode::EXECUTE.frame(&mut self.buf, |b| {
            put_cstring(b, name);
            b.put_u32(max_rows);
        });
        self
    }

    pub fn put_close(&mut self, close_kind: u8, name: &[u8]) -> &mut Self {
        frontend::MessageCode::CLOSE.frame(&mut self.buf, |b| {
            b.put_u8(close_kind);
            put_cstring(b, name);
        });
        self
    }

    pub fn put_flush(&mut self) -> &mut Self {
        frontend::MessageCode::FLUSH.frame(&mut self.buf, |_| {});
        self
    }

    pub fn put_sync(&mut self) -> &mut Self {
        frontend::MessageCode::SYNC.frame(&mut self.buf, |_| {});
        self
    }

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
    pub fn flush_blocking(&mut self) -> std::io::Result<()> {
        self.stream.write_all(&self.buf)?;
        self.buf.clear();
        self.stream.flush()
    }
}

impl<S: AsyncWrite + Unpin> PgStreamProto<S> {
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
