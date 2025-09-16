use std::io::{Read, Write};

use bytes::{BufMut, BytesMut};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::messages::{backend, frontend};

pub struct PgStream<S> {
    stream: S,
    buf: BytesMut,
}

impl<S> PgStream<S> {
    pub fn raw(stream: S) -> Self {
        PgStream {
            stream,
            buf: BytesMut::new(),
        }
    }

    pub fn into_parts(self) -> (S, Vec<u8>) {
        (self.stream, self.buf.to_vec())
    }

    pub fn put_bytes(&mut self, src: impl AsRef<[u8]>) {
        self.buf.put(src.as_ref());
    }

    pub fn put_query(&mut self, stmt: impl AsRef<str>) {
        frontend::MessageCode::QUERY.frame(&mut self.buf, |b| {
            b.put_slice(stmt.as_ref().as_bytes());
            b.put_u8(0);
        });
    }

    pub fn put_query_raw(&mut self, stmt: impl AsRef<[u8]>) {
        frontend::MessageCode::QUERY.frame(&mut self.buf, |b| {
            b.put_slice(stmt.as_ref());
        });
    }

    pub fn put_parse(
        &mut self,
        name: impl AsRef<str>,
        stmt: impl AsRef<str>,
        param_types: impl AsRef<[frontend::ParameterKind]>,
    ) {
        frontend::MessageCode::PARSE.frame(&mut self.buf, |b| {
            b.put_slice(name.as_ref().as_bytes());
            b.put_u8(0);

            b.put_slice(stmt.as_ref().as_bytes());
            b.put_u8(0);

            let param_types = param_types.as_ref();
            b.put_u16(param_types.len() as u16);
            for param_type in param_types {
                b.put_u32((*param_type).into());
            }
        });
    }

    pub fn put_parse_raw(
        &mut self,
        name: impl AsRef<[u8]>,
        stmt: impl AsRef<[u8]>,
        param_types: impl AsRef<[u32]>,
    ) {
        frontend::MessageCode::PARSE.frame(&mut self.buf, |b| {
            b.put_slice(name.as_ref());
            b.put_slice(stmt.as_ref());

            let param_types = param_types.as_ref();
            b.put_u16(param_types.len() as u16);
            for param_type in param_types {
                b.put_u32(*param_type);
            }
        });
    }

    pub fn put_describe(&mut self, target: frontend::TargetKind) {
        frontend::MessageCode::DESCRIBE.frame(&mut self.buf, |b| {
            let name = match target {
                frontend::TargetKind::Portal(name) => {
                    b.put_u8(b'P');
                    name
                }
                frontend::TargetKind::Statement(name) => {
                    b.put_u8(b'S');
                    name
                }
            };
            b.put_slice(name.as_bytes());
            b.put_u8(0);
        });
    }

    pub fn put_describe_raw(&mut self, describe_kind: impl Into<u8>, name: impl AsRef<[u8]>) {
        frontend::MessageCode::DESCRIBE.frame(&mut self.buf, |b| {
            b.put_u8(describe_kind.into());
            b.put_slice(name.as_ref());
        });
    }

    pub fn put_bind(
        &mut self,
        portal_name: impl AsRef<str>,
        stmt_name: impl AsRef<str>,
        params: impl AsRef<[frontend::BindParameter]>,
        result_codes: impl AsRef<[frontend::FormatCode]>,
    ) {
        frontend::MessageCode::BIND.frame(&mut self.buf, |b| {
            b.put_slice(portal_name.as_ref().as_bytes());
            b.put_u8(0);

            b.put_slice(stmt_name.as_ref().as_bytes());
            b.put_u8(0);

            let params = params.as_ref();
            // TODO: This can be zero to indicate that there are no parameters
            // or that the parameters all use the default format (text); or one,
            // in which case the specified format code is applied to all
            // parameters; or it can equal the actual number of parameters.
            b.put_u16(params.len() as u16);
            for param in params {
                b.put_u16(param.format_code() as u16);
            }

            b.put_u16(params.len() as u16);
            for param in params {
                param.encode(b);
            }

            let result_codes = result_codes.as_ref();
            // TODO: This can be zero to indicate that there are no result columns
            // or that the result columns all use the default format (text); or one,
            // in which case the specified format code is applied to all
            // result columns; or it can equal the actual number of result columns.
            b.put_u16(result_codes.len() as u16);
            for code in result_codes {
                b.put_u16((*code).into());
            }
        });
    }

    pub fn put_bind_raw<T: AsRef<[u8]>>(
        &mut self,
        portal_name: impl AsRef<[u8]>,
        stmt_name: impl AsRef<[u8]>,
        format_codes: impl AsRef<[u16]>,
        params: impl AsRef<[T]>,
        result_codes: impl AsRef<[u16]>,
    ) {
        frontend::MessageCode::BIND.frame(&mut self.buf, |b| {
            b.put_slice(portal_name.as_ref());
            b.put_slice(stmt_name.as_ref());

            let format_codes = format_codes.as_ref();
            b.put_u16(format_codes.len() as u16);
            for code in format_codes {
                b.put_u16(*code);
            }

            let params = params.as_ref();
            b.put_u16(params.len() as u16);
            for param in params {
                let param = param.as_ref();
                b.put_u32(param.len() as u32);
                b.put_slice(param);
            }

            let result_codes = result_codes.as_ref();
            b.put_u16(result_codes.len() as u16);
            for code in result_codes {
                b.put_u16(*code);
            }
        });
    }

    pub fn put_execute(&mut self, name: impl AsRef<str>, max_rows: impl Into<Option<u32>>) {
        frontend::MessageCode::EXECUTE.frame(&mut self.buf, |b| {
            b.put_slice(name.as_ref().as_bytes());
            b.put_u8(0);

            let max_rows = max_rows.into().unwrap_or_default();
            b.put_u32(max_rows);
        });
    }

    pub fn put_execute_raw(&mut self, name: impl AsRef<[u8]>, max_rows: impl Into<u32>) {
        frontend::MessageCode::EXECUTE.frame(&mut self.buf, |b| {
            b.put_slice(name.as_ref());
            b.put_u32(max_rows.into());
        });
    }

    pub fn put_close(&mut self, target: frontend::TargetKind) {
        frontend::MessageCode::CLOSE.frame(&mut self.buf, |b| {
            let name = match target {
                frontend::TargetKind::Portal(name) => {
                    b.put_u8(b'P');
                    name
                }
                frontend::TargetKind::Statement(name) => {
                    b.put_u8(b'S');
                    name
                }
            };
            b.put_slice(name.as_bytes());
            b.put_u8(0);
        });
    }

    pub fn put_close_raw(&mut self, close_kind: impl Into<u8>, name: impl AsRef<[u8]>) {
        frontend::MessageCode::CLOSE.frame(&mut self.buf, |b| {
            b.put_u8(close_kind.into());
            b.put_slice(name.as_ref());
        });
    }

    pub fn put_flush(&mut self) {
        frontend::MessageCode::FLUSH.frame(&mut self.buf, |_| {});
    }

    pub fn put_sync(&mut self) {
        frontend::MessageCode::SYNC.frame(&mut self.buf, |_| {});
    }
}

impl<S: Read> PgStream<S> {
    pub fn read_frame_blocking(&mut self) -> std::io::Result<backend::PgFrame> {
        let mut buf = [0; 1];
        self.stream.read_exact(&mut buf)?;
        let code: backend::MessageCode = u8::from_be_bytes(buf).into();

        let mut buf = [0; 4];
        self.stream.read_exact(&mut buf)?;
        let len = u32::from_be_bytes(buf) as usize - size_of::<u32>();

        // FIXME: Check len size before allocating too much space
        let mut body = BytesMut::with_capacity(len);
        // SAFETY: The uninitialized bytes are never read
        unsafe {
            body.set_len(len);
        }
        self.stream.read_exact(&mut body)?;

        Ok(backend::PgFrame::new(code, body))
    }
}

impl<S: Write> PgStream<S> {
    pub fn flush_blocking(&mut self) -> std::io::Result<()> {
        self.stream.write_all(&self.buf)?;
        self.stream.flush()
    }
}

impl<S: AsyncRead + Unpin> PgStream<S> {
    pub async fn read_frame(&mut self) -> std::io::Result<backend::PgFrame> {
        backend::read_frame(&mut self.stream).await
    }
}

impl<S: AsyncWrite + Unpin> PgStream<S> {
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.stream.write_all(&self.buf).await?;
        self.stream.flush().await
    }
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use crate::{
        PgStream,
        messages::{
            backend::MessageCode,
            frontend::{BindParameter, FormatCode, ParameterKind, TargetKind},
        },
    };

    #[test]
    fn test_put_query() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_query("SELECT 1");

        let mut expected = BytesMut::new();
        expected.put_u8(b'Q');
        expected.put_u32(13);
        expected.put(&b"SELECT 1\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_parse() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_parse("STMT", "SELECT 1", [ParameterKind::Unspecified]);

        let mut expected = BytesMut::new();
        expected.put_u8(b'P');
        expected.put_u32(24);
        expected.put(&b"STMT\0"[..]);
        expected.put(&b"SELECT 1\0"[..]);
        expected.put_u16(1);
        expected.put_u32(0);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_describe_stmt() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_describe(TargetKind::new_stmt("STMT"));

        let mut expected = BytesMut::new();
        expected.put_u8(b'D');
        expected.put_u32(10);
        expected.put_u8(b'S');
        expected.put(&b"STMT\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_describe_portal() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_describe(TargetKind::new_portal("PORTAL"));

        let mut expected = BytesMut::new();
        expected.put_u8(b'D');
        expected.put_u32(12);
        expected.put_u8(b'P');
        expected.put(&b"PORTAL\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_bind() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_bind(
            "PORTAL",
            "STMT",
            [BindParameter::Bool(false), BindParameter::Null],
            [FormatCode::Binary],
        );

        let mut expected = BytesMut::new();
        expected.put_u8(b'B');
        expected.put_u32(37);

        expected.put(&b"PORTAL\0"[..]);
        expected.put(&b"STMT\0"[..]);

        expected.put_u16(2);
        expected.put_u16(1);
        expected.put_u16(1);

        expected.put_u16(2);
        expected.put_u32(1);
        expected.put_u8(0);
        expected.put_i32(-1);

        expected.put_u16(1);
        expected.put_u16(1);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_execute() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_execute("PORTAL", None);

        let mut expected = BytesMut::new();
        expected.put_u8(b'E');
        expected.put_u32(15);
        expected.put(&b"PORTAL\0"[..]);
        expected.put_u32(0);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_close_stmt() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_close(TargetKind::new_stmt("STMT"));

        let mut expected = BytesMut::new();
        expected.put_u8(b'C');
        expected.put_u32(10);
        expected.put_u8(b'S');
        expected.put(&b"STMT\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_close_portal() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_close(TargetKind::new_portal("PORTAL"));

        let mut expected = BytesMut::new();
        expected.put_u8(b'C');
        expected.put_u32(12);
        expected.put_u8(b'P');
        expected.put(&b"PORTAL\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_flush() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_flush();

        let mut expected = BytesMut::new();
        expected.put_u8(b'H');
        expected.put_u32(4);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_sync() {
        let mut pg_stream = PgStream::raw(Vec::<u8>::new());
        pg_stream.put_sync();

        let mut expected = BytesMut::new();
        expected.put_u8(b'S');
        expected.put_u32(4);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_flush_blocking() {
        let stream = Vec::<u8>::new();
        let mut pg_stream = PgStream::raw(stream);
        pg_stream.put_sync();
        pg_stream.flush_blocking().unwrap();

        let mut expected = BytesMut::new();
        expected.put_u8(b'S');
        expected.put_u32(4);

        let (stream, _) = pg_stream.into_parts();

        assert_eq!(&stream, &expected);
    }

    #[test]
    fn test_read_frame_blocking() {
        let stream = vec![b'Z', 0, 0, 0, 5, b'I'];
        let mut pg_stream = PgStream::<&[u8]>::raw(stream.as_ref());
        let frame = pg_stream.read_frame_blocking().unwrap();

        assert_eq!(frame.code, MessageCode::READY_FOR_QUERY);
        assert_eq!(frame.body.as_ref(), &[b'I']);
    }
}
