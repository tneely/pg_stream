use std::io::Write;

use bytes::{BufMut, BytesMut};
use futures::{AsyncWrite, AsyncWriteExt};

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
}

impl<S: Write> PgStreamProto<S> {
    pub fn flush_blocking(&mut self) -> std::io::Result<()> {
        self.stream.write_all(&self.buf)?;
        self.stream.flush()
    }
}

impl<S: AsyncWrite + Unpin> PgStreamProto<S> {
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.stream.write_all(&self.buf).await?;
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
    use bytes::{BufMut, BytesMut};

    use crate::{PgStreamProto, messages::frontend::ParameterKind};

    #[test]
    fn test_put_query() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_query(b"SELECT 1");

        let mut expected = BytesMut::new();
        expected.put_u8(b'Q');
        expected.put_u32(13);
        expected.put(&b"SELECT 1\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_parse() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_parse(b"STMT", b"SELECT 1", &[ParameterKind::Unspecified as u32]);

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
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_describe(b'S', b"STMT");

        let mut expected = BytesMut::new();
        expected.put_u8(b'D');
        expected.put_u32(10);
        expected.put_u8(b'S');
        expected.put(&b"STMT\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_describe_portal() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_describe(b'P', b"PORTAL");

        let mut expected = BytesMut::new();
        expected.put_u8(b'D');
        expected.put_u32(12);
        expected.put_u8(b'P');
        expected.put(&b"PORTAL\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_bind() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_bind(b"PORTAL", b"STMT", &[1, 1], &[Some(&[0]), None], &[1]);

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
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_execute(b"PORTAL", 0);

        let mut expected = BytesMut::new();
        expected.put_u8(b'E');
        expected.put_u32(15);
        expected.put(&b"PORTAL\0"[..]);
        expected.put_u32(0);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_close_stmt() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_close(b'S', b"STMT");

        let mut expected = BytesMut::new();
        expected.put_u8(b'C');
        expected.put_u32(10);
        expected.put_u8(b'S');
        expected.put(&b"STMT\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_close_portal() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_close(b'P', b"PORTAL");

        let mut expected = BytesMut::new();
        expected.put_u8(b'C');
        expected.put_u32(12);
        expected.put_u8(b'P');
        expected.put(&b"PORTAL\0"[..]);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_flush() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_flush();

        let mut expected = BytesMut::new();
        expected.put_u8(b'H');
        expected.put_u32(4);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_put_sync() {
        let mut pg_stream = PgStreamProto::from_stream(Vec::<u8>::new());
        pg_stream.put_sync();

        let mut expected = BytesMut::new();
        expected.put_u8(b'S');
        expected.put_u32(4);

        assert_eq!(&pg_stream.buf, &expected);
    }

    #[test]
    fn test_flush_blocking() {
        let stream = Vec::<u8>::new();
        let mut pg_stream = PgStreamProto::from_stream(stream);
        pg_stream.put_sync();
        pg_stream.flush_blocking().unwrap();

        let mut expected = BytesMut::new();
        expected.put_u8(b'S');
        expected.put_u32(4);

        let (stream, _) = pg_stream.into_parts();

        assert_eq!(&stream, &expected);
    }
}
