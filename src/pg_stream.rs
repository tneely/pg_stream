use std::io::{Read, Write};

use bytes::BufMut;
use futures::{AsyncRead, AsyncWrite};

use crate::{
    messages::{
        backend,
        frontend::{self, ParameterKind},
    },
    pg_stream_proto::PgStreamProto,
    put_cstring,
};

pub struct PgStream<S> {
    proto: PgStreamProto<S>,
}

impl<S> PgStream<S> {
    pub fn from_stream(stream: S) -> Self {
        PgStream {
            proto: PgStreamProto::from_stream(stream),
        }
    }

    pub fn into_parts(self) -> (S, Vec<u8>) {
        self.proto.into_parts()
    }

    pub fn put_query(&mut self, stmt: impl AsRef<str>) -> &mut Self {
        self.proto.put_query(stmt.as_ref().as_bytes());
        self
    }

    pub fn put_parse(
        &mut self,
        name: impl AsRef<str>,
        stmt: impl AsRef<str>,
        param_types: impl AsRef<[frontend::ParameterKind]>,
    ) -> &mut Self {
        self.proto.put_parse(
            name.as_ref().as_bytes(),
            stmt.as_ref().as_bytes(),
            ParameterKind::as_u32_array(param_types.as_ref()),
        );
        self
    }

    pub fn put_describe(&mut self, target: frontend::TargetKind) -> &mut Self {
        match target {
            frontend::TargetKind::Portal(name) => {
                self.proto.put_describe(b'P', name.as_bytes());
            }
            frontend::TargetKind::Statement(name) => {
                self.proto.put_describe(b'S', name.as_bytes());
            }
        };
        self
    }

    pub fn put_bind(
        &mut self,
        portal_name: impl AsRef<str>,
        stmt_name: impl AsRef<str>,
        params: impl AsRef<[frontend::BindParameter]>,
        result_codes: impl AsRef<[frontend::FormatCode]>,
    ) -> &mut Self {
        frontend::MessageCode::BIND.frame(&mut self.proto.buf, |b| {
            put_cstring(b, portal_name.as_ref().as_bytes());
            put_cstring(b, stmt_name.as_ref().as_bytes());

            let params = params.as_ref();

            // Format codes can be zero to indicate that there are no parameters
            // or that the parameters all use the default format (text); or one,
            // in which case the specified format code is applied to all
            // parameters; or it can equal the actual number of parameters.
            match params {
                [first, rest @ ..]
                    if rest.iter().all(|p| p.format_code() == first.format_code()) =>
                {
                    if first.format_code() == frontend::FormatCode::Text {
                        b.put_u16(0);
                    } else {
                        b.put_u16(1);
                        b.put_u16(first.format_code() as u16);
                    }
                }
                _ => {
                    b.put_u16(params.len() as u16);
                    for param in params {
                        b.put_u16(param.format_code() as u16);
                    }
                }
            };

            b.put_u16(params.len() as u16);
            for param in params {
                param.encode(b);
            }

            let result_codes = result_codes.as_ref();
            b.put_u16(result_codes.len() as u16);
            for code in result_codes {
                b.put_u16((*code).into());
            }
        });
        self
    }

    pub fn put_execute(
        &mut self,
        name: impl AsRef<str>,
        max_rows: impl Into<Option<u32>>,
    ) -> &mut Self {
        self.proto.put_execute(
            name.as_ref().as_bytes(),
            max_rows.into().unwrap_or_default(),
        );
        self
    }

    pub fn put_close(&mut self, target: frontend::TargetKind) -> &mut Self {
        match target {
            frontend::TargetKind::Portal(name) => {
                self.proto.put_close(b'P', name.as_bytes());
            }
            frontend::TargetKind::Statement(name) => {
                self.proto.put_close(b'S', name.as_bytes());
            }
        };
        self
    }

    pub fn put_flush(&mut self) -> &mut Self {
        self.proto.put_flush();
        self
    }

    pub fn put_sync(&mut self) -> &mut Self {
        self.proto.put_sync();
        self
    }
}

impl<S: Read> PgStream<S> {
    pub fn read_frame_blocking(&mut self) -> std::io::Result<backend::PgFrame> {
        backend::read_frame_blocking(&mut self.proto.stream)
    }
}

impl<S: AsyncRead + Unpin> PgStream<S> {
    pub async fn read_frame(&mut self) -> std::io::Result<backend::PgFrame> {
        backend::read_frame(&mut self.proto.stream).await
    }
}

impl<S: Write> PgStream<S> {
    pub fn flush_blocking(&mut self) -> std::io::Result<()> {
        self.proto.flush_blocking()
    }
}

impl<S: AsyncWrite + Unpin> PgStream<S> {
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.proto.flush().await
    }
}

#[cfg(test)]
mod tests {

    use futures::io::AllowStdIo;

    use crate::{PgStream, messages::backend::MessageCode};

    #[test]
    fn test_read_frame_blocking() {
        let stream = vec![b'Z', 0, 0, 0, 5, b'I'];
        let mut pg_stream = PgStream::<&[u8]>::from_stream(stream.as_ref());
        let frame = pg_stream.read_frame_blocking().unwrap();

        assert_eq!(frame.code, MessageCode::READY_FOR_QUERY);
        assert_eq!(frame.body.as_ref(), &[b'I']);
    }

    #[tokio::test]
    async fn test_read_frame() {
        let stream = vec![b'Z', 0, 0, 0, 5, b'I'];
        let mut pg_stream =
            PgStream::<AllowStdIo<&[u8]>>::from_stream(AllowStdIo::new(stream.as_ref()));
        let frame = pg_stream.read_frame().await.unwrap();

        assert_eq!(frame.code, MessageCode::READY_FOR_QUERY);
        assert_eq!(frame.body.as_ref(), &[b'I']);
    }
}
