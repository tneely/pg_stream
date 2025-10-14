use bytes::BufMut;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    messages::{
        backend,
        frontend::{self, FormatCode, ParameterKind, ResultFormat},
    },
    pg_stream_proto::PgStreamProto,
    put_cstring,
};

/// High-level Postgres protocol stream with type-safe message construction.
///
/// Wraps `PgStreamProto` to provide a more ergonomic API with string types
/// and higher-level abstractions for Postgres protocol messages.
pub struct PgStream<S> {
    proto: PgStreamProto<S>,
}

impl<S> PgStream<S> {
    /// Creates a new Postgres stream from an underlying stream.
    pub fn from_stream(stream: S) -> Self {
        PgStream {
            proto: PgStreamProto::from_stream(stream),
        }
    }

    /// Consumes the stream and returns the underlying stream and buffered data.
    pub fn into_parts(self) -> (S, Vec<u8>) {
        self.proto.into_parts()
    }

    /// Adds a simple query message to the buffer.
    ///
    /// # Arguments
    ///
    /// * `stmt` - SQL statement to execute
    pub fn put_query(&mut self, stmt: impl AsRef<str>) -> &mut Self {
        self.proto.put_query(stmt.as_ref().as_bytes());
        self
    }

    /// Adds a Parse message to the buffer for prepared statement creation.
    ///
    /// # Arguments
    ///
    /// * `name` - Name for the prepared statement (empty for unnamed)
    /// * `stmt` - SQL statement text
    /// * `param_types` - Parameter data types (empty to infer)
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

    /// Adds a Describe message to the buffer.
    ///
    /// # Arguments
    ///
    /// * `target` - Portal or statement to describe
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

    /// Adds a Bind message to the buffer for binding parameters to a prepared statement.
    ///
    /// # Arguments
    ///
    /// * `portal_name` - Name for the portal (empty for unnamed)
    /// * `stmt_name` - Name of the prepared statement to bind
    /// * `params` - Parameter values with their format codes
    /// * `result_format` - Desired format for result columns
    pub fn put_bind(
        &mut self,
        portal_name: impl AsRef<str>,
        stmt_name: impl AsRef<str>,
        params: impl AsRef<[frontend::BindParameter]>,
        result_format: ResultFormat,
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

            match result_format {
                ResultFormat::Text => {
                    b.put_u16(0); // Default is text
                }
                ResultFormat::Binary => {
                    b.put_u16(1);
                    b.put_u16(FormatCode::Binary as u16);
                }
                ResultFormat::Mixed(codes) => {
                    b.put_u16(codes.len() as u16);
                    for code in codes {
                        b.put_u16(*code as u16);
                    }
                }
            }
        });
        self
    }

    /// Adds an Execute message to the buffer for executing a bound portal.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the portal to execute
    /// * `max_rows` - Maximum number of rows to return (None or 0 for unlimited)
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

    /// Adds a Close message to the buffer.
    ///
    /// # Arguments
    ///
    /// * `target` - Portal or statement to close
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

    /// Adds a Flush message to the buffer to force sending buffered messages.
    pub fn put_flush(&mut self) -> &mut Self {
        self.proto.put_flush();
        self
    }

    /// Adds a Sync message to the buffer to end an extended query protocol sequence.
    pub fn put_sync(&mut self) -> &mut Self {
        self.proto.put_sync();
        self
    }

    /// Adds a function call message to the buffer.
    ///
    /// # Arguments
    ///
    /// * `obj_id` - OID of the function to call
    /// * `args` - Function arguments
    /// * `result_code` - Desired format for the result (text or binary)
    pub fn put_fn_call(
        &mut self,
        obj_id: u32,
        args: impl AsRef<[frontend::FunctionArg]>,
        result_code: FormatCode,
    ) -> &mut Self {
        frontend::MessageCode::FUNCTION_CALL.frame(&mut self.proto.buf, |b| {
            b.put_u32(obj_id);

            let args = args.as_ref();

            // Format codes can be zero to indicate that there are no arguments
            // or that the arguments all use the default format (text); or one,
            // in which case the specified format code is applied to all
            // arguments; or it can equal the actual number of arguments.
            match args {
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
                    b.put_u16(args.len() as u16);
                    for arg in args {
                        b.put_u16(arg.format_code() as u16);
                    }
                }
            };

            b.put_u16(args.len() as u16);
            for arg in args {
                arg.encode(b);
            }

            b.put_u16(result_code as u16);
        });
        self
    }
}

impl<S: AsyncRead + Unpin> PgStream<S> {
    /// Reads a backend message frame from the stream (async).
    pub async fn read_frame(&mut self) -> std::io::Result<backend::PgFrame> {
        backend::read_frame(&mut self.proto.stream).await
    }
}

impl<S: AsyncWrite + Unpin> PgStream<S> {
    /// Flushes the buffered messages to the stream (async).
    pub async fn flush(&mut self) -> std::io::Result<()> {
        self.proto.flush().await
    }
}

#[cfg(test)]
mod tests {
    use crate::{PgStream, messages::backend::MessageCode};

    #[tokio::test]
    async fn test_read_frame() {
        let stream = vec![b'Z', 0, 0, 0, 5, b'I'];
        let mut pg_stream = PgStream::<&[u8]>::from_stream(stream.as_ref());
        let frame = pg_stream.read_frame().await.unwrap();

        assert_eq!(frame.code, MessageCode::READY_FOR_QUERY);
        assert_eq!(frame.body.as_ref(), &[b'I']);
    }
}
