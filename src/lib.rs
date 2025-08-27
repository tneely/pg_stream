mod connect;
mod message;

use bytes::BytesMut;
pub use message::{backend, frontend};

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
}
