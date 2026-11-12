//! The byte stream underneath a connection.

use std::{
    future::{Future, poll_fn},
    io::{self, Read, Write},
    pin::pin,
    task::{Context, Poll, Waker},
};

use crate::{Error, Frontend, Result, backend::BackendKey};

/// A bidirectional byte stream.
///
/// This is the only thing a [`Connection`](crate::Connection) asks of its I/O.
/// With the `tokio` feature it is implemented for everything that is
/// `AsyncRead + AsyncWrite + Unpin`; [`Blocking`] adapts `std::io` streams.
/// Implement it directly to run on another runtime.
///
/// Both directions are polled from the same task, and each must be able to
/// make progress while the other is pending. A connection relies on that to
/// keep taking replies while the server is slow to take requests.
pub trait Transport {
    /// Reads into `buf`, which is never empty, returning how many bytes were
    /// read. Zero means the peer closed the stream.
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>>;

    /// Writes from `buf`, returning how many bytes were written.
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>>;

    /// Pushes written bytes through any buffering layer, such as TLS.
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;
}

#[cfg(feature = "tokio")]
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Transport for T {
    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut buf = tokio::io::ReadBuf::new(buf);
        let read = std::pin::Pin::new(self).poll_read(cx, &mut buf);
        read.map_ok(|()| buf.filled().len())
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        std::pin::Pin::new(self).poll_write(cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        std::pin::Pin::new(self).poll_flush(cx)
    }
}

/// Adapts a blocking `std::io` stream into a [`Transport`] that is always
/// ready. Drive the resulting connection with [`block_on`].
///
/// A blocking stream does one thing at a time, so no reply is read while a
/// write waits. A server that has stopped taking requests, because the
/// replies it wants to send are not being read, then keeps the write waiting
/// for good. That takes a pipeline whose requests and replies both outgrow
/// what the socket buffers hold. To stay clear of it, read the replies to one
/// batch of requests before queueing the next, or use an async transport.
///
/// The stream must be in blocking mode: `WouldBlock` is reported as an error.
#[derive(Debug)]
pub struct Blocking<S>(pub S);

impl<S: Read + Write> Transport for Blocking<S> {
    fn poll_read(&mut self, _: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(uninterrupted(|| self.0.read(buf)))
    }

    fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(uninterrupted(|| self.0.write(buf)))
    }

    fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(uninterrupted(|| self.0.flush()))
    }
}

fn uninterrupted<T>(mut op: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match op() {
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

/// Runs a future over a [`Blocking`] transport to completion on the current
/// thread.
///
/// # Panics
///
/// If the future is not ready when first polled, which a future that only
/// awaits a `Blocking` transport always is.
// Compiled in the crate that calls it, which is otherwise free to leave it as
// a call around every future.
#[inline]
pub fn block_on<F: Future>(future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    match pin!(future).poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("block_on requires a future that only awaits Blocking transports"),
    }
}

/// Asks the server to switch to TLS, returning whether it agreed.
///
/// Call this on a freshly opened stream. On `true`, run the TLS handshake over
/// the same stream and build the [`Connection`](crate::Connection) on the
/// result; on `false`, either continue in plaintext or give up.
///
/// Exactly one byte is read, so nothing the server sends afterwards can be
/// mistaken for a reply that arrived before encryption began.
///
/// The exchange cannot be resumed. If this future is dropped before it
/// finishes, close the stream.
pub async fn request_tls<S: Transport>(stream: &mut S) -> Result<bool> {
    let mut request = Vec::new();
    request.ssl_request();
    write_all(stream, &request).await?;

    let mut reply = [0];
    if poll_fn(|cx| stream.poll_read(cx, &mut reply)).await? == 0 {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
    }
    match reply[0] {
        b'S' => Ok(true),
        b'N' => Ok(false),
        // Servers that predate SSLRequest answer with an ErrorResponse.
        code => Err(Error::unexpected(code, "an SSLRequest reply")),
    }
}

/// Asks the server to cancel whatever the session identified by `key` is
/// running.
///
/// `stream` must be a new connection to the same server, upgraded to TLS first
/// if the server requires it. It is consumed: a cancel connection carries
/// nothing else.
///
/// Cancellation is a race. The key names a session, not a query, so a request
/// that arrives late interrupts whatever that session runs next. Send nothing
/// more on the session until this returns and its
/// [`Response`](crate::Response) is finished.
pub async fn cancel<S: Transport>(mut stream: S, key: &BackendKey) -> Result<()> {
    let mut request = Vec::new();
    request.cancel_request(key.process_id, &key.secret_key);
    write_all(&mut stream, &request).await?;

    // The server acts on the request and then closes the connection, so
    // reading to the end is what confirms it was processed.
    let mut scratch = [0; 64];
    while let Ok(1..) = poll_fn(|cx| stream.poll_read(cx, &mut scratch)).await {}
    Ok(())
}

async fn write_all<S: Transport>(stream: &mut S, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        match poll_fn(|cx| stream.poll_write(cx, bytes)).await? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            written => bytes = &bytes[written..],
        }
    }
    poll_fn(|cx| stream.poll_flush(cx)).await
}
