//! Postgres connection handling.
//!
//! This module provides [`PgConnection`], a thin wrapper around a stream
//! that handles buffered message writing and frame reading.
//!
//! # Async Example
//!
//! ```no_run
//! # #[cfg(feature = "async")]
//! # async fn example() -> std::io::Result<()> {
//! use pg_stream::connection::PgConnection;
//! use pg_stream::message::PgProtocol;
//!
//! let stream = tokio::net::TcpStream::connect("localhost:5432").await?;
//! let mut conn = PgConnection::new(stream);
//!
//! // Build messages using the PgProtocol trait directly on the connection
//! conn.query("SELECT 1")
//!     .sync();
//!
//! // Send buffered messages
//! conn.flush().await?;
//!
//! // Read response
//! let msg = conn.recv().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Sync Example
//!
//! ```no_run
//! # #[cfg(feature = "sync")]
//! # fn example() -> std::io::Result<()> {
//! use std::net::TcpStream;
//! use pg_stream::connection::PgConnection;
//! use pg_stream::message::PgProtocol;
//!
//! let stream = TcpStream::connect("localhost:5432")?;
//! let mut conn = PgConnection::new(stream);
//!
//! // Build messages using the PgProtocol trait directly on the connection
//! conn.query("SELECT 1")
//!     .sync();
//!
//! // Send buffered messages
//! conn.flush_sync()?;
//!
//! // Read response
//! let msg = conn.recv_sync()?;
//! # Ok(())
//! # }
//! ```

use bytes::{BufMut, Bytes, BytesMut, buf::UninitSlice};

#[cfg(feature = "sync")]
use bytes::Buf;

#[cfg(feature = "async")]
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

#[cfg(feature = "sync")]
use std::io::{Read, Write};

use crate::message::backend::{MessageLimits, MessageReader, PgMessage};

/// A Postgres connection wrapping a stream with buffered message building.
///
/// `PgConnection` implements [`BufMut`] and [`PgProtocol`](crate::message::PgProtocol),
/// so protocol messages can be written directly on the connection:
///
/// ```
/// # use pg_stream::connection::PgConnection;
/// # use pg_stream::message::PgProtocol;
/// # let stream: Vec<u8> = vec![];
/// # let mut conn = PgConnection::new(stream);
/// conn.query("SELECT 1")
///     .sync();
/// ```
pub struct PgConnection<S> {
    stream: S,
    /// Outgoing write buffer for frontend messages.
    buf: BytesMut,
    /// Incoming read buffer / framer for backend messages.
    reader: MessageReader,
}

impl<S> PgConnection<S> {
    /// Create a new connection wrapping the given stream.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
            reader: MessageReader::new(),
        }
    }

    /// Create a new connection with a specified write-buffer capacity.
    pub fn with_capacity(stream: S, capacity: usize) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(capacity),
            reader: MessageReader::new(),
        }
    }

    /// Create a connection with custom backend-message size limits.
    pub fn with_message_limits(stream: S, limits: MessageLimits) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
            reader: MessageReader::with_limits(limits),
        }
    }

    /// Create a connection seeded with bytes already read from the stream, so
    /// the startup handshake can hand off anything buffered past `ReadyForQuery`.
    pub fn with_read_buffer(stream: S, prebuffered: BytesMut) -> Self {
        Self::with_read_buffer_and_limits(stream, prebuffered, MessageLimits::default())
    }

    /// Create a connection with custom limits and bytes already read from the
    /// stream.
    pub fn with_read_buffer_and_limits(
        stream: S,
        prebuffered: BytesMut,
        limits: MessageLimits,
    ) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
            reader: MessageReader::from_buffer_with_limits(prebuffered, limits),
        }
    }

    /// Take the buffered outgoing bytes, leaving an empty buffer.
    ///
    /// This is useful for manually sending the bytes or inspecting them.
    pub fn take_buf(&mut self) -> Bytes {
        self.buf.split().freeze()
    }

    /// Returns true if there are outgoing bytes waiting to be sent.
    pub fn has_pending(&self) -> bool {
        !self.buf.is_empty()
    }

    /// Returns the number of buffered outgoing bytes.
    pub fn pending_len(&self) -> usize {
        self.buf.len()
    }

    /// Returns `true` if a complete backend message is already buffered and can
    /// be returned by `recv` without reading from the stream.
    pub fn has_buffered_message(&self) -> bool {
        self.reader.has_message()
    }

    /// Consume the connection and return the underlying stream, the pending
    /// outgoing bytes, and any buffered incoming bytes.
    pub fn into_parts(self) -> (S, BytesMut, BytesMut) {
        (self.stream, self.buf, self.reader.into_buffer())
    }

    /// Get a reference to the underlying stream.
    pub fn stream(&self) -> &S {
        &self.stream
    }

    /// Get a mutable reference to the underlying stream when doing so cannot
    /// bypass buffered protocol data.
    ///
    /// Returns [`WouldBlock`](std::io::ErrorKind::WouldBlock) while outgoing
    /// bytes are pending or incoming bytes have been prefetched. Flush or
    /// receive that data first, or use [`into_parts`](Self::into_parts) to take
    /// explicit ownership of all three parts.
    pub fn stream_mut(&mut self) -> std::io::Result<&mut S> {
        if !self.buf.is_empty() || !self.reader.buffered().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "direct stream access would bypass buffered protocol data",
            ));
        }
        Ok(&mut self.stream)
    }
}

// Implement BufMut to enable PgProtocol trait methods directly on PgConnection
unsafe impl<S> BufMut for PgConnection<S> {
    fn remaining_mut(&self) -> usize {
        self.buf.remaining_mut()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        // SAFETY: caller guarantees cnt bytes have been initialized
        unsafe { self.buf.advance_mut(cnt) }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.buf.chunk_mut()
    }
}

// Async I/O implementation (feature-gated)
#[cfg(feature = "async")]
impl<S: AsyncWrite + Unpin> PgConnection<S> {
    /// Flush all buffered messages to the stream.
    ///
    /// This writes all pending bytes to the underlying stream and
    /// flushes the stream.
    pub async fn flush(&mut self) -> std::io::Result<()> {
        if !self.buf.is_empty() {
            self.stream.write_all_buf(&mut self.buf).await?;
        }
        self.stream.flush().await
    }

    /// Write raw bytes after any messages already buffered on the connection.
    ///
    /// The bytes join the normal outgoing queue before it is flushed, so a
    /// partial write or cancellation leaves only the unsent suffix pending.
    /// Resume a cancelled call with [`flush`](Self::flush), rather than calling
    /// `write_raw` with the same bytes again.
    pub async fn write_raw(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.buf.extend_from_slice(bytes);
        self.flush().await
    }
}

#[cfg(feature = "async")]
impl<S: AsyncRead + Unpin> PgConnection<S> {
    /// Read a single backend message. Reads are buffered internally, so one
    /// syscall may satisfy several `recv` calls and bodies are sliced zero-copy.
    pub async fn recv(&mut self) -> std::io::Result<PgMessage> {
        self.reader.read_message(&mut self.stream).await
    }
}

// Sync I/O implementation (feature-gated)
#[cfg(feature = "sync")]
impl<S: Write> PgConnection<S> {
    /// Flush all buffered messages to the stream (synchronous version).
    ///
    /// This writes all pending bytes to the underlying stream and
    /// flushes the stream.
    pub fn flush_sync(&mut self) -> std::io::Result<()> {
        while !self.buf.is_empty() {
            match self.stream.write(&self.buf) {
                Ok(0) => {
                    return Err(std::io::Error::from(std::io::ErrorKind::WriteZero));
                }
                Ok(written) => self.buf.advance(written),
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
        self.stream.flush()
    }

    /// Write raw bytes after any messages already buffered on the connection.
    ///
    /// A partial-write error leaves only the unsent suffix in the connection;
    /// call [`flush_sync`](Self::flush_sync) to resume it.
    pub fn write_raw_sync(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.buf.extend_from_slice(bytes);
        self.flush_sync()
    }
}

#[cfg(feature = "sync")]
impl<S: Read> PgConnection<S> {
    /// Read a single backend message from the stream (synchronous version).
    ///
    /// Reads are buffered internally like the async [`recv`](Self::recv).
    pub fn recv_sync(&mut self) -> std::io::Result<PgMessage> {
        self.reader.read_message_sync(&mut self.stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::PgProtocol;

    #[test]
    fn test_frontend_message_methods() {
        let stream: Vec<u8> = vec![];
        let mut conn = PgConnection::new(stream);

        conn.query("SELECT 1");

        assert!(conn.has_pending());
        assert!(conn.pending_len() > 0);
    }

    #[test]
    fn test_take_buf() {
        let stream: Vec<u8> = vec![];
        let mut conn = PgConnection::new(stream);

        conn.sync();
        let bytes = conn.take_buf();

        assert!(!bytes.is_empty());
        assert!(!conn.has_pending());
    }

    #[test]
    fn test_into_parts() {
        let stream: Vec<u8> = vec![];
        let mut conn = PgConnection::new(stream);

        conn.query("test");
        let (stream, buf, read_buf) = conn.into_parts();

        assert!(stream.is_empty());
        assert!(!buf.is_empty());
        assert!(read_buf.is_empty());
    }

    #[test]
    fn test_chaining() {
        let stream: Vec<u8> = vec![];
        let mut conn = PgConnection::new(stream);

        // Test that chaining works directly on PgConnection
        conn.query("SELECT 1").sync().terminate();

        assert!(conn.pending_len() > 0);
    }

    #[test]
    fn test_builder_chaining() {
        let stream: Vec<u8> = vec![];
        let mut conn = PgConnection::new(stream);

        // Test that builders return &mut PgConnection for chaining
        conn.parse(None)
            .query("SELECT $1")
            .finish()
            .execute(None, 0)
            .sync();

        assert!(conn.pending_len() > 0);
    }

    #[test]
    fn test_stream_mut_requires_empty_protocol_buffers() {
        let mut conn = PgConnection::new(Vec::<u8>::new());
        conn.sync();
        assert_eq!(
            conn.stream_mut().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );

        let _ = conn.take_buf();
        assert!(conn.stream_mut().is_ok());

        let mut conn =
            PgConnection::with_read_buffer(Vec::<u8>::new(), BytesMut::from(&b"prefetched"[..]));
        assert_eq!(
            conn.stream_mut().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[cfg(feature = "async")]
    mod async_tests {
        use std::{
            future::{Future, poll_fn},
            pin::Pin,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
            task::{Context, Poll},
        };

        use tokio::io::AsyncWrite;

        use super::*;

        struct PendingAfterFirstWrite {
            output: Vec<u8>,
            blocked: Arc<AtomicBool>,
            wrote_once: bool,
        }

        impl AsyncWrite for PendingAfterFirstWrite {
            fn poll_write(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                bytes: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                if self.wrote_once && self.blocked.load(Ordering::Relaxed) {
                    return Poll::Pending;
                }

                let written = bytes.len().min(2);
                self.output.extend_from_slice(&bytes[..written]);
                self.wrote_once = true;
                Poll::Ready(Ok(written))
            }

            fn poll_flush(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        struct ErrorAfterFirstWrite {
            output: Vec<u8>,
            writes: usize,
        }

        impl AsyncWrite for ErrorAfterFirstWrite {
            fn poll_write(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                bytes: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                self.writes += 1;
                if self.writes == 2 {
                    return Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)));
                }

                let written = if self.writes == 1 {
                    bytes.len().min(2)
                } else {
                    bytes.len()
                };
                self.output.extend_from_slice(&bytes[..written]);
                Poll::Ready(Ok(written))
            }

            fn poll_flush(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        #[tokio::test]
        async fn test_flush() {
            let mut output = Vec::new();
            let mut conn = PgConnection::new(&mut output);

            conn.sync();
            conn.flush().await.unwrap();

            // Sync message: 'S' + length(4)
            assert_eq!(output.len(), 5);
            assert_eq!(output[0], b'S');
        }

        #[tokio::test]
        async fn flush_preserves_progress_across_cancellation() {
            let blocked = Arc::new(AtomicBool::new(true));
            let writer = PendingAfterFirstWrite {
                output: Vec::new(),
                blocked: blocked.clone(),
                wrote_once: false,
            };
            let mut conn = PgConnection::new(writer);
            conn.sync();

            let mut flush = Box::pin(conn.flush());
            poll_fn(|cx| {
                assert!(flush.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(flush);

            assert_eq!(conn.pending_len(), 3);
            blocked.store(false, Ordering::Relaxed);
            conn.flush().await.unwrap();

            let (writer, pending, _) = conn.into_parts();
            assert!(pending.is_empty());
            assert_eq!(writer.output, [b'S', 0, 0, 0, 4]);
        }

        #[tokio::test]
        async fn flush_preserves_progress_across_write_error() {
            let writer = ErrorAfterFirstWrite {
                output: Vec::new(),
                writes: 0,
            };
            let mut conn = PgConnection::new(writer);
            conn.sync();

            let err = conn.flush().await.unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
            assert_eq!(conn.pending_len(), 3);

            conn.flush().await.unwrap();
            let (writer, pending, _) = conn.into_parts();
            assert!(pending.is_empty());
            assert_eq!(writer.output, [b'S', 0, 0, 0, 4]);
        }

        #[tokio::test]
        async fn write_raw_follows_buffered_messages() {
            let mut conn = PgConnection::new(Vec::<u8>::new());
            conn.sync();
            conn.write_raw(b"raw").await.unwrap();

            let (output, pending, _) = conn.into_parts();
            assert!(pending.is_empty());
            assert_eq!(output, [b'S', 0, 0, 0, 4, b'r', b'a', b'w']);
        }

        #[tokio::test]
        async fn test_recv() {
            // Create a buffer with a valid message: ReadyForQuery 'Z' + len=5 + 'I'
            let input: &[u8] = &[b'Z', 0, 0, 0, 5, b'I'];
            let mut conn = PgConnection::new(input);

            let msg = conn.recv().await.unwrap();

            assert!(matches!(msg, PgMessage::ReadyForQuery(_)));
        }

        #[tokio::test]
        async fn test_with_read_buffer_serves_prebuffered_then_stream() {
            // Startup handed off one complete ParseComplete plus the first two
            // bytes of a ReadyForQuery; the rest arrives from the stream.
            let prebuffered = BytesMut::from(&[b'1', 0, 0, 0, 4, b'Z', 0][..]);
            let stream: &[u8] = &[0, 0, 5, b'I'];
            let mut conn = PgConnection::with_read_buffer(stream, prebuffered);

            assert!(conn.has_buffered_message());
            assert!(matches!(
                conn.recv().await.unwrap(),
                PgMessage::ParseComplete
            ));
            assert!(matches!(
                conn.recv().await.unwrap(),
                PgMessage::ReadyForQuery(_)
            ));
        }
    }

    #[cfg(feature = "sync")]
    mod sync_tests {
        use super::*;
        use std::io::{Cursor, Write};

        struct ErrorAfterFirstWrite {
            output: Vec<u8>,
            writes: usize,
        }

        impl Write for ErrorAfterFirstWrite {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.writes += 1;
                if self.writes == 2 {
                    return Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
                }

                let written = if self.writes == 1 {
                    bytes.len().min(2)
                } else {
                    bytes.len()
                };
                self.output.extend_from_slice(&bytes[..written]);
                Ok(written)
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        #[test]
        fn test_flush_sync() {
            let mut output = Vec::new();
            let mut conn = PgConnection::new(&mut output);

            conn.sync();
            conn.flush_sync().unwrap();

            // Sync message: 'S' + length(4)
            assert_eq!(output.len(), 5);
            assert_eq!(output[0], b'S');
        }

        #[test]
        fn flush_sync_preserves_progress_across_write_error() {
            let writer = ErrorAfterFirstWrite {
                output: Vec::new(),
                writes: 0,
            };
            let mut conn = PgConnection::new(writer);
            conn.sync();

            let err = conn.flush_sync().unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
            assert_eq!(conn.pending_len(), 3);

            conn.flush_sync().unwrap();
            let (writer, pending, _) = conn.into_parts();
            assert!(pending.is_empty());
            assert_eq!(writer.output, [b'S', 0, 0, 0, 4]);
        }

        #[test]
        fn write_raw_sync_follows_buffered_messages() {
            let mut conn = PgConnection::new(Vec::<u8>::new());
            conn.sync();
            conn.write_raw_sync(b"raw").unwrap();

            let (output, pending, _) = conn.into_parts();
            assert!(pending.is_empty());
            assert_eq!(output, [b'S', 0, 0, 0, 4, b'r', b'a', b'w']);
        }

        #[test]
        fn test_recv_sync() {
            // Create a buffer with a valid message: ReadyForQuery 'Z' + len=5 + 'I'
            let input: &[u8] = &[b'Z', 0, 0, 0, 5, b'I'];
            let mut conn = PgConnection::new(Cursor::new(input));

            let msg = conn.recv_sync().unwrap();

            assert!(matches!(msg, PgMessage::ReadyForQuery(_)));
        }
    }
}
