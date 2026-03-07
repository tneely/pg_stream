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
//! use pg_stream::message::FrontendMessage;
//!
//! let stream = tokio::net::TcpStream::connect("localhost:5432").await?;
//! let mut conn = PgConnection::new(stream);
//!
//! // Build messages using the FrontendMessage trait
//! conn.buf()
//!     .query("SELECT 1")
//!     .sync();
//!
//! // Send buffered messages
//! conn.flush().await?;
//!
//! // Read response
//! let frame = conn.recv().await?;
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
//! use pg_stream::message::FrontendMessage;
//!
//! let stream = TcpStream::connect("localhost:5432")?;
//! let mut conn = PgConnection::new(stream);
//!
//! // Build messages using the FrontendMessage trait
//! conn.buf()
//!     .query("SELECT 1")
//!     .sync();
//!
//! // Send buffered messages
//! conn.flush_sync()?;
//!
//! // Read response
//! let frame = conn.recv_sync()?;
//! # Ok(())
//! # }
//! ```

use bytes::{Bytes, BytesMut};

#[cfg(feature = "async")]
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

#[cfg(feature = "sync")]
use std::io::{Read, Write};

use crate::message::backend::{self, PgFrame};

/// A Postgres connection wrapping a stream with buffered message building.
///
/// `PgConnection` provides:
/// - A write buffer accessible via [`buf()`](Self::buf) for building messages
/// - Async methods for flushing messages and receiving frames
///
/// Messages are built using the [`FrontendMessage`](crate::message::FrontendMessage)
/// extension trait on the buffer returned by `buf()`.
pub struct PgConnection<S> {
    stream: S,
    buf: BytesMut,
}

impl<S> PgConnection<S> {
    /// Create a new connection wrapping the given stream.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// Create a new connection with a specified buffer capacity.
    pub fn with_capacity(stream: S, capacity: usize) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(capacity),
        }
    }

    /// Returns a mutable reference to the write buffer.
    ///
    /// Use the [`FrontendMessage`](crate::message::FrontendMessage) trait
    /// to write protocol messages to this buffer.
    ///
    /// # Example
    ///
    /// ```
    /// # use bytes::BytesMut;
    /// # use pg_stream::connection::PgConnection;
    /// # use pg_stream::message::FrontendMessage;
    /// # let stream: Vec<u8> = vec![];
    /// # let mut conn = PgConnection::new(stream);
    /// conn.buf()
    ///     .query("SELECT 1")
    ///     .sync();
    /// ```
    pub fn buf(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// Take the buffered bytes, leaving an empty buffer.
    ///
    /// This is useful for manually sending the bytes or inspecting them.
    pub fn take_buf(&mut self) -> Bytes {
        self.buf.split().freeze()
    }

    /// Returns true if there are buffered bytes waiting to be sent.
    pub fn has_pending(&self) -> bool {
        !self.buf.is_empty()
    }

    /// Returns the number of buffered bytes.
    pub fn pending_len(&self) -> usize {
        self.buf.len()
    }

    /// Consume the connection and return the underlying stream and buffer.
    pub fn into_parts(self) -> (S, BytesMut) {
        (self.stream, self.buf)
    }

    /// Get a reference to the underlying stream.
    pub fn stream(&self) -> &S {
        &self.stream
    }

    /// Get a mutable reference to the underlying stream.
    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
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
            self.stream.write_all(&self.buf).await?;
            self.buf.clear();
        }
        self.stream.flush().await
    }

    /// Write raw bytes to the stream without buffering.
    ///
    /// This is useful for sending pre-built messages or SSL requests.
    pub async fn write_raw(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(bytes).await
    }
}

#[cfg(feature = "async")]
impl<S: AsyncRead + Unpin> PgConnection<S> {
    /// Read a single frame from the stream.
    ///
    /// This reads and parses one Postgres protocol frame from the
    /// underlying stream.
    pub async fn recv(&mut self) -> std::io::Result<PgFrame> {
        backend::read_frame(&mut self.stream).await
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
        if !self.buf.is_empty() {
            self.stream.write_all(&self.buf)?;
            self.buf.clear();
        }
        self.stream.flush()
    }

    /// Write raw bytes to the stream without buffering (synchronous version).
    ///
    /// This is useful for sending pre-built messages or SSL requests.
    pub fn write_raw_sync(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(bytes)
    }
}

#[cfg(feature = "sync")]
impl<S: Read> PgConnection<S> {
    /// Read a single frame from the stream (synchronous version).
    ///
    /// This reads and parses one Postgres protocol frame from the
    /// underlying stream.
    pub fn recv_sync(&mut self) -> std::io::Result<PgFrame> {
        backend::read_frame_sync(&mut self.stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::FrontendMessage;

    #[test]
    fn test_buf_access() {
        let stream: Vec<u8> = vec![];
        let mut conn = PgConnection::new(stream);

        conn.buf().query("SELECT 1");

        assert!(conn.has_pending());
        assert!(conn.pending_len() > 0);
    }

    #[test]
    fn test_take_buf() {
        let stream: Vec<u8> = vec![];
        let mut conn = PgConnection::new(stream);

        conn.buf().sync();
        let bytes = conn.take_buf();

        assert!(!bytes.is_empty());
        assert!(!conn.has_pending());
    }

    #[test]
    fn test_into_parts() {
        let stream: Vec<u8> = vec![];
        let mut conn = PgConnection::new(stream);

        conn.buf().query("test");
        let (stream, buf) = conn.into_parts();

        assert!(stream.is_empty());
        assert!(!buf.is_empty());
    }

    #[cfg(feature = "async")]
    mod async_tests {
        use super::*;

        #[tokio::test]
        async fn test_flush() {
            let mut output = Vec::new();
            let mut conn = PgConnection::new(&mut output);

            conn.buf().sync();
            conn.flush().await.unwrap();

            // Sync message: 'S' + length(4)
            assert_eq!(output.len(), 5);
            assert_eq!(output[0], b'S');
        }

        #[tokio::test]
        async fn test_recv() {
            // Create a buffer with a valid frame: ReadyForQuery 'Z' + len=5 + 'I'
            let input: &[u8] = &[b'Z', 0, 0, 0, 5, b'I'];
            let mut conn = PgConnection::new(input);

            let frame = conn.recv().await.unwrap();

            assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
            assert_eq!(&frame.body[..], &[b'I']);
        }
    }

    #[cfg(feature = "sync")]
    mod sync_tests {
        use super::*;
        use std::io::Cursor;

        #[test]
        fn test_flush_sync() {
            let mut output = Vec::new();
            let mut conn = PgConnection::new(&mut output);

            conn.buf().sync();
            conn.flush_sync().unwrap();

            // Sync message: 'S' + length(4)
            assert_eq!(output.len(), 5);
            assert_eq!(output[0], b'S');
        }

        #[test]
        fn test_recv_sync() {
            // Create a buffer with a valid frame: ReadyForQuery 'Z' + len=5 + 'I'
            let input: &[u8] = &[b'Z', 0, 0, 0, 5, b'I'];
            let mut conn = PgConnection::new(Cursor::new(input));

            let frame = conn.recv_sync().unwrap();

            assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
            assert_eq!(&frame.body[..], &[b'I']);
        }
    }
}
