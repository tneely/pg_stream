//! I/O for reading backend messages.
//!
//! [`MessageReader`] buffers the stream so one read can frame many messages
//! while preserving partial progress across errors and cancelled async reads.

use std::{fmt, mem::size_of, ops::RangeInclusive};

use bytes::{Buf, BytesMut};

use super::parse::{parse_borrowed, parse_message};
use super::{BackendKeyData, MessageCode, PgMessage};

/// PostgreSQL's hard maximum for the value in a message length field.
///
/// See: <https://github.com/postgres/postgres/blob/879c492480d0e9ad8155c4269f95c5e8add41901/src/include/utils/memutils.h#L40>
const POSTGRES_MAX_FRAME_SIZE: usize = 1 << 30;

/// Message header: 1-byte code + 4-byte length.
const HEADER_LEN: usize = 1 + size_of::<u32>();

/// Default read size when the pending frame length is not yet known.
const DEFAULT_READ_SIZE: usize = 8 * 1024;

/// Configurable limits for backend message frames.
///
/// Sizes refer to PostgreSQL's length field: the four-byte length itself plus
/// the body, excluding the one-byte message code. Large payload messages use
/// `max_large_frame_size`; control messages and unknown codes use the smaller
/// limit. PostgreSQL's 1 GiB protocol maximum is always enforced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageLimits {
    max_small_frame_size: usize,
    max_large_frame_size: usize,
}

impl MessageLimits {
    /// Default maximum for control and metadata messages: 1 MiB.
    pub const DEFAULT_MAX_SMALL_FRAME_SIZE: usize = 1 << 20;

    /// Default maximum for bulk payload messages: 64 MiB.
    pub const DEFAULT_MAX_LARGE_FRAME_SIZE: usize = 64 << 20;

    /// Create separate limits for small and large message classes.
    pub const fn new(max_small_frame_size: usize, max_large_frame_size: usize) -> Self {
        Self {
            max_small_frame_size: protocol_max(max_small_frame_size),
            max_large_frame_size: protocol_max(max_large_frame_size),
        }
    }

    /// Apply one limit to every message class.
    pub const fn uniform(max_frame_size: usize) -> Self {
        Self::new(max_frame_size, max_frame_size)
    }

    /// Maximum configured length for control and metadata messages.
    pub const fn max_small_frame_size(self) -> usize {
        self.max_small_frame_size
    }

    /// Maximum configured length for bulk payload messages.
    pub const fn max_large_frame_size(self) -> usize {
        self.max_large_frame_size
    }
}

impl Default for MessageLimits {
    fn default() -> Self {
        Self::new(
            Self::DEFAULT_MAX_SMALL_FRAME_SIZE,
            Self::DEFAULT_MAX_LARGE_FRAME_SIZE,
        )
    }
}

const fn protocol_max(configured: usize) -> usize {
    if configured < POSTGRES_MAX_FRAME_SIZE {
        configured
    } else {
        POSTGRES_MAX_FRAME_SIZE
    }
}

/// DataRow, CopyData, FunctionCallResponse, RowDescription, ErrorResponse, and
/// NoticeResponse may legitimately carry substantially larger payloads.
#[inline]
fn is_large_message(code: MessageCode) -> bool {
    matches!(u8::from(code), b'D' | b'd' | b'V' | b'T' | b'E' | b'N')
}

/// Returns the values the length field may take for messages of known size,
/// allowing malformed frames to be rejected before reading a body.
#[inline]
fn frame_len_bounds(code: MessageCode) -> Option<RangeInclusive<usize>> {
    match u8::from(code) {
        b'1' | b'2' | b'3' | b'I' | b'n' | b's' | b'c' => Some(4..=4),
        b'Z' => Some(5..=5),
        // Length field + process ID + a variable-length secret key (3.2).
        b'K' => {
            const OVERHEAD: usize = 2 * size_of::<u32>();
            Some(
                OVERHEAD + BackendKeyData::MIN_SECRET_KEY_LEN
                    ..=OVERHEAD + BackendKeyData::MAX_SECRET_KEY_LEN,
            )
        }
        _ => None,
    }
}

/// Rejects a length field that no valid frame of this code can have, before
/// waiting on a body that will never parse.
#[inline]
fn check_frame_len(code: MessageCode, len: usize) -> std::io::Result<()> {
    match frame_len_bounds(code) {
        Some(bounds) if !bounds.contains(&len) => Err(invalid_data(format!(
            "{code} length is {len}, expected {}..={}",
            bounds.start(),
            bounds.end()
        ))),
        _ => Ok(()),
    }
}

#[inline]
fn invalid_data(msg: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.into())
}

/// Validates a frame length field and returns the body length (excluding the
/// length field itself).
#[inline]
fn body_len_from_frame_len(
    code: MessageCode,
    len: usize,
    limits: MessageLimits,
) -> std::io::Result<usize> {
    // The length field counts itself (4 bytes) but not the leading code byte,
    // so a value below 4 is malformed and would underflow the body length.
    if len < size_of::<u32>() {
        return Err(invalid_data(format!(
            "frame length {len} is smaller than its length field"
        )));
    }

    let small_limit = limits.max_small_frame_size;
    if len > small_limit {
        let limit = if is_large_message(code) {
            limits.max_large_frame_size
        } else {
            small_limit
        };
        if len <= limit {
            return Ok(len - size_of::<u32>());
        }
        return Err(std::io::Error::new(
            std::io::ErrorKind::QuotaExceeded,
            format!("{code} frame size {len} exceeds configured limit {limit}B"),
        ));
    }
    Ok(len - size_of::<u32>())
}

/// Frames one message off `buf[..*valid_len]`, or `Ok(None)` if incomplete.
/// `valid_len` is decremented with `buf` so a rejected frame is consumed once.
#[inline]
fn try_parse_frame(
    buf: &mut BytesMut,
    valid_len: &mut usize,
    limits: MessageLimits,
) -> std::io::Result<Option<PgMessage>> {
    if *valid_len < HEADER_LEN {
        return Ok(None);
    }

    let code = MessageCode::from(buf[0]);
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    let body_len = body_len_from_frame_len(code, len, limits)?;
    let frame_len = HEADER_LEN + body_len;

    if *valid_len < frame_len {
        check_frame_len(code, len)?;
        return Ok(None);
    }

    // Messages that borrow nothing from the body skip the `Bytes` handle, whose
    // refcount traffic otherwise dominates framing a small control message.
    if let Some(parsed) = parse_borrowed(code, &buf[HEADER_LEN..frame_len]) {
        buf.advance(frame_len);
        *valid_len -= frame_len;
        return parsed.map(Some).map_err(invalid_data);
    }

    buf.advance(HEADER_LEN);
    let body = buf.split_to(body_len).freeze();
    *valid_len -= frame_len;
    parse_message(code, body).map(Some)
}

/// A buffered reader that frames Postgres backend messages from a stream.
///
/// Message bodies are sliced zero-copy from an internal buffer, and a single
/// read can yield multiple messages. Any bytes read past the message returned
/// by [`read_message`](Self::read_message) are retained for the next call.
///
/// Async reads are cancellation safe as long as the same reader and stream are
/// used for the next call: all consumed header and body progress lives here,
/// rather than in the returned future.
pub struct MessageReader {
    // `buf` may include initialized spare bytes after a synchronous read.
    // Only `buf[..filled]` contains bytes received from the stream.
    buf: BytesMut,
    #[cfg(feature = "sync")]
    filled: usize,
    limits: MessageLimits,
}

impl Default for MessageReader {
    fn default() -> Self {
        Self {
            buf: BytesMut::new(),
            #[cfg(feature = "sync")]
            filled: 0,
            limits: MessageLimits::default(),
        }
    }
}

impl fmt::Debug for MessageReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageReader")
            .field("buffered_len", &self.filled_len())
            .field("capacity", &self.buf.capacity())
            .field("limits", &self.limits)
            .finish()
    }
}

impl MessageReader {
    /// Creates an empty reader.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a reader with the given initial buffer capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
            ..Self::default()
        }
    }

    /// Creates an empty reader with custom frame-size limits.
    pub fn with_limits(limits: MessageLimits) -> Self {
        Self {
            limits,
            ..Self::default()
        }
    }

    /// Creates a reader seeded with bytes already consumed from a stream.
    pub fn from_buffer(buf: BytesMut) -> Self {
        Self::from_buffer_with_limits(buf, MessageLimits::default())
    }

    /// Creates a reader with custom limits and bytes already consumed from a
    /// stream.
    pub fn from_buffer_with_limits(buf: BytesMut, limits: MessageLimits) -> Self {
        #[cfg(feature = "sync")]
        let filled = buf.len();
        Self {
            buf,
            #[cfg(feature = "sync")]
            filled,
            limits,
        }
    }

    /// Bytes buffered but not yet framed into a message.
    pub fn buffered(&self) -> &[u8] {
        &self.buf[..self.filled_len()]
    }

    /// Returns `true` if a full message is already buffered and can be framed
    /// without reading from the stream.
    pub fn has_message(&self) -> bool {
        matches!(
            peek_frame_len(self.buffered()),
            Some(frame_len) if self.filled_len() >= frame_len
        )
    }

    /// Consumes the reader, returning any buffered-but-unframed bytes.
    pub fn into_buffer(self) -> BytesMut {
        #[cfg(feature = "sync")]
        {
            let mut buf = self.buf;
            buf.truncate(self.filled);
            buf
        }
        #[cfg(not(feature = "sync"))]
        {
            self.buf
        }
    }

    #[inline]
    fn filled_len(&self) -> usize {
        #[cfg(feature = "sync")]
        {
            self.filled
        }
        #[cfg(not(feature = "sync"))]
        {
            self.buf.len()
        }
    }

    #[inline]
    fn try_parse_frame(&mut self) -> std::io::Result<Option<PgMessage>> {
        #[cfg(feature = "sync")]
        return try_parse_frame(&mut self.buf, &mut self.filled, self.limits);
        #[cfg(not(feature = "sync"))]
        {
            let mut valid_len = self.buf.len();
            try_parse_frame(&mut self.buf, &mut valid_len, self.limits)
        }
    }

    /// Returns the preferred size of the next read. A known frame is requested
    /// in full; its length is already bounded by [`MessageLimits`].
    #[inline]
    fn read_size_hint(&self) -> usize {
        let filled = self.filled_len();
        match peek_frame_len(self.buffered()) {
            Some(frame_len) => frame_len.saturating_sub(filled).max(1),
            None => DEFAULT_READ_SIZE,
        }
    }

    /// Reads and parses one message, reading from the stream only as needed.
    #[cfg(feature = "async")]
    pub async fn read_message(
        &mut self,
        stream: &mut (impl tokio::io::AsyncRead + Unpin),
    ) -> std::io::Result<PgMessage> {
        use tokio::io::AsyncReadExt;

        loop {
            if let Some(msg) = self.try_parse_frame()? {
                return Ok(msg);
            }

            let hint = self.read_size_hint();
            let filled = self.filled_len();
            let read = if filled < self.buf.len() {
                let end = (filled + hint).min(self.buf.len());
                let read = stream.read(&mut self.buf[filled..end]).await?;
                #[cfg(feature = "sync")]
                {
                    self.filled += read;
                }
                read
            } else {
                self.buf.reserve(hint);
                let read = stream.read_buf(&mut self.buf).await?;
                #[cfg(feature = "sync")]
                {
                    self.filled += read;
                }
                read
            };

            if read == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }
        }
    }

    /// Reads and parses one message from a synchronous stream.
    #[cfg(feature = "sync")]
    pub fn read_message_sync(
        &mut self,
        stream: &mut impl std::io::Read,
    ) -> std::io::Result<PgMessage> {
        loop {
            if let Some(msg) = self.try_parse_frame()? {
                return Ok(msg);
            }

            let read_size = self.read_size_hint();
            match read_into_spare(stream, &mut self.buf, &mut self.filled, read_size) {
                Ok(0) => {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
                }
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err),
            }
        }
    }
}

/// Peeks the total frame length (header + body) if a full header is buffered.
#[inline]
fn peek_frame_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < HEADER_LEN {
        return None;
    }
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    Some(1 + len)
}

/// Reads into initialized spare bytes, retaining them across calls so short
/// reads do not repeatedly zero the full spare capacity.
#[cfg(feature = "sync")]
fn read_into_spare(
    stream: &mut impl std::io::Read,
    buf: &mut BytesMut,
    filled: &mut usize,
    read_size: usize,
) -> std::io::Result<usize> {
    let end = *filled + read_size;
    if buf.len() < end {
        buf.resize(end, 0);
    }

    let read = stream.read(&mut buf[*filled..end])?;
    *filled += read;
    Ok(read)
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use bytes::BufMut;

    use super::*;

    fn frame(code: u8, body: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(code);
        buf.put_u32((body.len() + 4) as u32);
        buf.extend_from_slice(body);
        buf
    }

    fn split_frame(buf: &mut BytesMut) -> std::io::Result<Option<PgMessage>> {
        let mut valid_len = buf.len();
        try_parse_frame(buf, &mut valid_len, MessageLimits::default())
    }

    #[test]
    fn message_limits_are_bounded_by_postgres_maximum() {
        let limits = MessageLimits::new(usize::MAX, usize::MAX);
        assert_eq!(limits.max_small_frame_size(), POSTGRES_MAX_FRAME_SIZE);
        assert_eq!(limits.max_large_frame_size(), POSTGRES_MAX_FRAME_SIZE);
    }

    #[test]
    fn message_limits_distinguish_small_and_large_frames() {
        let limits = MessageLimits::new(32, 128);

        let small = body_len_from_frame_len(MessageCode::PARAMETER_STATUS, 33, limits).unwrap_err();
        assert_eq!(small.kind(), ErrorKind::QuotaExceeded);

        assert_eq!(
            body_len_from_frame_len(MessageCode::COPY_DATA, 33, limits).unwrap(),
            29
        );
    }

    #[test]
    fn try_split_frame_rejects_short_length() {
        // Length field of 3 is smaller than the 4-byte length field itself.
        let mut buf = BytesMut::from(&[b'Z', 0, 0, 0, 3][..]);
        let err = split_frame(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[test]
    fn try_split_frame_rejects_malformed_fixed_size_message() {
        let mut buf = BytesMut::from(&[b'1', 0, 0, 0, 5][..]);
        let err = split_frame(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    /// A 3.2 BackendKeyData is bounded, not fixed at 12 bytes.
    #[test]
    fn try_split_frame_accepts_variable_length_backend_key_data() {
        let mut body = vec![0u8; 4];
        body.extend_from_slice(&[0xAB; 32]);
        let mut buf = BytesMut::from(&frame(b'K', &body)[..]);
        let PgMessage::BackendKeyData(parsed) = split_frame(&mut buf).unwrap().unwrap() else {
            panic!("expected BackendKeyData");
        };
        assert_eq!(parsed.secret_key(), &body[4..]);
    }

    #[test]
    fn try_split_frame_rejects_out_of_range_backend_key_data_len() {
        // Header only: the length field alone is enough to reject the frame.
        for len in [11u32, 4 + 4 + 257] {
            let mut buf = BytesMut::new();
            buf.put_u8(b'K');
            buf.put_u32(len);
            let err = split_frame(&mut buf).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData, "len {len}");
        }
    }

    #[test]
    fn try_split_frame_rejects_default_limit() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'd');
        buf.put_u32(MessageLimits::DEFAULT_MAX_LARGE_FRAME_SIZE as u32 + 1);
        let err = split_frame(&mut buf).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::QuotaExceeded);
    }

    #[test]
    fn try_split_frame_waits_for_full_body() {
        let mut buf = BytesMut::from(&frame(b'C', b"SELECT 1\0")[..2]);
        assert!(split_frame(&mut buf).unwrap().is_none());
    }

    #[test]
    fn try_split_frame_extracts_body_zero_copy() {
        let mut buf = BytesMut::from(&frame(b'C', b"SELECT 1\0")[..]);
        let PgMessage::CommandComplete(cmd) = split_frame(&mut buf).unwrap().unwrap() else {
            panic!("expected CommandComplete");
        };
        assert_eq!(cmd.tag(), "SELECT 1");
        assert!(buf.is_empty());
    }

    /// A body-less frame is consumed without leaving a `Bytes` handle alive, so
    /// the reader's buffer stays uniquely owned and reclaimable.
    #[test]
    fn body_less_frame_leaves_buffer_unshared() {
        let mut buf = BytesMut::with_capacity(64);
        buf.extend_from_slice(&frame(b'1', b""));
        buf.extend_from_slice(&frame(b'Z', b"I"));

        assert!(matches!(
            split_frame(&mut buf).unwrap().unwrap(),
            PgMessage::ParseComplete
        ));
        assert!(matches!(
            split_frame(&mut buf).unwrap().unwrap(),
            PgMessage::ReadyForQuery(_)
        ));
        assert!(buf.is_empty());

        // `try_reclaim` only succeeds while no other handle shares the storage.
        assert!(buf.try_reclaim(64));
    }

    #[cfg(feature = "async")]
    mod async_tests {
        use std::{
            future::{Future, poll_fn},
            pin::Pin,
            task::{Context, Poll},
        };

        use tokio::io::{AsyncRead, ReadBuf};

        use super::*;

        struct PendingOnceReader<'a> {
            bytes: &'a [u8],
            position: usize,
            pending: bool,
        }

        impl AsyncRead for PendingOnceReader<'_> {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                if self.position == 2 && self.pending {
                    self.pending = false;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                let remaining = &self.bytes[self.position..];
                let read = if self.position == 0 {
                    remaining.len().min(2)
                } else {
                    remaining.len().min(buf.remaining())
                };
                buf.put_slice(&remaining[..read]);
                self.position += read;
                Poll::Ready(Ok(()))
            }
        }

        #[tokio::test]
        async fn reader_frames_multiple_messages_from_one_buffer() {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&frame(b'1', b"")); // ParseComplete
            bytes.extend_from_slice(&frame(b'Z', b"I")); // ReadyForQuery
            let mut stream = bytes.as_slice();

            let mut reader = MessageReader::new();
            assert!(matches!(
                reader.read_message(&mut stream).await.unwrap(),
                PgMessage::ParseComplete
            ));
            assert!(matches!(
                reader.read_message(&mut stream).await.unwrap(),
                PgMessage::ReadyForQuery(_)
            ));
        }

        #[tokio::test]
        async fn reader_rejects_short_length() {
            let bytes = [b'Z', 0, 0, 0, 3];
            let mut stream = bytes.as_slice();
            let mut reader = MessageReader::new();
            let err = reader.read_message(&mut stream).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData);
        }

        #[tokio::test]
        async fn reader_eof_mid_frame_is_unexpected_eof() {
            let bytes = frame(b'C', b"SELECT 1\0");
            let mut stream = &bytes[..4];
            let mut reader = MessageReader::new();
            let err = reader.read_message(&mut stream).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        }

        #[tokio::test]
        async fn reader_preserves_progress_when_read_is_cancelled() {
            let bytes = frame(b'Z', b"I");
            let mut stream = PendingOnceReader {
                bytes: &bytes,
                position: 0,
                pending: true,
            };
            let mut reader = MessageReader::new();

            let mut read = Box::pin(reader.read_message(&mut stream));
            poll_fn(|cx| {
                assert!(read.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(read);

            assert_eq!(reader.buffered(), &bytes[..2]);
            assert!(matches!(
                reader.read_message(&mut stream).await.unwrap(),
                PgMessage::ReadyForQuery(_)
            ));
        }

        /// A frame the parser rejects must still be consumed, so the next read
        /// resumes at the following frame instead of re-reading the bad one.
        #[tokio::test]
        async fn reader_consumes_frame_rejected_by_parser() {
            let mut bytes = frame(b'Z', b"X"); // invalid transaction status
            bytes.extend_from_slice(&frame(b'1', b"")); // ParseComplete
            let mut stream = bytes.as_slice();
            let mut reader = MessageReader::new();

            let err = reader.read_message(&mut stream).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData);

            assert!(matches!(
                reader.read_message(&mut stream).await.unwrap(),
                PgMessage::ParseComplete
            ));
        }

        /// An over-limit frame must be rejected from the header alone, before its
        /// advertised size is ever reserved.
        #[tokio::test]
        async fn oversized_advertised_frame_is_rejected_without_allocating() {
            let mut header = [0u8; HEADER_LEN];
            header[0] = b'd';
            header[1..].copy_from_slice(&(POSTGRES_MAX_FRAME_SIZE as u32).to_be_bytes());
            let mut stream = header.as_slice();
            let mut reader = MessageReader::new();

            let err = reader.read_message(&mut stream).await.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::QuotaExceeded);
            assert!(reader.buf.capacity() < MessageLimits::DEFAULT_MAX_LARGE_FRAME_SIZE);
        }

        /// A frame within the configured limit is reserved in one step rather
        /// than grown incrementally.
        #[tokio::test]
        async fn in_limit_frame_reserves_once() {
            const BODY: usize = 256 * 1024;
            let mut bytes = vec![b'd'];
            bytes.extend_from_slice(&((BODY + 4) as u32).to_be_bytes());
            bytes.extend_from_slice(&vec![b'x'; BODY]);
            let mut stream = bytes.as_slice();
            let mut reader = MessageReader::new();

            let PgMessage::CopyData(body) = reader.read_message(&mut stream).await.unwrap() else {
                panic!("expected CopyData");
            };
            assert_eq!(body.len(), BODY);
        }
    }

    #[cfg(feature = "sync")]
    mod sync_tests {
        use std::io::{Cursor, Read};

        use super::*;

        struct ErrorAfterPrefix {
            bytes: Vec<u8>,
            position: usize,
            returned_error: bool,
        }

        impl Read for ErrorAfterPrefix {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.position == 3 && !self.returned_error {
                    self.returned_error = true;
                    return Err(std::io::Error::from(ErrorKind::WouldBlock));
                }

                let remaining = &self.bytes[self.position..];
                let read = if self.position == 0 {
                    remaining.len().min(3)
                } else {
                    remaining.len().min(buf.len())
                };
                buf[..read].copy_from_slice(&remaining[..read]);
                self.position += read;
                Ok(read)
            }
        }

        #[test]
        fn reader_frames_multiple_messages_sync() {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&frame(b'1', b""));
            bytes.extend_from_slice(&frame(b'Z', b"I"));
            let mut stream = Cursor::new(bytes);

            let mut reader = MessageReader::new();
            assert!(matches!(
                reader.read_message_sync(&mut stream).unwrap(),
                PgMessage::ParseComplete
            ));
            assert!(matches!(
                reader.read_message_sync(&mut stream).unwrap(),
                PgMessage::ReadyForQuery(_)
            ));
        }

        /// A frame the parser rejects must still be consumed, so the next read
        /// resumes at the following frame instead of re-reading the bad one.
        #[test]
        fn reader_consumes_frame_rejected_by_parser_sync() {
            let mut bytes = frame(b'Z', b"X"); // invalid transaction status
            bytes.extend_from_slice(&frame(b'1', b"")); // ParseComplete
            let mut stream = Cursor::new(bytes);
            let mut reader = MessageReader::new();

            let err = reader.read_message_sync(&mut stream).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData);

            assert!(matches!(
                reader.read_message_sync(&mut stream).unwrap(),
                PgMessage::ParseComplete
            ));
        }

        #[test]
        fn reader_error_does_not_expose_initialized_spare_as_wire_data() {
            let bytes = frame(b'Z', b"I");
            let mut stream = ErrorAfterPrefix {
                bytes: bytes.clone(),
                position: 0,
                returned_error: false,
            };
            let mut reader = MessageReader::new();

            let err = reader.read_message_sync(&mut stream).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::WouldBlock);
            assert_eq!(reader.buffered(), &bytes[..3]);
            assert!(reader.buf.len() > reader.filled);

            assert!(matches!(
                reader.read_message_sync(&mut stream).unwrap(),
                PgMessage::ReadyForQuery(_)
            ));
            assert!(reader.buffered().is_empty());
        }
    }
}
