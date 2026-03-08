//! I/O functions for reading backend messages.

use std::mem::size_of;

use bytes::BytesMut;

use super::parse::parse_message;
use super::{MessageCode, PgMessage};

/// Maximum allowed frame size from Postgres (1GiB).
///
/// This is an upper bound to prevent misbehaving servers from
/// allocating excessive memory or causing OOMs.
/// See: <https://github.com/postgres/postgres/blob/879c492480d0e9ad8155c4269f95c5e8add41901/src/include/utils/memutils.h#L40>
const MAX_FRAME_SIZE_BYTES: usize = 1 << 30; // 1GiB

/// Reads and parses a single Postgres message from an asynchronous `AsyncRead` stream.
#[cfg(feature = "async")]
pub async fn read_message(
    mut stream: impl tokio::io::AsyncRead + Unpin,
) -> std::io::Result<PgMessage> {
    use tokio::io::AsyncReadExt;

    let mut buf = [0; 1];
    stream.read_exact(&mut buf).await?;
    let code: MessageCode = u8::from_be_bytes(buf).into();

    let mut buf = [0; 4];
    stream.read_exact(&mut buf).await?;
    let len = u32::from_be_bytes(buf) as usize;

    if len > MAX_FRAME_SIZE_BYTES {
        let err_msg = format!("frame size exceeds {MAX_FRAME_SIZE_BYTES}B");
        return Err(std::io::Error::new(
            std::io::ErrorKind::QuotaExceeded,
            err_msg,
        ));
    }

    // SAFETY: The uninitialized bytes are never read
    let mut body = unsafe { init_body(len - size_of::<u32>())? };
    stream.read_exact(&mut body).await?;

    parse_message(code, body.freeze())
}

/// Reads and parses a single Postgres message from a synchronous `Read` stream.
#[cfg(feature = "sync")]
pub fn read_message_sync(mut stream: impl std::io::Read) -> std::io::Result<PgMessage> {
    use std::io::Read;

    let mut buf = [0; 1];
    stream.read_exact(&mut buf)?;
    let code: MessageCode = u8::from_be_bytes(buf).into();

    let mut buf = [0; 4];
    stream.read_exact(&mut buf)?;
    let len = u32::from_be_bytes(buf) as usize;

    if len > MAX_FRAME_SIZE_BYTES {
        let err_msg = format!("frame size exceeds {MAX_FRAME_SIZE_BYTES}B");
        return Err(std::io::Error::new(
            std::io::ErrorKind::QuotaExceeded,
            err_msg,
        ));
    }

    // SAFETY: The uninitialized bytes are never read
    let mut body = unsafe { init_body(len - size_of::<u32>())? };
    stream.read_exact(&mut body)?;

    parse_message(code, body.freeze())
}

#[inline]
unsafe fn init_body(len: usize) -> std::io::Result<BytesMut> {
    let mut body = BytesMut::with_capacity(len);
    unsafe {
        body.set_len(len);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use bytes::BufMut;

    use super::*;

    #[cfg(feature = "async")]
    mod async_tests {
        use super::*;

        #[tokio::test]
        async fn can_read_max_size_frame() {
            let mut buf = BytesMut::new();
            buf.put_u8(42);
            buf.put_u32(MAX_FRAME_SIZE_BYTES as u32);
            let err = read_message(buf.as_ref()).await.err().unwrap();
            // We only wrote 5 bytes but are trying to read 1 GiB so we'd expect an EoF
            assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        }

        #[tokio::test]
        async fn can_not_read_past_max_size_frame() {
            let mut buf = BytesMut::new();
            buf.put_u8(42);
            buf.put_u32(MAX_FRAME_SIZE_BYTES as u32 + 1);
            let err = read_message(buf.as_ref()).await.err().unwrap();
            assert_eq!(err.kind(), ErrorKind::QuotaExceeded);
        }
    }

    #[cfg(feature = "sync")]
    mod sync_tests {
        use std::io::Cursor;

        use super::*;

        #[test]
        fn can_read_max_size_frame_sync() {
            let mut buf = BytesMut::new();
            buf.put_u8(42);
            buf.put_u32(MAX_FRAME_SIZE_BYTES as u32);
            let err = read_message_sync(Cursor::new(buf.as_ref())).err().unwrap();
            // We only wrote 5 bytes but are trying to read 1 GiB so we'd expect an EoF
            assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        }

        #[test]
        fn can_not_read_past_max_size_frame_sync() {
            let mut buf = BytesMut::new();
            buf.put_u8(42);
            buf.put_u32(MAX_FRAME_SIZE_BYTES as u32 + 1);
            let err = read_message_sync(Cursor::new(buf.as_ref())).err().unwrap();
            assert_eq!(err.kind(), ErrorKind::QuotaExceeded);
        }
    }
}
