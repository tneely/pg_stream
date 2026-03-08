//! Low-level codec utilities for Postgres wire protocol framing.

use bytes::BufMut;

/// Writes a length-prefixed payload to a buffer.
///
/// The length field includes itself (4 bytes).
/// This is a low-level helper used by message encoding.
#[inline]
pub fn frame<B: BufMut>(buf: &mut B, payload_len: usize, payload_fn: impl FnOnce(&mut B)) {
    let len = frame_len(payload_len);
    buf.put_u32(len);
    payload_fn(buf);
}

/// Writes a null-terminated C string to the buffer.
#[inline]
pub fn put_cstring<B: BufMut>(buf: &mut B, s: &[u8]) {
    buf.put_slice(s);
    buf.put_u8(0);
}

/// Returns the number of bytes required for a C string in the protocol.
#[inline]
pub fn cstring_len(s: &[u8]) -> usize {
    s.len() + 1
}

#[inline]
fn frame_len(payload_len: usize) -> u32 {
    let len = payload_len
        .checked_add(4)
        .expect("frame payload length overflow");
    u32::try_from(len).expect("frame payload length exceeds u32::MAX - 4")
}

#[doc(hidden)]
#[macro_export]
macro_rules! __pg_frame_len {
    (u8, $arg:expr) => {
        1usize
    };
    (u16, $arg:expr) => {
        2usize
    };
    (u32, $arg:expr) => {
        4usize
    };
    (i16, $arg:expr) => {
        2usize
    };
    (i32, $arg:expr) => {
        4usize
    };
    (i64, $arg:expr) => {
        8usize
    };
    (f32, $arg:expr) => {
        4usize
    };
    (f64, $arg:expr) => {
        8usize
    };
    (bytes, $arg:expr) => {
        ::core::convert::AsRef::<[u8]>::as_ref(&$arg).len()
    };
    (cstring, $arg:expr) => {
        $crate::message::frontend::codec::cstring_len(::core::convert::AsRef::<[u8]>::as_ref(&$arg))
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __pg_frame_write {
    (u8, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_u8($buf, $arg);
    };
    (u16, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_u16($buf, $arg);
    };
    (u32, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_u32($buf, $arg);
    };
    (i16, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_i16($buf, $arg);
    };
    (i32, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_i32($buf, $arg);
    };
    (i64, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_i64($buf, $arg);
    };
    (f32, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_f32($buf, $arg);
    };
    (f64, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_f64($buf, $arg);
    };
    (bytes, $buf:expr, $arg:expr) => {
        bytes::BufMut::put_slice($buf, ::core::convert::AsRef::<[u8]>::as_ref(&$arg));
    };
    (cstring, $buf:expr, $arg:expr) => {
        $crate::message::frontend::codec::put_cstring(
            $buf,
            ::core::convert::AsRef::<[u8]>::as_ref(&$arg),
        );
    };
}

/// Encode a framed frontend message with a single-pass write.
///
/// The macro computes payload length from a list of primitive write ops, writes
/// the message code + length, then emits payload bytes exactly once.
///
/// Supported ops:
/// - `u8(expr)`, `u16(expr)`, `u32(expr)`
/// - `i16(expr)`, `i32(expr)`, `i64(expr)`
/// - `f32(expr)`, `f64(expr)`
/// - `bytes(expr)` where `expr: impl AsRef<[u8]>`
/// - `cstring(expr)` where `expr: impl AsRef<[u8]>`
#[macro_export]
macro_rules! pg_frame {
    ($buf:expr, $code:expr $(, $op:ident($arg:expr))* $(,)?) => {{
        let __pg_buf = &mut *$buf;
        let __pg_code = $code;
        let __pg_payload_len: usize = 0usize $(+ $crate::__pg_frame_len!($op, $arg))*;
        bytes::BufMut::put_u8(__pg_buf, __pg_code.as_u8());
        $crate::message::frontend::codec::frame(__pg_buf, __pg_payload_len, |__pg_out| {
            $(
                $crate::__pg_frame_write!($op, __pg_out, $arg);
            )*
        });
    }};
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::message::frontend::MessageCode;

    #[test]
    fn test_frame_length() {
        let mut buf = BytesMut::new();
        frame(&mut buf, 5, |b| {
            b.put_slice(b"hello");
        });

        // Length should be 4 (length field) + 5 (payload) = 9
        assert_eq!(&buf[0..4], &9u32.to_be_bytes());
        assert_eq!(&buf[4..], b"hello");
    }

    #[test]
    fn test_message_code_as_u8() {
        assert_eq!(MessageCode::QUERY.as_u8(), b'Q');
    }

    #[test]
    fn test_frame_message() {
        let mut buf = BytesMut::new();
        buf.put_u8(MessageCode::QUERY.as_u8());
        frame(&mut buf, cstring_len(b"SELECT 1"), |b| {
            put_cstring(b, b"SELECT 1");
        });

        assert_eq!(buf[0], b'Q');
        // Length: 4 + 8 + 1 = 13
        assert_eq!(&buf[1..5], &13u32.to_be_bytes());
        assert_eq!(&buf[5..], b"SELECT 1\0");
    }

    #[test]
    fn test_pg_frame_macro() {
        let mut buf = BytesMut::new();
        crate::pg_frame!(&mut buf, MessageCode::QUERY, cstring(b"SELECT 1"));

        assert_eq!(buf[0], b'Q');
        assert_eq!(&buf[1..5], &13u32.to_be_bytes());
        assert_eq!(&buf[5..], b"SELECT 1\0");
    }
}
