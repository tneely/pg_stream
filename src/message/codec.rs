//! Low-level codec utilities for Postgres wire protocol framing.

use bytes::BufMut;

/// Postgres frontend message codes.
///
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageCode(u8);

impl MessageCode {
    pub const BIND: Self = Self(b'B');
    pub const CANCEL_REQUEST: Self = Self(16);
    pub const CLOSE: Self = Self(b'C');
    pub const COPY_DATA: Self = Self(b'd');
    pub const COPY_DONE: Self = Self(b'c');
    pub const COPY_FAIL: Self = Self(b'f');
    pub const DESCRIBE: Self = Self(b'D');
    pub const EXECUTE: Self = Self(b'E');
    pub const FLUSH: Self = Self(b'H');
    pub const FUNCTION_CALL: Self = Self(b'F');
    pub const GSSENC_REQUEST: Self = Self(8);
    pub const GSS_RESPONSE: Self = Self(b'p');
    pub const PARSE: Self = Self(b'P');
    pub const PASSWORD_MESSAGE: Self = Self(b'p');
    pub const QUERY: Self = Self(b'Q');
    pub const SASL_RESPONSE: Self = Self(b'p');
    pub const SYNC: Self = Self(b'S');
    pub const TERMINATE: Self = Self(b'X');

    /// Writes a framed message to the buffer.
    ///
    /// Format: [code: u8][length: u32][payload...]
    /// The length includes itself (4 bytes) but not the code.
    #[inline]
    pub fn write<B: BufMut>(self, buf: &mut B, payload_fn: impl FnOnce(&mut Vec<u8>)) {
        buf.put_u8(self.0);
        frame(buf, payload_fn);
    }
}

impl std::fmt::Debug for MessageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageCode({:?})", self.0 as char)
    }
}

impl std::fmt::Display for MessageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match *self {
            Self::BIND => "Bind",
            Self::CLOSE => "Close",
            Self::COPY_DATA => "CopyData",
            Self::COPY_DONE => "CopyDone",
            Self::COPY_FAIL => "CopyFail",
            Self::DESCRIBE => "Describe",
            Self::EXECUTE => "Execute",
            Self::FLUSH => "Flush",
            Self::FUNCTION_CALL => "FunctionCall",
            Self::PARSE => "Parse",
            Self::PASSWORD_MESSAGE => "PasswordMessage",
            Self::QUERY => "Query",
            Self::SYNC => "Sync",
            Self::TERMINATE => "Terminate",
            _ => "Unknown",
        };
        write!(f, "{name}({:?})", self.0 as char)
    }
}

/// Writes a length-prefixed payload to a buffer.
///
/// The length field includes itself (4 bytes).
/// This is a low-level helper used by message encoding.
#[inline]
pub fn frame<B: BufMut>(buf: &mut B, payload_fn: impl FnOnce(&mut Vec<u8>)) {
    // We need to know the starting position to patch the length.
    // Using a temp Vec to collect the payload, then write length + payload.
    let mut temp = Vec::with_capacity(64);
    payload_fn(&mut temp);

    let len = (temp.len() + 4) as u32; // +4 for the length field itself
    buf.put_u32(len);
    buf.put_slice(&temp);
}

/// Writes a null-terminated C string to the buffer.
#[inline]
pub fn put_cstring<B: BufMut>(buf: &mut B, s: &[u8]) {
    buf.put_slice(s);
    buf.put_u8(0);
}

/// Parameter format codes for Bind messages.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatCode {
    Text = 0,
    Binary = 1,
}

impl From<FormatCode> for u16 {
    fn from(value: FormatCode) -> Self {
        value as u16
    }
}

/// Postgres type OIDs.
///
/// See: <https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat>
pub type Oid = u32;

/// Common Postgres type OIDs.
pub mod oid {
    use super::Oid;

    pub const UNSPECIFIED: Oid = 0;
    pub const BOOL: Oid = 16;
    pub const BYTEA: Oid = 17;
    pub const CHAR: Oid = 18;
    pub const NAME: Oid = 19;
    pub const INT8: Oid = 20;
    pub const INT2: Oid = 21;
    pub const INT4: Oid = 23;
    pub const TEXT: Oid = 25;
    pub const OID: Oid = 26;
    pub const JSON: Oid = 114;
    pub const XML: Oid = 142;
    pub const FLOAT4: Oid = 700;
    pub const FLOAT8: Oid = 701;
    pub const MONEY: Oid = 790;
    pub const BPCHAR: Oid = 1042;
    pub const VARCHAR: Oid = 1043;
    pub const DATE: Oid = 1082;
    pub const TIME: Oid = 1083;
    pub const TIMESTAMP: Oid = 1114;
    pub const TIMESTAMPTZ: Oid = 1184;
    pub const INTERVAL: Oid = 1186;
    pub const TIMETZ: Oid = 1266;
    pub const NUMERIC: Oid = 1700;
    pub const UUID: Oid = 2950;
    pub const JSONB: Oid = 3802;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_frame_length() {
        let mut buf = BytesMut::new();
        frame(&mut buf, |b| {
            b.put_slice(b"hello");
        });

        // Length should be 4 (length field) + 5 (payload) = 9
        assert_eq!(&buf[0..4], &9u32.to_be_bytes());
        assert_eq!(&buf[4..], b"hello");
    }

    #[test]
    fn test_message_code_write() {
        let mut buf = BytesMut::new();
        MessageCode::QUERY.write(&mut buf, |b| {
            put_cstring(b, b"SELECT 1");
        });

        assert_eq!(buf[0], b'Q');
        // Length: 4 + 8 + 1 = 13
        assert_eq!(&buf[1..5], &13u32.to_be_bytes());
        assert_eq!(&buf[5..], b"SELECT 1\0");
    }
}
