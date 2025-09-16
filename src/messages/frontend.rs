//! Logic for handling and representing Postgres frontend messages.

use bytes::{BufMut, Bytes, BytesMut};

pub const SSL_REQUEST: &[u8] = &[
    0x00, 0x00, 0x00, 0x08, // length: 8
    0x04, 0xD2, 0x16, 0x2F, // code: 80877103
];

/// Postgres frontend messages are framed by a 1 byte message code,
/// followed by a u32 integer delineating the length of the rest of
/// the message.
///
/// The message code identifies the type of message and format of its
/// payload.
///
/// For more information, see the official Postgres docs:
/// <https://www.postgresql.org/docs/current/protocol-message-formats.html>
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

    #[inline]
    pub fn frame(self, buf: &mut BytesMut, payload_fn: impl FnOnce(&mut BytesMut)) {
        buf.put_u8(self.0);
        frame(buf, payload_fn);
    }
}

impl From<u8> for MessageCode {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<MessageCode> for u8 {
    fn from(value: MessageCode) -> Self {
        value.0
    }
}

impl PartialEq<u8> for MessageCode {
    fn eq(&self, other: &u8) -> bool {
        self.0 == *other
    }
}

impl PartialEq<MessageCode> for u8 {
    fn eq(&self, other: &MessageCode) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for MessageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match *self {
            MessageCode::BIND => "Bind",
            MessageCode::CANCEL_REQUEST => "CancelRequest",
            MessageCode::CLOSE => "Close",
            MessageCode::COPY_DATA => "CopyData",
            MessageCode::COPY_DONE => "CopyDone",
            MessageCode::COPY_FAIL => "CopyFail",
            MessageCode::DESCRIBE => "Describe",
            MessageCode::EXECUTE => "Execute",
            MessageCode::FLUSH => "Flush",
            MessageCode::FUNCTION_CALL => "FunctionCall",
            MessageCode::GSSENC_REQUEST => "GSSENCRequest",
            MessageCode::PARSE => "Parse",
            #[allow(unreachable_patterns, reason = "messages all use the same char")]
            MessageCode::PASSWORD_MESSAGE
            | MessageCode::GSS_RESPONSE
            | MessageCode::SASL_RESPONSE => "PasswordMessage|GSSResponse|SASLResponse",
            MessageCode::QUERY => "Query",
            MessageCode::SYNC => "Sync",
            MessageCode::TERMINATE => "Terminate",
            _ => "Unknown",
        };
        write!(f, "{name}({})", self.0 as char)
    }
}

impl std::fmt::Debug for MessageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageCode({})", self.0 as char)
    }
}

/// The object ID of the parameter data type
// TODO: I generated this list with AI. x-ref with PG docs
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParameterKind {
    Unspecified = 0,
    Bool = 16,
    Bytea = 17,
    Char = 18,
    Name = 19,
    Int8 = 20,
    Int2 = 21,
    Int2Vector = 22,
    Int4 = 23,
    Regproc = 24,
    Text = 25,
    Oid = 26,
    Tid = 27,
    Xid = 28,
    Cid = 29,
    OidVector = 30,

    Json = 114,
    Xml = 142,
    PgNodeTree = 194,
    Jsonb = 3802,

    Float4 = 700,
    Float8 = 701,
    Money = 790,

    Bpchar = 1042,
    Varchar = 1043,
    Date = 1082,
    Time = 1083,
    Timestamp = 1114,
    Timestamptz = 1184,
    Interval = 1186,
    Timetz = 1266,

    Numeric = 1700,
    Uuid = 2950,
    Record = 2249,
    Cstring = 2275,
    Any = 2276,
    AnyArray = 2277,
    Void = 2278,
    Trigger = 2279,
    LanguageHandler = 2280,
    Internal = 2281,
    Opaque = 2282,
    AnyElement = 2283,
    AnyNonArray = 2776,
    AnyEnum = 3500,
    FdwHandler = 3115,
    AnyRange = 3831,
}

impl ParameterKind {
    pub fn as_u32_array(param_kinds: &[ParameterKind]) -> &[u32] {
        // SAFETY: The internal representation of ParameterKind
        // is u32
        unsafe { std::slice::from_raw_parts(param_kinds.as_ptr() as *const u32, param_kinds.len()) }
    }
}

impl From<ParameterKind> for u32 {
    fn from(value: ParameterKind) -> Self {
        value as u32
    }
}

#[inline]
pub fn frame(buf: &mut BytesMut, payload_fn: impl FnOnce(&mut BytesMut)) {
    let base = buf.len();
    buf.put_u32(0);

    payload_fn(buf);

    let len = (buf.len() - base) as u32;
    buf[base..base + size_of::<u32>()].copy_from_slice(&len.to_be_bytes());
}

fn raw_prefix(buf: &mut impl BufMut, b: &[u8]) {
    buf.put_u32(b.len() as u32);
    buf.put_slice(b);
}

fn string_prefix(buf: &mut impl BufMut, b: &[u8]) {
    buf.put_u32(b.len() as u32 + 1);
    buf.put_slice(b);
    buf.put_u32(0);
}

pub enum TargetKind {
    Portal(String),
    Statement(String),
}

impl TargetKind {
    pub fn new_portal(name: impl Into<String>) -> Self {
        TargetKind::Portal(name.into())
    }

    pub fn new_stmt(name: impl Into<String>) -> Self {
        TargetKind::Statement(name.into())
    }
}

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FormatCode {
    Text = 0,
    Binary = 1,
}

impl FormatCode {
    pub fn as_u16_array(codes: &[FormatCode]) -> &[u16] {
        // SAFETY: The internal representation of FormatCode
        // is u16
        unsafe { std::slice::from_raw_parts(codes.as_ptr() as *const u16, codes.len()) }
    }
}

impl From<FormatCode> for u16 {
    fn from(value: FormatCode) -> Self {
        value as u16
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum BindParameter {
    RawBinary(Bytes),
    RawText(String),
    String(String),
    Bool(bool),
    Int2(i16),
    Int4(i32),
    Int8(i64),
    Float4(f32),
    Float8(f64),
    Bytea(Bytes),
    Null,
}

impl BindParameter {
    /// The parameter format code. Each must presently be zero (text) or one (binary).
    pub fn format_code(&self) -> FormatCode {
        match self {
            BindParameter::RawText(_) | BindParameter::String(_) => FormatCode::Text,
            _ => FormatCode::Binary,
        }
    }

    /// Byte encoding
    pub fn encode(&self, buf: &mut impl BufMut) {
        match self {
            BindParameter::Null => buf.put_i32(-1),
            BindParameter::Bool(b) => raw_prefix(buf, &[*b as u8]),
            BindParameter::Int2(v) => raw_prefix(buf, &v.to_be_bytes()),
            BindParameter::Int4(v) => raw_prefix(buf, &v.to_be_bytes()),
            BindParameter::Int8(v) => raw_prefix(buf, &v.to_be_bytes()),
            BindParameter::Float4(v) => raw_prefix(buf, &v.to_be_bytes()),
            BindParameter::Float8(v) => raw_prefix(buf, &v.to_be_bytes()),
            BindParameter::Bytea(b) | BindParameter::RawBinary(b) => raw_prefix(buf, b),
            BindParameter::String(s) | BindParameter::RawText(s) => {
                string_prefix(buf, s.as_bytes())
            }
        }
    }
}

impl From<i16> for BindParameter {
    fn from(v: i16) -> Self {
        BindParameter::Int2(v)
    }
}

impl From<i32> for BindParameter {
    fn from(v: i32) -> Self {
        BindParameter::Int4(v)
    }
}

impl From<i64> for BindParameter {
    fn from(v: i64) -> Self {
        BindParameter::Int8(v)
    }
}

impl From<f32> for BindParameter {
    fn from(v: f32) -> Self {
        BindParameter::Float4(v)
    }
}

impl From<f64> for BindParameter {
    fn from(v: f64) -> Self {
        BindParameter::Float8(v)
    }
}

impl From<bool> for BindParameter {
    fn from(v: bool) -> Self {
        BindParameter::Bool(v)
    }
}

impl From<String> for BindParameter {
    fn from(v: String) -> Self {
        BindParameter::String(v)
    }
}

impl From<&str> for BindParameter {
    fn from(v: &str) -> Self {
        BindParameter::String(v.to_string())
    }
}

impl From<Bytes> for BindParameter {
    fn from(v: Bytes) -> Self {
        BindParameter::Bytea(v)
    }
}

impl From<BytesMut> for BindParameter {
    fn from(v: BytesMut) -> Self {
        BindParameter::Bytea(v.freeze())
    }
}

impl<T: Into<BindParameter>> From<Option<T>> for BindParameter {
    fn from(opt: Option<T>) -> Self {
        match opt {
            Some(v) => v.into(),
            None => BindParameter::Null,
        }
    }
}
