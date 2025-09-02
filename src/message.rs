pub mod frontend {
    //! Logic for handling and representing Postgres frontend messages.

    use std::fmt::Display;

    use bytes::{BufMut, Bytes, BytesMut};

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
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

    impl Display for MessageCode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let name = match *self {
                MessageCode::BIND => "Bind",
                // TODO: fill in rest
                _ => "Unknown",
            };
            write!(f, "{name}({})", self.0 as char)
        }
    }

    /// The object ID of the parameter data type
    #[repr(u32)]
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum ParameterKind {
        UNSPECIFIED = 0,
        // TODO: fill in the rest
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

    fn raw_prefix(buf: &mut BytesMut, b: &[u8]) {
        buf.put_u32(b.len() as u32);
        buf.extend_from_slice(b);
    }

    fn string_prefix(buf: &mut BytesMut, b: &[u8]) {
        buf.put_u32(b.len() as u32 + 1);
        buf.extend_from_slice(b);
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
        pub fn encode(&self, buf: &mut BytesMut) {
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
}

pub mod backend {
    //! Logic for handling and representing Postgres backend messages.

    use std::fmt::Display;

    use bytes::Bytes;

    /// Postgres backend messages are framed by a 1 byte message code,
    /// followed by a u32 integer delineating the length of the rest of
    /// the message.
    ///
    /// The message code identifies the type of message and format of its
    /// payload.
    ///
    /// For more information, see the official Postgres docs:
    /// <https://www.postgresql.org/docs/current/protocol-message-formats.html>
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct MessageCode(u8);

    impl MessageCode {
        pub const AUTHENTICATION: Self = Self(b'R');
        pub const BACKEND_KEY_DATA: Self = Self(b'K');
        pub const BIND_COMPLETE: Self = Self(b'2');
        pub const CLOSE_COMPLETE: Self = Self(b'3');
        pub const COMMAND_COMPLETE: Self = Self(b'C');
        pub const COPY_DATA: Self = Self(b'd');
        pub const COPY_DONE: Self = Self(b'c');
        pub const COPY_IN_RESPONSE: Self = Self(b'G');
        pub const COPY_OUT_RESPONSE: Self = Self(b'H');
        pub const COPY_BOTH_RESPONSE: Self = Self(b'W');
        pub const DATA_ROW: Self = Self(b'D');
        pub const EMPTY_QUERY_RESPONSE: Self = Self(b'I');
        pub const ERROR_RESPONSE: Self = Self(b'E');
        pub const FUNCTION_CALL_RESPONSE: Self = Self(b'V');
        pub const GSS_RESPONSE: Self = Self(b'p');
        pub const NEGOTIATE_PROTOCOL_VERSION: Self = Self(b'v');
        pub const NO_DATA: Self = Self(b'n');
        pub const NOTICE_RESPONSE: Self = Self(b'N');
        pub const NOTIFICATION_RESPONSE: Self = Self(b'A');
        pub const PARAMETER_DESCRIPTION: Self = Self(b't');
        pub const PARAMETER_STATUS: Self = Self(b'S');
        pub const PARSE_COMPLETE: Self = Self(b'1');
        pub const PORTAL_SUSPENDED: Self = Self(b's');
        pub const READY_FOR_QUERY: Self = Self(b'Z');
        pub const ROW_DESCRIPTION: Self = Self(b'T');
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

    impl Display for MessageCode {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let name = match *self {
                MessageCode::AUTHENTICATION => "Authentication",
                // TODO: fill in rest
                _ => "Unknown",
            };
            write!(f, "{name}({})", self.0 as char)
        }
    }

    pub struct PgFrame {
        pub code: MessageCode,
        pub body: Bytes,
    }

    impl PgFrame {
        pub fn new(code: impl Into<MessageCode>, body: impl Into<Bytes>) -> Self {
            Self {
                code: code.into(),
                body: body.into(),
            }
        }
    }

    impl Display for PgFrame {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}: {:?}", self.code, self.body)
        }
    }
}
