//! Frontend message codes.

/// Postgres frontend message codes.
///
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageCode(u8);

#[allow(unused)]
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
    pub const fn as_u8(self) -> u8 {
        self.0
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
