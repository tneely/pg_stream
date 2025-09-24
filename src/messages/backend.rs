//! Logic for handling and representing Postgres backend messages.

use std::io::Read;

use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt};

// Postgres won't allocate memory greater 1GiB. It probably won't
// write messages anywhere close to this size either, but this
// gives us a nice upper bound to prevent misbehaving servers
// from OOMing the client.
// <https://github.com/postgres/postgres/blob/879c492480d0e9ad8155c4269f95c5e8add41901/src/include/utils/memutils.h#L40>
const MAX_FRAME_SIZE_BYTES: usize = 1 << 30; // 1GiB

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
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
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

impl std::fmt::Display for MessageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match *self {
            MessageCode::AUTHENTICATION => "Authentication",
            MessageCode::BACKEND_KEY_DATA => "BackendKeyData",
            MessageCode::BIND_COMPLETE => "BindComplete",
            MessageCode::CLOSE_COMPLETE => "CloseComplete",
            MessageCode::COMMAND_COMPLETE => "CommandComplete",
            MessageCode::COPY_DATA => "CopyData",
            MessageCode::COPY_DONE => "CopyDone",
            MessageCode::COPY_IN_RESPONSE => "CopyInResponse",
            MessageCode::COPY_OUT_RESPONSE => "CopyOutResponse",
            MessageCode::COPY_BOTH_RESPONSE => "CopyBothResponse",
            MessageCode::DATA_ROW => "DataRow",
            MessageCode::EMPTY_QUERY_RESPONSE => "EmptyQueryResponse",
            MessageCode::ERROR_RESPONSE => "ErrorResponse",
            MessageCode::FUNCTION_CALL_RESPONSE => "FunctionCallResponse",
            MessageCode::NEGOTIATE_PROTOCOL_VERSION => "NegotiateProtocolVersion",
            MessageCode::NO_DATA => "NoData",
            MessageCode::NOTICE_RESPONSE => "NoticeResponse",
            MessageCode::NOTIFICATION_RESPONSE => "NotificationResponse",
            MessageCode::PARAMETER_DESCRIPTION => "ParameterDescription",
            MessageCode::PARAMETER_STATUS => "ParameterStatus",
            MessageCode::PARSE_COMPLETE => "ParseComplete",
            MessageCode::PORTAL_SUSPENDED => "PortalSuspended",
            MessageCode::READY_FOR_QUERY => "ReadyForQuery",
            MessageCode::ROW_DESCRIPTION => "RowDescription",
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

#[derive(Debug, Clone)]
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

impl std::fmt::Display for PgFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {:?}", self.code, self.body)
    }
}

pub fn read_frame_blocking(mut stream: impl Read) -> std::io::Result<PgFrame> {
    let mut buf = [0; 1];
    stream.read_exact(&mut buf)?;
    let code: MessageCode = u8::from_be_bytes(buf).into();

    let mut buf = [0; 4];
    stream.read_exact(&mut buf)?;
    let len = u32::from_be_bytes(buf) as usize - size_of::<u32>();
    // SAFETY: The uninitialized bytes are never read
    let mut body = unsafe { init_body(len)? };
    stream.read_exact(&mut body)?;

    Ok(PgFrame::new(code, body))
}

pub async fn read_frame(mut stream: impl AsyncRead + Unpin) -> std::io::Result<PgFrame> {
    let mut buf = [0; 1];
    stream.read_exact(&mut buf).await?;
    let code: MessageCode = u8::from_be_bytes(buf).into();

    let mut buf = [0; 4];
    stream.read_exact(&mut buf).await?;
    let len = u32::from_be_bytes(buf) as usize - size_of::<u32>();
    // SAFETY: The uninitialized bytes are never read
    let mut body = unsafe { init_body(len)? };
    stream.read_exact(&mut body).await?;

    Ok(PgFrame::new(code, body))
}

unsafe fn init_body(len: usize) -> std::io::Result<BytesMut> {
    if len > MAX_FRAME_SIZE_BYTES {
        let err_msg = format!("frame size exceeds {MAX_FRAME_SIZE_BYTES}B");
        return Err(std::io::Error::new(
            std::io::ErrorKind::QuotaExceeded,
            err_msg,
        ));
    }
    let mut body = BytesMut::with_capacity(len);
    unsafe {
        body.set_len(len);
    }
    Ok(body)
}

pub fn read_cstring(bytes: &mut Bytes) -> std::io::Result<String> {
    let Some(end) = bytes.iter().position(|&b| b == 0) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "null terminator missing",
        ));
    };

    let bytes = bytes.split_to(end + 1);
    match String::from_utf8(bytes[..end].to_vec()) {
        Ok(string) => Ok(string),
        Err(err) => Err(std::io::Error::other(err)),
    }
}
