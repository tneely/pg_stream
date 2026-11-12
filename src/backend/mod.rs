//! Messages sent by the server.
//!
//! A [`Message`] borrows its body from the connection's read buffer, so
//! receiving one allocates nothing and copies nothing. Each message's layout is
//! checked once, when it is parsed; after that its accessors cannot fail.
//!
//! # Text
//!
//! Postgres sends names, tags and error text in the session's
//! `client_encoding`. Accessors return `&str` and drop everything from the
//! first byte that is not valid UTF-8, so run sessions with
//! `client_encoding=UTF8` (the [`Startup`](crate::startup::Startup) default).
//! Column *values* are never interpreted and are always returned as bytes.

mod auth;
mod cursor;
mod fields;
mod misc;
mod row;

use bytes::Bytes;

pub use auth::{Authentication, SaslMechanisms};
pub use fields::ErrorResponse;
pub use misc::{
    BackendKey, BackendKeyData, CommandComplete, CopyResponse, NegotiateProtocolVersion,
    Notification, ParameterStatus, TransactionStatus,
};
pub use row::{DataRow, Field, Fields, ParameterDescription, RowDescription, Values};

use crate::ProtocolError;

/// The one-byte codes that identify backend messages.
pub mod code {
    pub const AUTHENTICATION: u8 = b'R';
    pub const BACKEND_KEY_DATA: u8 = b'K';
    pub const BIND_COMPLETE: u8 = b'2';
    pub const CLOSE_COMPLETE: u8 = b'3';
    pub const COMMAND_COMPLETE: u8 = b'C';
    pub const COPY_BOTH_RESPONSE: u8 = b'W';
    pub const COPY_DATA: u8 = b'd';
    pub const COPY_DONE: u8 = b'c';
    pub const COPY_IN_RESPONSE: u8 = b'G';
    pub const COPY_OUT_RESPONSE: u8 = b'H';
    pub const DATA_ROW: u8 = b'D';
    pub const EMPTY_QUERY_RESPONSE: u8 = b'I';
    pub const ERROR_RESPONSE: u8 = b'E';
    pub const FUNCTION_CALL_RESPONSE: u8 = b'V';
    pub const NEGOTIATE_PROTOCOL_VERSION: u8 = b'v';
    pub const NO_DATA: u8 = b'n';
    pub const NOTICE_RESPONSE: u8 = b'N';
    pub const NOTIFICATION_RESPONSE: u8 = b'A';
    pub const PARAMETER_DESCRIPTION: u8 = b't';
    pub const PARAMETER_STATUS: u8 = b'S';
    pub const PARSE_COMPLETE: u8 = b'1';
    pub const PORTAL_SUSPENDED: u8 = b's';
    pub const READY_FOR_QUERY: u8 = b'Z';
    pub const ROW_DESCRIPTION: u8 = b'T';

    /// The name of the message with this code, for diagnostics.
    pub fn name(code: u8) -> &'static str {
        super::name(code)
    }

    /// Whether the server may send this message at any time, unprompted by
    /// anything the client sent.
    pub fn is_async(code: u8) -> bool {
        matches!(
            code,
            NOTICE_RESPONSE | NOTIFICATION_RESPONSE | PARAMETER_STATUS
        )
    }
}

/// A message from the server. The variants are named as in the
/// [protocol documentation](https://www.postgresql.org/docs/current/protocol-message-formats.html).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message<'a> {
    Authentication(Authentication<'a>),
    BackendKeyData(BackendKeyData<'a>),
    BindComplete,
    CloseComplete,
    CommandComplete(CommandComplete<'a>),
    CopyBothResponse(CopyResponse<'a>),
    CopyData(&'a [u8]),
    CopyDone,
    CopyInResponse(CopyResponse<'a>),
    CopyOutResponse(CopyResponse<'a>),
    DataRow(DataRow<'a>),
    EmptyQueryResponse,
    ErrorResponse(ErrorResponse<'a>),
    /// The function's result; `None` is SQL NULL.
    FunctionCallResponse(Option<&'a [u8]>),
    NegotiateProtocolVersion(NegotiateProtocolVersion<'a>),
    NoData,
    NoticeResponse(ErrorResponse<'a>),
    NotificationResponse(Notification<'a>),
    ParameterDescription(ParameterDescription<'a>),
    ParameterStatus(ParameterStatus<'a>),
    ParseComplete,
    PortalSuspended,
    ReadyForQuery(TransactionStatus),
    RowDescription(RowDescription<'a>),
    /// A message type this crate does not know, passed through untouched.
    Unknown {
        code: u8,
        body: &'a [u8],
    },
}

impl<'a> Message<'a> {
    /// Parses a message from its code and body (everything after the length).
    #[inline]
    pub fn parse(code: u8, body: &'a [u8]) -> Result<Self, ProtocolError> {
        Self::try_parse(code, body).ok_or(ProtocolError::Malformed { code })
    }
}

/// Declares what each code stands for: its [`Message`] variant and what parses
/// the body. A variant without a parser is a message without a body.
///
/// Everything that depends on the pairing is generated from this one list, so
/// none of it can disagree: [`Message::code`], [`Message::try_parse`], the
/// names behind [`code::name`], and which messages have no body.
macro_rules! messages {
    ($($code:ident => $name:ident $(($parse:expr))?,)*) => {
        fn name(code: u8) -> &'static str {
            match code {
                $(code::$code => stringify!($name),)*
                _ => "an unknown message",
            }
        }

        /// Whether the message with this code has no body.
        fn bodiless(code: u8) -> bool {
            match code {
                $(code::$code => messages!(@bodiless $(($parse))?),)*
                _ => false,
            }
        }

        impl<'a> Message<'a> {
            /// The code this message has on the wire.
            pub fn code(&self) -> u8 {
                match *self {
                    $(Message::$name { .. } => code::$code,)*
                    Message::Unknown { code, .. } => code,
                }
            }

            #[inline]
            fn try_parse(code: u8, body: &'a [u8]) -> Option<Self> {
                Some(match code {
                    $(code::$code => messages!(@parse body, $name $(($parse))?),)*
                    code => Message::Unknown { code, body },
                })
            }
        }
    };
    (@bodiless) => { true };
    (@bodiless ($parse:expr)) => { false };
    (@parse $body:ident, $name:ident) => { empty($body, Message::$name)? };
    (@parse $body:ident, $name:ident ($parse:expr)) => { Message::$name($parse($body)?) };
}

messages! {
    AUTHENTICATION => Authentication(Authentication::try_parse),
    BACKEND_KEY_DATA => BackendKeyData(BackendKeyData::try_parse),
    BIND_COMPLETE => BindComplete,
    CLOSE_COMPLETE => CloseComplete,
    COMMAND_COMPLETE => CommandComplete(CommandComplete::try_parse),
    COPY_BOTH_RESPONSE => CopyBothResponse(CopyResponse::try_parse),
    COPY_DATA => CopyData(Some),
    COPY_DONE => CopyDone,
    COPY_IN_RESPONSE => CopyInResponse(CopyResponse::try_parse),
    COPY_OUT_RESPONSE => CopyOutResponse(CopyResponse::try_parse),
    DATA_ROW => DataRow(DataRow::try_parse),
    EMPTY_QUERY_RESPONSE => EmptyQueryResponse,
    ERROR_RESPONSE => ErrorResponse(ErrorResponse::try_parse),
    FUNCTION_CALL_RESPONSE => FunctionCallResponse(function_result),
    NEGOTIATE_PROTOCOL_VERSION => NegotiateProtocolVersion(NegotiateProtocolVersion::try_parse),
    NO_DATA => NoData,
    NOTICE_RESPONSE => NoticeResponse(ErrorResponse::try_parse),
    NOTIFICATION_RESPONSE => NotificationResponse(Notification::try_parse),
    PARAMETER_DESCRIPTION => ParameterDescription(ParameterDescription::try_parse),
    PARAMETER_STATUS => ParameterStatus(ParameterStatus::try_parse),
    PARSE_COMPLETE => ParseComplete,
    PORTAL_SUSPENDED => PortalSuspended,
    READY_FOR_QUERY => ReadyForQuery(TransactionStatus::try_parse),
    ROW_DESCRIPTION => RowDescription(RowDescription::try_parse),
}

/// A message the server may send at any time, whatever the client is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncMessage<'a> {
    /// A warning or informational message.
    Notice(ErrorResponse<'a>),
    /// A NOTIFY on a channel this session listens to.
    Notification(Notification<'a>),
    /// A run-time parameter changed, or is being reported at startup.
    ParameterStatus(ParameterStatus<'a>),
}

impl<'a> AsyncMessage<'a> {
    /// Parses a message with one of the [asynchronous](code::is_async) codes.
    pub(crate) fn try_parse(code: u8, body: &'a [u8]) -> Option<Self> {
        match Message::try_parse(code, body)? {
            Message::NoticeResponse(notice) => Some(AsyncMessage::Notice(notice)),
            Message::NotificationResponse(notify) => Some(AsyncMessage::Notification(notify)),
            Message::ParameterStatus(status) => Some(AsyncMessage::ParameterStatus(status)),
            _ => None,
        }
    }
}

fn empty<'a>(body: &[u8], message: Message<'a>) -> Option<Message<'a>> {
    body.is_empty().then_some(message)
}

pub(crate) fn function_result(body: &[u8]) -> Option<Option<&[u8]>> {
    let (len, value) = body.split_first_chunk::<4>()?;
    match i32::from_be_bytes(*len) {
        -1 => value.is_empty().then_some(None),
        len => (usize::try_from(len).ok()? == value.len()).then_some(Some(value)),
    }
}

/// The lengths (of the length field) a message of this type can have, for the
/// types whose size is fixed or tightly bounded. Lets an impossible frame be
/// rejected from its header, before its body is awaited.
pub(crate) fn length_bounds(code: u8) -> Option<std::ops::RangeInclusive<usize>> {
    let key = BackendKeyData::KEY_LEN;
    match code {
        code::READY_FOR_QUERY => Some(5..=5),
        code::BACKEND_KEY_DATA => Some(8 + key.start()..=8 + key.end()),
        _ => bodiless(code).then_some(4..=4),
    }
}

/// An owned message: its code and its body, sharing the read buffer's
/// allocation.
///
/// Where a [`Message`] must be handled before the next one is received, a
/// frame can be sent to another task and parsed there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub code: u8,
    pub body: Bytes,
}

impl Frame {
    /// Parses the frame into the message it holds.
    #[inline]
    pub fn parse(&self) -> Result<Message<'_>, ProtocolError> {
        Message::parse(self.code, &self.body)
    }
}

/// Owned messages: one or more whole messages in the order they arrived, as
/// a single piece of the read buffer's allocation.
///
/// Where a [`Frame`] is handed out for each message, this is handed out for
/// as many as have arrived. That costs less for each of them, and lets the
/// rows of a reply go to another task together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frames {
    wire: Bytes,
    ends_cycle: bool,
}

impl Frames {
    pub(crate) fn new(wire: Bytes, ends_cycle: bool) -> Self {
        Self { wire, ends_cycle }
    }

    /// Whether the last message is a ReadyForQuery. The messages never go on
    /// past one, so the replies to a cycle either end here or carry on in the
    /// `Frames` that comes next.
    pub fn ends_cycle(&self) -> bool {
        self.ends_cycle
    }

    /// Parses each message in turn.
    pub fn messages(&self) -> impl Iterator<Item = Result<Message<'_>, ProtocolError>> {
        let mut rest = &self.wire[..];
        std::iter::from_fn(move || {
            let (&[code, a, b, c, d], after) = rest.split_first_chunk()?;
            let body_len = (u32::from_be_bytes([a, b, c, d]) as usize).checked_sub(4)?;
            let (body, after) = after.split_at_checked(body_len)?;
            rest = after;
            Some(Message::parse(code, body))
        })
    }

    /// The messages as they came off the wire, each with its code and its
    /// length before its body.
    pub fn as_bytes(&self) -> &Bytes {
        &self.wire
    }
}

#[cfg(test)]
mod tests;
