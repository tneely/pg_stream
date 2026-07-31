//! Backend message types and parsing.
//!
//! This module provides types for representing and parsing PostgreSQL backend
//! (server-to-client) messages.

mod code;
mod io;
mod parse;
mod wrappers;

pub use code::MessageCode;
pub use io::{MessageLimits, MessageReader};
pub use wrappers::{
    Authentication, BackendKeyData, CommandComplete, CopyResponse, DataRow, DataRowIter,
    ErrorResponse, FunctionCallResponse, NegotiateProtocolVersion, NoticeResponse,
    NotificationResponse, ParameterDescription, ParameterStatus, ReadyForQuery, RowDescription,
    TransactionStatus,
};

use bytes::Bytes;

/// A parsed PostgreSQL backend message.
///
/// This enum represents all backend message types. Messages are automatically
/// parsed when reading from a connection.
#[derive(Debug, Clone)]
pub enum PgMessage {
    // Query results
    DataRow(DataRow),
    RowDescription(RowDescription),
    CommandComplete(CommandComplete),
    EmptyQueryResponse,

    // Errors and notices. Boxed: inline, their field ranges would dominate
    // `size_of::<PgMessage>()` for every message the reader returns.
    ErrorResponse(Box<ErrorResponse>),
    NoticeResponse(Box<NoticeResponse>),

    // Connection state
    ReadyForQuery(ReadyForQuery),
    BackendKeyData(BackendKeyData),
    ParameterStatus(ParameterStatus),

    // Prepared statements
    ParseComplete,
    BindComplete,
    CloseComplete,
    ParameterDescription(ParameterDescription),
    NoData,
    PortalSuspended,

    // Notifications
    NotificationResponse(NotificationResponse),

    // Copy protocol
    CopyData(Bytes),
    CopyDone,
    CopyInResponse(CopyResponse),
    CopyOutResponse(CopyResponse),
    CopyBothResponse(CopyResponse),

    // Authentication
    Authentication(Authentication),

    // Misc
    FunctionCallResponse(FunctionCallResponse),
    NegotiateProtocolVersion(NegotiateProtocolVersion),

    /// An unrecognized message code. The raw code and body are preserved.
    Unknown {
        code: MessageCode,
        body: Bytes,
    },
}
