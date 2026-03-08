//! Backend message types and parsing.
//!
//! This module provides types for representing and parsing PostgreSQL backend
//! (server-to-client) messages.

mod code;
mod io;
mod parse;
mod wrappers;

pub use code::MessageCode;
#[cfg(feature = "async")]
pub use io::read_message;
#[cfg(feature = "sync")]
pub use io::read_message_sync;
pub use wrappers::{
    BackendKeyData, CommandComplete, CopyResponse, DataRow, ErrorResponse, NoticeResponse,
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

    // Errors and notices
    ErrorResponse(ErrorResponse),
    NoticeResponse(NoticeResponse),

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

    // Authentication (body preserved for caller to interpret)
    Authentication(Bytes),

    // Misc
    FunctionCallResponse(Bytes),
    NegotiateProtocolVersion(Bytes),

    /// An unrecognized message code. The raw code and body are preserved.
    Unknown {
        code: MessageCode,
        body: Bytes,
    },
}
