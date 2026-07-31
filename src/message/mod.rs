//! Postgres wire protocol message encoding and decoding.
//!
//! This module provides:
//!
//! - [`PgProtocol`] - Extension trait for writing frontend messages to any buffer
//! - [`backend`] - Backend message parsing and types
//! - [`frontend`] - Frontend message encoding and types
//!
//! # Example
//!
//! ```
//! use bytes::BytesMut;
//! use pg_stream::{message::PgProtocol, params};
//!
//! let mut buf = BytesMut::new();
//!
//! // Simple query
//! buf.query("SELECT 1");
//!
//! // Extended query protocol
//! buf.parse(Some("stmt"))
//!    .query("SELECT $1::int")
//!    .finish()
//!    .bind(Some("stmt"))
//!    .finish(params![42i32])
//!    .execute(None, 0)
//!    .sync();
//! ```

pub(crate) mod backend;
pub(crate) mod frontend;

pub use backend::{
    Authentication, BackendKeyData, CommandComplete, CopyResponse, DataRow, DataRowIter,
    ErrorResponse, FunctionCallResponse, MessageLimits, MessageReader, NegotiateProtocolVersion,
    NoticeResponse, NotificationResponse, ParameterDescription, ParameterStatus, PgMessage,
    ReadyForQuery, RowDescription, TransactionStatus,
};
pub use frontend::{
    BindBuilder, Bindable, FnCallBuilder, FormatCode, NeedsQuery, Oid, ParseBuilder, PgProtocol,
    Ready, oid,
};

// `params!` is `#[macro_export]` (crate root); also expose it here so
// `message::params` resolves alongside the other message items.
pub use crate::params;
