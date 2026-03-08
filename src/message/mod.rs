//! Postgres wire protocol message encoding and decoding.
//!
//! This module provides:
//!
//! - [`FrontendMessage`] - Extension trait for writing frontend messages to any buffer
//! - [`backend`] - Backend message parsing and types
//! - [`frontend`] - Frontend message encoding and types
//!
//! # Example
//!
//! ```
//! use bytes::BytesMut;
//! use pg_stream::message::{FrontendMessage, Bindable};
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
//!    .finish(&[&42i32 as &dyn Bindable])
//!    .execute(None, 0)
//!    .sync();
//! ```

pub(crate) mod backend;
pub(crate) mod frontend;

pub use backend::{PgMessage, TransactionStatus, read_message};
pub use frontend::{
    BindBuilder, Bindable, FnCallBuilder, FormatCode, FrontendMessage, NeedsQuery, Oid,
    ParseBuilder, Ready, oid,
};
