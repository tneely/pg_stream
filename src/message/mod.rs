//! Postgres wire protocol message encoding and decoding.
//!
//! This module provides:
//!
//! - [`FrontendMessage`] - Extension trait for writing frontend messages to any buffer
//! - [`codec`] - Low-level framing utilities and type definitions
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

pub mod backend;
pub mod codec;
mod frontend;

pub use codec::{FormatCode, MessageCode, Oid, oid};
pub use frontend::{
    BindBuilder, Bindable, FnCallBuilder, FrontendMessage, NeedsQuery, ParseBuilder, Ready,
};
