//! PgStream - Low-level Postgres wire protocol library.
//!
//! This crate provides direct access to the Postgres frontend/backend protocol,
//! allowing you to build custom database clients or tools without the overhead
//! of higher-level abstractions.
//!
//! # Overview
//!
//! The crate is organized around a few core concepts:
//!
//! - **Connection establishment** via [`ConnectionBuilder`] with support for
//!   authentication and TLS
//! - **Message construction** using the [`FrontendMessage`] extension trait on any buffer
//! - **Frame reading** from backend responses via [`PgConnection`]
//!
//! # Example: Simple Query
//!
//! ```no_run
//! use pg_stream::startup::{ConnectionBuilder, AuthenticationMode};
//! use pg_stream::FrontendMessage;
//!
//! # #[tokio::main]
//! # async fn main() -> pg_stream::startup::Result<()> {
//! let stream = tokio::net::TcpStream::connect("localhost:5432").await?;
//!
//! let (mut conn, startup) = ConnectionBuilder::new("postgres")
//!     .database("mydb")
//!     .auth(AuthenticationMode::Password("secret".into()))
//!     .connect(stream)
//!     .await?;
//!
//! // Execute a query
//! conn.buf()
//!     .query("SELECT 1");
//! conn.flush().await?;
//!
//! // Read responses
//! loop {
//!     let frame = conn.recv().await?;
//!     // Process frame...
//!     # break;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Prepared Statements
//!
//! ```no_run
//! # use pg_stream::{startup::ConnectionBuilder, PgConnection, FrontendMessage};
//! # use pg_stream::message::{Bindable, oid};
//! # async fn example(mut conn: PgConnection<tokio::net::TcpStream>) -> std::io::Result<()> {
//! // Parse a prepared statement
//! conn.buf()
//!     .parse(Some("stmt"))
//!     .query("SELECT $1::int")
//!     .param_types(&[oid::INT4])
//!     .finish();
//! conn.flush().await?;
//!
//! // Bind and execute
//! conn.buf()
//!     .bind(Some("stmt"))
//!     .finish(&[&42i32 as &dyn Bindable])
//!     .execute(None, 0)
//!     .sync();
//! conn.flush().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Protocol Messages
//!
//! The [`FrontendMessage`] trait provides methods for constructing all major
//! frontend protocol messages on any buffer implementing [`BufMut`](bytes::BufMut):
//!
//! - **Query execution**: [`query`](FrontendMessage::query), [`execute`](FrontendMessage::execute)
//! - **Prepared statements**: [`parse`](FrontendMessage::parse), [`bind`](FrontendMessage::bind)
//! - **Metadata**: [`describe_statement`](FrontendMessage::describe_statement), [`describe_portal`](FrontendMessage::describe_portal)
//! - **Resource management**: [`close_statement`](FrontendMessage::close_statement), [`close_portal`](FrontendMessage::close_portal)
//! - **Flow control**: [`flush_msg`](FrontendMessage::flush_msg), [`sync`](FrontendMessage::sync)
//!
//! # Authentication
//!
//! Currently supported authentication modes:
//!
//! - [`AuthenticationMode::Trust`](startup::AuthenticationMode::Trust) - No authentication
//! - [`AuthenticationMode::Password`](startup::AuthenticationMode::Password) - Cleartext or SCRAM-SHA-256
//!
//! # TLS Support
//!
//! TLS can be negotiated using [`ConnectionBuilder::connect_with_tls`](startup::ConnectionBuilder::connect_with_tls)
//! with a custom async upgrade function.
//!
//! # Performance Considerations
//!
//! This crate is designed for low-level control and maximum performance:
//!
//! - Messages are buffered and sent together to minimize syscalls
//! - Direct buffer manipulation with [`bytes::BytesMut`]
//! - No unnecessary allocations in protocol framing
//! - Zero-copy reads where possible
//!
//! # Safety and Error Handling
//!
//! This is a low-level crate with minimal safety guarantees:
//!
//! - **No SQL injection protection** - sanitize your inputs
//! - **Manual resource management** - close your statements and portals
//! - **No connection pooling** - manage connections yourself

#[cfg(feature = "startup")]
pub mod auth;
#[cfg(feature = "startup")]
pub mod startup;

pub mod connection;
pub mod message;

pub use connection::PgConnection;
pub use message::FrontendMessage;
pub use message::backend::{ErrorResponse, PgMessage};
