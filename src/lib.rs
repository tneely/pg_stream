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
//! - **Message construction** using the [`PgProtocol`] extension trait on any buffer
//! - **Frame reading** from backend responses via [`PgConnection`]
//!
//! # Example: Simple Query
//!
//! ```no_run
//! use pg_stream::startup::{ConnectionBuilder, AuthenticationMode};
//! use pg_stream::PgProtocol;
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
//! conn.query("SELECT 1");
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
//! # use pg_stream::{startup::ConnectionBuilder, PgConnection, PgProtocol, params};
//! # use pg_stream::message::oid;
//! # async fn example(mut conn: PgConnection<tokio::net::TcpStream>) -> std::io::Result<()> {
//! // Parse a prepared statement
//! conn.parse(Some("stmt"))
//!     .query("SELECT $1::int")
//!     .param_types(&[oid::INT4])
//!     .finish();
//! conn.flush().await?;
//!
//! // Bind and execute
//! conn.bind(Some("stmt"))
//!     .finish(params![42i32])
//!     .execute(None, 0)
//!     .sync();
//! conn.flush().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Protocol Messages
//!
//! The [`PgProtocol`] trait provides methods for constructing all major
//! frontend protocol messages on any buffer implementing [`BufMut`](bytes::BufMut):
//!
//! - **Query execution**: [`query`](PgProtocol::query), [`execute`](PgProtocol::execute)
//! - **Prepared statements**: [`parse`](PgProtocol::parse), [`bind`](PgProtocol::bind)
//! - **Metadata**: [`describe_statement`](PgProtocol::describe_statement), [`describe_portal`](PgProtocol::describe_portal)
//! - **Resource management**: [`close_statement`](PgProtocol::close_statement), [`close_portal`](PgProtocol::close_portal)
//! - **Flow control**: [`flush_msg`](PgProtocol::flush_msg), [`sync`](PgProtocol::sync)
//!
//! # Authentication
//!
//! Every credential-bearing method a PostgreSQL 18 server can request is
//! supported:
//!
//! - [`AuthenticationMode::Trust`](startup::AuthenticationMode::Trust) - trust/peer/ident/cert
//! - [`AuthenticationMode::Password`](startup::AuthenticationMode::Password) - cleartext, MD5,
//!   SCRAM-SHA-256, and SCRAM-SHA-256-PLUS (channel binding over TLS)
//! - [`AuthenticationMode::OAuthBearer`](startup::AuthenticationMode::OAuthBearer) - OAUTHBEARER (PG18+)
//! - GSSAPI/SSPI via [`ConnectionBuilder::connect_with_gss`](startup::ConnectionBuilder::connect_with_gss)
//!   and the [`GssProvider`](startup::GssProvider) trait
//!
//! # TLS Support
//!
//! TLS is negotiated with [`ConnectionBuilder::connect_with_tls`](startup::ConnectionBuilder::connect_with_tls).
//! The upgrade closure may return the server certificate to enable channel binding.
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
pub use message::PgProtocol;
pub use message::backend::{ErrorResponse, PgMessage};
