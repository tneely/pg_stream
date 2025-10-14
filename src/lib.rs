//! PgStream.
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
//! - **Message construction** using the fluent API on [`PgStream`]
//! - **Frame reading** from backend responses
//!
//! # Example: Simple Query
//!
//! ```no_run
//! use pg_stream::{ConnectionBuilder, AuthenticationMode};
//!
//! # #[tokio::main]
//! # async fn main() -> pg_stream::ConnectResult<()> {
//! let stream = tokio::net::TcpStream::connect("localhost:5432").await?;
//!
//! let (mut conn, startup) = ConnectionBuilder::new("postgres")
//!     .database("mydb")
//!     .auth(AuthenticationMode::Password("secret".into()))
//!     .connect(stream)
//!     .await?;
//!
//! // Execute a query
//! conn.put_query("SELECT 1")
//!     .flush()
//!     .await?;
//!
//! // Read responses
//! loop {
//!     let frame = conn.read_frame().await?;
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
//! # use pg_stream::{ConnectionBuilder, PgStream};
//! # use pg_stream::messages::frontend::{ParameterKind, BindParameter, ResultFormat};
//! # async fn example(mut conn: PgStream<tokio::net::TcpStream>) -> std::io::Result<()> {
//! // Parse a prepared statement
//! conn.put_parse("stmt", "SELECT $1::int", &[ParameterKind::Int4])
//!     .flush()
//!     .await?;
//!
//! // Bind and execute
//! conn.put_bind("", "stmt", &[BindParameter::Text("42".to_string())], ResultFormat::Text)
//!     .put_execute("", None)
//!     .put_sync()
//!     .flush()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Function Calls
//!
//! ```no_run
//! # use pg_stream::PgStream;
//! # use pg_stream::messages::frontend::{FunctionArg, FormatCode};
//! # async fn example(mut conn: PgStream<tokio::net::TcpStream>) -> std::io::Result<()> {
//! // Call sqrt(9) - note that OID 1344 may vary by Postgres version
//! conn.put_fn_call(1344, &[FunctionArg::Text("9".to_string())], FormatCode::Text)
//!     .flush()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Protocol Messages
//!
//! The crate provides methods for constructing all major frontend protocol messages:
//!
//! - **Query execution**: [`PgStream::put_query`], [`PgStream::put_execute`]
//! - **Prepared statements**: [`PgStream::put_parse`], [`PgStream::put_bind`]
//! - **Metadata**: [`PgStream::put_describe`]
//! - **Resource management**: [`PgStream::put_close`]
//! - **Flow control**: [`PgStream::put_flush`], [`PgStream::put_sync`]
//! - **Function calls**: [`PgStream::put_fn_call`]
//!
//! # Authentication
//!
//! Currently supported authentication modes:
//!
//! - [`AuthenticationMode::Trust`] - No authentication
//! - [`AuthenticationMode::Password`] - Cleartext password
//!
//! Other authentication methods (SASL, SCRAM, MD5, Kerberos, etc.) are not
//! yet implemented and will return an error or panic.
//!
//! # TLS Support
//!
//! TLS can be negotiated using [`ConnectionBuilder::connect_with_tls`] with
//! a custom async upgrade function. The builder handles SSL request negotiation
//! with the server.
//!
//! # Format Codes
//!
//! The protocol supports text and binary formats for parameters and results.
//! The crate automatically optimizes format code encoding:
//!
//! - If all parameters use text format (the default), no format codes are sent
//! - If all parameters use the same format, a single format code is sent
//! - Otherwise, individual format codes are sent for each parameter
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
//! - **Incomplete error handling** - some errors currently panic (see TODOs)
//! - **Manual resource management** - close your statements and portals
//! - **No connection pooling** - manage connections yourself
//!
//! # Feature Flags
//!
//! Currently, this crate has no optional features.

mod connect;
mod error;
pub mod messages;
mod pg_stream;
mod pg_stream_proto;

pub use connect::*;
pub use error::*;
pub use pg_stream::*;
pub use pg_stream_proto::*;
