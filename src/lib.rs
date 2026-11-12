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
//! - **Connection establishment** via [`startup::Startup`] with support for
//!   authentication and TLS
//! - **Message construction** using the [`Frontend`] trait, on a
//!   [`Connection`], a `Vec<u8>`, a byte slice or a buffer of your own
//! - **Reading replies** from a [`Connection`]: one request at a time through
//!   a [`Response`], or message by message with [`Connection::recv`]
//!
//! # Example: Simple Query
//!
//! ```no_run
//! # async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
//! use pg_stream::{Frontend, Message};
//!
//! // Queue a query. Reading sends it, so there is no need to flush first.
//! conn.query("SELECT 1");
//!
//! // Read responses
//! let mut response = conn.response();
//! while let Some(message) = response.next().await? {
//!     match message {
//!         Message::DataRow(row) => println!("{row:?}"),
//!         Message::ErrorResponse(error) => eprintln!("{error}"),
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Example: Prepared Statements
//!
//! ```no_run
//! # async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
//! use pg_stream::{Format, Frontend, oid, params};
//!
//! // Parse a prepared statement, then bind and execute it (empty names
//! // select the unnamed statement/portal)
//! conn.parse("stmt", "SELECT $1::int", &[oid::INT4])
//!     .bind("", "stmt", params![42i32], Format::Text)
//!     .execute("", 0)
//!     .sync();
//!
//! // Read each reply, in the order its message was sent
//! let mut response = conn.response();
//! response.parse_complete().await?;
//! response.bind_complete().await?;
//! while let Some(row) = response.row().await? {
//!     println!("{:?}", row.get(0));
//! }
//! response.finish().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Protocol Messages
//!
//! The [`Frontend`] trait provides methods for constructing every frontend
//! protocol message, on a [`Connection`], a byte slice, or any
//! [`Buffer`](frontend::Buffer) such as a `Vec<u8>`:
//!
//! - **Query execution**: [`query`](Frontend::query), [`execute`](Frontend::execute)
//! - **Prepared statements**: [`parse`](Frontend::parse), [`bind`](Frontend::bind)
//! - **Metadata**: [`describe_statement`](Frontend::describe_statement), [`describe_portal`](Frontend::describe_portal)
//! - **Resource management**: [`close_statement`](Frontend::close_statement), [`close_portal`](Frontend::close_portal)
//! - **Flow control**: [`flush_request`](Frontend::flush_request), [`sync`](Frontend::sync)
//! - **COPY**: [`copy_data`](Frontend::copy_data), [`copy_done`](Frontend::copy_done), [`copy_fail`](Frontend::copy_fail)
//! - **Function calls**: [`function_call`](Frontend::function_call)
//!
//! Parameters are given with [`params!`] for the common types, or written in
//! place with [`params_with`](frontend::params_with) in any encoding.
//!
//! # Reading Replies
//!
//! | | Returns | Use it when |
//! | --- | --- | --- |
//! | [`Connection::response`] | replies, one request at a time | requests and replies are handled in the same place |
//! | [`Connection::recv`] | every [`Message`], borrowed | you want the stream as the server sent it |
//! | [`Connection::recv_frame`] | every message, owned | messages are routed to other tasks |
//! | [`Connection::recv_frames`] | the messages that have arrived, owned as one piece | whole replies are routed to other tasks |
//!
//! A [`Response`] covers one cycle: everything up to a Sync, a simple Query
//! or a FunctionCall. It applies the protocol's rules as it reads:
//!
//! - An error from the server becomes [`Error::Server`], and the replies the
//!   server skips because of it become [`Error::Aborted`].
//! - Notices, notifications and parameter changes can arrive between any two
//!   replies. They are set aside for [`Connection::async_messages`].
//! - A response dropped half-read, by an early return or a cancelled future,
//!   is skipped before anything else is read.
//!
//! # Pipelining
//!
//! Messages are queued until the connection is flushed or read from, so a
//! pipeline is just more messages queued before the first read. Reading sends
//! what is queued and writing keeps reading, so a pipeline of any depth cannot
//! deadlock on full socket buffers. A blocking stream is the exception; see
//! [`Blocking`].
//!
//! ```no_run
//! # async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
//! use pg_stream::{Error, Format, Frontend, params};
//!
//! conn.parse("add", "INSERT INTO log (line) VALUES ($1)", &[]).sync();
//! for line in ["one", "two", "three"] {
//!     conn.bind("", "add", params![line], Format::Text).execute("", 0).sync();
//! }
//!
//! conn.response().finish().await?;
//! for _ in 0..3 {
//!     match conn.response().finish().await {
//!         Ok(_) => {}
//!         // One insert failing does not disturb the cycles after it.
//!         Err(Error::Server(error)) => eprintln!("{error}"),
//!         Err(error) => return Err(error),
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Authentication
//!
//! Every credential-bearing method a PostgreSQL 18 server can request is
//! supported by [`startup::Startup`]:
//!
//! - trust/peer/ident/cert - nothing to configure
//! - [`password`](startup::Startup::password) - cleartext, MD5, SCRAM-SHA-256,
//!   and SCRAM-SHA-256-PLUS (channel binding over TLS)
//! - [`oauth_token`](startup::Startup::oauth_token) - OAUTHBEARER (PG18+)
//! - [`gss`](startup::Startup::gss) - GSSAPI/SSPI via the
//!   [`GssProvider`](auth::GssProvider) trait
//!
//! # TLS Support
//!
//! TLS is negotiated with [`request_tls`] before the session starts, and the
//! handshake is left to the TLS library of your choice. Passing the server
//! certificate to [`Startup::server_certificate`](startup::Startup::server_certificate)
//! enables channel binding.
//!
//! # Sync and Async
//!
//! A [`Connection`] runs over anything that implements [`Transport`]:
//!
//! - With the `tokio` feature, every `AsyncRead + AsyncWrite + Unpin` stream,
//!   including TLS streams
//! - Blocking `std::io` streams through [`Blocking`], driven with
//!   [`block_on`] and no runtime
//! - Other runtimes, by implementing the trait's three poll functions
//!
//! # Performance Considerations
//!
//! This crate is designed for low-level control and maximum performance:
//!
//! - Messages are buffered and sent together to minimize syscalls
//! - Backend messages are parsed in place and borrow from the read buffer
//! - Frontend messages are encoded in a single pass, straight into the send
//!   buffer
//! - No allocations per message in protocol framing
//!
//! # Safety and Error Handling
//!
//! This is a low-level crate with minimal safety guarantees:
//!
//! - **No SQL injection protection** - sanitize your inputs
//! - **Manual resource management** - close your statements and portals
//! - **No connection pooling** - manage connections yourself
//!
//! # Feature Flags
//!
//! - `tokio` (default) - [`Transport`] for tokio streams
//! - `startup` (default) - the [`startup`] handshake and the [`auth`]
//!   mechanisms behind it; without it the crate depends only on `bytes`

#[cfg(feature = "startup")]
pub mod auth;
pub mod backend;
mod buffer;
mod connection;
mod error;
pub mod frontend;
mod outbox;
mod response;
mod scan;
#[cfg(feature = "startup")]
pub mod startup;
mod transport;
mod types;

pub use backend::Message;
pub use connection::Connection;
pub use error::{AuthError, Error, ProtocolError, Result, ScramError, ServerError};
pub use frontend::Frontend;
pub use response::{Completion, Response};
pub use transport::{Blocking, Transport, block_on, cancel, request_tls};
pub use types::{AuthMethod, Format, Oid, ProtocolVersion, oid};

/// Compiles the README's examples as documentation tests.
#[cfg(all(doctest, feature = "tokio", feature = "startup"))]
#[doc = include_str!("../README.md")]
struct Readme;
