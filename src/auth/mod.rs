//! Authentication implementations for Postgres connections.
//!
//! This module provides implementations for the various authentication
//! mechanisms supported by PostgreSQL:
//!
//! - **SCRAM-SHA-256**: The modern, secure default authentication method
//! - **MD5**: Legacy password hashing (still widely used)
//! - **Cleartext**: Plain text password (use only over TLS)
//!
//! # Example
//!
//! ```rust
//! use pg_stream::auth::ScramClient;
//!
//! // Create a SCRAM client for authentication
//! let mut client = ScramClient::new("username", "password");
//!
//! // Generate client-first message
//! let client_first = client.client_first();
//!
//! // After receiving server-first, generate client-final
//! // let client_final = client.client_final(&server_first)?;
//! ```

mod cleartext;
mod md5;
mod scram;

pub use cleartext::cleartext_password;
pub use md5::md5_password;
pub use scram::{ScramClient, ScramError};
