use bytes::Bytes;

use crate::messages::backend::{self, PgFrame};

pub type ConnectResult<T> = std::result::Result<T, ConnectError>;

/// The error type for Postgres and associated I/O operations
/// encountered during connection and startup.
#[derive(Debug)]
pub enum ConnectError {
    Io(std::io::Error),
    PasswordRequired,
    TlsUnsupported,
    Server(PgErrorResponse),
    Unexpected(String),
}

impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectError::Io(e) => write!(f, "encountered I/O error: {e}"),
            ConnectError::PasswordRequired => write!(f, "password is required"),
            ConnectError::TlsUnsupported => write!(f, "server does not support TLS"),
            ConnectError::Server(e) => {
                write!(f, "encountered Postgres error response: {e:?}")
            }
            ConnectError::Unexpected(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for ConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ConnectError {
    fn from(value: std::io::Error) -> Self {
        ConnectError::Io(value)
    }
}

impl From<String> for ConnectError {
    fn from(value: String) -> Self {
        ConnectError::Unexpected(value)
    }
}

impl From<&str> for ConnectError {
    fn from(value: &str) -> Self {
        ConnectError::Unexpected(value.to_string())
    }
}

#[derive(Debug)]
// TODO: Parse the error response correctly
pub struct PgErrorResponse(Bytes);

impl PgErrorResponse {}

impl TryFrom<PgFrame> for PgErrorResponse {
    type Error = PgFrame;

    fn try_from(value: PgFrame) -> Result<Self, Self::Error> {
        if value.code == backend::MessageCode::ERROR_RESPONSE {
            Ok(PgErrorResponse(value.body))
        } else {
            Err(value)
        }
    }
}
