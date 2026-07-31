use crate::ErrorResponse;

pub type Result<T> = std::result::Result<T, Error>;

/// The error type for Postgres and associated I/O operations
/// encountered during connection and startup.
#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    PasswordRequired,
    TlsUnsupported,
    /// The server required SCRAM channel binding but no TLS certificate was
    /// available to bind to.
    ChannelBindingRequired,
    /// The server requested GSSAPI/SSPI but no provider was supplied (use
    /// [`ConnectionBuilder::connect_with_gss`](crate::startup::ConnectionBuilder::connect_with_gss)).
    GssUnavailable,
    Server(Box<ErrorResponse>),
    Unexpected(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "encountered I/O error: {e}"),
            Error::PasswordRequired => write!(f, "password is required"),
            Error::TlsUnsupported => write!(f, "server does not support TLS"),
            Error::ChannelBindingRequired => {
                write!(
                    f,
                    "server required channel binding but no TLS certificate was available"
                )
            }
            Error::GssUnavailable => {
                write!(
                    f,
                    "server requested GSSAPI/SSPI but no provider was supplied"
                )
            }
            Error::Server(e) => {
                write!(f, "encountered Postgres error response: {e:?}")
            }
            Error::Unexpected(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Error::Io(value)
    }
}

impl From<String> for Error {
    fn from(value: String) -> Self {
        Error::Unexpected(value)
    }
}

impl From<&str> for Error {
    fn from(value: &str) -> Self {
        Error::Unexpected(value.to_string())
    }
}
