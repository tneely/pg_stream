//! Error types.

use std::{error::Error as StdError, fmt, io};

use crate::{
    AuthMethod,
    backend::{ErrorResponse, code},
};

/// A specialized `Result` for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Anything that can go wrong while talking to a server.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The transport failed. A server that closes the connection surfaces as
    /// [`io::ErrorKind::UnexpectedEof`].
    Io(io::Error),
    /// The server sent something that is not valid protocol, or not valid at
    /// this point in the exchange.
    Protocol(ProtocolError),
    /// The server answered with an ErrorResponse.
    Server(ServerError),
    /// The server never produced this reply: an earlier message in the same
    /// cycle failed, so it discarded everything up to the next Sync.
    Aborted,
    /// Authentication could not be completed.
    Auth(AuthError),
}

impl Error {
    pub(crate) fn unexpected(code: u8, expected: &'static str) -> Self {
        Error::Protocol(ProtocolError::Unexpected { code, expected })
    }

    /// The server's ErrorResponse, if that is what this error is.
    pub fn as_server(&self) -> Option<&ServerError> {
        match self {
            Error::Server(e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "transport error: {e}"),
            Error::Protocol(e) => write!(f, "protocol error: {e}"),
            Error::Server(e) => write!(f, "server error: {e}"),
            Error::Aborted => f.write_str("skipped: an earlier message in the cycle failed"),
            Error::Auth(e) => write!(f, "authentication failed: {e}"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Protocol(e) => Some(e),
            Error::Server(e) => Some(e),
            Error::Auth(e) => Some(e),
            Error::Aborted => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<ProtocolError> for Error {
    fn from(e: ProtocolError) -> Self {
        Error::Protocol(e)
    }
}

impl From<AuthError> for Error {
    fn from(e: AuthError) -> Self {
        Error::Auth(e)
    }
}

impl From<ScramError> for Error {
    fn from(e: ScramError) -> Self {
        Error::Auth(e.into())
    }
}

/// A violation of the wire protocol by the server.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProtocolError {
    /// A length field no message of this type can have.
    InvalidLength { code: u8, len: u32 },
    /// A message larger than the connection's configured maximum.
    TooLarge { code: u8, len: u32, max: usize },
    /// A message whose body does not match its documented layout.
    Malformed { code: u8 },
    /// A valid message that the protocol does not allow at this point. The
    /// message is left unread.
    Unexpected { code: u8, expected: &'static str },
    /// The connection can no longer be matched up with the server's state and
    /// must be closed. Caused by dropping a response while the server was
    /// waiting for COPY data.
    Desynchronized,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ProtocolError::InvalidLength { code, len } => {
                write!(f, "{} has impossible length {len}", code::name(code))
            }
            ProtocolError::TooLarge { code, len, max } => {
                let name = code::name(code);
                write!(f, "{name} of {len} bytes exceeds the {max} byte limit")
            }
            ProtocolError::Malformed { code } => write!(f, "malformed {}", code::name(code)),
            ProtocolError::Unexpected { code, expected } => {
                write!(f, "expected {expected}, got {}", code::name(code))
            }
            ProtocolError::Desynchronized => f.write_str("connection is desynchronized"),
        }
    }
}

impl StdError for ProtocolError {}

/// An owned ErrorResponse.
///
/// Field text that is not valid UTF-8 has been replaced lossily.
#[derive(Clone, PartialEq, Eq)]
pub struct ServerError(Box<str>);

impl ServerError {
    pub(crate) fn new(fields: ErrorResponse<'_>) -> Self {
        Self(String::from_utf8_lossy(fields.as_bytes()).into())
    }

    /// Every field the server sent.
    pub fn fields(&self) -> ErrorResponse<'_> {
        ErrorResponse::from_validated(self.0.as_bytes())
    }

    /// The SQLSTATE code, e.g. `"23505"`.
    pub fn code(&self) -> &str {
        self.fields().code()
    }

    /// The primary human-readable message.
    pub fn message(&self) -> &str {
        self.fields().message()
    }
}

impl fmt::Debug for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.fields(), f)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.fields(), f)
    }
}

impl StdError for ServerError {}

/// Why authentication could not be completed.
#[derive(Debug)]
#[non_exhaustive]
pub enum AuthError {
    /// The server asked for a password and none was configured.
    PasswordRequired,
    /// The server asked for an authentication method this crate does not
    /// implement; the value is the AuthenticationRequest code.
    UnsupportedMethod(u32),
    /// The server asked for a method the caller did not allow.
    MethodNotAllowed(AuthMethod),
    /// None of the SASL mechanisms the server offered can be used with the
    /// configured credential.
    NoUsableMechanism,
    /// The server only offered channel-bound SCRAM, but no server certificate
    /// was supplied to bind to.
    ChannelBindingUnavailable,
    /// The server asked for GSSAPI/SSPI and no provider was configured.
    GssUnavailable,
    /// The GSSAPI/SSPI provider failed.
    Gss(Box<dyn StdError + Send + Sync>),
    /// The SCRAM exchange failed.
    Scram(ScramError),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::PasswordRequired => f.write_str("the server requires a password"),
            AuthError::UnsupportedMethod(code) => {
                write!(f, "unsupported authentication request {code}")
            }
            AuthError::MethodNotAllowed(method) => {
                write!(f, "the server asked for {method:?}, which is not allowed")
            }
            AuthError::NoUsableMechanism => f.write_str("no usable SASL mechanism was offered"),
            AuthError::ChannelBindingUnavailable => {
                f.write_str("the server requires channel binding but no certificate was supplied")
            }
            AuthError::GssUnavailable => f.write_str("the server requires GSSAPI/SSPI"),
            AuthError::Gss(e) => write!(f, "GSSAPI/SSPI failed: {e}"),
            AuthError::Scram(e) => write!(f, "SCRAM failed: {e}"),
        }
    }
}

impl StdError for AuthError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            AuthError::Gss(e) => Some(&**e),
            AuthError::Scram(e) => Some(e),
            _ => None,
        }
    }
}

impl From<ScramError> for AuthError {
    fn from(e: ScramError) -> Self {
        AuthError::Scram(e)
    }
}

/// Why a SCRAM exchange failed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScramError {
    /// A server message could not be parsed.
    InvalidMessage,
    /// The server's nonce does not extend the client's.
    InvalidNonce,
    /// The iteration count is zero or above
    /// [`MAX_SCRAM_ITERATIONS`](crate::auth::MAX_SCRAM_ITERATIONS).
    InvalidIterationCount,
    /// The server could not prove it knows the password.
    ServerSignatureMismatch,
    /// The server certificate could not be parsed for channel binding.
    InvalidCertificate,
}

impl fmt::Display for ScramError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ScramError::InvalidMessage => "malformed server message",
            ScramError::InvalidNonce => "server nonce does not extend the client nonce",
            ScramError::InvalidIterationCount => "iteration count out of range",
            ScramError::ServerSignatureMismatch => "server signature mismatch",
            ScramError::InvalidCertificate => "unparseable server certificate",
        })
    }
}

impl StdError for ScramError {}
