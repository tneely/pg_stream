use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    PgStream,
    messages::{backend, frontend},
};

/// Authentication mode for a Postgres connection.
pub enum AuthenticationMode {
    /// Trust authentication (no password required).
    Trust,
    /// Password authentication with the provided password.
    Password(String),
}

const CURRENT_VERSION: ProtocolVersion = ProtocolVersion::new(3, 0);

/// Postgres protocol version number.
///
/// The version is encoded as a 32-bit integer where the upper 16 bits represent
/// the major version and the lower 16 bits represent the minor version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct ProtocolVersion(u32);

impl ProtocolVersion {
    const fn new(major: u16, minor: u16) -> Self {
        Self(((major as u32) << 16) | (minor as u32))
    }

    fn major(&self) -> u16 {
        (self.0 >> 16) as u16
    }

    fn minor(&self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }
}

impl From<u32> for ProtocolVersion {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<ProtocolVersion> for u32 {
    fn from(value: ProtocolVersion) -> Self {
        value.0
    }
}

impl PartialEq<u32> for ProtocolVersion {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

impl PartialEq<ProtocolVersion> for u32 {
    fn eq(&self, other: &ProtocolVersion) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

/// Response data from a successful Postgres startup handshake.
#[derive(Debug, Clone)]
pub struct StartupResponse {
    /// Backend process ID for this connection.
    pub process_id: u32,
    /// Secret key for canceling queries on this connection.
    pub secret_key: u32,
    /// Server parameters returned during startup (e.g., server_version, client_encoding).
    pub parameters: HashMap<String, String>,
}

/// Builder for configuring and establishing Postgres connections.
pub struct ConnectionBuilder {
    database: Option<String>,
    user: String,
    auth: AuthenticationMode,
    protocol: ProtocolVersion,
    options: HashMap<String, String>,
}

impl ConnectionBuilder {
    /// Creates a new connection builder with the specified user.
    ///
    /// Defaults to trust authentication and protocol version 3.0.
    pub fn new(user: impl Into<String>) -> Self {
        Self {
            database: None,
            user: user.into(),
            auth: AuthenticationMode::Trust,
            protocol: CURRENT_VERSION,
            options: HashMap::new(),
        }
    }

    /// Sets the database name to connect to.
    ///
    /// If not specified, defaults to the username.
    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.database = Some(db.into());
        self
    }

    /// Sets the username for authentication.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    /// Sets the authentication mode.
    pub fn auth(mut self, auth: AuthenticationMode) -> Self {
        self.auth = auth;
        self
    }

    /// Sets the Postgres protocol version.
    pub fn protocol(mut self, protocol: impl Into<ProtocolVersion>) -> Self {
        self.protocol = protocol.into();
        self
    }

    /// Adds a startup parameter option.
    pub fn add_option(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.options.insert(key.into(), val.into());
        self
    }

    fn as_startup_message(&self) -> Vec<u8> {
        let mut buf = BytesMut::new();
        frontend::frame(&mut buf, |buf| {
            buf.put_u32(self.protocol.into());

            buf.put_slice("user".as_bytes());
            buf.put_u8(0);
            buf.put_slice(self.user.as_bytes());
            buf.put_u8(0);

            buf.put_slice("database".as_bytes());
            buf.put_u8(0);
            if let Some(db) = &self.database {
                buf.put_slice(db.as_bytes());
            } else {
                buf.put_slice(self.user.as_bytes());
            }
            buf.put_u8(0);

            // TODO: The rest of the startup message

            buf.put_u8(0);
        });

        buf.to_vec()
    }
}

impl ConnectionBuilder {
    /// Establishes a Postgres connection with TLS upgrade.
    ///
    /// Sends an SSL request to the server and upgrades the connection using the
    /// provided async upgrade function if the server supports TLS.
    pub async fn connect_with_tls<S, T, F, Fut>(
        &self,
        mut stream: S,
        upgrade_fn: F,
    ) -> std::io::Result<(PgStream<T>, StartupResponse)>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: AsyncRead + AsyncWrite + Unpin,
        F: FnOnce(S) -> Fut,
        Fut: Future<Output = std::io::Result<T>>,
    {
        stream.write_all(frontend::SSL_REQUEST).await?;
        stream.flush().await?;

        let mut buf = [0; 1];
        stream.read_exact(&mut buf).await?;
        let res = u8::from_be_bytes(buf) as char;

        let stream = match res {
            'S' => upgrade_fn(stream).await,
            'N' => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "TLS is not supported".to_string(),
            )),
            _ => Err(std::io::Error::other(format!(
                "unexpected SSL response code '{res}'"
            ))),
        }?;

        self.connect(stream).await
    }

    /// Establishes a Postgres connection over the provided stream.
    ///
    /// Performs the startup handshake, handles authentication, and waits for
    /// the server to be ready for queries.
    pub async fn connect<S>(&self, mut stream: S) -> std::io::Result<(PgStream<S>, StartupResponse)>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let startup = self.as_startup_message();
        stream.write_all(&startup).await?;
        stream.flush().await?;

        // FIXME: Move loop to state machine once I have a better understanding
        // of how the auth flow works
        loop {
            let message = backend::read_frame(&mut stream).await.expect("ok");

            const AUTH_OK: &[u8] = &0u32.to_be_bytes();
            const AUTH_KERBEROS_V5: &[u8] = &2u32.to_be_bytes();
            const AUTH_CLEARTEXT_PW: &[u8] = &3u32.to_be_bytes();
            const AUTH_MD5_PW: &[u8] = &5u32.to_be_bytes();
            const AUTH_GSS: &[u8] = &7u32.to_be_bytes();
            const AUTH_SSPI: &[u8] = &9u32.to_be_bytes();
            const AUTH_SASL: &[u8] = &10u32.to_be_bytes();

            match message.code {
                backend::MessageCode::ERROR_RESPONSE => {
                    panic!("handle me: {}", String::from_utf8_lossy(&message.body));
                }
                backend::MessageCode::AUTHENTICATION => match &message.body[..4] {
                    AUTH_OK => break,
                    AUTH_CLEARTEXT_PW => match &self.auth {
                        AuthenticationMode::Password(pw) => {
                            let mut msg = BytesMut::new();
                            frontend::MessageCode::PASSWORD_MESSAGE.frame(&mut msg, |buf| {
                                buf.put_slice(pw.as_bytes());
                                buf.put_u8(0);
                            });
                            stream.write_all(&msg).await?;
                            stream.flush().await?;
                        }
                        _ => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                "password required",
                            ));
                        }
                    },
                    AUTH_SASL => {
                        unimplemented!("SASL not supported yet")
                    }
                    AUTH_KERBEROS_V5 => {
                        unimplemented!("Kerberos not supported yet")
                    }
                    AUTH_MD5_PW => {
                        unimplemented!("MD5 not supported yet")
                    }
                    AUTH_GSS => {
                        unimplemented!("GSS not supported yet")
                    }
                    AUTH_SSPI => {
                        unimplemented!("SSPI not supported yet")
                    }
                    auth_code => {
                        return Err(std::io::Error::other(format!(
                            "unexpected auth response code {}",
                            u32::from_be_bytes(auth_code.try_into().unwrap())
                        )));
                    }
                },
                code => {
                    return Err(std::io::Error::other(format!(
                        "unexpected message code {code}",
                    )));
                }
            }
        }

        let mut startup_res = StartupResponse {
            process_id: 0,
            secret_key: 0,
            parameters: HashMap::new(),
        };

        loop {
            let mut frame = backend::read_frame(&mut stream).await?;
            match frame.code {
                backend::MessageCode::PARAMETER_STATUS => {
                    let key = backend::read_cstring(&mut frame.body)?;
                    let val = backend::read_cstring(&mut frame.body)?;
                    startup_res.parameters.insert(key, val);
                }
                backend::MessageCode::BACKEND_KEY_DATA => {
                    startup_res.process_id = frame.body.get_u32();
                    startup_res.secret_key = frame.body.get_u32();
                }
                backend::MessageCode::READY_FOR_QUERY => break,
                c => panic!("unexpected message code {c}"),
            }
        }

        Ok((PgStream::from_stream(stream), startup_res))
    }
}

#[cfg(test)]
mod tests {
    use crate::ProtocolVersion;

    #[test]
    fn test_protocol_version() {
        let major = 3;
        let minor = 0;
        let version = ProtocolVersion::new(major, minor);
        assert_eq!(major, version.major());
        assert_eq!(minor, version.minor());
        assert_eq!(196608, version.0);
    }
}
