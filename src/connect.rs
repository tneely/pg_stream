use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use scram::ScramClient;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    ConnectError, ConnectResult, PgStream,
    messages::{
        backend::{self},
        frontend,
    },
};

/// Authentication mode for a Postgres connection.
#[derive(Clone, Debug, PartialEq, Eq)]
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

    fn as_startup_message(&self) -> Bytes {
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

        buf.freeze()
    }

    /// Establishes a Postgres connection with TLS upgrade.
    ///
    /// Sends an SSL request to the server and upgrades the connection using the
    /// provided async upgrade function if the server supports TLS.
    pub async fn connect_with_tls<S, T, F, Fut>(
        &self,
        mut stream: S,
        upgrade_fn: F,
    ) -> ConnectResult<(PgStream<T>, StartupResponse)>
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
        let res = u8::from_be_bytes(buf);

        const SSL_SUCCESS: u8 = b'S';
        const SSL_FAILURE: u8 = b'N';

        let stream = match res {
            SSL_SUCCESS => upgrade_fn(stream).await?,
            SSL_FAILURE => Err(ConnectError::TlsUnsupported)?,
            _ => Err(format!("unexpected SSL response code '{res}'"))?,
        };

        self.connect(stream).await
    }

    /// Establishes a Postgres connection over the provided stream.
    ///
    /// Performs the startup handshake, handles authentication, and waits for
    /// the server to be ready for queries.
    pub async fn connect<S>(&self, mut stream: S) -> ConnectResult<(PgStream<S>, StartupResponse)>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.startup(&mut stream).await?;

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
                code => Err(format!("unexpected message code {code}"))?,
            }
        }

        Ok((PgStream::from_stream(stream), startup_res))
    }

    async fn startup<S>(&self, stream: &mut S) -> ConnectResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let startup_msg = self.as_startup_message();
        stream.write_all(&startup_msg).await?;
        stream.flush().await?;
        match read_auth_message(stream).await? {
            AuthMessage::Ok => Ok(()),
            AuthMessage::CleartextPassword => {
                let AuthenticationMode::Password(pw) = &self.auth else {
                    return Err(ConnectError::PasswordRequired);
                };
                let mut msg = BytesMut::new();
                frontend::MessageCode::PASSWORD_MESSAGE.frame(&mut msg, |buf| {
                    buf.put_slice(pw.as_bytes());
                    buf.put_u8(0);
                });
                stream.write_all(&msg).await?;
                stream.flush().await?;

                match read_auth_message(stream).await? {
                    AuthMessage::Ok => Ok(()),
                    code => Err(format!("unexpected authentication code {code}"))?,
                }
            }
            AuthMessage::Sasl(mech) => {
                let AuthenticationMode::Password(pw) = &self.auth else {
                    return Err(ConnectError::PasswordRequired);
                };

                let scram = ScramClient::new(&self.user, pw, None);
                let (scram, client_first) = scram.client_first();

                let mut msg = BytesMut::new();
                frontend::MessageCode::SASL_RESPONSE.frame(&mut msg, |buf| {
                    buf.put_slice(mech.to_string().as_bytes());
                    buf.put_u8(0);
                    buf.put_u32(client_first.len() as u32);
                    buf.put_slice(client_first.as_bytes());
                });
                stream.write_all(&msg).await?;
                stream.flush().await?;

                let res = read_auth_message(stream).await?;
                let AuthMessage::SaslContinue(server_first) = res else {
                    return Err(format!("unexpected authentication response {res}"))?;
                };

                let scram = scram
                    .handle_server_first(&server_first)
                    .map_err(|e| format!("scram handshake failed: {e}"))?;
                let (scram, client_final) = scram.client_final();

                let mut msg = BytesMut::new();
                frontend::MessageCode::SASL_RESPONSE.frame(&mut msg, |buf| {
                    buf.put_slice(client_final.as_bytes());
                });
                stream.write_all(&msg).await?;
                stream.flush().await?;

                let res = read_auth_message(stream).await?;
                let AuthMessage::SaslFinal(server_final) = res else {
                    return Err(format!("unexpected authentication response {res}"))?;
                };

                scram
                    .handle_server_final(&server_final)
                    .map_err(|e| format!("scram handshake failed: {e}"))?;

                match read_auth_message(stream).await? {
                    AuthMessage::Ok => Ok(()),
                    code => Err(format!("unexpected authentication code {code}"))?,
                }
            }
            code => unimplemented!("oops: {code}"),
        }
    }
}

enum AuthMessage {
    Ok,
    KerberosV5,
    CleartextPassword,
    Md5Password([u8; 4]),
    Gss,
    GssContinue,
    Sspi,
    Sasl(AuthMechanism),
    SaslContinue(String),
    SaslFinal(String),
}

impl std::fmt::Display for AuthMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMessage::Ok => write!(f, "AuthenticationOk"),
            AuthMessage::KerberosV5 => write!(f, "AuthenticationKerberosV5"),
            AuthMessage::CleartextPassword => write!(f, "AuthenticationCleartextPassword"),
            AuthMessage::Md5Password(_salt) => write!(f, "AuthenticationMD5Password"),
            AuthMessage::Gss => write!(f, "AuthenticationGSS"),
            AuthMessage::GssContinue => write!(f, "AuthenticationGSSContinue"),
            AuthMessage::Sspi => write!(f, "AuthenticationSSPI"),
            AuthMessage::Sasl(mech) => write!(f, "AuthenticationSASL({mech})"),
            AuthMessage::SaslContinue(_) => write!(f, "AuthenticationSASLContinue"),
            AuthMessage::SaslFinal(_) => write!(f, "AuthenticationSASLContinue"),
        }
    }
}

enum AuthMechanism {
    ScramSha256,
    // ScramSha256Plus,
    // OAuthBearer,
}

impl std::fmt::Display for AuthMechanism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::ScramSha256 => "SCRAM-SHA-256",
            // Self::ScramSha256Plus => "SCRAM-SHA-256-PLUS",
            // Self::OAuthBearer => "OAUTHBEARER",
        };
        write!(f, "{name}")
    }
}

impl TryFrom<&str> for AuthMechanism {
    type Error = ConnectError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "SCRAM-SHA-256" => Ok(AuthMechanism::ScramSha256),
            // "SCRAM-SHA-256-PLUS" => Ok(AuthMechanism::ScramSha256Plus),
            // "OAUTHBEARER" => Ok(AuthMechanism::OAuthBearer),
            _ => Err(format!("unsupported authentication mechanism {value}"))?,
        }
    }
}

async fn read_auth_message<S>(stream: &mut S) -> ConnectResult<AuthMessage>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let msg = backend::read_frame(stream).await?;

    match msg.code {
        backend::MessageCode::ERROR_RESPONSE => {
            let pg_err = msg.try_into().expect("is an error response");
            Err(ConnectError::Server(pg_err))
        }
        backend::MessageCode::AUTHENTICATION => {
            let auth_code = u32::from_be_bytes(msg.body[..4].try_into().unwrap());
            let msg = match auth_code {
                0 => AuthMessage::Ok,
                2 => AuthMessage::KerberosV5,
                3 => AuthMessage::CleartextPassword,
                5 => {
                    let salt = msg.body[4..]
                        .try_into()
                        .map_err(|_| "unexpected body length in md5 password challenge")?;
                    AuthMessage::Md5Password(salt)
                }
                7 => AuthMessage::Gss,
                8 => AuthMessage::GssContinue,
                9 => AuthMessage::Sspi,
                10 => {
                    let mech = msg.body[4..]
                        .split(|b| *b == 0)
                        .map(String::from_utf8_lossy)
                        .find_map(|m| AuthMechanism::try_from(m.as_ref()).ok())
                        .ok_or("no supported authentication mechanisms")?;
                    AuthMessage::Sasl(mech)
                }
                11 => {
                    let resp = &msg.body[4..];
                    AuthMessage::SaslContinue(String::from_utf8_lossy(resp).to_string())
                }
                12 => {
                    let resp = &msg.body[4..];
                    AuthMessage::SaslFinal(String::from_utf8_lossy(resp).to_string())
                }
                auth_code => Err(format!("unexpected auth response code {auth_code}",))?,
            };
            Ok(msg)
        }
        code => Err(format!("unexpected message code {code}"))?,
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
