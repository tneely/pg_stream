use std::collections::HashMap;

use bytes::{BufMut, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{PgStream, backend, frontend, read_frame};

pub enum AuthenticationMode {
    Trust,
    Password(String),
}

const CURRENT_VERSION: ProtocolVersion = ProtocolVersion::new(3, 0);

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

pub struct ConnectionBuilder {
    database: Option<String>,
    user: String,
    auth: AuthenticationMode,
    protocol: ProtocolVersion,
    options: HashMap<String, String>,
}

impl ConnectionBuilder {
    pub fn new(user: impl Into<String>) -> Self {
        Self {
            database: None,
            user: user.into(),
            auth: AuthenticationMode::Trust,
            protocol: CURRENT_VERSION,
            options: HashMap::new(),
        }
    }

    pub fn database(mut self, db: impl Into<Option<String>>) -> Self {
        self.database = db.into();
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    pub fn auth(mut self, auth: AuthenticationMode) -> Self {
        self.auth = auth;
        self
    }

    pub fn protocol(mut self, protocol: impl Into<ProtocolVersion>) -> Self {
        self.protocol = protocol.into();
        self
    }

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
    pub async fn connect_with_tls<S, T, F, Fut>(
        &self,
        mut stream: S,
        upgrade_fn: F,
    ) -> std::io::Result<PgStream<T>>
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
                "TLS not supported".to_string(),
            )),
            _ => Err(std::io::Error::other(format!(
                "unexpected SSL response code {res}"
            ))),
        }?;

        self.connect(stream).await
    }

    pub async fn connect<S>(&self, mut stream: S) -> std::io::Result<PgStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let startup = self.as_startup_message();
        stream.write_all(&startup).await?;
        stream.flush().await?;

        loop {
            let message = read_frame(&mut stream).await.expect("ok");

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
                            u32::from_be_bytes(
                                auth_code.try_into().expect("code should be 4 bytes")
                            )
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

        Ok(PgStream::raw(stream))
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
