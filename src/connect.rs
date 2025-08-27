use std::{
    collections::HashMap,
    net::{SocketAddr, ToSocketAddrs},
};

use bytes::{BufMut, Bytes, BytesMut};

#[cfg(feature = "tokio")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::PgStream;
use crate::frontend;

pub enum AuthenticationMode {
    Trust,
    Password(String),
}

const CURRENT_VERSION: ProtocolVersion = ProtocolVersion::new(3, 0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
struct ProtocolVersion(u32);

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

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

pub struct ConnectionBuilder {
    database: String,
    user: String,
    addrs: Vec<SocketAddr>,
    auth: AuthenticationMode,
    protocol: ProtocolVersion,
    options: HashMap<String, String>,
}

impl ConnectionBuilder {
    pub fn new(user: impl Into<String>, addrs: impl ToSocketAddrs) -> std::io::Result<Self> {
        let user = user.into();
        let addrs = addrs.to_socket_addrs()?.collect();

        Ok(Self {
            database: user.clone(),
            user,
            addrs,
            auth: AuthenticationMode::Trust,
            protocol: CURRENT_VERSION,
            options: HashMap::new(),
        })
    }

    pub fn database(mut self, db: impl Into<String>) -> Self {
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

    pub fn protocol(mut self, protocol: ProtocolVersion) -> Self {
        self.protocol = protocol;
        self
    }

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
            buf.put_slice(self.database.as_bytes());
            buf.put_u8(0);

            // TODO: The rest of the startup message
        });

        buf.freeze()
    }
}

pub trait Connect<S> {
    // FIXME: Model error type
    async fn connect(&self, stream: S) -> std::io::Result<PgStream<S>>;
}

#[cfg(feature = "tokio")]
impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Connect<S> for ConnectionBuilder {
    async fn connect(&self, mut stream: S) -> std::io::Result<PgStream<S>> {
        let startup = self.as_startup_message();
        stream.write_all(&startup).await?;

        Ok(PgStream::raw(stream))
    }
}
