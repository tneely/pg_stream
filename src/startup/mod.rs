use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    PgConnection, PgMessage,
    auth::{ChannelBinding, ScramClient, cleartext_password, md5_password, tls_server_end_point},
    message::{
        backend::{Authentication, MessageLimits, MessageReader},
        frontend::{MessageCode, cstring_len, frame},
    },
};

use self::auth_msg::read_auth_message;

mod auth_msg;
mod error;
mod gss;

pub use error::*;
pub use gss::GssProvider;

pub const SSL_REQUEST: &[u8] = &[
    0x00, 0x00, 0x00, 0x08, // length: 8
    0x04, 0xD2, 0x16, 0x2F, // code: 80877103
];

/// The magic request code for a CancelRequest message (1234 << 16 | 5678).
const CANCEL_REQUEST_CODE: u32 = 80877102;

/// Builds a CancelRequest packet from a backend's `process_id`/`secret_key`
/// (see [`StartupResponse`]). Send it on a *new* connection to cancel that
/// backend's in-progress query.
///
/// PostgreSQL cancellation keys identify a backend session, not a particular
/// query. Once cancellation is dispatched, the primary connection must be
/// quarantined: do not send another query until the cancellation attempt has
/// completed and responses on the primary connection have been drained through
/// the cancelled query's [`ReadyForQuery`](PgMessage::ReadyForQuery). A delayed
/// request can otherwise cancel the next query on the same backend.
pub fn cancel_request(process_id: u32, secret_key: &[u8]) -> Vec<u8> {
    // Length field + request code + process ID, then the key to end of packet.
    let len = 3 * size_of::<u32>() + secret_key.len();
    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(&(len as u32).to_be_bytes());
    buf.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
    buf.extend_from_slice(&process_id.to_be_bytes());
    buf.extend_from_slice(secret_key);
    buf
}

/// Opens a throwaway connection via `connect_fn` and sends a CancelRequest for
/// the given backend, then drops the connection (the server sends no reply).
///
/// The primary connection must remain quarantined until this call completes and
/// it has been drained through [`ReadyForQuery`](PgMessage::ReadyForQuery); see
/// [`cancel_request`] for the query-reuse race.
#[cfg(feature = "async")]
pub async fn send_cancel_request<S, F, Fut>(
    process_id: u32,
    secret_key: &[u8],
    connect_fn: F,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncWrite + Unpin,
    F: FnOnce() -> Fut,
    Fut: Future<Output = std::io::Result<S>>,
{
    use tokio::io::AsyncWriteExt;
    let mut stream = connect_fn().await?;
    stream
        .write_all(&cancel_request(process_id, secret_key))
        .await?;
    stream.flush().await?;
    stream.shutdown().await.ok();
    Ok(())
}

/// Credential for a Postgres connection. The server picks the challenge
/// (cleartext/MD5/SCRAM/SCRAM-PLUS); [`Password`](Self::Password) answers all of
/// them. GSSAPI/SSPI is driven via [`ConnectionBuilder::connect_with_gss`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthenticationMode {
    /// Trust authentication (no credentials; also covers peer/ident/cert).
    Trust,
    /// Password authentication, answering cleartext, MD5, or SCRAM as requested.
    Password(String),
    /// OAuth 2.0 bearer token authentication (`OAUTHBEARER`, PostgreSQL 18+).
    OAuthBearer(String),
}

/// Requested by default. 3.0 is what libpq asks for unless a newer feature is
/// needed, and it avoids a NegotiateProtocolVersion round trip on older servers.
const CURRENT_VERSION: ProtocolVersion = ProtocolVersion::V3_0;

/// Postgres protocol version number.
///
/// The version is encoded as a 32-bit integer where the upper 16 bits represent
/// the major version and the lower 16 bits represent the minor version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct ProtocolVersion(u32);

impl ProtocolVersion {
    /// Protocol 3.0, supported by every PostgreSQL release since 7.4.
    pub const V3_0: Self = Self::new(3, 0);

    /// Protocol 3.2, added in PostgreSQL 18. Widens the cancel key; servers that
    /// predate it answer with NegotiateProtocolVersion and stay on 3.0.
    pub const V3_2: Self = Self::new(3, 2);

    pub const fn new(major: u16, minor: u16) -> Self {
        Self(((major as u32) << 16) | (minor as u32))
    }

    /// The major version, which has been 3 since PostgreSQL 7.4.
    pub const fn major(&self) -> u16 {
        (self.0 >> 16) as u16
    }

    /// The minor version.
    pub const fn minor(&self) -> u16 {
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
#[derive(Debug, Clone, Default)]
pub struct StartupResponse {
    /// Backend process ID for this connection.
    pub process_id: u32,
    /// Secret key for canceling queries on this connection. Four bytes under
    /// protocol 3.0/3.1, variable-length under 3.2; empty if the server sent no
    /// BackendKeyData (some non-PostgreSQL backends omit it).
    pub secret_key: Vec<u8>,
    /// Server parameters returned during startup (e.g., server_version, client_encoding).
    pub parameters: HashMap<String, String>,
}

/// Builder for configuring and establishing Postgres connections.
pub struct ConnectionBuilder {
    auth: AuthenticationMode,
    protocol: ProtocolVersion,
    options: HashMap<String, String>,
    message_limits: MessageLimits,
}

impl ConnectionBuilder {
    /// Creates a new connection builder with the specified user.
    ///
    /// Defaults to trust authentication and protocol version 3.0.
    pub fn new(user: impl Into<String>) -> Self {
        let user = user.into();

        let mut options = HashMap::new();
        options.insert("application_name".into(), "pg_stream".into());
        options.insert("database".into(), user.clone());
        options.insert("user".into(), user);

        Self {
            auth: AuthenticationMode::Trust,
            protocol: CURRENT_VERSION,
            options,
            message_limits: MessageLimits::default(),
        }
    }

    /// Sets the database name to connect to.
    ///
    /// If not specified, defaults to the username.
    pub fn database(self, db: impl Into<String>) -> Self {
        self.add_option("database", db.into())
    }

    /// Sets the username for authentication.
    pub fn user(self, user: impl Into<String>) -> Self {
        self.add_option("user", user.into())
    }

    /// Sets the authentication mode.
    pub fn auth(mut self, auth: AuthenticationMode) -> Self {
        self.auth = auth;
        self
    }

    /// Sets the application name.
    pub fn application_name(self, app: impl Into<String>) -> Self {
        self.add_option("application_name", app.into())
    }

    /// Sets the Postgres protocol version (defaults to
    /// [`V3_0`](ProtocolVersion::V3_0); [`V3_2`](ProtocolVersion::V3_2) requires
    /// PostgreSQL 18+).
    pub fn protocol(mut self, protocol: impl Into<ProtocolVersion>) -> Self {
        self.protocol = protocol.into();
        self
    }

    /// Sets backend frame-size limits for startup and the returned connection.
    pub fn message_limits(mut self, limits: MessageLimits) -> Self {
        self.message_limits = limits;
        self
    }

    /// Adds a startup parameter option.
    pub fn add_option(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.options.insert(key.into(), val.into());
        self
    }

    fn get_user(&self) -> &str {
        self.options.get("user").expect("user should always be set")
    }

    fn as_startup_message(&self) -> Bytes {
        let mut buf = BytesMut::new();
        let payload_len = {
            let mut len = 4 + 1; // protocol version + trailing terminator
            for (key, val) in &self.options {
                len += cstring_len(key.as_bytes()) + cstring_len(val.as_bytes());
            }
            len
        };

        frame(&mut buf, payload_len, |buf| {
            buf.put_u32(self.protocol.into());

            for (key, val) in &self.options {
                buf.put_slice(key.as_bytes());
                buf.put_u8(0);
                buf.put_slice(val.as_bytes());
                buf.put_u8(0);
            }

            buf.put_u8(0);
        });

        buf.freeze()
    }

    /// Establishes a Postgres connection with TLS upgrade.
    ///
    /// Sends an SSL request and, if the server accepts, upgrades via `upgrade_fn`.
    /// The closure returns the TLS stream and, optionally, the server's
    /// certificate in DER form; supplying it enables `SCRAM-SHA-256-PLUS`
    /// channel binding.
    pub async fn connect_with_tls<S, T, F, Fut>(
        &self,
        mut stream: S,
        upgrade_fn: F,
    ) -> Result<(PgConnection<T>, StartupResponse)>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: AsyncRead + AsyncWrite + Unpin,
        F: FnOnce(S) -> Fut,
        Fut: Future<Output = std::io::Result<(T, Option<Vec<u8>>)>>,
    {
        stream.write_all(SSL_REQUEST).await?;
        stream.flush().await?;

        let mut buf = [0; 1];
        stream.read_exact(&mut buf).await?;
        let res = u8::from_be_bytes(buf);

        const SSL_SUCCESS: u8 = b'S';
        const SSL_FAILURE: u8 = b'N';

        let (stream, cert_der) = match res {
            SSL_SUCCESS => upgrade_fn(stream).await?,
            SSL_FAILURE => Err(Error::TlsUnsupported)?,
            _ => Err(format!("unexpected SSL response code '{res}'"))?,
        };

        let cert_hash = match cert_der {
            Some(der) => Some(
                tls_server_end_point(&der)
                    .map_err(|e| format!("channel binding setup failed: {e}"))?,
            ),
            None => None,
        };

        self.establish(stream, Tls::Active { cert_hash }, None)
            .await
    }

    /// Establishes a Postgres connection over the provided stream.
    ///
    /// Performs the startup handshake, handles authentication, and waits for
    /// the server to be ready for queries.
    pub async fn connect<S>(&self, stream: S) -> Result<(PgConnection<S>, StartupResponse)>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.establish(stream, Tls::None, None).await
    }

    /// Establishes a connection using GSSAPI/SSPI authentication.
    ///
    /// The `provider` supplies the security tokens (see [`GssProvider`]); this
    /// crate only frames them. Password/SASL challenges are still handled if the
    /// server requests them instead.
    pub async fn connect_with_gss<S, G>(
        &self,
        stream: S,
        provider: &mut G,
    ) -> Result<(PgConnection<S>, StartupResponse)>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        G: GssProvider,
    {
        let mut adapter = GssAdapter(provider);
        self.establish(stream, Tls::None, Some(&mut adapter)).await
    }

    async fn establish<S>(
        &self,
        mut stream: S,
        tls: Tls,
        gss: Option<&mut dyn GssStep>,
    ) -> Result<(PgConnection<S>, StartupResponse)>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut reader = MessageReader::with_limits(self.message_limits);
        self.authenticate(&mut stream, &mut reader, &tls, gss)
            .await?;

        let mut startup_res = StartupResponse::default();

        loop {
            match reader.read_message(&mut stream).await? {
                PgMessage::ParameterStatus(ps) => {
                    startup_res
                        .parameters
                        .insert(ps.name().into_owned(), ps.value().into_owned());
                }
                PgMessage::BackendKeyData(bkd) => {
                    startup_res.process_id = bkd.process_id();
                    startup_res.secret_key = bkd.secret_key().to_vec();
                }
                // The server may downgrade the minor version we asked for.
                PgMessage::NegotiateProtocolVersion(_) => {}
                // Notices during startup are informational; keep reading.
                PgMessage::NoticeResponse(_) => {}
                PgMessage::ErrorResponse(err) => return Err(Error::Server(err)),
                PgMessage::ReadyForQuery(_) => break,
                msg => Err(format!("unexpected message: {:?}", msg))?,
            }
        }

        Ok((
            PgConnection::with_read_buffer_and_limits(
                stream,
                reader.into_buffer(),
                self.message_limits,
            ),
            startup_res,
        ))
    }

    async fn authenticate<S>(
        &self,
        stream: &mut S,
        reader: &mut MessageReader,
        tls: &Tls,
        gss: Option<&mut dyn GssStep>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let startup_msg = self.as_startup_message();
        stream.write_all(&startup_msg).await?;
        stream.flush().await?;

        match read_auth_message(stream, reader).await? {
            Authentication::Ok => Ok(()),
            Authentication::CleartextPassword => {
                let pw = self.password()?;
                self.send_and_expect_ok(stream, reader, &cleartext_password(pw))
                    .await
            }
            Authentication::Md5Password { salt } => {
                let pw = self.password()?;
                let msg = md5_password(self.get_user(), pw, &salt);
                self.send_and_expect_ok(stream, reader, &msg).await
            }
            Authentication::Sasl(mechs) => self.sasl(stream, reader, &mechs, tls).await,
            Authentication::Gss | Authentication::Sspi => {
                let gss = gss.ok_or(Error::GssUnavailable)?;
                self.gss_loop(stream, reader, gss, &[]).await
            }
            other => Err(format!("unsupported authentication request: {other:?}"))?,
        }
    }

    /// Runs a SASL exchange, choosing `SCRAM-SHA-256(-PLUS)` or `OAUTHBEARER`
    /// from the offered mechanisms and the configured credential.
    async fn sasl<S>(
        &self,
        stream: &mut S,
        reader: &mut MessageReader,
        offered: &[u8],
        tls: &Tls,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mechs: Vec<&str> = offered
            .split(|&b| b == 0)
            .filter(|m| !m.is_empty())
            .filter_map(|m| std::str::from_utf8(m).ok())
            .collect();

        if let AuthenticationMode::OAuthBearer(token) = &self.auth {
            if mechs.contains(&"OAUTHBEARER") {
                return self.oauthbearer(stream, reader, token).await;
            }
            return Err(Error::Unexpected(format!(
                "OAuth configured but server offered only {mechs:?}"
            )));
        }

        let pw = self.password()?;
        let binding = select_channel_binding(tls.cert_hash(), &mechs)?;

        self.scram(stream, reader, pw, binding).await
    }

    async fn scram<S>(
        &self,
        stream: &mut S,
        reader: &mut MessageReader,
        password: &str,
        binding: ChannelBinding,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut scram = ScramClient::with_channel_binding(self.get_user(), password, binding);
        let mech = scram.mechanism();
        let client_first = scram.client_first();

        self.send(
            stream,
            &sasl_initial_response(mech, client_first.as_bytes()),
        )
        .await?;

        let Authentication::SaslContinue(server_first) = read_auth_message(stream, reader).await?
        else {
            return Err("expected SASLContinue".to_string())?;
        };
        let server_first = String::from_utf8(server_first.to_vec())
            .map_err(|e| format!("SCRAM server-first message is not UTF-8: {e}"))?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|e| Error::Unexpected(format!("SCRAM requires a Tokio runtime: {e}")))?;
        let (scram, client_final) = runtime
            .spawn_blocking(move || {
                let client_final = scram.client_final(&server_first)?;
                Ok::<_, crate::auth::ScramError>((scram, client_final))
            })
            .await
            .map_err(|e| Error::Unexpected(format!("SCRAM derivation task failed: {e}")))?
            .map_err(|e| Error::Unexpected(format!("SCRAM handshake failed: {e}")))?;

        self.send(stream, &sasl_response(client_final.as_bytes()))
            .await?;

        let Authentication::SaslFinal(server_final) = read_auth_message(stream, reader).await?
        else {
            return Err("expected SASLFinal".to_string())?;
        };
        let server_final = std::str::from_utf8(&server_final)
            .map_err(|e| format!("SCRAM server-final message is not UTF-8: {e}"))?;
        scram
            .verify_server(server_final)
            .map_err(|e| format!("scram handshake failed: {e}"))?;

        self.expect_ok(reader, stream).await
    }

    /// RFC 7628 OAUTHBEARER: send the bearer token; on a server error challenge,
    /// send the required kvsep so the server can report the error.
    async fn oauthbearer<S>(
        &self,
        stream: &mut S,
        reader: &mut MessageReader,
        token: &str,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let client_first = oauthbearer_client_first(token);
        self.send(
            stream,
            &sasl_initial_response("OAUTHBEARER", client_first.as_bytes()),
        )
        .await?;

        match read_auth_message(stream, reader).await? {
            Authentication::Ok => Ok(()),
            Authentication::SaslContinue(_) => {
                self.send(stream, &sasl_response(b"\x01")).await?;
                match read_auth_message(stream, reader).await? {
                    Authentication::Ok => Ok(()),
                    other => Err(format!("OAuth authentication failed: {other:?}"))?,
                }
            }
            other => Err(format!("unexpected OAuth response: {other:?}"))?,
        }
    }

    async fn gss_loop<S>(
        &self,
        stream: &mut S,
        reader: &mut MessageReader,
        gss: &mut dyn GssStep,
        initial: &[u8],
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut input = initial.to_vec();
        loop {
            let token = gss.step_dyn(&input).map_err(Error::Unexpected)?;
            if !token.is_empty() {
                self.send(stream, &gss_response(&token)).await?;
            }
            match read_auth_message(stream, reader).await? {
                Authentication::Ok => return Ok(()),
                Authentication::GssContinue(data) => input = data.to_vec(),
                other => Err(format!("unexpected GSSAPI response: {other:?}"))?,
            }
        }
    }

    fn password(&self) -> Result<&str> {
        match &self.auth {
            AuthenticationMode::Password(pw) => Ok(pw),
            _ => Err(Error::PasswordRequired),
        }
    }

    async fn send<S>(&self, stream: &mut S, msg: &[u8]) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        stream.write_all(msg).await?;
        stream.flush().await?;
        Ok(())
    }

    async fn send_and_expect_ok<S>(
        &self,
        stream: &mut S,
        reader: &mut MessageReader,
        msg: &[u8],
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.send(stream, msg).await?;
        self.expect_ok(reader, stream).await
    }

    async fn expect_ok<S>(&self, reader: &mut MessageReader, stream: &mut S) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        match read_auth_message(stream, reader).await? {
            Authentication::Ok => Ok(()),
            other => Err(format!("expected AuthenticationOk, got {other:?}"))?,
        }
    }
}

/// Picks the SCRAM channel binding for a server's offered mechanisms. Only a
/// supplied certificate makes the client binding-capable, so `y` requires one.
fn select_channel_binding(cert_hash: Option<&[u8]>, mechs: &[&str]) -> Result<ChannelBinding> {
    let has_plus = mechs.contains(&"SCRAM-SHA-256-PLUS");
    let has_plain = mechs.contains(&"SCRAM-SHA-256");

    match (cert_hash, has_plus, has_plain) {
        (Some(hash), true, _) => Ok(ChannelBinding::TlsServerEndPoint(hash.to_vec())),
        (Some(_), false, true) => Ok(ChannelBinding::NotUsed),
        (None, _, true) => Ok(ChannelBinding::NotSupported),
        (None, true, false) => Err(Error::ChannelBindingRequired),
        _ => Err(Error::Unexpected(format!(
            "no supported SASL mechanism offered: {mechs:?}"
        ))),
    }
}

/// TLS state used to select a SCRAM channel-binding mechanism.
enum Tls {
    /// Plaintext connection.
    None,
    /// TLS connection, with the `tls-server-end-point` hash if the caller
    /// supplied the server certificate.
    Active { cert_hash: Option<Vec<u8>> },
}

impl Tls {
    fn cert_hash(&self) -> Option<&[u8]> {
        match self {
            Tls::Active {
                cert_hash: Some(h), ..
            } => Some(h),
            _ => None,
        }
    }
}

/// Object-safe wrapper over [`GssProvider`] so the handshake need not be generic.
trait GssStep {
    fn step_dyn(&mut self, input: &[u8]) -> std::result::Result<Vec<u8>, String>;
}

struct GssAdapter<'a, G>(&'a mut G);

impl<G: GssProvider> GssStep for GssAdapter<'_, G> {
    fn step_dyn(&mut self, input: &[u8]) -> std::result::Result<Vec<u8>, String> {
        self.0.step(input).map_err(|e| e.to_string())
    }
}

/// Frames a SASLInitialResponse: mechanism name, then the initial response.
fn sasl_initial_response(mech: &str, initial: &[u8]) -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(MessageCode::SASL_RESPONSE.as_u8());
    frame(
        &mut msg,
        cstring_len(mech.as_bytes()) + 4 + initial.len(),
        |buf| {
            buf.put_slice(mech.as_bytes());
            buf.put_u8(0);
            buf.put_u32(initial.len() as u32);
            buf.put_slice(initial);
        },
    );
    msg
}

/// Frames a SASLResponse (bare payload, length-delimited by the frame).
fn sasl_response(data: &[u8]) -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(MessageCode::SASL_RESPONSE.as_u8());
    frame(&mut msg, data.len(), |buf| buf.put_slice(data));
    msg
}

/// Frames a GSSResponse carrying a security token.
fn gss_response(token: &[u8]) -> BytesMut {
    let mut msg = BytesMut::new();
    msg.put_u8(MessageCode::GSS_RESPONSE.as_u8());
    frame(&mut msg, token.len(), |buf| buf.put_slice(token));
    msg
}

/// Builds the RFC 7628 OAUTHBEARER client-first message: GS2 header, then
/// `\x01`-separated `auth=Bearer <token>` terminated by an empty key/value.
fn oauthbearer_client_first(token: &str) -> String {
    format!("n,,\x01auth=Bearer {token}\x01\x01")
}

#[cfg(test)]
mod tests {
    use super::{
        AuthenticationMode, CANCEL_REQUEST_CODE, ChannelBinding, ConnectionBuilder, Error,
        GssProvider, MessageLimits, cancel_request, gss_response, oauthbearer_client_first,
        sasl_initial_response, sasl_response, select_channel_binding,
    };
    use crate::message::frontend::MessageCode;
    use crate::startup::ProtocolVersion;

    fn frame_len(msg: &[u8]) -> usize {
        u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]) as usize
    }

    #[test]
    fn test_message_limits_configuration() {
        let limits = MessageLimits::new(1_024, 4_096);
        let builder = ConnectionBuilder::new("user").message_limits(limits);
        assert_eq!(builder.message_limits, limits);
    }

    #[test]
    fn test_sasl_initial_response_framing() {
        let msg = sasl_initial_response("SCRAM-SHA-256", b"n,,n=,r=abc");
        assert_eq!(msg[0], MessageCode::SASL_RESPONSE.as_u8());
        // len = 4 + cstring("SCRAM-SHA-256") + 4 (initial len) + initial.
        assert_eq!(frame_len(&msg), 4 + 14 + 4 + 11);
        assert_eq!(&msg[5..18], b"SCRAM-SHA-256");
        assert_eq!(msg[18], 0);
        assert_eq!(u32::from_be_bytes(msg[19..23].try_into().unwrap()), 11);
        assert_eq!(&msg[23..], b"n,,n=,r=abc");
    }

    #[test]
    fn test_sasl_response_framing() {
        let msg = sasl_response(b"c=biws,r=xyz,p=proof");
        assert_eq!(msg[0], MessageCode::SASL_RESPONSE.as_u8());
        assert_eq!(frame_len(&msg), 4 + 20);
        assert_eq!(&msg[5..], b"c=biws,r=xyz,p=proof");
    }

    #[test]
    fn test_gss_response_framing() {
        let msg = gss_response(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(msg[0], MessageCode::GSS_RESPONSE.as_u8());
        assert_eq!(frame_len(&msg), 4 + 4);
        assert_eq!(&msg[5..], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_oauthbearer_client_first() {
        assert_eq!(
            oauthbearer_client_first("tok123"),
            "n,,\x01auth=Bearer tok123\x01\x01"
        );
    }

    #[test]
    fn test_protocol_version() {
        let major = 3;
        let minor = 0;
        let version = ProtocolVersion::new(major, minor);
        assert_eq!(major, version.major());
        assert_eq!(minor, version.minor());
        assert_eq!(196608, version.0);
    }

    /// A binding-capable server rejects `y,,` (SCRAM downgrade detection), so it
    /// must only be sent when the client actually holds a certificate.
    #[test]
    fn test_channel_binding_selection() {
        const PLUS: &str = "SCRAM-SHA-256-PLUS";
        const PLAIN: &str = "SCRAM-SHA-256";
        let hash = [0xABu8; 32];

        // Cert available and PLUS offered: bind to the certificate.
        assert_eq!(
            select_channel_binding(Some(&hash), &[PLAIN, PLUS]).unwrap(),
            ChannelBinding::TlsServerEndPoint(hash.to_vec())
        );

        // Cert available but PLUS stripped: flag the downgrade.
        assert_eq!(
            select_channel_binding(Some(&hash), &[PLAIN]).unwrap(),
            ChannelBinding::NotUsed
        );

        // No cert: not binding-capable, so `n,,` even when PLUS is offered.
        for mechs in [vec![PLAIN], vec![PLAIN, PLUS]] {
            assert_eq!(
                select_channel_binding(None, &mechs).unwrap(),
                ChannelBinding::NotSupported,
                "{mechs:?}"
            );
        }

        // Only PLUS offered with no cert: cannot proceed.
        assert!(matches!(
            select_channel_binding(None, &[PLUS]),
            Err(Error::ChannelBindingRequired)
        ));

        // Nothing usable offered.
        assert!(matches!(
            select_channel_binding(None, &["OAUTHBEARER"]),
            Err(Error::Unexpected(_))
        ));
    }

    #[test]
    fn test_cancel_request_encoding() {
        let packet = cancel_request(0x1234_5678, &0x9abc_def0u32.to_be_bytes());
        assert_eq!(packet.len(), 16);
        assert_eq!(u32::from_be_bytes(packet[0..4].try_into().unwrap()), 16);
        assert_eq!(
            u32::from_be_bytes(packet[4..8].try_into().unwrap()),
            CANCEL_REQUEST_CODE
        );
        assert_eq!(
            u32::from_be_bytes(packet[8..12].try_into().unwrap()),
            0x1234_5678
        );
        assert_eq!(
            u32::from_be_bytes(packet[12..16].try_into().unwrap()),
            0x9abc_def0
        );
    }

    /// A 3.2 key of any length is carried verbatim, with the length field
    /// covering it.
    #[test]
    fn test_cancel_request_variable_length_key() {
        let key = vec![0x5Au8; 32];
        let packet = cancel_request(7, &key);
        assert_eq!(packet.len(), 12 + key.len());
        assert_eq!(
            u32::from_be_bytes(packet[0..4].try_into().unwrap()) as usize,
            packet.len()
        );
        assert_eq!(&packet[12..], &key[..]);
    }

    /// A GSS provider that echoes scripted tokens and records server input.
    struct MockGss {
        tokens: Vec<Vec<u8>>,
        seen: Vec<Vec<u8>>,
        idx: usize,
    }

    impl GssProvider for MockGss {
        type Error = std::convert::Infallible;
        fn step(&mut self, input: &[u8]) -> Result<Vec<u8>, Self::Error> {
            self.seen.push(input.to_vec());
            let tok = self.tokens.get(self.idx).cloned().unwrap_or_default();
            self.idx += 1;
            Ok(tok)
        }
    }

    /// Builds a backend Authentication message: code byte 'R', length, subcode.
    fn auth_msg(subcode: u32, body: &[u8]) -> Vec<u8> {
        let mut m = vec![b'R'];
        m.extend_from_slice(&((4 + 4 + body.len()) as u32).to_be_bytes());
        m.extend_from_slice(&subcode.to_be_bytes());
        m.extend_from_slice(body);
        m
    }

    fn empty_backend(code: u8) -> Vec<u8> {
        vec![code, 0, 0, 0, 4]
    }

    #[tokio::test]
    async fn test_gss_handshake_flow() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (client, mut server) = tokio::io::duplex(4096);

        // Scripted server: GSS -> GSSContinue -> Ok, then ReadyForQuery.
        let server_task = tokio::spawn(async move {
            // Consume the startup packet (length-prefixed, no code byte).
            let mut len = [0u8; 4];
            server.read_exact(&mut len).await.unwrap();
            let rest = u32::from_be_bytes(len) as usize - 4;
            let mut buf = vec![0u8; rest];
            server.read_exact(&mut buf).await.unwrap();

            server.write_all(&auth_msg(7, &[])).await.unwrap(); // AuthenticationGSS

            // Read the first GSSResponse ('p').
            read_one_msg(&mut server).await;
            server.write_all(&auth_msg(8, b"server-tok")).await.unwrap(); // GSSContinue

            read_one_msg(&mut server).await;
            server.write_all(&auth_msg(0, &[])).await.unwrap(); // AuthenticationOk

            let mut rfq = empty_backend(b'Z');
            rfq[4] = 5;
            rfq.push(b'I');
            server.write_all(&rfq).await.unwrap();
            server.flush().await.unwrap();
        });

        let mut provider = MockGss {
            tokens: vec![b"client-tok-1".to_vec(), b"client-tok-2".to_vec()],
            seen: Vec::new(),
            idx: 0,
        };
        let res = match ConnectionBuilder::new("gssuser")
            .connect_with_gss(client, &mut provider)
            .await
        {
            Ok((_, res)) => res,
            Err(e) => panic!("gss handshake should succeed: {e}"),
        };

        server_task.await.unwrap();
        assert_eq!(res.process_id, 0);
        // First step sees empty input, second sees the server's continue token.
        assert_eq!(provider.seen, vec![b"".to_vec(), b"server-tok".to_vec()]);
    }

    /// A PostgreSQL 18 startup: a 32-byte cancel key survives the handshake and
    /// round-trips into a CancelRequest.
    #[tokio::test]
    async fn test_startup_accepts_protocol_3_2_cancel_key() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const KEY: [u8; 32] = [0x7Cu8; 32];

        let (client, mut server) = tokio::io::duplex(4096);
        let server_task = tokio::spawn(async move {
            let mut len = [0u8; 4];
            server.read_exact(&mut len).await.unwrap();
            let rest = u32::from_be_bytes(len) as usize - 4;
            let mut buf = vec![0u8; rest];
            server.read_exact(&mut buf).await.unwrap();
            assert_eq!(
                u32::from_be_bytes(buf[..4].try_into().unwrap()),
                u32::from(ProtocolVersion::V3_2)
            );

            server.write_all(&auth_msg(0, &[])).await.unwrap();

            let mut bkd = vec![b'K'];
            bkd.extend_from_slice(&((4 + 4 + KEY.len()) as u32).to_be_bytes());
            bkd.extend_from_slice(&4242u32.to_be_bytes());
            bkd.extend_from_slice(&KEY);
            server.write_all(&bkd).await.unwrap();

            let mut rfq = empty_backend(b'Z');
            rfq[4] = 5;
            rfq.push(b'I');
            server.write_all(&rfq).await.unwrap();
            server.flush().await.unwrap();
        });

        let (_conn, res) = ConnectionBuilder::new("u")
            .protocol(ProtocolVersion::V3_2)
            .connect(client)
            .await
            .expect("3.2 startup should succeed");

        server_task.await.unwrap();
        assert_eq!(res.process_id, 4242);
        assert_eq!(res.secret_key, KEY);
        assert_eq!(&cancel_request(res.process_id, &res.secret_key)[12..], &KEY);
    }

    #[tokio::test]
    async fn test_oauth_rejected_when_not_offered() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (client, mut server) = tokio::io::duplex(4096);
        let server_task = tokio::spawn(async move {
            let mut len = [0u8; 4];
            server.read_exact(&mut len).await.unwrap();
            let rest = u32::from_be_bytes(len) as usize - 4;
            let mut buf = vec![0u8; rest];
            server.read_exact(&mut buf).await.unwrap();
            // Offer only plain SCRAM, not OAUTHBEARER.
            server
                .write_all(&auth_msg(10, b"SCRAM-SHA-256\0\0"))
                .await
                .unwrap();
            server.flush().await.unwrap();
        });

        let result = ConnectionBuilder::new("u")
            .auth(AuthenticationMode::OAuthBearer("tok".into()))
            .connect(client)
            .await;
        match result {
            Err(Error::Unexpected(_)) => {}
            Ok(_) => panic!("should fail: OAuth not offered"),
            Err(e) => panic!("unexpected error variant: {e}"),
        }
        server_task.await.unwrap();
    }

    async fn read_one_msg(server: &mut (impl tokio::io::AsyncRead + Unpin)) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut hdr = [0u8; 5];
        server.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        server.read_exact(&mut body).await.unwrap();
        body
    }
}
