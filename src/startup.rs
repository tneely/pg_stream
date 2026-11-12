//! Opening a session: the startup packet, authentication, and the server's
//! greeting.

use crate::{
    AuthError, AuthMethod, Connection, Error, Frontend, ProtocolVersion, Result, Transport,
    auth::{ChannelBinding, GssProvider, Scram, ScramProof, md5_password, tls_server_end_point},
    backend::{Authentication, BackendKey, Message, SaslMechanisms, code},
};

type BoxedGss<'a> = Box<dyn GssProvider + Send + 'a>;

/// Everything needed to open a session on a new connection.
///
/// ```no_run
/// # async fn example<S: pg_stream::Transport>(stream: S) -> pg_stream::Result<()> {
/// use pg_stream::startup::Startup;
///
/// // `stream` is a freshly opened connection, such as a tokio `TcpStream`.
/// let mut startup = Startup::new("app");
/// startup.database("orders").password("secret");
/// let (mut conn, session) = startup.connect(stream).await?;
/// println!("connected to PostgreSQL {:?}", session.parameter("server_version"));
/// # Ok(())
/// # }
/// ```
///
/// Where the settings are not needed again, they can be given in the
/// expression that connects:
///
/// ```no_run
/// # async fn example<S: pg_stream::Transport>(stream: S) -> pg_stream::Result<()> {
/// # use pg_stream::startup::Startup;
/// let (mut conn, session) = Startup::new("app").password("secret").connect(stream).await?;
/// # Ok(())
/// # }
/// ```
///
/// The server chooses the authentication method. A password answers
/// cleartext, MD5 and SCRAM-SHA-256 requests; give the server's certificate as
/// well and SCRAM is bound to the TLS channel when the server offers it.
///
/// # TLS
///
/// TLS is negotiated before the session starts. Ask for it with
/// [`request_tls`](crate::request_tls), run the handshake with the TLS library
/// of your choice, and connect over the stream it gives back:
///
/// ```no_run
/// # async fn example<S: pg_stream::Transport, T: pg_stream::Transport>(
/// #     mut tcp: S,
/// #     handshake: impl AsyncFnOnce(S) -> std::io::Result<(T, Vec<u8>)>,
/// # ) -> pg_stream::Result<()> {
/// use pg_stream::{request_tls, startup::Startup};
///
/// if !request_tls(&mut tcp).await? {
///     return Err(std::io::Error::other("the server does not accept TLS").into());
/// }
/// // Returns the encrypted stream and the certificate the server presented.
/// let (tls, certificate) = handshake(tcp).await?;
///
/// let mut startup = Startup::new("app");
/// startup.password("secret").server_certificate(&certificate);
/// let (mut conn, session) = startup.connect(tls).await?;
/// # Ok(())
/// # }
/// ```
///
/// A server set up for direct TLS (PostgreSQL 17, `sslnegotiation=direct`)
/// skips the request: start the TLS handshake at once, with the ALPN protocol
/// `postgresql`.
pub struct Startup<'a> {
    user: &'a str,
    parameters: Vec<(&'a str, &'a str)>,
    protocol: ProtocolVersion,
    password: Option<&'a str>,
    oauth_token: Option<&'a str>,
    server_certificate: Option<&'a [u8]>,
    gss: Option<BoxedGss<'a>>,
    allowed: Option<&'a [AuthMethod]>,
}

impl<'a> Startup<'a> {
    /// Starts a session as `user`, with no credentials and protocol 3.0.
    pub fn new(user: &'a str) -> Self {
        Self {
            user,
            parameters: Vec::new(),
            protocol: ProtocolVersion::V3_0,
            password: None,
            oauth_token: None,
            server_certificate: None,
            gss: None,
            allowed: None,
        }
    }

    /// The database to connect to. The server defaults to the user's name.
    pub fn database(&mut self, database: &'a str) -> &mut Self {
        self.parameter("database", database)
    }

    /// Sets a run-time parameter for the session, such as `application_name`
    /// or `search_path`, replacing any earlier value.
    ///
    /// `client_encoding` is sent as `UTF8` unless set here.
    pub fn parameter(&mut self, name: &'a str, value: &'a str) -> &mut Self {
        let mut parameters = self.parameters.iter_mut();
        match parameters.find(|(existing, _)| *existing == name) {
            Some(parameter) => parameter.1 = value,
            None => self.parameters.push((name, value)),
        }
        self
    }

    /// The protocol version to ask for. A server that does not speak it
    /// negotiates down; see [`Session::protocol`].
    pub fn protocol(&mut self, protocol: ProtocolVersion) -> &mut Self {
        self.protocol = protocol;
        self
    }

    /// The password to answer cleartext, MD5 and SCRAM requests with.
    pub fn password(&mut self, password: &'a str) -> &mut Self {
        self.password = Some(password);
        self
    }

    /// An OAuth 2.0 bearer token, used when the server offers `OAUTHBEARER`
    /// (PostgreSQL 18).
    pub fn oauth_token(&mut self, token: &'a str) -> &mut Self {
        self.oauth_token = Some(token);
        self
    }

    /// The certificate the server presented in the TLS handshake, in DER form.
    ///
    /// With it, SCRAM is bound to the TLS channel (`SCRAM-SHA-256-PLUS`), so
    /// that a server impersonating the real one cannot relay the exchange.
    /// Without it, a server that insists on channel binding cannot be
    /// authenticated to.
    pub fn server_certificate(&mut self, der: &'a [u8]) -> &mut Self {
        self.server_certificate = Some(der);
        self
    }

    /// The source of tokens for GSSAPI or SSPI authentication.
    ///
    /// A provider holds the state of one exchange, so the handshake that
    /// needs it uses it up.
    pub fn gss(&mut self, provider: impl GssProvider + Send + 'a) -> &mut Self {
        self.gss = Some(Box::new(provider));
        self
    }

    /// Restricts authentication to the methods listed.
    ///
    /// By default the server's choice of method is final, as it is with libpq.
    /// Whoever answers the connection can then ask for the password in
    /// cleartext, and will be sent it. With a list, a request for any other
    /// method fails with [`AuthError::MethodNotAllowed`] before a credential
    /// leaves the client.
    ///
    /// `&[AuthMethod::ScramSha256Plus]` is libpq's `channel_binding=require`.
    pub fn allow_only(&mut self, methods: &'a [AuthMethod]) -> &mut Self {
        self.allowed = Some(methods);
        self
    }

    /// Opens a session over `stream`. The settings are kept, so the same
    /// `Startup` can open another.
    pub async fn connect<S: Transport>(&mut self, stream: S) -> Result<(Connection<S>, Session)> {
        let mut conn = Connection::new(stream);
        let session = self.handshake(&mut conn).await?;
        Ok((conn, session))
    }

    /// Opens a session on a connection that has not exchanged anything yet.
    /// On success the connection is ready for queries.
    ///
    /// A handshake cannot be resumed. If it fails, or this future is dropped
    /// before it finishes, close the connection.
    pub async fn handshake<S: Transport>(&mut self, conn: &mut Connection<S>) -> Result<Session> {
        let mut parameters = vec![("user", self.user)];
        parameters.extend(&self.parameters);
        if !self.sets("client_encoding") {
            parameters.push(("client_encoding", "UTF8"));
        }
        conn.startup_message(self.protocol, parameters);

        let mut session = Session::new(self.protocol);
        let mut authenticator = Authenticator::new(self);
        // Replies are composed here because a received message borrows the
        // connection for as long as it is being looked at.
        let mut reply = Vec::new();
        loop {
            match conn.recv().await? {
                Message::Authentication(request) => authenticator.answer(request, &mut reply)?,
                Message::NegotiateProtocolVersion(offer) => session.negotiate(offer.minor_version),
                Message::ParameterStatus(status) => session.report(status.name, status.value),
                Message::BackendKeyData(key) => session.backend_key = Some(key.to_owned()),
                Message::NoticeResponse(_) => {}
                Message::ErrorResponse(error) => return Err(Error::Server(error.to_error())),
                Message::ReadyForQuery(_) => return authenticator.finish(session),
                other => return Err(Error::unexpected(other.code(), "a startup message")),
            }
            conn.buf().append(&mut reply);
        }
    }

    fn sets(&self, name: &str) -> bool {
        let mut names = self.parameters.iter().map(|(name, _)| *name);
        names.any(|existing| existing == name)
    }

    fn require_password(&self) -> Result<&'a str, AuthError> {
        self.password.ok_or(AuthError::PasswordRequired)
    }

    /// Chooses how to bind SCRAM to the TLS channel, given what the server
    /// offers and whether there is a certificate to bind to.
    fn channel_binding(&self, offered: SaslMechanisms<'_>) -> Result<ChannelBinding, AuthError> {
        let bound = offered.contains("SCRAM-SHA-256-PLUS");
        let unbound = offered.contains("SCRAM-SHA-256");
        match (self.server_certificate, bound, unbound) {
            (Some(certificate), true, _) => {
                let hash = tls_server_end_point(certificate)?;
                Ok(ChannelBinding::TlsServerEndPoint(hash))
            }
            (Some(_), false, true) => Ok(ChannelBinding::Unused),
            (None, _, true) => Ok(ChannelBinding::Unsupported),
            (None, true, false) => Err(AuthError::ChannelBindingUnavailable),
            (_, false, false) => Err(AuthError::NoUsableMechanism),
        }
    }
}

impl std::fmt::Debug for Startup<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Credentials are left out.
        let mut debug = f.debug_struct("Startup");
        debug.field("user", &self.user);
        debug.field("parameters", &self.parameters);
        debug.field("protocol", &self.protocol);
        debug.finish_non_exhaustive()
    }
}

/// Answers the server's authentication requests over one handshake.
struct Authenticator<'s, 'a> {
    startup: &'s mut Startup<'a>,
    exchange: Exchange<'a>,
    /// The method the server has asked for; `None` while it has asked for
    /// nothing.
    method: AuthMethod,
}

/// Where the authentication exchange stands.
enum Exchange<'a> {
    /// Nothing is in progress: either the server has yet to ask for anything,
    /// or it has been answered and its verdict is awaited.
    Idle,
    /// SCRAM: the client-first message has been sent.
    Scram(Scram),
    /// SCRAM: the client's proof has been sent and the server's is owed.
    ScramProof(ScramProof),
    /// OAUTHBEARER: the token has been sent.
    OAuth,
    /// GSSAPI or SSPI: tokens are going back and forth.
    Gss(BoxedGss<'a>),
    /// The server has declared authentication successful.
    Authenticated,
}

impl<'s, 'a> Authenticator<'s, 'a> {
    fn new(startup: &'s mut Startup<'a>) -> Self {
        Self {
            startup,
            exchange: Exchange::Idle,
            method: AuthMethod::None,
        }
    }

    /// Settles on the method the server asked for, if the caller allows it.
    fn choose(&mut self, method: AuthMethod) -> Result<(), AuthError> {
        let allowed = self.startup.allowed;
        if allowed.is_some_and(|allowed| !allowed.contains(&method)) {
            return Err(AuthError::MethodNotAllowed(method));
        }
        self.method = method;
        Ok(())
    }

    /// Answers one authentication request, writing any response to `reply`.
    fn answer(&mut self, request: Authentication<'_>, reply: &mut Vec<u8>) -> Result<()> {
        let out_of_order = Error::unexpected(code::AUTHENTICATION, "the next authentication step");
        let exchange = std::mem::replace(&mut self.exchange, Exchange::Idle);
        match (request, exchange) {
            (Authentication::Ok, Exchange::Idle | Exchange::OAuth | Exchange::Gss(_)) => {
                // A server that asked for nothing has chosen a method too.
                self.choose(self.method)?;
                self.exchange = Exchange::Authenticated;
            }
            (Authentication::CleartextPassword, Exchange::Idle) => {
                self.choose(AuthMethod::Cleartext)?;
                reply.password_message(self.startup.require_password()?);
            }
            (Authentication::Md5Password { salt }, Exchange::Idle) => {
                self.choose(AuthMethod::Md5)?;
                let password = self.startup.require_password()?;
                reply.password_message(&md5_password(self.startup.user, password, salt));
            }
            (Authentication::Sasl(offered), Exchange::Idle) => {
                self.exchange = self.start_sasl(offered, reply)?;
            }
            (Authentication::SaslContinue(server_first), Exchange::Scram(scram)) => {
                let proof = scram.respond(server_first)?;
                reply.sasl_response(proof.client_final().as_bytes());
                self.exchange = Exchange::ScramProof(proof);
            }
            (Authentication::SaslFinal(server_final), Exchange::ScramProof(proof)) => {
                proof.verify(server_final)?;
            }
            // RFC 7628: the server has rejected the token and said why. It
            // waits for this acknowledgement before failing the exchange with
            // an ErrorResponse.
            (Authentication::SaslContinue(_), Exchange::OAuth) => {
                reply.sasl_response(b"\x01");
            }
            (Authentication::Gss, Exchange::Idle) => self.start_gss(AuthMethod::Gss, reply)?,
            (Authentication::Sspi, Exchange::Idle) => self.start_gss(AuthMethod::Sspi, reply)?,
            (Authentication::GssContinue(token), Exchange::Gss(provider)) => {
                self.gss_step(provider, token, reply)?;
            }
            (request @ (Authentication::KerberosV5 | Authentication::Unknown { .. }), _) => {
                return Err(AuthError::UnsupportedMethod(request.code()).into());
            }
            // Anything else is out of order. In particular the server may not
            // declare success while it still owes proof that it knows the
            // password.
            _ => return Err(out_of_order),
        }
        Ok(())
    }

    /// Ends the handshake at the server's ReadyForQuery, which only means
    /// anything once the server has declared authentication successful.
    fn finish(self, session: Session) -> Result<Session> {
        match self.exchange {
            Exchange::Authenticated => Ok(session),
            _ => Err(Error::unexpected(code::READY_FOR_QUERY, "AuthenticationOk")),
        }
    }

    fn start_sasl(
        &mut self,
        offered: SaslMechanisms<'_>,
        reply: &mut Vec<u8>,
    ) -> Result<Exchange<'a>, AuthError> {
        match (self.startup.oauth_token, self.startup.password) {
            (Some(token), _) if offered.contains("OAUTHBEARER") => {
                self.choose(AuthMethod::OAuthBearer)?;
                let message = format!("n,,\x01auth=Bearer {token}\x01\x01");
                reply.sasl_initial_response("OAUTHBEARER", message.as_bytes());
                Ok(Exchange::OAuth)
            }
            (_, Some(password)) => {
                let binding = self.startup.channel_binding(offered)?;
                self.choose(match binding {
                    ChannelBinding::TlsServerEndPoint(_) => AuthMethod::ScramSha256Plus,
                    ChannelBinding::Unsupported | ChannelBinding::Unused => AuthMethod::ScramSha256,
                })?;
                let scram = Scram::new(password, binding);
                reply.sasl_initial_response(scram.mechanism(), scram.client_first().as_bytes());
                Ok(Exchange::Scram(scram))
            }
            (Some(_), None) => Err(AuthError::NoUsableMechanism),
            (None, None) => Err(AuthError::PasswordRequired),
        }
    }

    fn start_gss(&mut self, method: AuthMethod, reply: &mut Vec<u8>) -> Result<(), AuthError> {
        self.choose(method)?;
        let provider = self.startup.gss.take().ok_or(AuthError::GssUnavailable)?;
        self.gss_step(provider, &[], reply)
    }

    /// Gives the provider the server's token and sends on what it answers.
    fn gss_step(
        &mut self,
        mut provider: BoxedGss<'a>,
        token: &[u8],
        reply: &mut Vec<u8>,
    ) -> Result<(), AuthError> {
        let token = provider.step(token).map_err(AuthError::Gss)?;
        if !token.is_empty() {
            reply.gss_response(&token);
        }
        self.exchange = Exchange::Gss(provider);
        Ok(())
    }
}

/// What the server said about the session while it was being opened.
#[derive(Debug, Clone)]
pub struct Session {
    /// The protocol version in use, which is lower than the one asked for if
    /// the server negotiated down.
    pub protocol: ProtocolVersion,
    /// The key for cancelling this session's queries with
    /// [`cancel`](crate::cancel). Some poolers and Postgres-compatible servers
    /// do not send one.
    pub backend_key: Option<BackendKey>,
    /// The run-time parameters the server reported, in the order it sent them.
    pub parameters: Vec<(String, String)>,
}

impl Session {
    fn new(protocol: ProtocolVersion) -> Self {
        Self {
            protocol,
            backend_key: None,
            parameters: Vec::new(),
        }
    }

    /// Records a run-time parameter the server reported.
    fn report(&mut self, name: &str, value: &str) {
        self.parameters.push((name.into(), value.into()));
    }

    /// Lowers the protocol version to the newest minor version the server
    /// speaks.
    fn negotiate(&mut self, server_minor: u32) {
        let server_minor = u16::try_from(server_minor).unwrap_or(u16::MAX);
        let minor = self.protocol.minor().min(server_minor);
        self.protocol = ProtocolVersion::new(self.protocol.major(), minor);
    }

    /// The value of a parameter the server reported, such as `server_version`.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        let mut parameters = self.parameters.iter();
        let (_, value) = parameters.find(|(reported, _)| reported == name)?;
        Some(value)
    }
}
