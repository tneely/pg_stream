//! The startup handshake against a scripted server: authentication methods a
//! test server is not set up for, and servers that do not follow the rules.
#![cfg(feature = "startup")]

mod mock;

use std::{error::Error as StdError, pin::pin};

use mock::*;
use pg_stream::{
    AuthError, AuthMethod, Connection, Error, Message, ProtocolError, ProtocolVersion, Result,
    ScramError,
    auth::{GssProvider, md5_password},
    backend::TransactionStatus,
    startup::{Session, Startup},
};

const OK: u32 = 0;
const KERBEROS_V5: u32 = 2;
const CLEARTEXT: u32 = 3;
const MD5: u32 = 5;
const GSS: u32 = 7;
const GSS_CONTINUE: u32 = 8;
const SSPI: u32 = 9;
const SASL: u32 = 10;
const SASL_CONTINUE: u32 = 11;
const SASL_FINAL: u32 = 12;

/// What a server sends once authentication has succeeded.
fn welcome() -> Vec<u8> {
    let key = message(b'K', &[0, 0, 0, 42, 1, 2, 3, 4]);
    let parts = [
        authentication(OK, b""),
        parameter_status("server_version", "18.0"),
        parameter_status("client_encoding", "UTF8"),
        key,
        ready(b'I'),
    ];
    parts.concat()
}

fn connect(startup: &mut Startup<'_>, stream: Stream) -> Result<(Connection<Stream>, Session)> {
    now(startup.connect(stream))
}

/// The settings of a user who has a password.
fn alice<'a>() -> Startup<'a> {
    let mut startup = Startup::new("alice");
    startup.password("secret");
    startup
}

/// The error from a handshake that fails.
fn failure(startup: &mut Startup<'_>, stream: Stream) -> Error {
    let result = connect(startup, stream);
    result.err().expect("the handshake should fail")
}

/// The parameters of the startup packet the client sent, after the version.
fn startup_packet(server: &Server) -> (Vec<u8>, Vec<(String, String)>) {
    let (code, body) = client_messages(&server.received()).remove(0);
    assert_eq!(code, 0);
    let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).unwrap();
    let mut fields = body[4..].split(|&byte| byte == 0).map(text);
    let mut parameters = Vec::new();
    while let (Some(name), Some(value)) = (fields.next(), fields.next()) {
        // The list ends with an empty name.
        if name.is_empty() {
            break;
        }
        parameters.push((name, value));
    }
    (body[..4].to_vec(), parameters)
}

fn pairs(parameters: &[(&str, &str)]) -> Vec<(String, String)> {
    let owned = |&(name, value): &(&str, &str)| (name.to_owned(), value.to_owned());
    parameters.iter().map(owned).collect()
}

/// The mechanism and first message of a SASLInitialResponse.
fn sasl_initial(body: &[u8]) -> (String, String) {
    let end = body.iter().position(|&byte| byte == 0).unwrap();
    let len = u32::from_be_bytes(body[end + 1..end + 5].try_into().unwrap()) as usize;
    let message = &body[end + 5..];
    assert_eq!(message.len(), len);
    let text = |bytes: &[u8]| String::from_utf8(bytes.to_vec()).unwrap();
    (text(&body[..end]), text(message))
}

/// Runs the handshake until the client has answered a SASL offer, and returns
/// that answer.
fn sasl_answer(startup: &mut Startup<'_>, offered: &[u8]) -> (String, String) {
    let (server, stream) = pair();
    server.reply(0, &authentication(SASL, offered));
    let mut connecting = pin!(startup.connect(stream));
    assert!(poll(connecting.as_mut()).is_pending());

    let (code, body) = client_messages(&server.received()).remove(1);
    assert_eq!(code, b'p');
    sasl_initial(&body)
}

fn certificate() -> Vec<u8> {
    pem_to_der(include_str!("data/certs/server.crt"))
}

#[test]
fn trust_reports_the_session() {
    let (server, stream) = pair();
    server.reply(0, &welcome());

    let (conn, session) = connect(&mut Startup::new("alice"), stream).unwrap();
    assert_eq!(session.protocol, ProtocolVersion::V3_0);
    assert_eq!(session.parameter("server_version"), Some("18.0"));
    assert_eq!(session.parameter("no_such_parameter"), None);
    let key = session.backend_key.unwrap();
    assert_eq!(
        (key.process_id, &key.secret_key[..]),
        (42, &[1, 2, 3, 4][..])
    );
    assert_eq!(conn.transaction_status(), TransactionStatus::Idle);

    let (version, parameters) = startup_packet(&server);
    assert_eq!(version, [0, 3, 0, 0]);
    assert_eq!(
        parameters,
        pairs(&[("user", "alice"), ("client_encoding", "UTF8")])
    );
}

#[test]
fn parameters_are_sent_once_in_order() {
    let (server, stream) = pair();
    server.reply(0, &welcome());

    let mut startup = Startup::new("alice");
    startup.database("first");
    startup.parameter("application_name", "tests");
    startup.parameter("client_encoding", "LATIN1");
    startup.database("orders").protocol(ProtocolVersion::V3_2);
    connect(&mut startup, stream).unwrap();

    let (version, parameters) = startup_packet(&server);
    assert_eq!(version, [0, 3, 0, 2]);
    let expected = [
        ("user", "alice"),
        ("database", "orders"),
        ("application_name", "tests"),
        ("client_encoding", "LATIN1"),
    ];
    assert_eq!(parameters, pairs(&expected));
}

#[test]
fn one_startup_opens_several_sessions() {
    let mut startup = alice();
    startup.database("orders");

    for _ in 0..2 {
        let (server, stream) = pair();
        let password = authentication(CLEARTEXT, b"");
        server.reply(0, &password).reply(b'p', &welcome());
        connect(&mut startup, stream).unwrap();

        let sent = client_messages(&server.received());
        let database = b"database\0orders\0";
        let packet = &sent[0].1;
        assert!(
            packet
                .windows(database.len())
                .any(|bytes| bytes == database)
        );
        assert_eq!(sent[1], (b'p', b"secret\0".to_vec()));
    }
}

#[test]
fn the_server_can_negotiate_the_protocol_down() {
    let (server, stream) = pair();
    // Minor version 0, and one protocol option it did not recognize.
    let negotiate = message(b'v', b"\0\0\0\0\0\0\0\x01_pq_.extension\0");
    server.reply(0, &[negotiate, welcome()].concat());

    let mut startup = Startup::new("alice");
    let (_, session) = connect(startup.protocol(ProtocolVersion::V3_2), stream).unwrap();
    assert_eq!(session.protocol, ProtocolVersion::V3_0);
}

#[test]
fn a_connection_is_ready_to_use_after_the_handshake() {
    let (server, stream) = pair();
    // A notice that arrives with the greeting belongs to whoever reads next.
    server.reply(0, &[notice("before"), welcome(), notice("after")].concat());

    let (mut conn, _) = connect(&mut Startup::new("alice"), stream).unwrap();
    match now(conn.recv()).unwrap() {
        Message::NoticeResponse(notice) => assert_eq!(notice.message(), "after"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn cleartext_and_md5_passwords_are_sent_on_request() {
    let salt = [0xde, 0xad, 0xbe, 0xef];
    let hashed = md5_password("alice", "secret", salt);
    let requests = [
        (authentication(CLEARTEXT, b""), "secret"),
        (authentication(MD5, &salt), &hashed),
    ];

    for (request, expected) in requests {
        let (server, stream) = pair();
        server.reply(0, &request).reply(b'p', &welcome());
        connect(Startup::new("alice").password("secret"), stream).unwrap();

        let (code, body) = client_messages(&server.received()).remove(1);
        assert_eq!((code, body), (b'p', format!("{expected}\0").into_bytes()));
    }
}

#[test]
fn a_password_is_required_when_the_server_asks_for_one() {
    let sasl = authentication(SASL, b"SCRAM-SHA-256\0\0");
    for request in [
        authentication(CLEARTEXT, b""),
        authentication(MD5, &[1, 2, 3, 4]),
        sasl,
    ] {
        let (server, stream) = pair();
        server.reply(0, &request);
        let error = failure(&mut Startup::new("alice"), stream);
        assert!(
            matches!(error, Error::Auth(AuthError::PasswordRequired)),
            "{error:?}"
        );
    }
}

#[test]
fn scram_is_bound_to_the_channel_when_it_can_be() {
    const BOTH: &[u8] = b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0";
    const UNBOUND: &[u8] = b"SCRAM-SHA-256\0\0";
    let certificate = certificate();

    // A certificate and a server offering to bind: bind.
    let (mechanism, first) = sasl_answer(alice().server_certificate(&certificate), BOTH);
    assert_eq!(mechanism, "SCRAM-SHA-256-PLUS");
    assert!(
        first.starts_with("p=tls-server-end-point,,n=,r="),
        "{first}"
    );

    // A certificate, but the offer to bind is missing. The server may never
    // have made one, or an attacker may have removed it; saying so lets a
    // server that did make one tell the difference.
    let (mechanism, first) = sasl_answer(alice().server_certificate(&certificate), UNBOUND);
    assert_eq!(mechanism, "SCRAM-SHA-256");
    assert!(first.starts_with("y,,n=,r="), "{first}");

    // No certificate: this client cannot bind, whatever is offered.
    for offered in [BOTH, UNBOUND] {
        let (mechanism, first) = sasl_answer(&mut alice(), offered);
        assert_eq!(mechanism, "SCRAM-SHA-256");
        assert!(first.starts_with("n,,n=,r="), "{first}");
    }
}

#[test]
fn unusable_sasl_offers_are_refused() {
    let offer = |offered: &[u8], startup: &mut Startup<'_>| {
        let (server, stream) = pair();
        server.reply(0, &authentication(SASL, offered));
        failure(startup, stream)
    };

    let error = offer(b"SCRAM-SHA-256-PLUS\0\0", &mut alice());
    assert!(
        matches!(error, Error::Auth(AuthError::ChannelBindingUnavailable)),
        "{error:?}"
    );

    let error = offer(b"SCRAM-SHA-512\0\0", &mut alice());
    assert!(
        matches!(error, Error::Auth(AuthError::NoUsableMechanism)),
        "{error:?}"
    );

    let mut token_only = Startup::new("alice");
    let error = offer(b"SCRAM-SHA-256\0\0", token_only.oauth_token("token"));
    assert!(
        matches!(error, Error::Auth(AuthError::NoUsableMechanism)),
        "{error:?}"
    );

    let mut garbage = alice();
    garbage.server_certificate(b"not a certificate");
    let error = offer(b"SCRAM-SHA-256-PLUS\0\0", &mut garbage);
    let invalid = AuthError::Scram(ScramError::InvalidCertificate);
    assert!(
        matches!(&error, Error::Auth(e) if e.to_string() == invalid.to_string()),
        "{error:?}"
    );
}

/// A scripted server that plays the first half of a SCRAM exchange honestly,
/// extending whatever nonce the client chose.
fn scram_server_first(server: &Server) {
    server.reply(0, &authentication(SASL, b"SCRAM-SHA-256\0\0"));
    server.reply_with(b'p', |body| {
        let (_, client_first) = sasl_initial(body);
        let nonce = client_first.rsplit("r=").next().unwrap();
        let server_first = format!("r={nonce}server,s=c2FsdA==,i=4096");
        authentication(SASL_CONTINUE, server_first.as_bytes())
    });
}

#[test]
fn a_server_must_prove_it_knows_the_password() {
    let mut startup = alice();

    // A forged signature.
    let (server, stream) = pair();
    scram_server_first(&server);
    let forged = b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    server.reply(
        b'p',
        &[authentication(SASL_FINAL, forged), welcome()].concat(),
    );
    let error = failure(&mut startup, stream);
    let mismatch = AuthError::Scram(ScramError::ServerSignatureMismatch);
    assert!(
        matches!(&error, Error::Auth(e) if e.to_string() == mismatch.to_string()),
        "{error:?}"
    );

    // No signature at all: success declared straight after the client's proof.
    let (server, stream) = pair();
    scram_server_first(&server);
    server.reply(b'p', &welcome());
    let error = failure(&mut startup, stream);
    let out_of_order = ProtocolError::Unexpected {
        code: b'R',
        expected: "the next authentication step",
    };
    assert!(
        matches!(&error, Error::Protocol(e) if *e == out_of_order),
        "{error:?}"
    );

    // Success declared before the client has even proven itself.
    let (server, stream) = pair();
    server.reply(0, &authentication(SASL, b"SCRAM-SHA-256\0\0"));
    server.reply(b'p', &welcome());
    let error = failure(&mut startup, stream);
    assert!(
        matches!(&error, Error::Protocol(e) if *e == out_of_order),
        "{error:?}"
    );
}

/// ReadyForQuery is what opens the session, so it is not to be believed from a
/// server that has not said the client is authenticated. Least of all from one
/// that still owes its SCRAM signature: skipping to the end is how a server
/// that does not know the password would go about it.
#[test]
fn the_session_does_not_open_without_authentication_ok() {
    let greeting = [parameter_status("server_version", "18.0"), ready(b'I')].concat();
    let premature = ProtocolError::Unexpected {
        code: b'Z',
        expected: "AuthenticationOk",
    };
    let refused = |error: Error| matches!(&error, Error::Protocol(e) if *e == premature);

    let (server, stream) = pair();
    server.reply(0, &greeting);
    assert!(refused(failure(&mut Startup::new("alice"), stream)));

    let (server, stream) = pair();
    server
        .reply(0, &authentication(CLEARTEXT, b""))
        .reply(b'p', &greeting);
    assert!(refused(failure(
        Startup::new("alice").password("secret"),
        stream
    )));

    let (server, stream) = pair();
    scram_server_first(&server);
    server.reply(b'p', &greeting);
    assert!(refused(failure(
        Startup::new("alice").password("secret"),
        stream
    )));
}

#[test]
fn only_allowed_methods_are_answered() {
    let certificate = certificate();
    let allowed = [AuthMethod::ScramSha256Plus];
    let mut startup = alice();
    startup
        .server_certificate(&certificate)
        .allow_only(&allowed);

    let requests = [
        (welcome(), AuthMethod::None),
        (authentication(CLEARTEXT, b""), AuthMethod::Cleartext),
        (authentication(MD5, &[1, 2, 3, 4]), AuthMethod::Md5),
        (
            authentication(SASL, b"SCRAM-SHA-256\0\0"),
            AuthMethod::ScramSha256,
        ),
        (authentication(GSS, b""), AuthMethod::Gss),
        (authentication(SSPI, b""), AuthMethod::Sspi),
    ];
    for (request, method) in requests {
        let (server, stream) = pair();
        server.reply(0, &request);
        let error = failure(&mut startup, stream);
        let refused = matches!(error, Error::Auth(AuthError::MethodNotAllowed(m)) if m == method);
        assert!(refused, "{method:?}: {error:?}");
        // Nothing but the startup packet has left the client.
        assert_eq!(client_messages(&server.received()).len(), 1, "{method:?}");
    }

    let (mechanism, _) = sasl_answer(&mut startup, b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0");
    assert_eq!(mechanism, "SCRAM-SHA-256-PLUS");
}

#[test]
fn scram_messages_out_of_turn_are_refused() {
    let out_of_order = ProtocolError::Unexpected {
        code: b'R',
        expected: "the next authentication step",
    };
    for unprompted in [SASL_CONTINUE, SASL_FINAL] {
        let (server, stream) = pair();
        server.reply(0, &authentication(unprompted, b"r=nonce,s=c2FsdA==,i=4096"));
        let error = failure(Startup::new("alice").password("secret"), stream);
        assert!(
            matches!(&error, Error::Protocol(e) if *e == out_of_order),
            "{error:?}"
        );
    }

    // A nonce that is not the client's own, extended.
    let (server, stream) = pair();
    server.reply(0, &authentication(SASL, b"SCRAM-SHA-256\0\0"));
    server.reply(
        b'p',
        &authentication(SASL_CONTINUE, b"r=somebody-else,s=c2FsdA==,i=4096"),
    );
    let error = failure(Startup::new("alice").password("secret"), stream);
    let invalid = AuthError::Scram(ScramError::InvalidNonce);
    assert!(
        matches!(&error, Error::Auth(e) if e.to_string() == invalid.to_string()),
        "{error:?}"
    );
}

#[test]
fn oauth_sends_the_bearer_token() {
    let (server, stream) = pair();
    server.reply(0, &authentication(SASL, b"OAUTHBEARER\0\0"));
    server.reply(b'p', &welcome());

    // The token is preferred to the password when the server takes tokens.
    let mut startup = Startup::new("alice");
    connect(startup.oauth_token("t0ken").password("secret"), stream).unwrap();

    let (_, body) = client_messages(&server.received()).remove(1);
    let (mechanism, first) = sasl_initial(&body);
    assert_eq!(
        (&mechanism[..], &first[..]),
        ("OAUTHBEARER", "n,,\x01auth=Bearer t0ken\x01\x01")
    );
}

#[test]
fn a_rejected_oauth_token_is_acknowledged_so_the_server_can_say_why() {
    let (server, stream) = pair();
    server.reply(0, &authentication(SASL, b"OAUTHBEARER\0\0"));
    server.reply(
        b'p',
        &authentication(SASL_CONTINUE, br#"{"status":"invalid_token"}"#),
    );
    server.reply(b'p', &fatal("28000", "bearer authentication failed"));

    let error = failure(Startup::new("alice").oauth_token("expired"), stream);
    assert_eq!(error.as_server().unwrap().code(), "28000");

    let (_, acknowledgement) = client_messages(&server.received()).remove(2);
    assert_eq!(acknowledgement, b"\x01");
}

/// A provider that hands out scripted tokens and records what it was given.
struct ScriptedGss {
    tokens: Vec<&'static [u8]>,
    seen: Vec<Vec<u8>>,
}

impl ScriptedGss {
    fn new(tokens: &[&'static [u8]]) -> Self {
        Self {
            tokens: tokens.to_vec(),
            seen: Vec::new(),
        }
    }
}

impl GssProvider for ScriptedGss {
    fn step(&mut self, token: &[u8]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        self.seen.push(token.to_vec());
        match self.tokens.is_empty() {
            true => Err("the security context could not be established".into()),
            false => Ok(self.tokens.remove(0).to_vec()),
        }
    }
}

#[test]
fn gss_and_sspi_exchange_tokens_until_the_server_is_satisfied() {
    for request in [GSS, SSPI] {
        let mut provider = ScriptedGss::new(&[b"first", b"second", b""]);

        let (server, stream) = pair();
        server.reply(0, &authentication(request, b""));
        server.reply(b'p', &authentication(GSS_CONTINUE, b"challenge one"));
        // The last step produces no token, so the server carries on unprompted.
        let rest = [authentication(GSS_CONTINUE, b"challenge two"), welcome()];
        server.reply(b'p', &rest.concat());
        connect(Startup::new("alice").gss(&mut provider), stream).unwrap();

        let sent = client_messages(&server.received());
        let tokens: Vec<_> = sent[1..].iter().map(|(_, body)| &body[..]).collect();
        assert_eq!(tokens, [&b"first"[..], b"second"]);
        assert_eq!(
            provider.seen,
            [&b""[..], b"challenge one", b"challenge two"]
        );
    }
}

#[test]
fn a_gss_token_out_of_turn_is_refused() {
    let (server, stream) = pair();
    server.reply(0, &authentication(GSS_CONTINUE, b"unprompted"));
    let mut provider = ScriptedGss::new(&[b"token"]);
    let error = failure(Startup::new("alice").gss(&mut provider), stream);
    assert!(matches!(
        error,
        Error::Protocol(ProtocolError::Unexpected { code: b'R', .. })
    ));
    assert!(provider.seen.is_empty());
}

#[test]
fn gss_needs_a_provider_that_works() {
    let (server, stream) = pair();
    server.reply(0, &authentication(GSS, b""));
    let error = failure(Startup::new("alice").password("secret"), stream);
    assert!(
        matches!(error, Error::Auth(AuthError::GssUnavailable)),
        "{error:?}"
    );

    let (server, stream) = pair();
    server.reply(0, &authentication(GSS, b""));
    let mut provider = ScriptedGss::new(&[]);
    let error = failure(Startup::new("alice").gss(&mut provider), stream);
    let expected = "authentication failed: GSSAPI/SSPI failed: \
        the security context could not be established";
    assert_eq!(error.to_string(), expected);
    assert!(error.source().unwrap().source().is_some());
}

/// A provider holds the security context of one exchange, so the handshake
/// that uses it takes it, and one that does not leaves it for the next.
#[test]
fn a_gss_provider_serves_one_handshake() {
    let mut provider = ScriptedGss::new(&[b"token", b"another"]);
    let mut startup = alice();
    startup.gss(&mut provider);

    let (server, stream) = pair();
    let password = authentication(CLEARTEXT, b"");
    server.reply(0, &password).reply(b'p', &welcome());
    connect(&mut startup, stream).unwrap();

    let (server, stream) = pair();
    let gss = authentication(GSS, b"");
    server.reply(0, &gss).reply(b'p', &welcome());
    connect(&mut startup, stream).unwrap();

    let (server, stream) = pair();
    server.reply(0, &gss);
    let error = failure(&mut startup, stream);
    assert!(
        matches!(error, Error::Auth(AuthError::GssUnavailable)),
        "{error:?}"
    );
    drop(startup);
    assert_eq!(provider.seen, [&b""[..]]);
}

#[test]
fn unsupported_methods_are_named() {
    for (request, code) in [
        (authentication(KERBEROS_V5, b""), 2),
        (authentication(99, b"?"), 99),
    ] {
        let (server, stream) = pair();
        server.reply(0, &request);
        let error = failure(Startup::new("alice").password("secret"), stream);
        assert!(matches!(error, Error::Auth(AuthError::UnsupportedMethod(c)) if c == code));
    }
}

#[test]
fn a_refusal_is_the_servers_error() {
    let (server, stream) = pair();
    let refusal = fatal("28P01", "password authentication failed for user \"alice\"");
    server
        .reply(0, &authentication(CLEARTEXT, b""))
        .reply(b'p', &refusal);

    let error = failure(Startup::new("alice").password("wrong"), stream);
    let refusal = error.as_server().unwrap();
    assert_eq!(refusal.code(), "28P01");
    assert!(refusal.fields().is_fatal());
    assert_eq!(
        refusal.to_string(),
        "FATAL: password authentication failed for user \"alice\" (SQLSTATE 28P01)"
    );
}

#[test]
fn anything_else_during_startup_is_a_protocol_error() {
    let (server, stream) = pair();
    server.reply(
        0,
        &[authentication(OK, b""), data_row(&[Some("1")])].concat(),
    );

    let error = failure(&mut Startup::new("alice"), stream);
    let unexpected = ProtocolError::Unexpected {
        code: b'D',
        expected: "a startup message",
    };
    assert!(
        matches!(&error, Error::Protocol(e) if *e == unexpected),
        "{error:?}"
    );
}

#[test]
fn a_handshake_can_use_a_connection_configured_beforehand() {
    let (server, stream) = pair();
    let long = "x".repeat(4096);
    let greeting = [
        authentication(OK, b""),
        parameter_status("long", &long),
        ready(b'I'),
    ];
    server.reply(0, &greeting.concat());

    let mut conn = Connection::new(stream);
    conn.set_max_message_len(1024);
    let error = now(Startup::new("alice").handshake(&mut conn)).unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol(ProtocolError::TooLarge { code: b'S', .. })
    ));
}

/// Connecting on a spawned task needs the handshake to be `Send`, whatever it
/// was configured with.
#[cfg(feature = "tokio")]
#[test]
fn the_handshake_over_a_send_transport_is_send() {
    fn assert_send<T: Send>(_: &T) {}
    struct Provider;
    impl GssProvider for Provider {
        fn step(&mut self, _: &[u8]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
            Ok(Vec::new())
        }
    }

    let (stream, _server) = tokio::io::duplex(64);
    let certificate = certificate();
    let mut startup = alice();
    startup.gss(Provider).server_certificate(&certificate);
    assert_send(&startup.connect(stream));
}
