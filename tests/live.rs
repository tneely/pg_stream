//! Live integration tests against a running Postgres server.
//!
//! These run only when `PGSTREAM_TEST_PORT` is set, so `cargo test` stays
//! hermetic by default. Point at a server with roles
//! `md5_user`/`sha_user`/`pw_user` (passwords `md5`/`sha`/`pw`) and a trusted
//! `postgres` superuser. Set `PGSTREAM_TEST_TLS=1` if the server has TLS and a
//! `hostssl` scram rule for `sha_user`, to exercise `SCRAM-SHA-256-PLUS`:
//!
//! ```text
//! PGSTREAM_TEST_PORT=54329 PGSTREAM_TEST_TLS=1 cargo test --test live --all-features
//! ```

use std::sync::Arc;

use pg_stream::{
    PgConnection, PgMessage, PgProtocol,
    message::{FormatCode, TransactionStatus, oid, params},
    startup::{AuthenticationMode, ConnectionBuilder, Error, StartupResponse},
};
use tokio::net::TcpStream;

fn test_port() -> Option<u16> {
    std::env::var("PGSTREAM_TEST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
}

fn tls_enabled() -> bool {
    std::env::var("PGSTREAM_TEST_TLS").is_ok()
}

macro_rules! require_server {
    () => {
        match test_port() {
            Some(port) => port,
            None => {
                eprintln!("skipping live test: PGSTREAM_TEST_PORT not set");
                return;
            }
        }
    };
}

async fn connect(
    user: &str,
    password: Option<&str>,
    port: u16,
) -> (PgConnection<TcpStream>, StartupResponse) {
    let mut cb = ConnectionBuilder::new(user).database("postgres");
    if let Some(pw) = password {
        cb = cb.auth(AuthenticationMode::Password(pw.to_string()));
    }
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.set_nodelay(true).unwrap();
    cb.connect(stream).await.unwrap()
}

#[tokio::test]
async fn trust_auth_and_simple_query() {
    let port = require_server!();
    let (mut conn, res) = connect("postgres", None, port).await;
    assert!(res.parameters.contains_key("server_version"));

    conn.query("SELECT 1").flush_msg();
    conn.flush().await.unwrap();

    let PgMessage::RowDescription(desc) = conn.recv().await.unwrap() else {
        panic!("expected RowDescription");
    };
    assert_eq!(desc.column_count(), 1);

    let PgMessage::DataRow(row) = conn.recv().await.unwrap() else {
        panic!("expected DataRow");
    };
    assert_eq!(row.column(0), Some(b"1".as_slice()));

    let PgMessage::CommandComplete(cmd) = conn.recv().await.unwrap() else {
        panic!("expected CommandComplete");
    };
    assert_eq!(cmd.tag(), "SELECT 1");
    assert!(matches!(
        conn.recv().await.unwrap(),
        PgMessage::ReadyForQuery(_)
    ));
}

#[tokio::test]
async fn md5_auth() {
    let port = require_server!();
    let (_, res) = connect("md5_user", Some("md5"), port).await;
    assert_eq!(res.parameters.get("application_name").unwrap(), "pg_stream");
}

#[tokio::test]
async fn scram_auth() {
    let port = require_server!();
    let (_, res) = connect("sha_user", Some("sha"), port).await;
    assert_eq!(res.parameters.get("application_name").unwrap(), "pg_stream");
}

#[tokio::test]
async fn cleartext_auth() {
    let port = require_server!();
    let (_, res) = connect("pw_user", Some("pw"), port).await;
    assert_eq!(res.parameters.get("application_name").unwrap(), "pg_stream");
}

#[tokio::test]
async fn extended_protocol_roundtrip() {
    let port = require_server!();
    let (mut conn, _) = connect("postgres", None, port).await;

    conn.parse(Some("stmt1"))
        .query("SELECT $1::int + 1")
        .param_types(&[oid::INT4])
        .finish()
        .bind(Some("portal1"))
        .statement("stmt1")
        .finish(params![2i32])
        .execute(Some("portal1"), 0)
        .sync();
    conn.flush().await.unwrap();

    assert!(matches!(
        conn.recv().await.unwrap(),
        PgMessage::ParseComplete
    ));
    assert!(matches!(
        conn.recv().await.unwrap(),
        PgMessage::BindComplete
    ));

    let PgMessage::DataRow(row) = conn.recv().await.unwrap() else {
        panic!("expected DataRow");
    };
    assert_eq!(row.column(0), Some(b"3".as_slice()));

    assert!(matches!(
        conn.recv().await.unwrap(),
        PgMessage::CommandComplete(_)
    ));
    let PgMessage::ReadyForQuery(rfq) = conn.recv().await.unwrap() else {
        panic!("expected ReadyForQuery");
    };
    assert_eq!(rfq.status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn multi_row_data_iteration() {
    let port = require_server!();
    let (mut conn, _) = connect("postgres", None, port).await;

    conn.query("SELECT g, g * 2 FROM generate_series(1, 3) g")
        .flush_msg();
    conn.flush().await.unwrap();

    assert!(matches!(
        conn.recv().await.unwrap(),
        PgMessage::RowDescription(_)
    ));

    let mut rows = 0;
    loop {
        match conn.recv().await.unwrap() {
            PgMessage::DataRow(row) => {
                let cols: Vec<Option<&[u8]>> = row.iter().collect();
                assert_eq!(cols.len(), 2);
                rows += 1;
            }
            PgMessage::CommandComplete(_) => break,
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(rows, 3);
    assert!(matches!(
        conn.recv().await.unwrap(),
        PgMessage::ReadyForQuery(_)
    ));
}

#[tokio::test]
async fn error_response_fields() {
    let port = require_server!();
    let (mut conn, _) = connect("postgres", None, port).await;

    conn.query("SELECT * FROM definitely_not_a_real_table");
    conn.flush().await.unwrap();

    let PgMessage::ErrorResponse(err) = conn.recv().await.unwrap() else {
        panic!("expected ErrorResponse");
    };
    assert_eq!(err.code(), "42P01");
    assert!(matches!(
        conn.recv().await.unwrap(),
        PgMessage::ReadyForQuery(_)
    ));
}

#[tokio::test]
async fn fn_call_sqrt() {
    let port = require_server!();
    let (mut conn, _) = connect("postgres", None, port).await;

    conn.fn_call(1344)
        .result_format(FormatCode::Text)
        .finish(params!["9"]);
    conn.flush().await.unwrap();

    let PgMessage::FunctionCallResponse(resp) = conn.recv().await.unwrap() else {
        panic!("expected FunctionCallResponse");
    };
    assert_eq!(resp.value(), Some(b"3".as_slice()));
    assert!(matches!(
        conn.recv().await.unwrap(),
        PgMessage::ReadyForQuery(_)
    ));
}

/// Wrong passwords must be rejected for every challenge-based method, proving
/// the challenge is real (not a silent trust fallback).
#[tokio::test]
async fn wrong_password_is_rejected() {
    let port = require_server!();
    for user in ["md5_user", "sha_user", "pw_user"] {
        let cb = ConnectionBuilder::new(user)
            .database("postgres")
            .auth(AuthenticationMode::Password("wrong".into()));
        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        match cb.connect(stream).await {
            Err(Error::Server(e)) => assert_eq!(e.code(), "28P01", "{user}"),
            Ok(_) => panic!("{user} accepted a wrong password"),
            Err(e) => panic!("{user}: unexpected error {e}"),
        }
    }
}

#[tokio::test]
async fn scram_sha256_plus_channel_binding() {
    let port = require_server!();
    if !tls_enabled() {
        eprintln!("skipping SCRAM-SHA-256-PLUS test: PGSTREAM_TEST_TLS not set");
        return;
    }
    use tokio_rustls::TlsConnector;
    use tokio_rustls::rustls::{ClientConfig, pki_types::ServerName};

    tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let cb = ConnectionBuilder::new("sha_user")
        .database("postgres")
        .auth(AuthenticationMode::Password("sha".into()));
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.set_nodelay(true).unwrap();

    let (mut conn, _) = cb
        .connect_with_tls(stream, async move |s| {
            // Trust any cert: this test validates channel binding, not the PKI.
            let config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerify))
                .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(config));
            let name = ServerName::try_from("localhost").unwrap();
            let tls = connector.connect(name, s).await?;
            let cert = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|c| c.first())
                .map(|c| c.as_ref().to_vec());
            Ok((tls, cert))
        })
        .await
        .expect("SCRAM-SHA-256-PLUS should authenticate");

    conn.query("SELECT 1").flush_msg();
    conn.flush().await.unwrap();
    let mut got_one = false;
    loop {
        match conn.recv().await.unwrap() {
            PgMessage::DataRow(row) => got_one = row.column(0) == Some(b"1".as_slice()),
            PgMessage::ReadyForQuery(_) => break,
            PgMessage::ErrorResponse(e) => panic!("query failed: {e}"),
            _ => {}
        }
    }
    assert!(got_one);
}

/// Accepts any certificate; the SCRAM-PLUS test binds to the cert regardless of
/// PKI validity, and Postgres independently rejects a mismatched binding.
#[derive(Debug)]
struct NoVerify;

impl tokio_rustls::rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>],
        _server_name: &tokio_rustls::rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: tokio_rustls::rustls::pki_types::UnixTime,
    ) -> Result<tokio_rustls::rustls::client::danger::ServerCertVerified, tokio_rustls::rustls::Error>
    {
        Ok(tokio_rustls::rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
        _dss: &tokio_rustls::rustls::DigitallySignedStruct,
    ) -> Result<
        tokio_rustls::rustls::client::danger::HandshakeSignatureValid,
        tokio_rustls::rustls::Error,
    > {
        Ok(tokio_rustls::rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<tokio_rustls::rustls::SignatureScheme> {
        tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
