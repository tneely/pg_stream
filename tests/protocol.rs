use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use pg_stream::{
    FrontendMessage, PgConnection, PgErrorResponse,
    message::{Bindable, FormatCode, backend, oid},
    startup::{AuthenticationMode, ConnectionBuilder, StartupResponse},
};
use postgresql_embedded::{PostgreSQL, Settings, Status};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

static PG_INSTANCE: OnceCell<PostgreSQL> = OnceCell::const_new();

async fn run_pg() -> &'static PostgreSQL {
    PG_INSTANCE
        .get_or_try_init(|| async {
            let settings = Settings {
                data_dir: PathBuf::from("data/db"),
                port: 5432,
                username: "postgres".into(),
                ..Settings::default()
            };

            let mut pg = PostgreSQL::new(settings);
            pg.setup().await?;
            tokio::fs::copy("tests/data/pg/postgresql.conf", "data/db/postgresql.conf").await?;
            tokio::fs::copy("tests/data/pg/pg_hba.conf", "data/db/pg_hba.conf").await?;
            tokio::fs::copy("tests/data/certs/server.crt", "data/db/server.crt").await?;
            tokio::fs::copy("tests/data/certs/server.key", "data/db/server.key").await?;
            pg.start().await?;

            let (mut conn, _) = connect_pg(&pg).await;
            conn.buf().query(
                "
                CREATE ROLE pw_user WITH LOGIN PASSWORD 'pw';
                CREATE ROLE md5_user WITH LOGIN PASSWORD 'md5';
                CREATE ROLE sha_user WITH LOGIN PASSWORD 'sha';
                ",
            );
            conn.flush().await?;

            loop {
                let frame = conn.recv().await?;
                if frame.code == backend::MessageCode::READY_FOR_QUERY {
                    break;
                }
            }

            Ok::<PostgreSQL, postgresql_embedded::Error>(pg)
        })
        .await
        .unwrap()
}

#[ctor::dtor]
fn drop_pg() {
    use postgresql_commands::{
        CommandBuilder,
        pg_ctl::{Mode, PgCtlBuilder, ShutdownMode},
    };
    use std::fs::{remove_dir_all, remove_file};

    // XXX: there's no guarantee that PostgreSQL will be dropped when the tests end,
    // so we need to take matters into our own hands and clean up manually.
    if let Some(pg) = PG_INSTANCE.get() {
        let settings = pg.settings();
        if pg.status() == Status::Started {
            let mut pg_ctl = PgCtlBuilder::from(settings)
                .mode(Mode::Stop)
                .pgdata(&settings.data_dir)
                .shutdown_mode(ShutdownMode::Fast)
                .wait()
                .build();

            pg_ctl.output().expect("postgres should stop");
        }

        if settings.temporary {
            remove_dir_all(&settings.data_dir).expect("dir should delete");
            remove_file(&settings.password_file).expect("pw file should delete");
        }
    }
}

async fn connect_pg(pg: &PostgreSQL) -> (PgConnection<TcpStream>, StartupResponse) {
    let Settings {
        username,
        password,
        port,
        ..
    } = pg.settings();
    connect(username, password, *port).await
}

async fn connect(
    username: &str,
    password: &str,
    port: u16,
) -> (PgConnection<TcpStream>, StartupResponse) {
    let cb = ConnectionBuilder::new(username)
        .database("postgres")
        .auth(AuthenticationMode::Password(password.to_string()));

    let addr = format!("localhost:{port}");
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    cb.connect(stream).await.unwrap()
}

#[tokio::test]
async fn test_pg_startup_options() {
    let pg = run_pg().await;

    let cb = ConnectionBuilder::new("pw_user")
        .database("postgres")
        .application_name("test")
        .add_option("client_encoding", "LATIN1")
        .auth(AuthenticationMode::Password("pw".to_string()));

    let addr = format!("localhost:{}", pg.settings().port);
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (_, res) = cb.connect(stream).await.unwrap();

    let encoding = res.parameters.get("application_name").unwrap();
    assert_eq!(encoding, "test");

    let encoding = res.parameters.get("client_encoding").unwrap();
    assert_eq!(encoding, "LATIN1");
}

#[tokio::test]
async fn test_pg_startup_md5() {
    let pg = run_pg().await;
    let (_, res) = connect("md5_user", "md5", pg.settings().port).await;

    let encoding = res.parameters.get("application_name").unwrap();
    assert_eq!(encoding, "pg_stream");
}

#[tokio::test]
async fn test_pg_startup_sha() {
    let pg = run_pg().await;
    let (_, res) = connect("sha_user", "sha", pg.settings().port).await;

    let encoding = res.parameters.get("application_name").unwrap();
    assert_eq!(encoding, "pg_stream");
}

async fn pem_to_der(pem_path: &str) -> std::io::Result<Vec<u8>> {
    let pem_data = tokio::fs::read_to_string(pem_path).await?;

    // extract the Base64 section between "-----BEGIN CERTIFICATE-----" and "-----END CERTIFICATE-----"
    let base64_cert = pem_data
        .lines()
        .filter(|line| !line.starts_with("-----"))
        .collect::<Vec<_>>()
        .join("");
    let der_bytes = BASE64_STANDARD.decode(base64_cert).unwrap();

    Ok(der_bytes)
}

#[tokio::test]
async fn test_pg_startup_tls() {
    let pg = run_pg().await;
    let cb = ConnectionBuilder::new("postgres")
        .auth(AuthenticationMode::Password(pg.settings().password.clone()));

    let stream = TcpStream::connect("localhost:5432").await.unwrap();
    stream.set_nodelay(true).unwrap();
    let (_, res) = cb
        .connect_with_tls(stream, async |s| {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .unwrap();

            let mut root_cert_store = RootCertStore::empty();
            let cert_bytes = pem_to_der("tests/data/certs/ca.crt").await?;
            root_cert_store.add(cert_bytes.into()).unwrap();

            let config = ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth();

            let connector = TlsConnector::from(Arc::new(config));

            let server_name = "localhost".try_into().unwrap();
            let stream = connector.connect(server_name, s).await?;
            Ok(stream)
        })
        .await
        .unwrap();

    let encoding = res.parameters.get("client_encoding").unwrap();
    assert_eq!(encoding, "UTF8");
}

#[tokio::test]
async fn test_pg_extended_protocol() {
    let pg = run_pg().await;
    let (mut conn, _) = connect_pg(&pg).await;

    //
    // 1. Parse a named statement
    //
    conn.buf()
        .parse(Some("stmt1"))
        .query("SELECT $1 + 1")
        .param_types(&[oid::INT4])
        .finish()
        .flush_msg();
    conn.flush().await.unwrap();

    // Expect ParseComplete
    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::PARSE_COMPLETE);

    //
    // 2. Bind to create a named portal
    //
    conn.buf()
        .bind(Some("portal1"))
        .statement("stmt1")
        .finish(&[&2i32 as &dyn Bindable])
        .flush_msg();
    conn.flush().await.unwrap();

    // Expect BindComplete
    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::BIND_COMPLETE);

    //
    // 3. Describe the portal
    //
    conn.buf().describe_portal(Some("portal1")).flush_msg();
    conn.flush().await.unwrap();

    // Expect RowDescription
    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::ROW_DESCRIPTION);
    // For "SELECT $1::int + 1", we expect one column: "?column?"
    let col = b"?column?";
    assert!(frame.body.windows(col.len()).any(|w| w == col));

    //
    // 4. Execute the portal
    //
    conn.buf().execute(Some("portal1"), 0).flush_msg();
    conn.flush().await.unwrap();

    // Expect DataRow + CommandComplete
    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::DATA_ROW);
    // The value should be "3" since 2 + 1 = 3
    assert_eq!(&frame.body[6..], b"3");

    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::COMMAND_COMPLETE);
    assert_eq!(frame.body, b"SELECT 1\0".as_ref());

    //
    // 5. Close both portal and statement
    //
    conn.buf()
        .close_portal(Some("portal1"))
        .close_statement(Some("stmt1"))
        .flush_msg();
    conn.flush().await.unwrap();

    // Expect two CloseComplete
    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::CLOSE_COMPLETE);

    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::CLOSE_COMPLETE);

    //
    // 6. Sync (end of extended protocol)
    //
    conn.buf().sync();
    conn.flush().await.unwrap();

    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
    assert_eq!(frame.body, b"I".as_ref());
}

#[tokio::test]
async fn test_put_query_select_1() {
    let pg = run_pg().await;
    let (mut conn, _) = connect_pg(&pg).await;

    conn.buf().query("SELECT 1").flush_msg();
    conn.flush().await.unwrap();

    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::ROW_DESCRIPTION);
    assert!(!frame.body.is_empty());

    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::DATA_ROW);
    assert_eq!(&frame.body[6..], b"1".as_ref());

    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::COMMAND_COMPLETE);
    assert_eq!(frame.body, b"SELECT 1\0".as_ref());

    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
}

#[tokio::test]
async fn test_put_fn_call_sqrt() {
    let pg = run_pg().await;
    let (mut conn, _) = connect_pg(&pg).await;

    // Call built-in function 1344 = sqrt(float8)
    // This may change depending on the version (SELECT oid FROM pg_proc WHERE proname = 'sqrt';)
    conn.buf()
        .fn_call(1344)
        .result_format(FormatCode::Text)
        .finish(&[&"9" as &dyn Bindable]);
    conn.flush().await.unwrap();

    // 1. Expect FunctionCallResponse
    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::FUNCTION_CALL_RESPONSE);
    assert_eq!(&frame.body[4..], b"3".as_ref());

    // 2. Expect ReadyForQuery
    let frame = conn.recv().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
}

#[tokio::test]
async fn test_parse_error_response() {
    let pg = run_pg().await;
    let (mut conn, _) = connect_pg(&pg).await;

    conn.buf().query("SELECT * FROM fake_table");
    conn.flush().await.unwrap();

    let frame = conn.recv().await.unwrap();
    let err: PgErrorResponse = frame.try_into().unwrap();

    assert_eq!(Some("ERROR".into()), err.local_severity());
    assert_eq!(Some("ERROR".into()), err.severity());
    assert_eq!(Some("42P01".into()), err.code());
    assert_eq!(
        Some("relation \"fake_table\" does not exist".into()),
        err.message()
    );
    assert_eq!(None, err.detail());
    assert_eq!(None, err.hint());
    assert_eq!(Some("15".into()), err.position());
    assert_eq!(None, err.r#where());
    assert_eq!(Some("parse_relation.c".into()), err.file());
    assert_eq!(Some("1452".into()), err.line());
    assert_eq!(Some("parserOpenTable".into()), err.routine());

    assert_eq!(
        "[ERROR] 42P01: relation \"fake_table\" does not exist",
        err.to_string()
    );
    assert_eq!(
        "PgErrorResponse { local_severity: Some(\"ERROR\"), severity: Some(\"ERROR\"), \
        code: Some(\"42P01\"), message: Some(\"relation \\\"fake_table\\\" does not exist\"), \
        detail: None, hint: None, position: Some(\"15\"), where: None, \
        file: Some(\"parse_relation.c\"), line: Some(\"1452\"), \
        routine: Some(\"parserOpenTable\"), .. }",
        format!("{err:?}")
    );
}
