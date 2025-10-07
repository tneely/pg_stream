use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use pg_stream::messages::frontend;
use pg_stream::{AuthenticationMode, ConnectionBuilder, messages::backend};
use pg_stream::{PgStream, StartupResponse};
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
            tokio::fs::copy("tests/data/certs/server.crt", "data/db/server.crt").await?;
            tokio::fs::copy("tests/data/certs/server.key", "data/db/server.key").await?;
            pg.start().await?;

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

async fn connect_pg(pg: &PostgreSQL) -> (PgStream<TcpStream>, StartupResponse) {
    let Settings {
        username,
        password,
        port,
        ..
    } = pg.settings().clone();
    let cb = ConnectionBuilder::new(username).auth(AuthenticationMode::Password(password));

    let addr = format!("localhost:{port}");
    let stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();
    cb.connect(stream).await.unwrap()
}

#[tokio::test]
async fn test_pg_startup_tcp() {
    let pg = run_pg().await;
    let (_, res) = connect_pg(&pg).await;

    let encoding = res.parameters.get("client_encoding").unwrap();
    assert_eq!(encoding, "UTF8");
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
    let (mut pg_stream, _) = connect_pg(&pg).await;

    //
    // 1. Parse a named statement
    //
    pg_stream.put_parse("stmt1", "SELECT $1 + 1", &[frontend::ParameterKind::Int4]);
    pg_stream.put_flush();
    pg_stream.flush().await.unwrap();

    // Expect ParseComplete
    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::PARSE_COMPLETE);

    //
    // 2. Bind to create a named portal
    //
    let params = [frontend::BindParameter::Int4(2)];
    pg_stream.put_bind("portal1", "stmt1", &params, frontend::ResultFormat::Text);
    pg_stream.put_flush();
    pg_stream.flush().await.unwrap();

    // Expect BindComplete
    let frame = pg_stream.read_frame().await.unwrap();
    println!("{frame:?}");
    assert_eq!(frame.code, backend::MessageCode::BIND_COMPLETE);

    //
    // 3. Describe the portal
    //
    pg_stream.put_describe(frontend::TargetKind::new_portal("portal1"));
    pg_stream.put_flush();
    pg_stream.flush().await.unwrap();

    // Expect RowDescription
    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::ROW_DESCRIPTION);
    // For "SELECT $1::int + 1", we expect one column: "?column?"
    let col = b"?column?";
    assert!(frame.body.windows(col.len()).any(|w| w == col));

    //
    // 4. Execute the portal
    //
    pg_stream.put_execute("portal1", 0);
    pg_stream.put_flush();
    pg_stream.flush().await.unwrap();

    // Expect DataRow + CommandComplete
    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::DATA_ROW);
    // The value should be "3" since 2 + 1 = 3
    assert_eq!(&frame.body[6..], b"3");

    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::COMMAND_COMPLETE);
    assert_eq!(frame.body, b"SELECT 1\0".as_ref());

    //
    // 5. Close both portal and statement
    //
    pg_stream.put_close(frontend::TargetKind::new_portal("portal1"));
    pg_stream.put_close(frontend::TargetKind::new_stmt("stmt1"));
    pg_stream.put_flush();
    pg_stream.flush().await.unwrap();

    // Expect two CloseComplete
    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::CLOSE_COMPLETE);

    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::CLOSE_COMPLETE);

    //
    // 6. Sync (end of extended protocol)
    //
    pg_stream.put_sync();
    pg_stream.flush().await.unwrap();

    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
    assert_eq!(frame.body, b"I".as_ref());
}

#[tokio::test]
async fn test_put_query_select_1() {
    let pg = run_pg().await;
    let (mut pg_stream, _) = connect_pg(&pg).await;

    pg_stream.put_query("SELECT 1");
    pg_stream.put_flush();
    pg_stream.flush().await.unwrap();

    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::ROW_DESCRIPTION);
    assert!(!frame.body.is_empty());

    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::DATA_ROW);
    assert_eq!(&frame.body[6..], b"1".as_ref());

    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::COMMAND_COMPLETE);
    assert_eq!(frame.body, b"SELECT 1\0".as_ref());

    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
}

#[tokio::test]
async fn test_put_fn_call_sqrt() {
    let pg = run_pg().await;
    let (mut pg_stream, _) = connect_pg(&pg).await;

    // Call built-in function 1344 = sqrt(float8)
    // This may change depending on the version (SELECT oid FROM pg_proc WHERE proname = 'sqrt';)
    pg_stream.put_fn_call(1344, &["9".into()], frontend::FormatCode::Text);
    pg_stream.flush().await.unwrap();

    // 1. Expect FunctionCallResponse
    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::FUNCTION_CALL_RESPONSE);
    assert_eq!(&frame.body[4..], b"3".as_ref());

    // 2. Expect ReadyForQuery
    let frame = pg_stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
}
