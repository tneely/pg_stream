use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use pg_stream::messages::frontend;
use pg_stream::{AuthenticationMode, ConnectionBuilder, messages::backend};
use postgresql_embedded::{PostgreSQL, Settings, Status};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_util::compat::TokioAsyncReadCompatExt;

static PG_INSTANCE: OnceCell<PostgreSQL> = OnceCell::const_new();

async fn run_pg() -> Result<&'static PostgreSQL, postgresql_embedded::Error> {
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

            Ok(pg)
        })
        .await
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

#[tokio::test]
async fn test_pg_startup_tcp() {
    let pg = run_pg().await.unwrap();
    let cb = ConnectionBuilder::new("postgres")
        .auth(AuthenticationMode::Password(pg.settings().password.clone()));

    let stream = tokio::net::TcpStream::connect("localhost:5432")
        .await
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let (_, res) = cb.connect(stream.compat()).await.unwrap();

    let encoding = res.parameters.get("client_encoding").unwrap();
    assert_eq!(encoding, "UTF8");
}

#[tokio::test]
async fn test_pg_startup_tls() {
    let pg = run_pg().await.unwrap();
    let cb = ConnectionBuilder::new("postgres")
        .auth(AuthenticationMode::Password(pg.settings().password.clone()));

    let stream = tokio::net::TcpStream::connect("localhost:5432")
        .await
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let (_, res) = cb
        .connect_with_tls(stream.compat(), async |s| {
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
            let stream = connector.connect(server_name, s.into_inner()).await?;
            Ok(stream.compat())
        })
        .await
        .unwrap();

    let encoding = res.parameters.get("client_encoding").unwrap();
    assert_eq!(encoding, "UTF8");
}

#[tokio::test]
async fn test_pg_extended_protocol() {
    let pg = run_pg().await.unwrap();
    let cb = ConnectionBuilder::new("postgres")
        .auth(AuthenticationMode::Password(pg.settings().password.clone()));

    let stream = tokio::net::TcpStream::connect("localhost:5432")
        .await
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let (mut stream, _) = cb.connect(stream.compat()).await.unwrap();

    stream
        .put_parse("stmt", "SELECT $1", [frontend::ParameterKind::Int4])
        .put_describe(frontend::TargetKind::new_stmt("stmt"))
        .put_bind("", "stmt", [frontend::BindParameter::Int4(1)], [])
        .put_execute("", None)
        .put_sync();
    stream.flush().await.unwrap();

    let frame = stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::PARSE_COMPLETE);
    let frame = stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::PARAMETER_DESCRIPTION);
    let frame = stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::ROW_DESCRIPTION);
    let frame = stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::BIND_COMPLETE);
    let frame = stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::DATA_ROW);
    let frame = stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::COMMAND_COMPLETE);
    let frame = stream.read_frame().await.unwrap();
    assert_eq!(frame.code, backend::MessageCode::READY_FOR_QUERY);
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
