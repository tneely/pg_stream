use pg_embed::pg_enums::PgAuthMethod;
use pg_embed::pg_fetch::{PG_V15, PgFetchSettings};
use pg_embed::postgres::{PgEmbed, PgSettings};
use pg_stream::{AuthenticationMode, Connect, ConnectionBuilder, backend};
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::compat::TokioAsyncReadCompatExt;

async fn run_pg() -> Result<PgEmbed, pg_embed::pg_errors::PgEmbedError> {
    let pg_settings = PgSettings {
        database_dir: PathBuf::from("data/db"),
        port: 5432,
        user: "postgres".to_string(),
        password: "password".to_string(),
        auth_method: PgAuthMethod::Plain,
        persistent: false,
        timeout: Some(Duration::from_secs(15)),
        migration_dir: None,
    };

    let fetch_settings = PgFetchSettings {
        version: PG_V15,
        ..Default::default()
    };

    let mut pg = PgEmbed::new(pg_settings, fetch_settings).await?;

    pg.setup().await?;
    pg.start_db().await?;

    Ok(pg)
}

#[tokio::test]
async fn test_pg_startup() {
    let _pg = run_pg().await.unwrap();
    let cb = ConnectionBuilder::new("postgres")
        .auth(AuthenticationMode::Password("password".to_string()));

    let stream = tokio::net::TcpStream::connect("localhost:5432")
        .await
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let mut stream = cb.connect(stream.compat()).await.unwrap();

    stream.put_sync();
    stream.flush().await.unwrap();

    loop {
        let frame = stream.read_frame().await.unwrap();
        match frame.code {
            backend::MessageCode::PARAMETER_STATUS => continue,
            backend::MessageCode::BACKEND_KEY_DATA => continue,
            backend::MessageCode::READY_FOR_QUERY => break,
            c => panic!("unexpected message code {c}"),
        }
    }
}
