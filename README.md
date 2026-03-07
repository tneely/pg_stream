# pg_stream

A low-level, zero-overhead Rust implementation of the Postgres wire protocol.

## Overview

`pg_stream` provides direct access to the Postgres frontend/backend protocol, giving you full control over connection management, query execution, and data transfer. Unlike higher-level database libraries, this crate focuses on protocol implementation without abstraction overhead.

## Features

- **Zero-copy protocol handling** - Direct buffer manipulation for maximum performance
- **TLS support** - Built-in SSL/TLS negotiation with custom upgrade functions
- **Extended query protocol** - Full support for prepared statements, portals, and parameter binding
- **Function calls** - Direct invocation of Postgres functions via protocol messages
- **Extension trait API** - Write messages to any buffer implementing `BufMut`
- **Format optimization** - Automatic optimization of format codes in bind and function call messages

## Quick Start

```rust
use pg_stream::startup::{ConnectionBuilder, AuthenticationMode};
use pg_stream::FrontendMessage;
use pg_stream::messages::backend;

#[tokio::main]
async fn main() -> pg_stream::startup::Result<()> {
    let stream = tokio::net::TcpStream::connect("localhost:5432").await?;

    let (mut conn, startup) = ConnectionBuilder::new("postgres")
        .database("mydb")
        .auth(AuthenticationMode::Password("secret".into()))
        .connect(stream)
        .await?;

    println!("Connected to server version: {}",
        startup.parameters.get("server_version").unwrap());

    // Execute a simple query
    conn.buf().query("SELECT version()");
    conn.flush().await?;

    // Read response
    loop {
        let frame = conn.recv().await?;
        // Handle frame...
        if matches!(frame.code, backend::MessageCode::READY_FOR_QUERY) {
            break;
        }
    }

    Ok(())
}
```

## Authentication

Supported authentication modes:

- `AuthenticationMode::Trust` - No password required
- `AuthenticationMode::Password(String)` - Cleartext or SCRAM-SHA-256 password authentication

## Extended Query Protocol

The crate provides full support for the extended query protocol with prepared statements:

```rust
use pg_stream::FrontendMessage;
use pg_stream::message::oid;

// Parse a prepared statement
conn.buf()
    .parse(Some("my_stmt"))
    .query("SELECT $1::int + $2::int")
    .param_types(&[oid::INT4, oid::INT4])
    .finish();
conn.flush().await?;

// Bind parameters and execute
conn.buf()
    .bind("my_stmt")
    .param(5i32)
    .param(10i32)
    .finish()
    .execute("", 0)
    .sync();
conn.flush().await?;
```

## Function Calls

Call Postgres functions directly via the protocol:

```rust
use pg_stream::FrontendMessage;
use pg_stream::message::FormatCode;

// Call sqrt function (OID 1344)
conn.buf()
    .fn_call(1344)
    .arg("9")
    .result_format(FormatCode::Text)
    .finish();
conn.flush().await?;
```

**Note**: Function OIDs are not guaranteed to be stable across Postgres versions or installations. Look them up dynamically via system catalogs for production use.

## TLS Support

Connect with TLS using a custom upgrade function:

```rust
let stream = tokio::net::TcpStream::connect("localhost:5432").await.unwrap();
stream.set_nodelay(true).unwrap();

let (conn, startup) = ConnectionBuilder::new("postgres")
    .connect_with_tls(stream, async |s| {
        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
        let cert_bytes = pem_to_der("/certs/ca.crt").await?;
        root_cert_store.add(cert_bytes.into()).unwrap();

        let config = tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));

        let server_name = "localhost".try_into().unwrap();
        let stream = connector.connect(server_name, s).await?;
        Ok(stream)
    })
    .await
    .unwrap();
```

## Protocol Messages

The `FrontendMessage` trait provides methods for all major frontend protocol messages:

- **Simple Query** - `query()`
- **Parse** - `parse()` builder for prepared statements
- **Bind** - `bind()` builder to bind parameters
- **Describe** - `describe_statement()`, `describe_portal()`
- **Execute** - `execute()` to run a portal
- **Close** - `close_statement()`, `close_portal()`
- **Flush** - `flush_msg()` to send buffered messages
- **Sync** - `sync()` to end an extended query sequence
- **Function Call** - `fn_call()` builder to invoke functions

## Performance

This crate is designed for scenarios where you need maximum control and minimum overhead:

- Direct buffer manipulation with `bytes::BytesMut`
- No allocations in the hot path for protocol framing
- Zero-copy reads where possible
- Efficient format code optimization
- Minimum dependencies

## Safety and Limitations

- **No SQL injection protection** - You are responsible for sanitizing inputs
- **No connection pooling** - Single connection per `PgConnection`
- **Manual resource management** - You must close statements and portals
- **Incomplete auth support** - Only Trust, SCRAM-SHA-256, and cleartext password
