# pg_stream

A low-level, zero-overhead Rust implementation of the Postgres wire protocol.

## Overview

`pg_stream` provides direct access to the Postgres frontend/backend protocol, giving you full control over connection management, query execution, and data transfer. Unlike higher-level database libraries, this crate focuses on protocol implementation without abstraction overhead.

## Features

- **Zero-copy protocol handling** - Direct buffer manipulation for maximum performance
- **TLS support** - Built-in SSL/TLS negotiation with custom upgrade functions
- **Extended query protocol** - Full support for prepared statements, portals, and parameter binding
- **Function calls** - Direct invocation of Postgres functions via protocol messages
- **Type-safe message construction** - Fluent API for building protocol messages
- **Format optimization** - Automatic optimization of format codes in bind and function call messages

## Quick Start

```rust
use pg_proto::{ConnectionBuilder, AuthenticationMode};

#[tokio::main]
async fn main() -> pg_stream::ConnectResult<()> {
    let stream = tokio::net::TcpStream::connect("localhost:5432").await?;
    
    let (mut conn, startup) = ConnectionBuilder::new("postgres")
        .database("mydb")
        .auth(AuthenticationMode::Password("secret".into()))
        .connect(stream)
        .await?;
    
    println!("Connected to server version: {}", 
        startup.parameters.get("server_version").unwrap());
    
    // Execute a simple query
    conn.put_query("SELECT version()")
        .flush()
        .await?;
    
    // Read response
    loop {
        let frame = conn.read_frame().await?;
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
- `AuthenticationMode::Password(String)` - Cleartext password authentication

Other authentication methods (SASL, MD5, Kerberos, etc.) are not yet implemented.

## Extended Query Protocol

The crate provides full support for the extended query protocol with prepared statements:

```rust
// Parse a prepared statement
conn.put_parse("my_stmt", "SELECT $1::int + $2::int", &[
    ParameterKind::Int4,
    ParameterKind::Int4,
])
.flush()
.await?;

// Bind parameters and execute
conn.put_bind("", "my_stmt", &[
    BindParameter::text("5"),
    BindParameter::text("10"),
], ResultFormat::Text)
.put_execute("", None)
.put_sync()
.flush()
.await?;
```

## Function Calls

Call Postgres functions directly via the protocol:

```rust
use pg_proto::messages::frontend::{FunctionArg, FormatCode};

// Call sqrt function (OID 1344)
conn.put_fn_call(
    1344,
    &[FunctionArg::text("9")],
    FormatCode::Text
)
.flush()
.await?;
```

**Note**: Function OIDs are not guaranteed to be stable across Postgres versions or installations. Look them up dynamically via system catalogs for production use.

## TLS Support

Connect with TLS using a custom upgrade function:

```rust
let stream = tokio::net::TcpStream::connect("localhost:5432").await.unwrap();
stream.set_nodelay(true).unwrap();

let (pg_stream, startup) = ConnectionBuilder::new("postgres")
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

The crate supports all major frontend protocol messages:

- **Simple Query** - `put_query()`
- **Parse** - `put_parse()` for prepared statements
- **Bind** - `put_bind()` to bind parameters
- **Describe** - `put_describe()` for statement/portal metadata
- **Execute** - `put_execute()` to run a portal
- **Close** - `put_close()` to deallocate resources
- **Flush** - `put_flush()` to send buffered messages
- **Sync** - `put_sync()` to end an extended query sequence
- **Function Call** - `put_fn_call()` to invoke functions

## Performance

This crate is designed for scenarios where you need maximum control and minimum overhead:

- Direct buffer manipulation with `bytes::BytesMut`
- No allocations in the hot path for protocol framing
- Zero-copy reads where possible
- Efficient format code optimization
- Minimum dependencies

## Safety and Limitations

- **No SQL injection protection** - You are responsible for sanitizing inputs
- **No connection pooling** - Single connection per `PgStream`
- **Limited error handling** - Protocol errors may panic (marked with TODOs)
- **Manual resource management** - You must close statements and portals
- **Incomplete auth support** - Only Trust and cleartext password

## Remaining Work
- [ ] Finish startup flow
- [ ] Publish to cargo
- [ ] Backend message parsing
- [ ] Feature flag auth / connection builder
- [ ] Organize connect.rs better