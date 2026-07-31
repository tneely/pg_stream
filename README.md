# pg_stream

A low-level, high-performance Rust implementation of the PostgreSQL wire protocol.

## Overview

`pg_stream` gives you direct, allocation-conscious access to the Postgres frontend/backend protocol: connection startup, authentication, the simple and extended query protocols, COPY, and function calls. It does not impose a query builder, connection pool, or type-mapping layer, so you keep full control over the byte stream while the crate handles framing, buffering, and message encoding/decoding.

## Features

- **Buffered streaming reads** - a single read can frame many backend messages, and message bodies are sliced zero-copy from the read buffer
- **Zero-copy message wrappers** - typed accessors over `bytes::Bytes` with no per-field allocation
- **Ergonomic parameters** - the `params!` macro builds bind/function-call parameter lists without `as &dyn Bindable` casts
- **Extended query protocol** - prepared statements, portals, parameter binding, and describe
- **Complete authentication** - trust, cleartext, MD5, SCRAM-SHA-256, SCRAM-SHA-256-PLUS (channel binding), OAUTHBEARER, and GSSAPI/SSPI plumbing
- **TLS** - SSL negotiation with a caller-supplied async upgrade function
- **Query cancellation** - build and send a `CancelRequest` on a side connection
- **Sync and async** - both I/O backends behind feature flags
- **Extension-trait API** - write frontend messages to any `bytes::BufMut`

## Quick Start

```rust
use pg_stream::{PgMessage, PgProtocol, params};
use pg_stream::startup::{AuthenticationMode, ConnectionBuilder};
use pg_stream::message::oid;

#[tokio::main]
async fn main() -> pg_stream::startup::Result<()> {
    let stream = tokio::net::TcpStream::connect("localhost:5432").await?;

    let (mut conn, startup) = ConnectionBuilder::new("postgres")
        .database("mydb")
        .auth(AuthenticationMode::Password("secret".into()))
        .connect(stream)
        .await?;

    println!("server version: {}", startup.parameters["server_version"]);

    // Extended query protocol with bound parameters.
    conn.parse(Some("stmt"))
        .query("SELECT $1::int + $2::int")
        .param_types(&[oid::INT4, oid::INT4])
        .finish()
        .bind(Some("portal"))
        .statement("stmt")
        .finish(params![5i32, 10i32])
        .execute(Some("portal"), 0)
        .sync();
    conn.flush().await?;

    loop {
        match conn.recv().await? {
            PgMessage::DataRow(row) => {
                for col in &row {
                    println!("{:?}", col);
                }
            }
            PgMessage::ReadyForQuery(_) => break,
            _ => {}
        }
    }

    Ok(())
}
```

## Parameters

Use the `params!` macro to build parameter lists. It accepts any mix of
[`Bindable`] values without per-value casts, including owned values, references,
and `Option` for NULL:

```rust
use pg_stream::{PgProtocol, params};
use bytes::BytesMut;

let mut buf = BytesMut::new();
let name = String::from("Ada");
let blob: Vec<u8> = vec![0xde, 0xad];

buf.bind(None)
    .finish(params![42i32, &name, blob, Option::<i64>::None]);
```

`Bindable` is implemented for the integer and float types, `bool`, `str`,
`String`, `[u8]`, `Vec<u8>`, `bytes::Bytes`, `Option<T>`, and `&T` for any
bindable `T`. Implement it yourself for custom encodings.

## Reading responses

`conn.recv()` returns a parsed [`PgMessage`]. Reads are buffered internally, so
one syscall may satisfy several `recv` calls. Row values are exposed zero-copy:

```rust
# use pg_stream::PgMessage;
# fn handle(msg: PgMessage) {
if let PgMessage::DataRow(row) = msg {
    // Single O(n) pass over the row; `None` for SQL NULL.
    for value in &row {
        // value: Option<&[u8]>
    }
}
# }
```

Backend frames use separate default limits for control messages (1 MiB) and
bulk payload messages such as rows and COPY data (64 MiB). Fixed-size messages
are checked against their protocol size, and large bodies grow incrementally.
Use [`MessageLimits`] when an application needs tighter or larger bounds:

```rust
use pg_stream::message::MessageLimits;
use pg_stream::startup::ConnectionBuilder;

# let stream = Vec::<u8>::new();
let limits = MessageLimits::new(256 * 1024, 16 * 1024 * 1024);
let builder = ConnectionBuilder::new("postgres").message_limits(limits);
```

## Authentication

Every credential-bearing method a PostgreSQL 18 server can request over the wire
is supported:

| Server `pg_hba` method | Handled by |
| --- | --- |
| `trust`, `peer`, `ident`, `cert` | `AuthenticationMode::Trust` |
| `password`, `ldap`, `radius`, `pam`, `bsd` | `AuthenticationMode::Password` (cleartext) |
| `md5` | `AuthenticationMode::Password` (MD5) |
| `scram-sha-256` | `AuthenticationMode::Password` (SCRAM-SHA-256) |
| `scram-sha-256` over TLS | `AuthenticationMode::Password` (SCRAM-SHA-256-PLUS, channel binding) |
| `oauth` (PostgreSQL 18+) | `AuthenticationMode::OAuthBearer(token)` |
| `gss`, `sspi` | `ConnectionBuilder::connect_with_gss` |

```rust,no_run
# use pg_stream::startup::{AuthenticationMode, ConnectionBuilder};
// Password: the server chooses cleartext / MD5 / SCRAM / SCRAM-PLUS.
ConnectionBuilder::new("user").auth(AuthenticationMode::Password("secret".into()));

// OAuth 2.0 bearer token (OAUTHBEARER).
ConnectionBuilder::new("user").auth(AuthenticationMode::OAuthBearer("ya29...".into()));
```

Channel binding (`SCRAM-SHA-256-PLUS`) is selected automatically when the
connection is over TLS and the closure passed to `connect_with_tls` returns the
server certificate (see below). Over TLS without a certificate, the client sends
the SCRAM downgrade-detection flag so a stripped-`PLUS` offer is caught.

### GSSAPI / SSPI

Token generation requires a platform Kerberos/SSPI library, so `pg_stream` frames
the exchange and takes tokens from a caller-supplied [`GssProvider`]:

```rust,ignore
struct MyGss { /* wraps libgssapi or Windows SSPI */ }
impl pg_stream::startup::GssProvider for MyGss {
    type Error = std::io::Error;
    fn step(&mut self, input: &[u8]) -> Result<Vec<u8>, Self::Error> { /* ... */ }
}

let (conn, startup) =
    ConnectionBuilder::new("user").connect_with_gss(stream, &mut MyGss::new()).await?;
```

## Query cancellation

The startup response carries the backend's `process_id` and `secret_key`. To
cancel an in-progress query, send a `CancelRequest` on a *new* connection:

```rust,no_run
# use pg_stream::startup::{StartupResponse, send_cancel_request};
# async fn cancel(startup: &StartupResponse) -> std::io::Result<()> {
send_cancel_request(startup.process_id, &startup.secret_key, || async {
    tokio::net::TcpStream::connect("localhost:5432").await
})
.await
# }
```

`secret_key` is a byte string, not a `u32`: protocol 3.2 (PostgreSQL 18) made
the cancel key variable-length (4 to 256 bytes, up to 32 from PostgreSQL
itself). Store and forward it verbatim rather than assuming a width.

Cancellation keys identify the backend session, not an individual query. Keep
the primary connection quarantined while cancellation is in flight, then drain
its responses through the cancelled query's `ReadyForQuery` before sending
another query. Reusing it earlier lets a delayed cancellation request cancel
the next query on that session.

## TLS

The upgrade closure returns the TLS stream and, optionally, the server
certificate in DER form. Returning the certificate enables `SCRAM-SHA-256-PLUS`
channel binding; returning `None` still works but binds nothing.

```rust,ignore
let (conn, startup) = ConnectionBuilder::new("postgres")
    .connect_with_tls(stream, async |s| {
        let tls = connector.connect(server_name, s).await?; // e.g. tokio-rustls
        let cert = tls.get_ref().1.peer_certificates()
            .and_then(|c| c.first())
            .map(|c| c.as_ref().to_vec());
        Ok((tls, cert))
    })
    .await?;
```

## Feature flags

- `async` (default) - Tokio-based async I/O
- `sync` - blocking `std::io` I/O
- `startup` (default) - connection builder and authentication (implies `async`)

## Performance

- One buffered read frames many messages; bodies are zero-copy slices
- Frontend messages are size-computed and written in a single pass, no scratch buffers
- Bind and function-call format codes are collapsed to the compact protocol form
- Minimal dependencies (`bytes`, `tokio` io-util, and crypto only under `startup`)

## Testing

Unit and doc tests are hermetic (`cargo test`). The integration suite in
`tests/live.rs` runs only when `PGSTREAM_TEST_PORT` points at a Postgres server
with roles `md5_user` / `sha_user` / `pw_user` (passwords `md5` / `sha` / `pw`)
and a trusted `postgres` superuser:

```sh
PGSTREAM_TEST_PORT=5432 cargo test --test live --all-features
```

## Safety and limitations

- **No SQL injection protection** - sanitize your own inputs
- **No connection pooling** - one connection per `PgConnection`
- **Manual resource management** - close statements and portals yourself

[`Bindable`]: https://docs.rs/pg_stream/latest/pg_stream/message/trait.Bindable.html
[`PgMessage`]: https://docs.rs/pg_stream/latest/pg_stream/message/enum.PgMessage.html
[`GssProvider`]: https://docs.rs/pg_stream/latest/pg_stream/startup/trait.GssProvider.html
[`MessageLimits`]: https://docs.rs/pg_stream/latest/pg_stream/message/struct.MessageLimits.html
