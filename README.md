# pg_stream

A low-level, high-performance Rust implementation of the PostgreSQL wire protocol.

## Overview

`pg_stream` gives you direct, allocation-conscious access to the Postgres frontend/backend protocol: connection startup, authentication, the simple and extended query protocols, COPY, and function calls. It does not impose a query builder, connection pool, or type-mapping layer, so you keep full control over the byte stream while the crate handles framing, buffering, and message encoding/decoding.

## Features

- **Zero-copy reads** - backend messages are parsed in place and borrow from the read buffer, and a single read can frame many of them
- **Single-pass writes** - frontend messages are encoded straight into the send buffer, a `Vec<u8>`, a byte slice or a buffer of your own, with no measuring pass and no scratch buffers
- **Pipelining** - queue any number of requests before reading; reads send what is queued and writes keep reading, so a deep pipeline cannot deadlock on an async transport
- **Typed replies** - `Response` reads one request's replies at a time, turns server errors into `Err`, and accounts for the replies an error makes the server skip
- **Resynchronization** - a response dropped half-read, by an early return or a cancelled future, is skipped before the next read
- **Owned frames** - `recv_frame` and `recv_frames` return messages that share the read buffer's allocation, one at a time or as many as have arrived, for routing between tasks
- **Ergonomic parameters** - the `params!` macro for the common types, and `params_with` to write values in place in any encoding
- **Extended query protocol** - prepared statements, portals, parameter binding, and describe
- **Complete authentication** - trust, cleartext, MD5, SCRAM-SHA-256, SCRAM-SHA-256-PLUS (channel binding), OAUTHBEARER, and GSSAPI/SSPI plumbing
- **TLS** - SSL negotiation, with the handshake left to the TLS library of your choice
- **Query cancellation** - send a `CancelRequest` on a side connection
- **Sync and async** - tokio streams out of the box, blocking `std::io` streams without a runtime, and a small `Transport` trait for anything else

## Quick Start

```rust,no_run
use pg_stream::{Format, Frontend, params, startup::Startup};

#[tokio::main]
async fn main() -> pg_stream::Result<()> {
    let stream = tokio::net::TcpStream::connect("localhost:5432").await?;
    let mut startup = Startup::new("postgres");
    startup.database("mydb").password("secret");
    let (mut conn, session) = startup.connect(stream).await?;
    println!("server version: {:?}", session.parameter("server_version"));

    // Extended query protocol with bound parameters.
    // Empty names select the unnamed statement/portal.
    conn.parse("stmt", "SELECT $1::int + $2::int", &[])
        .bind("portal", "stmt", params![5i32, 10i32], Format::Text)
        .execute("portal", 0)
        .sync();

    // Read each reply, in the order its message was sent.
    let mut response = conn.response();
    response.parse_complete().await?;
    response.bind_complete().await?;
    while let Some(row) = response.row().await? {
        for col in row {
            // `Option<&[u8]>`, borrowed from the read buffer; `None` for SQL NULL.
            println!("{:?}", col);
        }
    }
    response.finish().await?;
    Ok(())
}
```

Nothing above flushes: reading sends what is queued.

## Pipelining

A pipeline is just more messages queued before the first read. Each Sync, simple Query or FunctionCall ends a cycle, and each cycle is read with its own `Response`:

```rust,no_run
# async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
use pg_stream::{Error, Format, Frontend, params};

conn.parse("add", "INSERT INTO log (line) VALUES ($1)", &[]).sync();
for line in ["one", "two", "three"] {
    conn.bind("", "add", params![line], Format::Text).execute("", 0).sync();
}

conn.response().finish().await?;
for _ in 0..3 {
    match conn.response().finish().await {
        Ok(_) => {}
        // One insert failing does not disturb the cycles after it.
        Err(Error::Server(error)) => eprintln!("{error}"),
        Err(error) => return Err(error),
    }
}
# Ok(())
# }
```

`finish` reads through the cycle's ReadyForQuery and returns the first error nobody read.

Reads send whatever is queued and writes keep reading, so neither side can stall the other with a full socket buffer, however deep the pipeline. A blocking stream cannot do both at once; see [Sync and async](#sync-and-async).

## Parameters

Use the `params!` macro to build parameter lists. It accepts any mix of `Param` values without per-value casts, including owned values, references, and `Option` for NULL:

```rust
use pg_stream::{Format, Frontend, params};

let mut buf = Vec::new();
let name = String::from("Ada");
let blob: Vec<u8> = vec![0xde, 0xad];

buf.bind("", "", params![42i32, &name, blob, Option::<i64>::None], Format::Text);
```

`Param` is implemented for the integer and float types, `bool`, `str`, `String`, `[u8]`, `[u8; N]`, `Vec<u8>`, `bytes::Bytes`, `Option<T>`, and `&T` for any `T` that has it. Implement it yourself for custom encodings.

To skip the trait and write values in place, use `params_with`. You declare the formats, and each value's length is filled in after it is written, so nothing is measured or buffered first:

```rust
use pg_stream::{Format, Frontend, frontend::params_with};

let mut buf = Vec::new();
let params = params_with(Format::Binary, |params| {
    params.write(|value| value.extend_from_slice(&7i64.to_be_bytes()));
    params.null();
    params.bytes(b"already encoded");
});
buf.bind("", "stmt", params, Format::Binary);
```

`try_params_with` and `Frontend::try_bind` do the same for encoders that can fail, and leave the buffer untouched if one does.

## Buffers

`Frontend` writes to more than a connection. It is implemented for anything that is a `frontend::Buffer`: a `Vec<u8>`, a `std::io::Cursor` over a byte slice, or a type of your own with four methods. A plain `&mut [u8]` works too, and is shortened to what is left of it, as it is by `std::io::Write`:

```rust
use pg_stream::Frontend;

let mut storage = [0; 64];
let mut buf = &mut storage[..];
buf.query("SELECT 1").sync();
let written = 64 - buf.len();

assert_eq!(&storage[..written], b"Q\0\0\0\x0dSELECT 1\0S\0\0\0\x04");
```

A buffer that cannot grow panics when a message does not fit. Parameter values are encoded into a `Vec`, so for any other buffer they pass through a temporary one.

## Reading responses

There are four ways to read, from most help to least:

| | Returns | Use it when |
| --- | --- | --- |
| `conn.response()` | replies, one request at a time | requests and replies are handled in the same place |
| `conn.recv()` | every `Message`, borrowed | you want the stream exactly as the server sent it |
| `conn.recv_frame()` | every message, owned | messages are routed to other tasks |
| `conn.recv_frames()` | the messages that have arrived, owned as one piece | whole replies are routed to other tasks |

Reads are buffered internally, so one syscall may satisfy many calls, and messages borrow from that buffer:

```rust
# use pg_stream::Message;
# fn handle(msg: Message<'_>) {
if let Message::DataRow(row) = msg {
    // Single O(n) pass over the row; `None` for SQL NULL.
    for value in row {
        // value: Option<&[u8]>
    }
}
# }
```

A `Response` covers one cycle and applies the protocol's rules as it reads:

- An error from the server becomes `Error::Server`. The server then discards every message up to the Sync, so asking for the replies that will never come returns `Error::Aborted` without touching the wire.
- Notices, notifications and parameter changes can arrive between any two replies. They are set aside for `conn.async_messages()`.
- A reply that does not match is reported as `ProtocolError::Unexpected` and left unread.
- A response dropped half-read, by an early `?` or a cancelled future, needs no cleaning up: the next read skips what is left of the cycle first.

```rust,no_run
# async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
# use pg_stream::{Error, Format, Frontend};
conn.parse("", "SELECT * FROM no_such_table", &[])
    .bind("", "", (), Format::Text)
    .execute("", 0)
    .sync();

let mut response = conn.response();
assert!(matches!(response.parse_complete().await, Err(Error::Server(_))));
assert!(matches!(response.bind_complete().await, Err(Error::Aborted)));
response.finish().await?; // the error was already reported, so this is Ok
# Ok(())
# }
```

`Response::next` iterates a cycle whose shape is not known in advance, such as a simple Query holding several statements:

```rust,no_run
# async fn example<S: pg_stream::Transport>(mut conn: pg_stream::Connection<S>) -> pg_stream::Result<()> {
use pg_stream::{Frontend, Message};

conn.query("BEGIN; SELECT 1; SELECT 2, 3; COMMIT");
let mut response = conn.response();
while let Some(message) = response.next().await? {
    match message {
        Message::RowDescription(columns) => println!("{} columns", columns.len()),
        Message::DataRow(row) => println!("{row:?}"),
        Message::CommandComplete(done) => println!("{}", done.tag),
        Message::ErrorResponse(error) => eprintln!("{error}"),
        _ => {}
    }
}
# Ok(())
# }
```

Backend messages are limited to the protocol's own maximum of 1 GiB by default, and the buffer only grows as bytes actually arrive. Use `Connection::set_max_message_len` when an application needs a tighter bound.

### Sharing a connection between tasks

To share one connection among many callers, own it on one task and route owned frames to whoever is waiting. Frame bodies share the read buffer's allocation, so routing copies nothing, and callers encode their requests into a `Vec<u8>` of their own:

```rust,no_run
use std::collections::VecDeque;

use pg_stream::{Connection, Frontend, backend::{Frame, code}};
use tokio::{net::TcpStream, sync::mpsc};

struct Request {
    /// Whole messages, ending in a Sync.
    encoded: Vec<u8>,
    replies: mpsc::UnboundedSender<Frame>,
}

async fn run(
    mut conn: Connection<TcpStream>,
    mut requests: mpsc::Receiver<Request>,
) -> pg_stream::Result<()> {
    let mut waiting = VecDeque::new();
    loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else { return Ok(()) };
                conn.buf().extend_from_slice(&request.encoded);
                waiting.push_back(request.replies);
            }
            // Waiting for a frame also sends what was just queued, and can be
            // dropped by `select!` at any point without losing bytes.
            frame = conn.recv_frame() => {
                let frame = frame?;
                if code::is_async(frame.code) {
                    continue; // a notice or notification: nobody asked for it
                }
                let last = frame.code == code::READY_FOR_QUERY;
                if let Some(replies) = waiting.front() {
                    let _ = replies.send(frame);
                }
                if last {
                    waiting.pop_front();
                }
            }
        }
    }
}
```

`recv_frames` hands over every message that has arrived, up to the end of a cycle, as one `Frames`. A reply of many rows then takes one send rather than one for each row, and `Frames::messages` parses them where they are read.

`examples/client.rs` builds this out into a small asynchronous client: cloneable handles, prepared statements, typed parameters and rows, and notifications.

```sh
PGHOST=localhost PGPORT=5432 PGUSER=postgres cargo run --example client
```

## Authentication

Every credential-bearing method a PostgreSQL 18 server can request over the wire is supported:

| Server `pg_hba` method | Handled by |
| --- | --- |
| `trust`, `peer`, `ident`, `cert` | `Startup::new(user)` |
| `password`, `ldap`, `radius`, `pam`, `bsd` | `.password(..)` (cleartext) |
| `md5` | `.password(..)` (MD5) |
| `scram-sha-256` | `.password(..)` (SCRAM-SHA-256) |
| `scram-sha-256` over TLS | `.password(..)` and `.server_certificate(..)` (SCRAM-SHA-256-PLUS, channel binding) |
| `oauth` (PostgreSQL 18+) | `.oauth_token(..)` |
| `gss`, `sspi` | `.gss(..)` |

```rust,no_run
# use pg_stream::startup::Startup;
// Password: the server chooses cleartext / MD5 / SCRAM / SCRAM-PLUS.
Startup::new("user").password("secret");

// OAuth 2.0 bearer token (OAUTHBEARER).
Startup::new("user").oauth_token("ya29...");
```

Channel binding (`SCRAM-SHA-256-PLUS`) is selected automatically when the connection is over TLS and the server certificate is given to `server_certificate` (see below). With a certificate in hand but only the non-`PLUS` mechanism offered, the client sends the SCRAM downgrade-detection flag (`y`) so a binding-capable server rejects a stripped-`PLUS` offer. Without the certificate the client is not binding-capable and flags `n`.

Because the server chooses the method, whoever answers the connection can ask for the password in cleartext and be sent it, as with libpq's defaults. `allow_only` lists the methods you accept, and a request for any other fails before a credential leaves the client. `&[AuthMethod::ScramSha256Plus]` is libpq's `channel_binding=require`.

### GSSAPI / SSPI

Token generation requires a platform Kerberos/SSPI library, so `pg_stream` frames the exchange and takes tokens from a caller-supplied `GssProvider`:

```rust,ignore
struct MyGss { /* wraps libgssapi or Windows SSPI */ }
impl pg_stream::auth::GssProvider for MyGss {
    fn step(&mut self, token: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> { /* ... */ }
}

let (conn, session) = Startup::new("user").gss(MyGss::new()).connect(stream).await?;
```

## Query cancellation

The session carries the backend's `process_id` and `secret_key` as `Session::backend_key`. To cancel an in-progress query, send a `CancelRequest` on a *new* connection:

```rust,no_run
# async fn example(session: pg_stream::startup::Session) -> pg_stream::Result<()> {
let stream = tokio::net::TcpStream::connect("localhost:5432").await?;
if let Some(key) = &session.backend_key {
    pg_stream::cancel(stream, key).await?;
}
# Ok(())
# }
```

`secret_key` is a byte string, not a `u32`: protocol 3.2 (PostgreSQL 18) made the cancel key variable-length (4 to 256 bytes, up to 32 from PostgreSQL itself). Store and forward it verbatim rather than assuming a width.

Cancellation keys identify the backend session, not an individual query. Keep the primary connection quarantined while cancellation is in flight, then drain its responses through the cancelled query's `ReadyForQuery` before sending another query. Reusing it earlier lets a delayed cancellation request cancel the next query on that session.

## TLS

TLS is negotiated before the session starts: `request_tls` asks the server, and the handshake is left to the TLS library of your choice. Passing the server certificate in DER form to `server_certificate` enables `SCRAM-SHA-256-PLUS` channel binding; leaving it out still works but binds nothing.

```rust,no_run
# async fn example(
#     connector: tokio_rustls::TlsConnector,
#     server_name: tokio_rustls::rustls::pki_types::ServerName<'static>,
# ) -> pg_stream::Result<()> {
use pg_stream::{request_tls, startup::Startup};

let mut tcp = tokio::net::TcpStream::connect("localhost:5432").await?;
if !request_tls(&mut tcp).await? {
    return Err(std::io::Error::other("server does not support TLS").into());
}
let tls = connector.connect(server_name, tcp).await?; // e.g. tokio-rustls
let certs = tls.get_ref().1.peer_certificates().unwrap_or_default();
let cert = certs.first().map(|c| c.as_ref().to_vec()).unwrap_or_default();

let mut startup = Startup::new("postgres");
startup.password("secret").server_certificate(&cert);
let (conn, session) = startup.connect(tls).await?;
# Ok(())
# }
```

For direct TLS (PostgreSQL 17, `sslnegotiation=direct`), skip `request_tls` and start the handshake at once with the ALPN protocol `postgresql`.

## Sync and async

A `Connection` runs over anything that implements `Transport`: three poll functions for reading, writing and flushing.

- With the `tokio` feature, every `AsyncRead + AsyncWrite + Unpin` stream is a transport, including TLS streams.
- `Blocking` adapts a `std::io` stream. Its futures never wait, so `block_on` runs them without a runtime:

  ```rust,no_run
  use pg_stream::{Blocking, Frontend, block_on, startup::Startup};

  let stream = std::net::TcpStream::connect("localhost:5432")?;
  let (mut conn, _) = block_on(Startup::new("postgres").connect(Blocking(stream)))?;
  conn.query("SELECT 1");
  block_on(conn.response().finish())?;
  # Ok::<_, pg_stream::Error>(())
  ```

  A blocking stream does one thing at a time, so it reads nothing while a write waits. A pipeline whose requests and replies both outgrow the socket buffers then stalls for good, as it does in libpq's blocking mode. Read the replies to one batch before queueing the next, or use an async transport.

- Other runtimes implement the trait for their own streams.

## Feature flags

- `tokio` (default) - `Transport` for Tokio streams
- `startup` (default) - the startup handshake and authentication

## Performance

- One buffered read frames many messages; bodies are borrowed from the read buffer, not copied
- Frontend messages are written in a single pass, with lengths filled in afterwards and no scratch buffers
- Bind and function-call format codes are collapsed to the compact protocol form
- No allocation per message in either direction
- Pure-Rust dependency tree: `bytes`, `tokio` (optional), and small crypto crates (`sha2`, `hmac`, `md5`, `base64`, `getrandom`) only under `startup`. No C toolchain required; cold builds take seconds.

Against 0.2.1, measured with criterion on one machine, reading from and writing to memory through each version's blocking I/O:

| Workload | 0.2.1 | 0.3.0 |
| --- | --- | --- |
| Encode Parse, Bind, Execute, Sync | 146 ns | 26 ns |
| Encode Bind with eight mixed parameters | 202 ns | 68 ns |
| Receive 100 one-column rows | 5.0 µs | 1.1 µs |
| Receive 1000 ten-column rows, reading every value | 210 µs | 46 µs |
| The same, as owned frames | 210 µs | 66 µs |
| The same, as owned frames in batches | 210 µs | 52 µs |

`cargo bench` runs the current set.

## Testing

Unit, integration and doc tests are hermetic (`cargo test`). The connection is driven against a scripted in-memory server that controls every byte, and over an in-memory pipe of limited size, where a future only runs again when it is woken.

The suite in `tests/live.rs` runs only when `PGSTREAM_TEST_PORT` points at a Postgres server with roles `md5_user` / `sha_user` / `pw_user` (passwords `md5` / `sha` / `pw`) and a trusted `postgres` superuser. `tests/server.sh` starts one in Docker, with TLS:

```sh
tests/server.sh start
PGSTREAM_TEST_PORT=54329 PGSTREAM_TEST_TLS=1 cargo test --test live --all-features
tests/server.sh stop
```

## Safety and limitations

- **No SQL injection protection** - sanitize your own inputs
- **No connection pooling** - one connection per `Connection`
- **Manual resource management** - close statements and portals yourself
- **Values are bytes** - column and parameter values are not interpreted; names, tags and error text are returned as `&str` and expect `client_encoding` to be `UTF8`, which `Startup` requests
- **No SASLprep** - a SCRAM password with non-ASCII characters must be given in the form it was set in
- **No built-in encryption** - bring your own TLS stream; GSSAPI encryption is not supported
