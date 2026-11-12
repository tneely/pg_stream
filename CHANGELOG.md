# Changelog

## 0.3.0

The connection and message layers were redesigned for this release. Every
public item changed; the table at the end maps the 0.2 API to the new one.

### Fixed

- **Protocol 3.2 cancel keys (PostgreSQL 18)**: `BackendKeyData` secret keys are
  variable-length (4 to 256 bytes) rather than a fixed 4, which 0.2 could not
  parse, so a 3.2 handshake failed. The key is now carried as bytes:
  `BackendKeyData::secret_key` is `&[u8]` and `BackendKey::secret_key` is
  `Box<[u8]>`.
- **NegotiateProtocolVersion during startup** is no longer a fatal
  "unexpected message", so a server that downgrades the requested minor version
  no longer breaks the handshake. The version in use is reported as
  `Session::protocol`.
- **Startup message handling**: `NoticeResponse` during startup is no longer
  treated as a fatal "unexpected message", and an `ErrorResponse` during the
  startup parameter phase now surfaces as `Error::Server` instead of a generic
  unexpected-message error.
- **Cancellation-safe writes**: a flush that is retried after a partial write no
  longer sends the bytes already written a second time.
- **Cancellation-safe reads**: a read whose future is dropped mid-message no
  longer loses the bytes it had consumed. Every future a connection returns can
  be dropped at any point.
- **Frame length underflow (DoS)**: a backend length field below 4 no longer
  underflows into a near-`usize::MAX` allocation. It is rejected as
  `ProtocolError::InvalidLength`.
- **Allocation from a length field alone**: a message's advertised size (up to
  1 GiB) was allocated as soon as its header arrived. The read buffer now grows
  only as bytes arrive, and `Connection::set_max_message_len` bounds it.
- **Authentication panic**: an Authentication message with a body shorter than 4
  bytes previously panicked on slice indexing; it now returns an error.
- **Bounded SCRAM work**: server iteration counts must be in
  `1..=MAX_SCRAM_ITERATIONS`. Any count was accepted before, and zero panicked.

### Added

- **`Response`**: reads the replies to one cycle, one request at a time.
  Server errors become `Error::Server`; replies the server skipped because of
  an earlier error become `Error::Aborted` without reading; a reply that does
  not match is reported and left unread. `finish` reads through ReadyForQuery
  and returns the first error nobody read, including one raised by the Sync
  itself.
- **Resynchronization**: a response dropped half-read, by an early return or a
  cancelled future, is skipped before anything else is read. Abandoning a
  COPY FROM STDIN that was never ended is reported as
  `ProtocolError::Desynchronized` instead of hanging.
- **Reads send, writes read**: waiting for the server writes whatever is
  queued, so a forgotten flush cannot hang, and a flush that cannot make
  progress keeps taking replies off the wire, so a pipeline of any depth
  cannot deadlock on full socket buffers. This holds for async transports; a
  blocking stream cannot read while it waits to write.
- **Buffered reads**: a single read can frame many messages. 0.2 made three
  reads per message.
- **Asynchronous messages** (notices, notifications, parameter changes) are
  set aside while a `Response` is read and collected with
  `Connection::async_messages`.
- **`Transport`**: the connection runs over a small poll-based trait. It is
  implemented for every Tokio stream (feature `tokio`) and, through `Blocking`
  and `block_on`, for `std::io` streams without a runtime.
- **`Connection::recv_frame`** returns owned frames that share the read
  buffer's allocation, for routing messages between tasks, and
  **`recv_frames`** returns every message that has arrived, up to the end of
  a cycle, as one `Frames`.
- **`params!` macro**: builds parameter lists without per-value
  `as &dyn Bindable` casts.
- **`params_with` / `try_params_with`** write parameter values directly into
  the message in caller-declared formats. `try_bind` rolls the message back
  if an encoder fails.
- **`DataRow::iter`** (and `IntoIterator for DataRow`): single-pass O(n)
  iteration over column values, replacing O(n) per-index access.
  `RowDescription::iter` yields each column as a `Field`. `as_bytes` returns
  the body of either, to be kept and parsed again later.
- **Typed messages**: `Authentication`, `NegotiateProtocolVersion` and
  function-call results replace the previous raw `Bytes` payloads.
- **Query cancellation**: `cancel` sends a CancelRequest on a side connection
  and waits for the server to close it.
- **`ProtocolVersion::V3_0`/`V3_2`** constants, plus public `new`, `major`, and
  `minor`, so 3.2 can be requested without a magic `u32`. The default request
  stays 3.0, matching libpq.
- **Complete PostgreSQL 18 authentication**:
  - `SCRAM-SHA-256-PLUS` channel binding (`tls-server-end-point`), selected
    automatically when TLS is in use and the server certificate is given to
    `Startup::server_certificate`. Includes the `y,,` downgrade-detection flag
    when the certificate is available but only the non-PLUS mechanism is
    offered.
  - `OAUTHBEARER` (RFC 7628) via `Startup::oauth_token`, new for
    PostgreSQL 18's `oauth` method.
  - GSSAPI/SSPI via `Startup::gss` and the `GssProvider` trait: `pg_stream`
    frames the token exchange while the caller supplies tokens from their own
    Kerberos/SSPI library (no new dependency).
  - `auth::tls_server_end_point` and `auth::ChannelBinding` for computing and
    selecting channel binding directly.
- **`Startup::allow_only`** restricts which authentication methods may be
  used, so that whoever answers the connection cannot ask for a weaker one than
  intended. `&[AuthMethod::ScramSha256Plus]` is libpq's
  `channel_binding=require`.
- **`Connection::set_max_message_len`**, `Connection::transaction_status`,
  `Connection::unsent`, `Message::code`, `ErrorResponse::is_fatal`.
- `examples/client.rs`: a small asynchronous client built on the crate, with a
  connection task, cloneable handles, prepared statements, typed parameters
  and rows, and notifications.
- A hermetic test suite that drives the connection against a scripted server
  and over an in-memory pipe with real wakeups, `tests/server.sh` to run the
  live suite against Postgres in Docker, and a CI workflow running fmt,
  clippy, tests, docs, a feature matrix, and the live suite (including TLS
  channel binding).

### Changed

- **Messages are borrowed.** `Message<'a>` (formerly `PgMessage`) and its views
  point into the read buffer: receiving one allocates nothing, copies nothing
  and touches no reference count. Layouts are checked once at parse time, so
  accessors and iterators cannot fail. Text accessors return `&str` rather
  than `Cow<str>`.
- **Encoding is single-pass.** Lengths are filled in after the body is
  written, so nothing is measured first. That takes a buffer whose bytes can
  be rewritten, so `Frontend` (formerly `PgProtocol`) writes to a
  `frontend::Buffer` rather than to every `BufMut`: a `Vec<u8>`, an
  `io::Cursor` over a byte slice, or a type of your own. It is implemented
  for `Connection` as well, and for `&mut [u8]`, which is shortened as it is
  written to.
- **Statement/portal names are `&str`, not `Option<&str>`**. The protocol
  defines the empty string as the unnamed statement/portal, so the API now says
  the same: `conn.execute("portal", 0)` or `conn.execute("", 0)`.
- **Parse, Bind and FunctionCall are single calls**:
  `parse(name, query, param_types)` and
  `bind(portal, statement, params, result_formats)`. Result formats can be
  given per column.
- **Errors are typed.** One `Error` covers transport, protocol, server and
  authentication failures; protocol violations are a `ProtocolError`, not an
  `io::Error` or a `String`.
- **`Startup`** (formerly `ConnectionBuilder`) borrows its configuration and
  reports a `Session`: negotiated protocol version, backend key, and the
  server's parameters in the order sent. Its setters take `&mut self` and
  return `&mut Self`, as the `Frontend` methods do, and `connect` leaves the
  settings in place, so one `Startup` can open several sessions. TLS is
  negotiated with `request_tls` before the handshake rather than through an
  upgrade callback. `client_encoding=UTF8` is requested unless overridden,
  and `application_name` is no longer set by default.
- **SCRAM** is a pair of consuming steps (`Scram`, `ScramProof`), so the
  server's signature cannot be checked before the proof exists. It sends an
  empty user name, as libpq does, and compares the server signature in
  constant time.
- **`aws-lc-rs` replaced by `sha2` + `hmac` + `getrandom`**. SCRAM needed only
  SHA-256/384/512, HMAC-SHA-256, PBKDF2 (now a small loop over HMAC, pinned to
  the RFC 7677 test vector), and 18 random bytes. The dependency tree is pure
  Rust: no C toolchain or cmake. SHA-256 uses the CPU's SHA instructions where
  it has them, on x86-64 and aarch64 alike.
- **No `unsafe`.** The crate previously read into uninitialized memory and
  implemented `BufMut` by hand.
- **Features are `tokio` and `startup`.** `async` and `sync` are gone:
  blocking I/O goes through `Blocking`, with the same API.

### Removed

- The stateless `read_message` function. Framing that keeps no state cannot
  buffer, and cannot preserve consumed bytes when its future is cancelled.
- `PgConnection::write_raw`, `take_buf`, `has_pending`, `pending_len`,
  `with_capacity` and `into_parts`. Messages are queued in `Connection::buf`,
  and `Connection::unsent` counts the bytes not yet written.
- The `ParseBuilder` typestate (`ParseBuilder`, `NeedsQuery`, `Ready`),
  `BindBuilder` and `FnCallBuilder`, subsumed by the single-call messages.
- `startup::SSL_REQUEST`, `AuthenticationMode`, and `auth::cleartext_password`.

### Performance

Against 0.2.1, measured with criterion on one machine, reading from and
writing to memory through each version's blocking I/O:

| Workload | 0.2.1 | 0.3.0 |
| --- | --- | --- |
| Encode Parse, Bind, Execute, Sync | 146 ns | 26 ns |
| Encode Bind with eight mixed parameters | 202 ns | 68 ns |
| Receive 100 one-column rows | 5.0 µs | 1.1 µs |
| Receive 1000 ten-column rows, reading every value | 210 µs | 46 µs |
| The same, as owned frames | 210 µs | 66 µs |
| The same, as owned frames in batches | 210 µs | 52 µs |

### Migrating

| 0.2 | 0.3 |
| --- | --- |
| `PgConnection` | `Connection` |
| `PgProtocol` on any `BufMut` | `Frontend` on `Connection`, `Vec<u8>`, `&mut [u8]` and any `frontend::Buffer` |
| `None` for the unnamed statement or portal | `""` |
| `conn.recv()` returning `PgMessage` | `conn.recv()` returning `Message<'_>`, or `conn.response()` |
| a `PgMessage` kept past the next `recv` | `conn.recv_frame()`, then `Frame::parse` |
| `read_message(&mut stream)` | `Connection::new(stream).recv()` |
| `conn.flush()` before every read | optional |
| `conn.recv_sync()`, `flush_sync()` | `Connection::new(Blocking(stream))` and `block_on` |
| `conn.write_raw(bytes)` | `conn.buf().extend_from_slice(bytes)` |
| `conn.has_pending()`, `pending_len()` | `conn.unsent()` |
| `conn.stream()`, `stream_mut()`, `into_parts()` | `get_ref()`, `get_mut()`, `into_inner()` |
| `buf.parse(name).query(sql).param_types(types).finish()` | `buf.parse(name, sql, types)` |
| `buf.bind(p).statement(s).result_format(f).finish(&[&a as &dyn Bindable])` | `buf.bind(p, s, params![a], f)` |
| `buf.fn_call(oid).result_format(f).finish(args)` | `buf.function_call(oid, params![..], f)` |
| `buf.flush_msg()` | `buf.flush_request()` |
| `Bindable` | `frontend::Param` |
| `FormatCode` | `Format` |
| `row.column_count()`, `row.column(i)` | `row.len()`, `row.iter()` or `row.get(i)` |
| `desc.column_name(i)`, `desc.type_oid(i)`, … | `desc.iter()` yielding `Field` |
| `cmd.tag()`, `cmd.rows_affected()` | `cmd.tag`, `cmd.rows()` |
| `ready.status()` | `Message::ReadyForQuery(status)` |
| `ConnectionBuilder::new(user).auth(AuthenticationMode::Password(pw))` | `Startup::new(user).password(pw)` |
| `let builder = ConnectionBuilder::new(user).database(db);` | `let mut startup = Startup::new(user);` then `startup.database(db);` |
| `.application_name(name)`, `.add_option(key, value)` | `.parameter(key, value)` |
| `.protocol(196608)` | `.protocol(ProtocolVersion::V3_0)` |
| `connect_with_tls(stream, upgrade)` | `request_tls(&mut stream)`, upgrade, `.connect(tls)` |
| `StartupResponse { process_id, secret_key, parameters }` | `Session { backend_key, parameters, protocol }` |
| `startup::Error`, `io::Error` | `Error` |
| `ScramClient` | `auth::Scram`, `auth::ScramProof` |
| `auth::md5_password` returning a message | `auth::md5_password` returning the hash, for `Frontend::password_message` |
