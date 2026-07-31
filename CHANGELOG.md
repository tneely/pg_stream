# Changelog

## 0.3.0

### Fixed

- **Protocol 3.2 cancel keys (PostgreSQL 18)**: `BackendKeyData` secret keys are
  variable-length (4 to 256 bytes) rather than a fixed 4. The parser and the
  pre-body frame-length check previously rejected any `BackendKeyData` other than
  12 bytes, so a 3.2 handshake failed with `InvalidData`. `BackendKeyData::secret_key`,
  `StartupResponse::secret_key`, and `cancel_request`/`send_cancel_request` now
  carry the key as bytes. **Breaking**: `secret_key` is `&[u8]`/`Vec<u8>`, not
  `u32`, and `cancel_request` returns `Vec<u8>`, not `[u8; 16]`.
- **NegotiateProtocolVersion during startup** is no longer a fatal
  "unexpected message", so a server that downgrades the requested minor version
  no longer breaks the handshake.
- **Cancellation/error-safe writes**: async and sync connection flushes now
  remove bytes as they are written, so retrying a partial write cannot duplicate
  protocol data. Raw writes share the same ordered queue.
- **Cancellation/error-safe reads**: partial frame progress is owned by
  `MessageReader`. Sync read errors no longer expose zero-filled spare capacity
  as wire data, and initialized spare memory is reused across short reads.
- **Bounded SCRAM work**: server iteration counts must be in
  `1..=MAX_SCRAM_ITERATIONS`; PBKDF2 during async startup runs on Tokio's
  blocking pool.
- **Authentication notice streams** are handled by an iterative loop rather
  than recursively boxed futures.
- **Frame length underflow (DoS)**: a backend length field below 4 no longer
  underflows into a near-`usize::MAX` allocation. Frames with `len < 4` are now
  rejected as `InvalidData`.
- **Authentication panic**: an Authentication message with a body shorter than 4
  bytes previously panicked on slice indexing; it now returns an error.
- **Startup message handling**: `NoticeResponse` during startup is no longer
  treated as a fatal "unexpected message", and an `ErrorResponse` during the
  startup parameter phase now surfaces as `Error::Server` instead of a generic
  unexpected-message error.

### Added

- **`MessageReader`**: a buffered framing reader. `PgConnection::recv` now uses
  it, so a single read can frame many messages and message bodies are sliced
  zero-copy from the read buffer.
- **`MessageLimits`**: configurable small/large backend frame limits, defaulting
  to 1 MiB and 64 MiB. Over-limit frames are rejected from the header alone,
  before their advertised size is reserved, and fixed-size messages are rejected
  before further reads when their header is malformed. Limits can be set on
  `MessageReader`, `PgConnection`, or `ConnectionBuilder`.
- **`params!` macro**: builds `&[&dyn Bindable]` parameter lists without
  per-value `as &dyn Bindable` casts.
- **`DataRow::iter`** (and `IntoIterator for &DataRow`): single-pass O(n)
  iteration over column values, replacing O(n) per-index access.
- **Typed message wrappers**: `Authentication`, `FunctionCallResponse`, and
  `NegotiateProtocolVersion` replace the previous raw `Bytes` payloads.
- **Query cancellation**: `startup::cancel_request` builds a CancelRequest
  packet and `startup::send_cancel_request` sends it on a side connection.
- **`ProtocolVersion::V3_0`/`V3_2`** constants, plus public `new`, `major`, and
  `minor`, so 3.2 can be requested without a magic `u32`. The default request
  stays 3.0, matching libpq.
- **`BackendKeyData::MIN_SECRET_KEY_LEN`/`MAX_SECRET_KEY_LEN`** (4 and 256).
- **`Bindable`** now covers `Vec<u8>` and a blanket `&T where T: Bindable`.
- **Complete PostgreSQL 18 authentication**:
  - `SCRAM-SHA-256-PLUS` channel binding (`tls-server-end-point`), selected
    automatically when TLS is in use and the server certificate is available.
    Includes the `y,,` downgrade-detection flag when TLS is present but only the
    non-PLUS mechanism is offered.
  - `OAUTHBEARER` (RFC 7628) via `AuthenticationMode::OAuthBearer(token)`, new
    for PostgreSQL 18's `oauth` method.
  - GSSAPI/SSPI via `ConnectionBuilder::connect_with_gss` and the `GssProvider`
    trait: `pg_stream` frames the token exchange while the caller supplies
    tokens from their own Kerberos/SSPI library (no new dependency).
  - `auth::tls_server_end_point` and `auth::ChannelBinding` for computing and
    selecting channel binding directly.
- CI workflow running fmt, clippy, unit/doc tests, a feature-flag build matrix,
  and live integration tests (including TLS channel binding) against a real
  Postgres.

### Changed

- **`PgConnection::into_parts`** now returns `(stream, write_buf, read_buf)`
  (was `(stream, buf)`) so buffered incoming bytes are not lost.
- **`PgConnection::stream_mut`** now returns `io::Result<&mut S>` and denies
  access while incoming or outgoing protocol bytes are buffered.
- **`PgConnection::write_raw` / `write_raw_sync`** now preserve ordering by
  joining and flushing the connection's normal outgoing queue.
- **`ConnectionBuilder::connect_with_tls`**: the upgrade closure now returns
  `(stream, Option<Vec<u8>>)` (the second element is the server certificate in
  DER form) instead of just `stream`, enabling channel binding. Return
  `(stream, None)` to keep the previous behavior.
- **`AuthenticationMode`** gained an `OAuthBearer(String)` variant.
- **`PgMessage::Authentication` / `FunctionCallResponse` /
  `NegotiateProtocolVersion`** now carry typed wrappers instead of `Bytes`.
- Query-cancellation docs now require quarantining and draining the primary
  connection through `ReadyForQuery` before reuse.

### Removed

- The stateless `read_message` / `read_message_sync` functions. Use a persistent
  `MessageReader`; stateless async framing cannot preserve consumed bytes when
  its future is cancelled.
- The redundant explicit `Bindable for &str` / `Bindable for &[u8]` impls are
  subsumed by the new blanket `Bindable for &T`.
