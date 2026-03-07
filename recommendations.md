# pg-stream Architecture Review & Recommendations

## Executive Summary

pg-stream has a solid foundation with correct protocol implementation, good test coverage, and reasonable performance characteristics. However, the current architecture creates friction between the byte-level and mid-level APIs, leads to unnecessary allocations, and has an awkward builder experience. This document outlines specific improvements to make pg-stream a best-in-class low-level Postgres wire protocol library.

---

## Current Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Public API                              │
├─────────────────────────────────────────────────────────────┤
│  PgStream<S>           │  ConnectionBuilder (startup)       │
│  - put_query()         │  - new(), database(), auth()       │
│  - put_parse()         │  - connect(), connect_with_tls()   │
│  - put_bind()          │                                    │
│  - put_describe()      │  PgErrorResponse (zero-copy)       │
│  - put_execute()       │  - code(), message(), detail()     │
│  - put_close()         │                                    │
│  - put_fn_call()       │  BindParameter, TargetKind,        │
│  - read_frame()        │  ResultFormat, ParameterKind       │
├─────────────────────────────────────────────────────────────┤
│                    Internal Layer                            │
├─────────────────────────────────────────────────────────────┤
│  PgStreamProto<S>      │  frontend.rs                       │
│  - put_query(&[u8])    │  - MessageCode, frame()            │
│  - put_parse(...)      │  - FormatCode, ParameterKind       │
│  - put_bind(...)       │                                    │
│  - put_describe(u8,..) │  backend.rs                        │
│  - flush()             │  - MessageCode, PgFrame            │
│                        │  - read_frame(), read_cstring()    │
└─────────────────────────────────────────────────────────────┘
```

---

## Issue Analysis

### 1. API Layer Friction

**Problem**: `PgStream` wraps `PgStreamProto` but duplicates encoding logic instead of delegating.

**Evidence** (`pg_stream.rs:97-147`):
```rust
// put_bind in PgStream encodes directly to self.proto.buf
// instead of calling self.proto.put_bind()
frontend::MessageCode::BIND.frame(&mut self.proto.buf, |b| {
    put_cstring(b, portal_name.as_ref().as_bytes());
    // ... 50 lines of encoding logic
});
```

Meanwhile, `PgStreamProto::put_bind` exists with similar but different encoding. This creates:
- Duplicated logic (format code optimization appears twice)
- Confusion about which layer to use
- Two incompatible APIs for the same operation

**Recommendation**: Unify into a single API layer. The "proto" layer should handle all encoding, the wrapper should only handle type conversion.

---

### 2. TargetKind Allocates Unnecessarily

**Problem** (`frontend.rs:193-208`):
```rust
pub enum TargetKind {
    Portal(String),      // Allocates
    Statement(String),   // Allocates
}

impl TargetKind {
    pub fn new_portal(name: impl Into<String>) -> Self {
        TargetKind::Portal(name.into())  // Forces allocation
    }
}
```

Usage pattern shows these strings are immediately converted to bytes:
```rust
// pg_stream.rs:71-78
match target {
    frontend::TargetKind::Portal(name) => {
        self.proto.put_describe(b'P', name.as_bytes());  // Just needs &[u8]
    }
    ...
}
```

**Recommendation**: Use a zero-copy design:
```rust
pub struct Portal<'a>(&'a str);
pub struct Statement<'a>(&'a str);

pub trait Target {
    fn kind_byte(&self) -> u8;
    fn name(&self) -> &[u8];
}
```

Or simpler: accept `&str` directly with separate methods:
```rust
fn put_describe_portal(&mut self, name: &str) -> &mut Self
fn put_describe_statement(&mut self, name: &str) -> &mut Self
```

---

### 3. Duplicated Format Code Optimization

**Problem**: The same optimization logic appears in both `put_bind` and `put_fn_call`:

```rust
// This exact block appears TWICE (pg_stream.rs:107-124 and 219-236)
match params {
    [first, rest @ ..]
        if rest.iter().all(|p| p.format_code() == first.format_code()) =>
    {
        if first.format_code() == frontend::FormatCode::Text {
            b.put_u16(0);
        } else {
            b.put_u16(1);
            b.put_u16(first.format_code() as u16);
        }
    }
    _ => {
        b.put_u16(params.len() as u16);
        for param in params {
            b.put_u16(param.format_code() as u16);
        }
    }
};
```

**Recommendation**: Extract to helper:
```rust
fn encode_format_codes(buf: &mut BytesMut, codes: impl Iterator<Item = FormatCode>) {
    // Single implementation
}
```

---

### 4. BindParameter Ownership Requirements

**Problem** (`frontend.rs:241-252`):
```rust
pub enum BindParameter {
    RawBinary(Bytes),  // Owns data
    Text(String),      // Owns data - forces allocation
    Bool(bool),
    Int2(i16),
    // ...
    Bytea(Bytes),      // Owns data
    Null,
}
```

The `From<&str>` impl forces allocation:
```rust
impl From<&str> for BindParameter {
    fn from(v: &str) -> Self {
        BindParameter::Text(v.to_string())  // Allocates!
    }
}
```

**Recommendation**: Support borrowed variants or use a trait-based encoding approach:

Option A - Borrowed enum (adds lifetime):
```rust
pub enum BindParameter<'a> {
    Text(&'a str),
    RawBinary(&'a [u8]),
    // primitives stay owned
}
```

Option B - Trait-based encoding (more flexible, your suggested approach):
```rust
pub trait Encode {
    fn format_code(&self) -> FormatCode;
    fn encode(&self, buf: &mut impl BufMut);
}

impl Encode for &str { ... }
impl Encode for i32 { ... }
impl<T: Encode> Encode for Option<T> { ... }
```

---

### 5. read_cstring Allocates

**Problem** (`backend.rs:179-192`):
```rust
pub(crate) fn read_cstring(bytes: &mut Bytes) -> std::io::Result<String> {
    // ...
    match String::from_utf8(bytes[..end].to_vec()) {  // to_vec() allocates
        Ok(string) => Ok(string),
        // ...
    }
}
```

This is used during startup to parse parameter names/values, causing allocations for every parameter.

**Recommendation**: Return `Cow<'_, str>` or a zero-copy `Bytes` slice:
```rust
pub(crate) fn read_cstring_bytes(bytes: &mut Bytes) -> std::io::Result<Bytes> {
    // Return the slice, let caller convert if needed
}
```

---

### 6. PgErrorResponse Clones Body

**Problem** (`error.rs:58-60`):
```rust
fn new(body: Bytes) -> Self {
    let mut resp = PgErrorResponse {
        body: body.clone(),  // Clones the Bytes (cheap but unnecessary)
        // ...
    };
```

**Recommendation**: Take ownership instead of cloning:
```rust
fn new(body: Bytes) -> Self {  // Take by value, iterate over &body
```

---

### 7. Startup/Auth Allocations & Dependencies

**Problem** (`startup/mod.rs`):
```rust
// Line 99
options: HashMap<String, String>,  // Every option allocates

// Line 222-226 (StartupResponse)
parameters: HashMap<String, String>,  // Every parameter allocates

// Line 282-288 (SASL handling)
let mut msg = BytesMut::new();  // Creates new buffer each time
```

**Problem** (`Cargo.toml:21`):
```toml
scram = { version = "0.6", optional = true }
```

The `scram` crate adds an external dependency for SCRAM-SHA-256 authentication. SCRAM is not complex enough to warrant a dependency.

**Recommendation**:
- Remove `scram` dependency entirely
- Implement SCRAM-SHA-256 ourselves using `sha2`/`hmac`/`pbkdf2` (which may already be available via TLS deps)
- Use `Cow<'static, str>` for known option keys
- Pre-allocate BytesMut with reasonable capacity

---

### 8. ResultFormat Lifetime Awkwardness

**Problem** (`frontend.rs:233-237`):
```rust
pub enum ResultFormat<'a> {
    Binary,
    Text,
    Mixed(&'a [FormatCode]),  // Requires lifetime management
}
```

**Recommendation**: Use owned data or simplify:
```rust
pub enum ResultFormat {
    Binary,
    Text,
    AllSame(FormatCode),
    // Accept slice in method signature instead
}

// Method takes slice directly for mixed case
fn put_bind_with_formats(&mut self, ..., result_formats: &[FormatCode])
```

---

### 9. Missing Builder Pattern

**Problem**: Current API for bind is positional and error-prone:
```rust
conn.put_bind(
    "",           // portal name - easy to forget what this is
    "stmt",       // statement name
    &[BindParameter::Text("42".to_string())],  // awkward to construct
    ResultFormat::Text,
)
```

**Recommendation**: Builder pattern as you suggested:
```rust
conn.bind("stmt")
    .portal("")  // or .unnamed_portal()
    .param(42i32)
    .param("text")
    .result_format(ResultFormat::Binary)
    .encode();  // Returns &mut Self for chaining
```

This also eliminates the need for `BindParameter` enum entirely - use the `Encode` trait.

---

### 10. Async Surface Area

**Current state** (`Cargo.toml:19`):
```toml
tokio = { version = "1.49", default-features = false, features = ["io-util"] }
```

Tokio is used minimally for:
- `AsyncRead`/`AsyncWrite` traits
- `AsyncReadExt`/`AsyncWriteExt` extension methods
- Only in: `pg_stream_proto.rs`, `backend.rs`, `startup/mod.rs`, `startup/auth.rs`

**Recommendation**: Feature-gate async support:
```toml
[features]
default = ["async", "startup"]
async = ["dep:tokio"]
startup = ["async", "dep:scram"]

[dependencies]
tokio = { version = "1", optional = true, default-features = false, features = ["io-util"] }
```

This requires:
1. Making `read_frame` work with `std::io::Read` (sync version)
2. Making `flush` work with `std::io::Write` (sync version)
3. Using `cfg` attributes to provide both versions

---

### 11. Missing Backend Message Parsers

**Problem**: Only `PgErrorResponse` has a zero-copy parser. Other common messages require manual parsing:

- `DataRow` - returned for every row, critical for performance
- `RowDescription` - column metadata
- `ParameterDescription` - parameter types
- `CommandComplete` - affected row count
- `NotificationResponse` - LISTEN/NOTIFY

**Recommendation**: Add zero-copy wrappers similar to `PgErrorResponse`:
```rust
pub struct DataRow {
    body: Bytes,
    column_count: u16,
    // Lazily parsed column offsets
}

impl DataRow {
    pub fn column_count(&self) -> u16;
    pub fn column(&self, idx: usize) -> Option<&[u8]>;
    pub fn is_null(&self, idx: usize) -> bool;
}
```

---

### 12. Missing COPY Protocol

**Problem**: No support for COPY IN/OUT, which is essential for bulk data transfer.

Messages needed:
- `CopyInResponse` / `CopyOutResponse` - server announces COPY mode
- `CopyData` - data chunks
- `CopyDone` - end of data
- `CopyFail` - abort COPY

**Recommendation**: Add COPY support, potentially feature-gated.

---

### 13. ParameterKind OID Verification

**Problem** (`frontend.rs:109`):
```rust
// TODO: I generated this list with AI. x-ref with PG docs
#[repr(u32)]
pub enum ParameterKind {
    Unspecified = 0,
    Bool = 16,
    // ...
}
```

**Recommendation**: Verify against official PostgreSQL source:
- `src/include/catalog/pg_type.dat`
- https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat

---

## Proposed New Architecture

The key insight: **message encoding is an extension trait on `BufMut`**. This means:
- Works on any buffer type (`BytesMut`, `Vec<u8>`, custom buffers)
- Zero intermediate allocations - writes directly to buffer
- Simple messages are single method calls
- Complex messages use builders that write on completion
- Connection is a thin I/O wrapper

```
┌─────────────────────────────────────────────────────────────┐
│              Extension Trait: FrontendMessage                │
├─────────────────────────────────────────────────────────────┤
│  impl<B: BufMut> FrontendMessage for B {                    │
│    // Simple messages - immediate write                      │
│    fn query(&mut self, stmt: &str) -> &mut Self;            │
│    fn sync(&mut self) -> &mut Self;                         │
│    fn flush_msg(&mut self) -> &mut Self;                    │
│    fn terminate(&mut self) -> &mut Self;                    │
│    fn execute(&mut self, portal: &str, max: u32) -> &mut Self│
│    fn describe_portal(&mut self, name: &str) -> &mut Self;  │
│    fn describe_statement(&mut self, name: &str) -> &mut Self│
│    fn close_portal(&mut self, name: &str) -> &mut Self;     │
│    fn close_statement(&mut self, name: &str) -> &mut Self;  │
│                                                              │
│    // Complex messages - return builders                     │
│    fn parse(&mut self, name: Option<&str>) -> ParseBuilder; │
│    fn bind(&mut self, stmt: &str) -> BindBuilder;           │
│  }                                                           │
├─────────────────────────────────────────────────────────────┤
│              Backend Parsers (zero-copy)                     │
├─────────────────────────────────────────────────────────────┤
│  DataRow, RowDescription, ErrorResponse, CommandComplete,   │
│  NotificationResponse, ParameterDescription, etc.           │
│  All parsed from PgFrame with zero-copy access              │
├─────────────────────────────────────────────────────────────┤
│              Connection Layer (I/O only)                     │
├─────────────────────────────────────────────────────────────┤
│  PgConnection<S> {                                           │
│    buf: BytesMut,  // implements FrontendMessage             │
│    stream: S,                                                │
│  }                                                           │
│  - Exposes buf for message building                          │
│  - flush() writes buf to stream                              │
│  - recv() reads PgFrame from stream                          │
└─────────────────────────────────────────────────────────────┘
```

### Extension Trait for Frontend Messages

```rust
use bytes::BufMut;

/// Extension trait for writing Postgres frontend messages to any buffer
pub trait FrontendMessage: BufMut + Sized {
    /// Simple query protocol
    fn query(&mut self, stmt: &str) -> &mut Self {
        MessageCode::QUERY.frame(self, |buf| {
            buf.put_slice(stmt.as_bytes());
            buf.put_u8(0);
        });
        self
    }

    /// Sync - end of extended query
    fn sync(&mut self) -> &mut Self {
        MessageCode::SYNC.frame(self, |_| {});
        self
    }

    /// Flush - force server to send pending results
    fn flush_msg(&mut self) -> &mut Self {
        MessageCode::FLUSH.frame(self, |_| {});
        self
    }

    /// Terminate connection
    fn terminate(&mut self) -> &mut Self {
        MessageCode::TERMINATE.frame(self, |_| {});
        self
    }

    /// Execute a bound portal
    fn execute(&mut self, portal: &str, max_rows: u32) -> &mut Self {
        MessageCode::EXECUTE.frame(self, |buf| {
            buf.put_slice(portal.as_bytes());
            buf.put_u8(0);
            buf.put_u32(max_rows);
        });
        self
    }

    /// Describe a portal
    fn describe_portal(&mut self, name: &str) -> &mut Self {
        MessageCode::DESCRIBE.frame(self, |buf| {
            buf.put_u8(b'P');
            buf.put_slice(name.as_bytes());
            buf.put_u8(0);
        });
        self
    }

    /// Describe a prepared statement
    fn describe_statement(&mut self, name: &str) -> &mut Self {
        MessageCode::DESCRIBE.frame(self, |buf| {
            buf.put_u8(b'S');
            buf.put_slice(name.as_bytes());
            buf.put_u8(0);
        });
        self
    }

    /// Close a portal
    fn close_portal(&mut self, name: &str) -> &mut Self {
        MessageCode::CLOSE.frame(self, |buf| {
            buf.put_u8(b'P');
            buf.put_slice(name.as_bytes());
            buf.put_u8(0);
        });
        self
    }

    /// Close a prepared statement
    fn close_statement(&mut self, name: &str) -> &mut Self {
        MessageCode::CLOSE.frame(self, |buf| {
            buf.put_u8(b'S');
            buf.put_slice(name.as_bytes());
            buf.put_u8(0);
        });
        self
    }

    /// Start building a Parse message
    fn parse<'a>(&'a mut self, name: Option<&'a str>) -> ParseBuilder<'a, Self> {
        ParseBuilder::new(self, name)
    }

    /// Start building a Bind message
    fn bind<'a>(&'a mut self, statement: &'a str) -> BindBuilder<'a, Self> {
        BindBuilder::new(self, statement)
    }
}

// Implement for anything that implements BufMut
impl<B: BufMut> FrontendMessage for B {}
```

### ParseBuilder

```rust
pub struct ParseBuilder<'a, B: BufMut> {
    buf: &'a mut B,
    name: Option<&'a str>,
    query: Option<&'a str>,
    param_types: &'a [Oid],
}

impl<'a, B: BufMut> ParseBuilder<'a, B> {
    fn new(buf: &'a mut B, name: Option<&'a str>) -> Self {
        Self {
            buf,
            name,
            query: None,
            param_types: &[],
        }
    }

    /// Set the query string (required)
    pub fn query(mut self, query: &'a str) -> Self {
        self.query = Some(query);
        self
    }

    /// Set parameter types (optional - server will infer if not provided)
    pub fn param_types(mut self, types: &'a [Oid]) -> Self {
        self.param_types = types;
        self
    }

    /// Finish building and write to buffer
    pub fn finish(self) -> &'a mut B {
        let query = self.query.expect("query is required");

        MessageCode::PARSE.frame(self.buf, |buf| {
            // Statement name (empty string for unnamed)
            if let Some(name) = self.name {
                buf.put_slice(name.as_bytes());
            }
            buf.put_u8(0);

            // Query string
            buf.put_slice(query.as_bytes());
            buf.put_u8(0);

            // Parameter types
            buf.put_u16(self.param_types.len() as u16);
            for oid in self.param_types {
                buf.put_u32(*oid);
            }
        });

        self.buf
    }
}
```

### BindBuilder

```rust
pub struct BindBuilder<'a, B: BufMut> {
    buf: &'a mut B,
    portal: &'a str,
    statement: &'a str,
    params: BytesMut,           // Accumulated parameter data
    param_formats: Vec<FormatCode>,
    result_format: FormatCode,
}

impl<'a, B: BufMut> BindBuilder<'a, B> {
    fn new(buf: &'a mut B, statement: &'a str) -> Self {
        Self {
            buf,
            portal: "",
            statement,
            params: BytesMut::with_capacity(64),
            param_formats: Vec::new(),
            result_format: FormatCode::Text,
        }
    }

    /// Set the portal name (default: unnamed "")
    pub fn portal(mut self, name: &'a str) -> Self {
        self.portal = name;
        self
    }

    /// Add a parameter value
    pub fn param<T: Encode>(mut self, value: T) -> Self {
        self.param_formats.push(value.format_code());
        value.encode(&mut self.params);
        self
    }

    /// Add a NULL parameter
    pub fn null(mut self) -> Self {
        self.param_formats.push(FormatCode::Binary);
        self.params.put_i32(-1);
        self
    }

    /// Set result format (default: Text)
    pub fn result_format(mut self, format: FormatCode) -> Self {
        self.result_format = format;
        self
    }

    /// Finish building and write to buffer
    pub fn finish(self) -> &'a mut B {
        MessageCode::BIND.frame(self.buf, |buf| {
            // Portal name
            buf.put_slice(self.portal.as_bytes());
            buf.put_u8(0);

            // Statement name
            buf.put_slice(self.statement.as_bytes());
            buf.put_u8(0);

            // Parameter format codes (optimized encoding)
            encode_format_codes(buf, &self.param_formats);

            // Parameter values (already encoded)
            buf.put_u16(self.param_formats.len() as u16);
            buf.put_slice(&self.params);

            // Result format codes
            if self.result_format == FormatCode::Text {
                buf.put_u16(0);  // Use default (text)
            } else {
                buf.put_u16(1);
                buf.put_u16(self.result_format as u16);
            }
        });

        self.buf
    }
}

// Helper for optimized format code encoding
fn encode_format_codes(buf: &mut impl BufMut, formats: &[FormatCode]) {
    if formats.is_empty() {
        buf.put_u16(0);
        return;
    }

    let first = formats[0];
    if formats.iter().all(|&f| f == first) {
        // All same format
        if first == FormatCode::Text {
            buf.put_u16(0);  // Default is text
        } else {
            buf.put_u16(1);
            buf.put_u16(first as u16);
        }
    } else {
        // Mixed formats
        buf.put_u16(formats.len() as u16);
        for &f in formats {
            buf.put_u16(f as u16);
        }
    }
}
```

### Encode Trait (for bind parameters)

```rust
pub trait Encode {
    fn format_code(&self) -> FormatCode;
    fn encode(&self, buf: &mut impl BufMut);
}

impl Encode for i16 {
    fn format_code(&self) -> FormatCode { FormatCode::Binary }
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(2);
        buf.put_i16(*self);
    }
}

impl Encode for i32 {
    fn format_code(&self) -> FormatCode { FormatCode::Binary }
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(4);
        buf.put_i32(*self);
    }
}

impl Encode for i64 {
    fn format_code(&self) -> FormatCode { FormatCode::Binary }
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(8);
        buf.put_i64(*self);
    }
}

impl Encode for &str {
    fn format_code(&self) -> FormatCode { FormatCode::Text }
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.len() as i32);
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for &[u8] {
    fn format_code(&self) -> FormatCode { FormatCode::Binary }
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(self.len() as i32);
        buf.put_slice(self);
    }
}

impl<T: Encode> Encode for Option<T> {
    fn format_code(&self) -> FormatCode {
        match self {
            Some(v) => v.format_code(),
            None => FormatCode::Binary,
        }
    }
    fn encode(&self, buf: &mut impl BufMut) {
        match self {
            Some(v) => v.encode(buf),
            None => buf.put_i32(-1),
        }
    }
}
```

### Connection (Thin I/O Wrapper)

```rust
/// Connection wraps a stream and provides buffered message building
pub struct PgConnection<S> {
    stream: S,
    buf: BytesMut,
}

impl<S> PgConnection<S> {
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            buf: BytesMut::with_capacity(4096),
        }
    }

    /// Access the write buffer for message building
    /// Buffer implements FrontendMessage trait
    pub fn buf(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    /// Take the buffered bytes (for manual sending)
    pub fn take_buf(&mut self) -> Bytes {
        self.buf.split().freeze()
    }
}

#[cfg(feature = "async")]
impl<S: AsyncRead + AsyncWrite + Unpin> PgConnection<S> {
    /// Flush buffered messages to the stream
    pub async fn flush(&mut self) -> io::Result<()> {
        self.stream.write_all_buf(&mut self.buf).await?;
        self.stream.flush().await
    }

    /// Read a frame from the stream
    pub async fn recv(&mut self) -> io::Result<PgFrame> {
        read_frame(&mut self.stream).await
    }
}

#[cfg(feature = "sync")]
impl<S: Read + Write> PgConnection<S> {
    pub fn flush(&mut self) -> io::Result<()> {
        self.stream.write_all(&self.buf)?;
        self.buf.clear();
        self.stream.flush()
    }

    pub fn recv(&mut self) -> io::Result<PgFrame> {
        read_frame_sync(&mut self.stream)
    }
}
```

### SCRAM-SHA-256 Implementation (No Dependencies)

Replace the `scram` crate with a minimal implementation:

```rust
// src/auth/scram.rs
use sha2::{Sha256, Digest};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;

pub struct ScramClient {
    username: String,
    password: String,
    nonce: String,
}

impl ScramClient {
    pub fn new(username: &str, password: &str) -> Self;

    /// Generate client-first message
    pub fn client_first(&self) -> String;

    /// Process server-first, return client-final
    pub fn client_final(&mut self, server_first: &str) -> Result<String, ScramError>;

    /// Verify server-final
    pub fn verify_server(&self, server_final: &str) -> Result<(), ScramError>;
}
```

Dependencies needed (all pure Rust, no C):
- `sha2` for SHA-256
- `hmac` for HMAC
- `pbkdf2` for key derivation
- `base64` for encoding (already in dev-deps)
- `rand` for nonce generation

Or use `ring`/`aws-lc-rs` if already available for TLS.

### Feature Structure

```toml
[features]
default = ["async"]
async = ["dep:tokio"]
sync = []  # Sync I/O support

# Auth is always available, no external deps for SCRAM
# TLS support uses whatever crypto the user provides

[dependencies]
bytes = "1"
tokio = { version = "1", optional = true, default-features = false, features = ["io-util"] }

# Crypto for SCRAM (pick one approach):
sha2 = "0.10"
hmac = "0.12"
pbkdf2 = { version = "0.12", default-features = false }
rand = { version = "0.8", default-features = false, features = ["std"] }
```

### SCRAM-SHA-256 Implementation Details

SCRAM (Salted Challenge Response Authentication Mechanism) is straightforward:

```
Client                                  Server
  |                                        |
  |-- client-first: n,,n=user,r=nonce ---->|
  |                                        |
  |<-- server-first: r=nonce+server,s=salt,i=iterations
  |                                        |
  |-- client-final: c=biws,r=nonce,p=proof |
  |                                        |
  |<-- server-final: v=verifier -----------|
```

Key derivation (RFC 5802):
```rust
// SaltedPassword = Hi(Normalize(password), salt, i)
let salted_password = pbkdf2_hmac::<Sha256>(password.as_bytes(), &salt, iterations);

// ClientKey = HMAC(SaltedPassword, "Client Key")
let client_key = hmac_sha256(&salted_password, b"Client Key");

// StoredKey = H(ClientKey)
let stored_key = sha256(&client_key);

// AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
// ClientSignature = HMAC(StoredKey, AuthMessage)
let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());

// ClientProof = ClientKey XOR ClientSignature
let client_proof = xor(&client_key, &client_signature);

// ServerKey = HMAC(SaltedPassword, "Server Key")
// ServerSignature = HMAC(ServerKey, AuthMessage)  <- verify this matches server-final
```

Total implementation: ~150-200 lines of Rust. No need for external SCRAM crate.

---

## Execution Plan

### Phase 1: Extension Trait (Core)
1. Create `FrontendMessage` extension trait on `BufMut`
2. Implement simple messages: `query()`, `sync()`, `flush_msg()`, `terminate()`
3. Implement `execute()`, `describe_portal()`, `describe_statement()`
4. Implement `close_portal()`, `close_statement()`
5. Delete `TargetKind` enum entirely

### Phase 2: Builders
1. Create `ParseBuilder` with `query()`, `param_types()`, `finish()`
2. Create `BindBuilder` with `portal()`, `param()`, `null()`, `result_format()`, `finish()`
3. Implement `Encode` trait for all primitive types
4. Delete `BindParameter` enum entirely
5. Delete `ResultFormat` enum (use `FormatCode` directly)

### Phase 3: Connection Layer
1. Create `PgConnection<S>` with `buf()` exposing `BytesMut`
2. Implement `flush()` for async/sync
3. Implement `recv()` for reading frames
4. Delete `PgStream` and `PgStreamProto` entirely

### Phase 4: SCRAM Implementation
1. Implement SCRAM-SHA-256 client (~150-200 lines)
2. Add MD5 auth support
3. Add cleartext password support
4. Delete the `scram` crate dependency
5. Update `ConnectionBuilder` to use new auth

### Phase 5: Backend Parsers
1. Add `DataRow` zero-copy wrapper
2. Add `RowDescription` parser
3. Add `CommandComplete` parser
4. Add `NotificationResponse` parser
5. Add `ParameterDescription` parser

### Phase 6: Feature Gating & Polish
1. Make tokio optional with `async` feature
2. Add `sync` feature for std::io support
3. Verify ParameterKind OIDs against Postgres source
4. Add COPY protocol support (optional)

---

## Files to Delete

These files/items will be removed entirely (no deprecation):

```
src/pg_stream.rs          -> replaced by src/connection.rs
src/pg_stream_proto.rs    -> logic moves to FrontendMessage trait

Types to delete:
- TargetKind              -> describe_portal()/describe_statement() methods
- BindParameter           -> Encode trait
- ResultFormat            -> use FormatCode directly
- FunctionArg             -> Encode trait

Dependencies to remove:
- scram                   -> implement ourselves
```

---

## Comparison: Current vs Proposed

### Current API
```rust
let params = [
    BindParameter::Text("hello".to_string()),  // Allocates!
    BindParameter::Int4(42),
    BindParameter::Null,
];

conn.put_parse("stmt", "SELECT $1, $2, $3", &[
    ParameterKind::Text,
    ParameterKind::Int4,
    ParameterKind::Unspecified,
]);
conn.put_bind("", "stmt", &params, ResultFormat::Binary);
conn.put_execute("", None);
conn.put_sync();
conn.flush().await?;
```

### Proposed API
```rust
use pg_stream::FrontendMessage;  // Extension trait

// Build messages directly on any BufMut
conn.buf()
    .parse(Some("stmt"))
        .query("SELECT $1, $2, $3")
        .param_types(&[Oid::TEXT, Oid::INT4])
        .finish()
    .bind("stmt")
        .param("hello")     // No allocation - uses &str directly
        .param(42i32)
        .null()
        .result_format(FormatCode::Binary)
        .finish()
    .execute("", 0)
    .sync();

conn.flush().await?;
```

### Or use standalone buffer
```rust
use pg_stream::FrontendMessage;

// Works on any BufMut - useful for testing or custom I/O
let mut buf = BytesMut::new();
buf.query("BEGIN");
buf.parse(None)
    .query("SELECT $1::int")
    .finish()
.bind("")
    .param(42i32)
    .finish()
.execute("", 0)
.sync();

// Send manually
stream.write_all(&buf).await?;
```

Key improvements:
- Extension trait works on any `BufMut`
- No allocations for string parameters
- Builder pattern for complex messages
- Connection is thin I/O layer
- Messages testable without connection
- No scram dependency
