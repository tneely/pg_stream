# pg-stream Revamp Tasks

## Phase 1: Extension Trait (Core)

### 1.1 Create module structure
- [x] Create `src/message/mod.rs`
- [x] Create `src/message/frontend.rs` for `FrontendMessage` trait
- [x] Create `src/message/codec.rs` for `MessageCode` and `frame()` helper
- [x] Update `src/lib.rs` to export new modules

### 1.2 Implement FrontendMessage trait
- [x] Define `FrontendMessage` trait extending `BufMut`
- [x] Implement blanket impl `impl<B: BufMut> FrontendMessage for B {}`

### 1.3 Simple message methods
- [x] `query(&mut self, stmt: &str) -> &mut Self`
- [x] `sync(&mut self) -> &mut Self`
- [x] `flush_msg(&mut self) -> &mut Self`
- [x] `terminate(&mut self) -> &mut Self`
- [x] `execute(&mut self, portal: &str, max_rows: u32) -> &mut Self`

### 1.4 Describe/Close methods (replaces TargetKind)
- [x] `describe_portal(&mut self, name: &str) -> &mut Self`
- [x] `describe_statement(&mut self, name: &str) -> &mut Self`
- [x] `close_portal(&mut self, name: &str) -> &mut Self`
- [x] `close_statement(&mut self, name: &str) -> &mut Self`

### 1.5 Cleanup
- [x] New code does not use `TargetKind` (deletion deferred to Phase 3)

---

## Phase 2: Builders

### 2.1 Encode trait
- [x] Define `Encode` trait with `format_code()` and `encode()` (in `src/message/frontend.rs`)
- [x] Implement for `i16`
- [x] Implement for `i32`
- [x] Implement for `i64`
- [x] Implement for `f32`
- [x] Implement for `f64`
- [x] Implement for `bool`
- [x] Implement for `&str`
- [x] Implement for `String`
- [x] Implement for `&[u8]`
- [x] Implement for `Bytes`
- [x] Implement for `Option<T: Encode>`

### 2.2 ParseBuilder
- [x] Create `ParseBuilder<'a, B: BufMut>` struct
- [x] Implement `parse(&mut self, name: Option<&str>) -> ParseBuilder` on trait
- [x] `query(self, query: &str) -> Self`
- [x] `param_types(self, types: &[Oid]) -> Self`
- [x] `finish(self) -> &mut B` - writes message to buffer

### 2.3 BindBuilder
- [x] Create `BindBuilder<'a, B: BufMut>` struct
- [x] Implement `bind(&mut self, statement: &str) -> BindBuilder` on trait
- [x] `portal(self, name: &str) -> Self`
- [x] `param<T: Encode>(self, value: T) -> Self`
- [x] `null(self) -> Self`
- [x] `result_format(self, format: FormatCode) -> Self`
- [x] `finish(self) -> &mut B` - writes message to buffer
- [x] Extract `encode_format_codes()` helper function

### 2.4 Cleanup
- [x] New code does not use `BindParameter`, `ResultFormat`, `FunctionArg` (deletion deferred to Phase 3)

---

## Phase 3: Connection Layer

### 3.1 Create PgConnection
- [x] Create `src/connection.rs`
- [x] Define `PgConnection<S>` struct with `stream: S` and `buf: BytesMut`
- [x] Implement `new(stream: S) -> Self`
- [x] Implement `buf(&mut self) -> &mut BytesMut`
- [x] Implement `take_buf(&mut self) -> Bytes`

### 3.2 Async I/O (feature-gated)
- [x] Implement `flush(&mut self) -> io::Result<()>` for async
- [x] Implement `recv(&mut self) -> io::Result<PgFrame>` for async

### 3.3 Sync I/O (feature-gated)
- [x] Implement `flush(&mut self) -> io::Result<()>` for sync
- [x] Implement `recv(&mut self) -> io::Result<PgFrame>` for sync
- [x] Create sync version of `read_frame()`

### 3.4 Cleanup
- [x] Delete `src/pg_stream.rs`
- [x] Delete `src/pg_stream_proto.rs`
- [x] Update `src/lib.rs` exports
- [x] Update all tests to use new API
- [x] Delete old types (TargetKind, BindParameter, etc.)

---

## Phase 4: SCRAM Implementation

### 4.1 Create auth module
- [x] Create `src/auth/mod.rs`
- [x] Create `src/auth/scram.rs`
- [x] Create `src/auth/md5.rs`
- [x] Create `src/auth/cleartext.rs`

### 4.2 SCRAM-SHA-256
- [x] Implement `ScramClient` struct
- [x] Implement `client_first()` - generate client-first message
- [x] Implement `client_final()` - process server challenge and generate client-final with proof
- [x] Implement `verify_server()` - verify server signature
- [x] Add helper: `pbkdf2_sha256()` (PBKDF2)
- [x] Add helper: `hmac_sha256()`
- [x] Add helper: `xor_bytes()`
- [x] Add nonce generation

### 4.3 MD5 auth
- [x] Implement MD5 password hashing (`md5(md5(password + user) + salt)`)

### 4.4 Cleartext auth
- [x] Implement cleartext password message

### 4.5 Update startup
- [x] Update `ConnectionBuilder` to use new auth modules
- [x] Remove `scram` crate dependency from `Cargo.toml`
- [x] Add crypto dependencies (`sha2`, `hmac`, `pbkdf2`, `md5`, `rand`, `base64`)

### 4.6 Cleanup
- [x] Rename `src/startup/auth.rs` to `src/startup/auth_msg.rs` (avoids module collision)
- [x] Update tests (all 32 unit tests, 8 integration tests, 14 doc tests pass)

---

## Phase 5: Backend Parsers

### 5.1 Create backend message module
- [x] Extend existing `src/messages/backend.rs`

### 5.2 DataRow (zero-copy)
- [x] Create `DataRow` struct holding `Bytes`
- [x] Implement `TryFrom<PgFrame>`
- [x] `column_count(&self) -> u16`
- [x] `column(&self, idx: usize) -> Option<&[u8]>`
- [x] `is_null(&self, idx: usize) -> bool`

### 5.3 RowDescription
- [x] Create `RowDescription` struct
- [x] Implement `TryFrom<PgFrame>`
- [x] `column_count(&self) -> u16`
- [x] `column(&self, idx: usize) -> Option<ColumnDesc>`
- [x] Create `ColumnDesc` with name, oid, format, etc.

### 5.4 CommandComplete
- [x] Create `CommandComplete` struct
- [x] Implement `TryFrom<PgFrame>`
- [x] `tag(&self) -> &str` (e.g., "SELECT 5", "INSERT 0 1")
- [x] `rows_affected(&self) -> Option<u64>`

### 5.5 NotificationResponse
- [x] Create `NotificationResponse` struct
- [x] Implement `TryFrom<PgFrame>`
- [x] `process_id(&self) -> u32`
- [x] `channel(&self) -> &str`
- [x] `payload(&self) -> &str`

### 5.6 ParameterDescription
- [x] Create `ParameterDescription` struct
- [x] Implement `TryFrom<PgFrame>`
- [x] `param_count(&self) -> u16`
- [x] `param_oid(&self, idx: usize) -> Option<u32>`

---

## Phase 6: Feature Gating & Polish

### 6.1 Feature flags
- [x] Make `tokio` optional behind `async` feature
- [x] Add `sync` feature for std::io support
- [x] Ensure `startup` feature works correctly
- [x] Update `Cargo.toml` with proper feature definitions

### 6.2 Verify OIDs
- [x] Cross-reference OIDs with Postgres `pg_type.dat`
- [x] All OIDs verified correct
- [x] Common OIDs defined in `oid` module

### 6.3 Documentation
- [x] Updated lib.rs doc comments
- [x] Examples included in doc comments
- [ ] Update README if needed (deferred)

### 6.4 Testing
- [x] All tests updated for new API
- [x] Unit tests for `FrontendMessage` trait methods
- [x] Unit tests for `Encode` implementations
- [x] Unit tests for builders
- [x] Unit tests for SCRAM implementation
- [x] Unit tests for backend parsers
- [x] Full test suite passes (44 unit + 8 integration + 19 doc tests)

---

## Phase 7: COPY Protocol (Optional)

### 7.1 Frontend messages
- [ ] Add `copy_data(&mut self, data: &[u8]) -> &mut Self`
- [ ] Add `copy_done(&mut self) -> &mut Self`
- [ ] Add `copy_fail(&mut self, msg: &str) -> &mut Self`

### 7.2 Backend parsers
- [ ] Create `CopyInResponse` parser
- [ ] Create `CopyOutResponse` parser
- [ ] Create `CopyBothResponse` parser
- [ ] Create `CopyData` wrapper

---

## Progress Summary

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1: Extension Trait | Complete | `FrontendMessage` trait implemented |
| Phase 2: Builders | Complete | `ParseBuilder`, `BindBuilder`, `Encode` trait |
| Phase 3: Connection | Complete | `PgConnection`, async/sync I/O, old code deleted |
| Phase 4: SCRAM | Complete | Native SCRAM-SHA-256, MD5, cleartext auth |
| Phase 5: Backend Parsers | Complete | DataRow, RowDescription, CommandComplete, etc. |
| Phase 6: Polish | Complete | Feature flags, OID verification, testing |
| Phase 7: COPY | Not Started | Optional |
