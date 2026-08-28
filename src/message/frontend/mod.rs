//! Frontend message types and encoding.
//!
//! This module provides types and traits for encoding PostgreSQL frontend
//! (client-to-server) messages.

mod bindable;
mod builders;
mod code;
mod codec;
mod types;

pub use bindable::Bindable;
pub use builders::{BindBuilder, FnCallBuilder};
pub use code::MessageCode;
// Low-level framing helpers, also used by the startup/auth handshake.
pub use codec::{cstring_len, frame};
pub use types::{FormatCode, Oid, oid};

use bytes::BufMut;

use codec::{pg_frame, put_cstring};

/// Extension trait for writing Postgres frontend protocol messages.
///
/// This trait extends any `BufMut` implementation with methods to write
/// Postgres wire protocol messages directly to the buffer.
///
/// # Example
///
/// ```
/// use bytes::BytesMut;
/// use pg_stream::message::PgProtocol;
///
/// let mut buf = BytesMut::new();
/// buf.query("SELECT 1")
///    .sync();
/// ```
pub trait PgProtocol: BufMut + Sized {
    /// Write a simple Query message.
    fn query(&mut self, stmt: &str) -> &mut Self {
        pg_frame!(self, MessageCode::QUERY, cstring(stmt));
        self
    }

    /// Write a Sync message.
    fn sync(&mut self) -> &mut Self {
        pg_frame!(self, MessageCode::SYNC);
        self
    }

    /// Write a Flush message.
    fn flush_msg(&mut self) -> &mut Self {
        pg_frame!(self, MessageCode::FLUSH);
        self
    }

    /// Write a Terminate message.
    fn terminate(&mut self) -> &mut Self {
        pg_frame!(self, MessageCode::TERMINATE);
        self
    }

    /// Write an Execute message. An empty `portal` executes the unnamed
    /// portal; a `max_rows` of 0 means unlimited.
    fn execute(&mut self, portal: &str, max_rows: u32) -> &mut Self {
        pg_frame!(self, MessageCode::EXECUTE, cstring(portal), u32(max_rows));
        self
    }

    /// Write a Describe message for a portal (empty `name` for the unnamed one).
    fn describe_portal(&mut self, name: &str) -> &mut Self {
        pg_frame!(self, MessageCode::DESCRIBE, u8(b'P'), cstring(name));
        self
    }

    /// Write a Describe message for a prepared statement (empty `name` for the
    /// unnamed one).
    fn describe_statement(&mut self, name: &str) -> &mut Self {
        pg_frame!(self, MessageCode::DESCRIBE, u8(b'S'), cstring(name));
        self
    }

    /// Write a Close message for a portal (empty `name` for the unnamed one).
    fn close_portal(&mut self, name: &str) -> &mut Self {
        pg_frame!(self, MessageCode::CLOSE, u8(b'P'), cstring(name));
        self
    }

    /// Write a Close message for a prepared statement (empty `name` for the
    /// unnamed one).
    fn close_statement(&mut self, name: &str) -> &mut Self {
        pg_frame!(self, MessageCode::CLOSE, u8(b'S'), cstring(name));
        self
    }

    /// Write a CopyData message.
    ///
    /// Used during COPY operations to send data to the server.
    fn copy_data(&mut self, data: &[u8]) -> &mut Self {
        pg_frame!(self, MessageCode::COPY_DATA, bytes(data));
        self
    }

    /// Write a CopyDone message.
    ///
    /// Signals successful completion of a COPY operation.
    fn copy_done(&mut self) -> &mut Self {
        pg_frame!(self, MessageCode::COPY_DONE);
        self
    }

    /// Write a CopyFail message.
    ///
    /// Signals that the COPY operation should be aborted with an error message.
    fn copy_fail(&mut self, msg: &str) -> &mut Self {
        pg_frame!(self, MessageCode::COPY_FAIL, cstring(msg));
        self
    }

    /// Write a Parse message preparing `query` as statement `name` (empty for
    /// the unnamed statement). Types omitted from `param_types` are inferred.
    fn parse(&mut self, name: &str, query: &str, param_types: &[Oid]) -> &mut Self {
        let payload_len = cstring_len(name.as_bytes())
            + cstring_len(query.as_bytes())
            + 2
            + param_types.len() * 4;

        self.put_u8(MessageCode::PARSE.as_u8());
        frame(self, payload_len, |buf| {
            put_cstring(buf, name.as_bytes());
            put_cstring(buf, query.as_bytes());
            buf.put_u16(param_types.len() as u16);
            for &oid in param_types {
                buf.put_u32(oid);
            }
        });
        self
    }

    /// Start building a Bind message (empty `portal` for the unnamed portal).
    fn bind<'a>(&'a mut self, portal: &'a str) -> BindBuilder<'a, Self> {
        BindBuilder::new(self, portal)
    }

    /// Start building a FunctionCall message.
    fn fn_call(&mut self, oid: Oid) -> FnCallBuilder<'_, Self> {
        FnCallBuilder::new(self, oid)
    }
}

impl<B: BufMut> PgProtocol for B {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    #[test]
    fn test_query() {
        let mut buf = BytesMut::new();
        buf.query("SELECT 1");

        assert_eq!(buf.get_u8(), b'Q');
        let len = buf.get_u32();
        assert_eq!(len, 13);
        assert_eq!(&buf[..], b"SELECT 1\0");
    }

    #[test]
    fn test_sync() {
        let mut buf = BytesMut::new();
        buf.sync();

        assert_eq!(buf[0], b'S');
        assert_eq!(&buf[1..5], &4u32.to_be_bytes());
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn test_parse_with_param_types() {
        let mut buf = BytesMut::new();
        buf.parse("stmt1", "SELECT $1::int", &[23]);

        assert_eq!(buf[0], b'P');
        // name, query, count, one OID
        assert_eq!(&buf[5..], b"stmt1\0SELECT $1::int\0\0\x01\0\0\0\x17");
    }

    #[test]
    fn test_parse_no_param_types() {
        let mut buf = BytesMut::new();
        buf.parse("", "SELECT 1", &[]);

        assert_eq!(buf[0], b'P');
        assert_eq!(&buf[5..], b"\0SELECT 1\0\0\0");
    }

    #[test]
    fn test_bind_with_params() {
        let mut buf = BytesMut::new();
        buf.bind("portal1")
            .statement("stmt1")
            .finish(crate::params![42i32, "hello"]);

        assert_eq!(buf[0], b'B');
    }

    #[test]
    fn test_bind_no_params() {
        let mut buf = BytesMut::new();
        buf.bind("portal1")
            .statement("stmt1")
            .finish(crate::params![]);

        assert_eq!(buf[0], b'B');
    }

    #[test]
    fn test_params_macro_matches_explicit_dyn() {
        let vec_data: Vec<u8> = vec![1, 2, 3];
        let owned = String::from("owned");

        let mut with_macro = BytesMut::new();
        with_macro.bind("").finish(crate::params![
            42i32,
            "hello",
            &owned,
            &vec_data,
            Option::<i64>::None
        ]);

        let mut with_dyn = BytesMut::new();
        with_dyn.bind("").finish(&[
            &42i32 as &dyn Bindable,
            &"hello" as &dyn Bindable,
            &owned as &dyn Bindable,
            &vec_data as &dyn Bindable,
            &None::<i64> as &dyn Bindable,
        ]);

        assert_eq!(with_macro, with_dyn);
    }

    #[test]
    fn test_fn_call_with_args() {
        let mut buf = BytesMut::new();
        buf.fn_call(1234)
            .result_format(FormatCode::Binary)
            .finish(crate::params!["test", 42i32]);

        assert_eq!(buf[0], b'F');
    }

    #[test]
    fn test_chaining() {
        let mut buf = BytesMut::new();
        buf.parse("s", "SELECT $1", &[])
            .bind("s")
            .finish(crate::params![1i32])
            .execute("", 0)
            .sync();

        // Count messages
        let mut count = 0;
        let mut pos = 0;
        while pos < buf.len() {
            count += 1;
            pos += 1;
            let len = u32::from_be_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]);
            pos += len as usize;
        }
        assert_eq!(count, 4);
    }

    #[test]
    fn test_encode_i32() {
        let mut buf = Vec::new();
        42i32.encode(&mut buf);
        assert_eq!(buf, vec![0, 0, 0, 4, 0, 0, 0, 42]);
    }

    #[test]
    fn test_encode_str() {
        let mut buf = Vec::new();
        "hi".encode(&mut buf);
        assert_eq!(buf, vec![0, 0, 0, 2, b'h', b'i']);
    }

    #[test]
    fn test_encode_option_some() {
        let mut buf = Vec::new();
        Some(1i16).encode(&mut buf);
        assert_eq!(buf, vec![0, 0, 0, 2, 0, 1]);
    }

    #[test]
    fn test_encode_option_none() {
        let mut buf = Vec::new();
        let none: Option<i32> = None;
        none.encode(&mut buf);
        assert_eq!(buf, vec![255, 255, 255, 255]);
    }

    #[test]
    fn test_copy_data() {
        let mut buf = BytesMut::new();
        buf.copy_data(b"hello\tworld\n");

        assert_eq!(buf[0], b'd');
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len, 4 + 12); // length field + data
        assert_eq!(&buf[5..], b"hello\tworld\n");
    }

    #[test]
    fn test_copy_done() {
        let mut buf = BytesMut::new();
        buf.copy_done();

        assert_eq!(buf[0], b'c');
        assert_eq!(&buf[1..5], &4u32.to_be_bytes());
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn test_copy_fail() {
        let mut buf = BytesMut::new();
        buf.copy_fail("invalid data format");

        assert_eq!(buf[0], b'f');
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        assert_eq!(len, 4 + 20); // length field + "invalid data format\0"
        assert_eq!(&buf[5..], b"invalid data format\0");
    }
}
