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
pub use builders::{BindBuilder, FnCallBuilder, NeedsQuery, ParseBuilder, Ready};
pub use code::MessageCode;
pub use codec::{cstring_len, frame};
pub use types::{FormatCode, Oid, oid};

use bytes::BufMut;

use crate::pg_frame;

/// Extension trait for writing Postgres frontend protocol messages.
///
/// This trait extends any `BufMut` implementation with methods to write
/// Postgres wire protocol messages directly to the buffer.
///
/// # Example
///
/// ```
/// use bytes::BytesMut;
/// use pg_stream::message::FrontendMessage;
///
/// let mut buf = BytesMut::new();
/// buf.query("SELECT 1")
///    .sync();
/// ```
pub trait FrontendMessage: BufMut + Sized {
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

    /// Write an Execute message.
    ///
    /// Specify `None` to execute an unnamed portal.
    fn execute(&mut self, portal: Option<&str>, max_rows: u32) -> &mut Self {
        pg_frame!(
            self,
            MessageCode::EXECUTE,
            cstring(portal.unwrap_or("")),
            u32(max_rows)
        );
        self
    }

    /// Write a Describe message for a portal.
    ///
    /// Specify `None` to describe an unnamed portal.
    fn describe_portal(&mut self, name: Option<&str>) -> &mut Self {
        pg_frame!(
            self,
            MessageCode::DESCRIBE,
            u8(b'P'),
            cstring(name.unwrap_or(""))
        );
        self
    }

    /// Write a Describe message for a prepared statement.
    ///
    /// Specify `None` to describe an unnamed statement.
    fn describe_statement(&mut self, name: Option<&str>) -> &mut Self {
        pg_frame!(
            self,
            MessageCode::DESCRIBE,
            u8(b'S'),
            cstring(name.unwrap_or(""))
        );
        self
    }

    /// Write a Close message for a portal.
    ///
    /// Specify `None` to prepare an unnamed portal.
    fn close_portal(&mut self, name: Option<&str>) -> &mut Self {
        pg_frame!(
            self,
            MessageCode::CLOSE,
            u8(b'P'),
            cstring(name.unwrap_or(""))
        );
        self
    }

    /// Write a Close message for a prepared statement.
    ///
    /// Specify `None` to close an unnamed statement.
    fn close_statement(&mut self, name: Option<&str>) -> &mut Self {
        pg_frame!(
            self,
            MessageCode::CLOSE,
            u8(b'S'),
            cstring(name.unwrap_or(""))
        );
        self
    }

    /// Start building a Parse message.
    ///
    /// Specify `None` to prepare an unnamed statement.
    fn parse<'a>(&'a mut self, name: Option<&'a str>) -> ParseBuilder<'a, Self, NeedsQuery> {
        ParseBuilder::new(self, name)
    }

    /// Start building a Bind message.
    ///
    /// Specify `None` to bind to an unnamed portal.
    fn bind<'a>(&'a mut self, name: Option<&'a str>) -> BindBuilder<'a, Self> {
        BindBuilder::new(self, name)
    }

    /// Start building a FunctionCall message.
    fn fn_call(&mut self, oid: Oid) -> FnCallBuilder<'_, Self> {
        FnCallBuilder::new(self, oid)
    }
}

impl<B: BufMut> FrontendMessage for B {}

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
    fn test_parse_typestate() {
        let mut buf = BytesMut::new();
        buf.parse(Some("stmt1"))
            .query("SELECT $1::int")
            .param_types(&[23])
            .finish();

        assert_eq!(buf[0], b'P');
    }

    #[test]
    fn test_parse_no_param_types() {
        let mut buf = BytesMut::new();
        buf.parse(None).query("SELECT 1").finish();

        assert_eq!(buf[0], b'P');
    }

    #[test]
    fn test_bind_with_params() {
        let mut buf = BytesMut::new();
        buf.bind(Some("portal1"))
            .statement("stmt1")
            .finish(&[&42i32 as &dyn Bindable, &"hello" as &dyn Bindable]);

        assert_eq!(buf[0], b'B');
    }

    #[test]
    fn test_bind_no_params() {
        let mut buf = BytesMut::new();
        buf.bind(Some("portal1")).statement("stmt1").finish(&[]);

        assert_eq!(buf[0], b'B');
    }

    #[test]
    fn test_fn_call_with_args() {
        let mut buf = BytesMut::new();
        buf.fn_call(1234)
            .result_format(FormatCode::Binary)
            .finish(&[&"test" as &dyn Bindable, &42i32 as &dyn Bindable]);

        assert_eq!(buf[0], b'F');
    }

    #[test]
    fn test_chaining() {
        let mut buf = BytesMut::new();
        buf.parse(Some("s"))
            .query("SELECT $1")
            .finish()
            .bind(Some("s"))
            .finish(&[&1i32 as &dyn Bindable])
            .execute(None, 0)
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
}
