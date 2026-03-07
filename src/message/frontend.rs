//! Extension trait for writing Postgres frontend messages to any buffer.

use std::marker::PhantomData;

use bytes::BufMut;

use crate::message::codec::{FormatCode, MessageCode, Oid, put_cstring};

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
        MessageCode::QUERY.write(self, |buf| {
            put_cstring(buf, stmt.as_bytes());
        });
        self
    }

    /// Write a Sync message.
    fn sync(&mut self) -> &mut Self {
        MessageCode::SYNC.write(self, |_: &mut Vec<u8>| {});
        self
    }

    /// Write a Flush message.
    fn flush_msg(&mut self) -> &mut Self {
        MessageCode::FLUSH.write(self, |_: &mut Vec<u8>| {});
        self
    }

    /// Write a Terminate message.
    fn terminate(&mut self) -> &mut Self {
        MessageCode::TERMINATE.write(self, |_: &mut Vec<u8>| {});
        self
    }

    /// Write an Execute message.
    ///
    /// Specify `None` to execute an unnamed portal.
    fn execute(&mut self, portal: Option<&str>, max_rows: u32) -> &mut Self {
        MessageCode::EXECUTE.write(self, |buf| {
            put_cstring(buf, portal.unwrap_or("").as_bytes());
            buf.put_u32(max_rows);
        });
        self
    }

    /// Write a Describe message for a portal.
    ///
    /// Specify `None` to describe an unnamed portal.
    fn describe_portal(&mut self, name: Option<&str>) -> &mut Self {
        MessageCode::DESCRIBE.write(self, |buf| {
            buf.put_u8(b'P');
            put_cstring(buf, name.unwrap_or("").as_bytes());
        });
        self
    }

    /// Write a Describe message for a prepared statement.
    ///
    /// Specify `None` to describe an unnamed statement.
    fn describe_statement(&mut self, name: Option<&str>) -> &mut Self {
        MessageCode::DESCRIBE.write(self, |buf| {
            buf.put_u8(b'S');
            put_cstring(buf, name.unwrap_or("").as_bytes());
        });
        self
    }

    /// Write a Close message for a portal.
    ///
    /// Specify `None` to prepare an unnamed portal.
    fn close_portal(&mut self, name: Option<&str>) -> &mut Self {
        MessageCode::CLOSE.write(self, |buf| {
            buf.put_u8(b'P');
            put_cstring(buf, name.unwrap_or("").as_bytes());
        });
        self
    }

    /// Write a Close message for a prepared statement.
    ///
    /// Specify `None` to close an unnamed statement.
    fn close_statement(&mut self, name: Option<&str>) -> &mut Self {
        MessageCode::CLOSE.write(self, |buf| {
            buf.put_u8(b'S');
            put_cstring(buf, name.unwrap_or("").as_bytes());
        });
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
    /// Specify `None` to bind an unnamed portal.
    fn bind<'a>(&'a mut self, name: Option<&'a str>) -> BindBuilder<'a, Self> {
        BindBuilder::new(self, name)
    }

    /// Start building a FunctionCall message.
    fn fn_call(&mut self, oid: Oid) -> FnCallBuilder<'_, Self> {
        FnCallBuilder::new(self, oid)
    }
}

impl<B: BufMut> FrontendMessage for B {}

/// State marker: query not yet provided.
pub struct NeedsQuery;

/// State marker: query provided, ready to finish.
pub struct Ready;

/// Builder for Parse messages.
///
/// # Example
///
/// ```
/// use bytes::BytesMut;
/// use pg_stream::message::FrontendMessage;
/// use pg_stream::message::oid;
///
/// let mut buf = BytesMut::new();
/// buf.parse(Some("stmt1"))
///    .query("SELECT $1::int")  // Required
///    .param_types(&[oid::INT4])
///    .finish();
/// ```
pub struct ParseBuilder<'a, B: BufMut, S = NeedsQuery> {
    buf: &'a mut B,
    name: Option<&'a str>,
    query: &'a str,
    param_types: &'a [Oid],
    _state: PhantomData<S>,
}

impl<'a, B: BufMut> ParseBuilder<'a, B, NeedsQuery> {
    fn new(buf: &'a mut B, name: Option<&'a str>) -> Self {
        Self {
            buf,
            name,
            query: "",
            param_types: &[],
            _state: PhantomData,
        }
    }

    /// Set the query string (required).
    pub fn query(self, query: &'a str) -> ParseBuilder<'a, B, Ready> {
        ParseBuilder {
            buf: self.buf,
            name: self.name,
            query,
            param_types: self.param_types,
            _state: PhantomData,
        }
    }
}

impl<'a, B: BufMut> ParseBuilder<'a, B, Ready> {
    /// Set the parameter type OIDs.
    pub fn param_types(mut self, types: &'a [Oid]) -> Self {
        self.param_types = types;
        self
    }

    /// Finish building and write the Parse message.
    pub fn finish(self) -> &'a mut B {
        MessageCode::PARSE.write(self.buf, |buf| {
            put_cstring(buf, self.name.unwrap_or("").as_bytes());
            put_cstring(buf, self.query.as_bytes());
            buf.put_u16(self.param_types.len() as u16);
            for &oid in self.param_types {
                buf.put_u32(oid);
            }
        });
        self.buf
    }
}

/// Builder for Bind messages.
///
/// # Example
///
/// ```
/// use bytes::BytesMut;
/// use pg_stream::message::{FrontendMessage, FormatCode, Bindable};
///
/// let mut buf = BytesMut::new();
/// buf.bind(Some("stmt1"))
///    .portal("portal1")
///    .result_format(FormatCode::Binary)
///    .finish(&[&42i32 as &dyn Bindable, &"hello" as &dyn Bindable]);
/// ```
pub struct BindBuilder<'a, B: BufMut> {
    buf: &'a mut B,
    portal: &'a str,
    statement: &'a str,
    result_format: FormatCode,
}

impl<'a, B: BufMut> BindBuilder<'a, B> {
    fn new(buf: &'a mut B, portal: Option<&'a str>) -> Self {
        Self {
            buf,
            portal: portal.unwrap_or(""),
            statement: "",
            result_format: FormatCode::Text,
        }
    }

    /// Set the statement name.
    pub fn statement(mut self, name: &'a str) -> Self {
        self.statement = name;
        self
    }

    /// Set the result format for all columns.
    pub fn result_format(mut self, format: FormatCode) -> Self {
        self.result_format = format;
        self
    }

    /// Finish building and write the Bind message.
    pub fn finish(self, params: &[&dyn Bindable]) -> &'a mut B {
        let portal = self.portal;
        let statement = self.statement;
        let result_format = self.result_format;

        MessageCode::BIND.write(self.buf, |buf| {
            put_cstring(buf, portal.as_bytes());
            put_cstring(buf, statement.as_bytes());

            // First pass: collect format codes
            write_format_codes(buf, params.iter().map(|p| p.format_code()));

            // Second pass: write param values
            buf.put_u16(params.len() as u16);
            for param in params {
                param.encode(buf);
            }

            // Result format
            if result_format == FormatCode::Text {
                buf.put_u16(0);
            } else {
                buf.put_u16(1);
                buf.put_u16(result_format as u16);
            }
        });

        self.buf
    }
}

/// Builder for FunctionCall messages.
///
/// # Example
///
/// ```
/// use bytes::BytesMut;
/// use pg_stream::message::{FrontendMessage, FormatCode, Bindable};
///
/// let mut buf = BytesMut::new();
/// buf.fn_call(1344)
///    .result_format(FormatCode::Text)
///    .finish(&[&"9" as &dyn Bindable]);
/// ```
pub struct FnCallBuilder<'a, B: BufMut> {
    buf: &'a mut B,
    oid: Oid,
    result_format: FormatCode,
}

impl<'a, B: BufMut> FnCallBuilder<'a, B> {
    fn new(buf: &'a mut B, oid: Oid) -> Self {
        Self {
            buf,
            oid,
            result_format: FormatCode::Text,
        }
    }

    /// Set the result format.
    pub fn result_format(mut self, format: FormatCode) -> Self {
        self.result_format = format;
        self
    }

    /// Finish building and write the FunctionCall message.
    pub fn finish(self, args: &[&dyn Bindable]) -> &'a mut B {
        let oid = self.oid;
        let result_format = self.result_format;

        MessageCode::FUNCTION_CALL.write(self.buf, |buf| {
            buf.put_u32(oid);

            // First pass: format codes
            write_format_codes(buf, args.iter().map(|a| a.format_code()));

            // Second pass: arg values
            buf.put_u16(args.len() as u16);
            for arg in args {
                arg.encode(buf);
            }

            buf.put_u16(result_format as u16);
        });

        self.buf
    }
}

/// Trait for types that can be bound as Postgres parameters.
pub trait Bindable {
    /// Returns the format code for this parameter type.
    fn format_code(&self) -> FormatCode;

    /// Encode this value to the buffer (including 4-byte length prefix).
    fn encode(&self, buf: &mut dyn BufMut);
}

impl Bindable for bool {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(1);
        buf.put_u8(*self as u8);
    }
}

impl Bindable for i16 {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(2);
        buf.put_i16(*self);
    }
}

impl Bindable for i32 {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(4);
        buf.put_i32(*self);
    }
}

impl Bindable for i64 {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(8);
        buf.put_i64(*self);
    }
}

impl Bindable for f32 {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(4);
        buf.put_f32(*self);
    }
}

impl Bindable for f64 {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(8);
        buf.put_f64(*self);
    }
}

impl Bindable for str {
    fn format_code(&self) -> FormatCode {
        FormatCode::Text
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.len() as i32);
        buf.put_slice(self.as_bytes());
    }
}

impl Bindable for &str {
    fn format_code(&self) -> FormatCode {
        FormatCode::Text
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.len() as i32);
        buf.put_slice(self.as_bytes());
    }
}

impl Bindable for String {
    fn format_code(&self) -> FormatCode {
        FormatCode::Text
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.len() as i32);
        buf.put_slice(self.as_bytes());
    }
}

impl Bindable for [u8] {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.len() as i32);
        buf.put_slice(self);
    }
}

impl Bindable for &[u8] {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.len() as i32);
        buf.put_slice(self);
    }
}

impl Bindable for bytes::Bytes {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        buf.put_i32(self.len() as i32);
        buf.put_slice(self);
    }
}

impl<T: Bindable> Bindable for Option<T> {
    fn format_code(&self) -> FormatCode {
        match self {
            Some(v) => v.format_code(),
            None => FormatCode::Binary,
        }
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        match self {
            Some(v) => v.encode(buf),
            None => buf.put_i32(-1),
        }
    }
}

/// Write format codes with protocol optimization.
///
/// - 0 codes: all use default (text)
/// - 1 code: all use same format
/// - N codes: individual formats
fn write_format_codes(buf: &mut impl BufMut, formats: impl Iterator<Item = FormatCode> + Clone) {
    let formats_clone = formats.clone();
    let mut iter = formats_clone.peekable();

    let Some(&first) = iter.peek() else {
        buf.put_u16(0);
        return;
    };

    if iter.all(|f| f == first) {
        if first == FormatCode::Text {
            buf.put_u16(0);
        } else {
            buf.put_u16(1);
            buf.put_u16(first as u16);
        }
    } else {
        // Mixed formats - write all
        let (lower, upper) = formats.size_hint();
        assert_eq!(Some(lower), upper, "iterator must have exact size");
        buf.put_u16(lower as u16);
        for f in formats {
            buf.put_u16(f as u16);
        }
    }
}

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
