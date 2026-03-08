//! Builder types for complex frontend messages.

use std::marker::PhantomData;

use bytes::BufMut;

use super::bindable::Bindable;
use super::code::MessageCode;
use super::codec::{cstring_len, frame, put_cstring};
use super::types::{FormatCode, Oid};

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
    pub(super) fn new(buf: &'a mut B, name: Option<&'a str>) -> Self {
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
        let payload_len = cstring_len(self.name.unwrap_or("").as_bytes())
            + cstring_len(self.query.as_bytes())
            + 2
            + self.param_types.len() * 4;

        self.buf.put_u8(MessageCode::PARSE.as_u8());
        frame(self.buf, payload_len, |buf| {
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
/// buf.bind(Some("portal1"))
///    .statement("stmt1")
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
    pub(super) fn new(buf: &'a mut B, portal: Option<&'a str>) -> Self {
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
        let payload_len = cstring_len(portal.as_bytes())
            + cstring_len(statement.as_bytes())
            + format_codes_len(params.iter().map(|p| p.format_code()))
            + 2
            + params.iter().map(|p| p.encoded_len()).sum::<usize>()
            + if result_format == FormatCode::Text {
                2
            } else {
                4
            };

        self.buf.put_u8(MessageCode::BIND.as_u8());
        frame(self.buf, payload_len, |buf| {
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
    pub(super) fn new(buf: &'a mut B, oid: Oid) -> Self {
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
        let payload_len = 4
            + format_codes_len(args.iter().map(|a| a.format_code()))
            + 2
            + args.iter().map(|a| a.encoded_len()).sum::<usize>()
            + 2;

        self.buf.put_u8(MessageCode::FUNCTION_CALL.as_u8());
        frame(self.buf, payload_len, |buf| {
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

/// Write format codes with protocol optimization.
///
/// - 0 codes: all use default (text)
/// - 1 code: all use same format
/// - N codes: individual formats
fn write_format_codes(buf: &mut impl BufMut, formats: impl Iterator<Item = FormatCode> + Clone) {
    let mut iter = formats.clone();
    let Some(first) = iter.next() else {
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

fn format_codes_len(formats: impl Iterator<Item = FormatCode> + Clone) -> usize {
    let mut iter = formats.clone();
    let Some(first) = iter.next() else {
        return 2;
    };

    if iter.all(|f| f == first) {
        if first == FormatCode::Text { 2 } else { 4 }
    } else {
        let (lower, upper) = formats.size_hint();
        assert_eq!(Some(lower), upper, "iterator must have exact size");
        2 + (lower * 2)
    }
}
