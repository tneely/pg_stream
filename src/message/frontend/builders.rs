//! Builder types for complex frontend messages.

use bytes::BufMut;

use super::bindable::Bindable;
use super::code::MessageCode;
use super::codec::{cstring_len, frame, put_cstring};
use super::types::{FormatCode, Oid};

/// Builder for Bind messages.
///
/// # Example
///
/// ```
/// use bytes::BytesMut;
/// use pg_stream::{message::{PgProtocol, FormatCode}, params};
///
/// let mut buf = BytesMut::new();
/// buf.bind("portal1")
///    .statement("stmt1")
///    .result_format(FormatCode::Binary)
///    .finish(params![42i32, "hello"]);
/// ```
pub struct BindBuilder<'a, B: BufMut> {
    buf: &'a mut B,
    portal: &'a str,
    statement: &'a str,
    result_format: FormatCode,
}

impl<'a, B: BufMut> BindBuilder<'a, B> {
    pub(super) fn new(buf: &'a mut B, portal: &'a str) -> Self {
        Self {
            buf,
            portal,
            statement: "",
            result_format: FormatCode::Text,
        }
    }

    /// Set the statement name (empty for the unnamed statement).
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
        let formats = FormatCodes::of(params);
        let payload_len = cstring_len(portal.as_bytes())
            + cstring_len(statement.as_bytes())
            + formats.encoded_len(params.len())
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

            formats.write(buf, params);

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
/// use pg_stream::{message::{PgProtocol, FormatCode}, params};
///
/// let mut buf = BytesMut::new();
/// buf.fn_call(1344)
///    .result_format(FormatCode::Text)
///    .finish(params!["9"]);
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
        let formats = FormatCodes::of(args);
        let payload_len = 4
            + formats.encoded_len(args.len())
            + 2
            + args.iter().map(|a| a.encoded_len()).sum::<usize>()
            + 2;

        self.buf.put_u8(MessageCode::FUNCTION_CALL.as_u8());
        frame(self.buf, payload_len, |buf| {
            buf.put_u32(oid);

            formats.write(buf, args);

            buf.put_u16(args.len() as u16);
            for arg in args {
                arg.encode(buf);
            }

            buf.put_u16(result_format as u16);
        });

        self.buf
    }
}

/// The parameter format-code section of a Bind/FunctionCall message, collapsed
/// to the compact protocol form.
enum FormatCodes {
    /// All text (or no parameters): zero codes on the wire.
    AllText,
    /// Every parameter shares one non-text format: a single code on the wire.
    Uniform(FormatCode),
    /// Mixed formats: one code per parameter.
    PerParam,
}

impl FormatCodes {
    fn of(params: &[&dyn Bindable]) -> Self {
        let Some((first, rest)) = params.split_first() else {
            return Self::AllText;
        };
        let first = first.format_code();
        if rest.iter().any(|p| p.format_code() != first) {
            Self::PerParam
        } else if first == FormatCode::Text {
            Self::AllText
        } else {
            Self::Uniform(first)
        }
    }

    fn encoded_len(&self, param_count: usize) -> usize {
        2 + match self {
            Self::AllText => 0,
            Self::Uniform(_) => 2,
            Self::PerParam => param_count * 2,
        }
    }

    fn write(&self, buf: &mut impl BufMut, params: &[&dyn Bindable]) {
        match self {
            Self::AllText => buf.put_u16(0),
            Self::Uniform(code) => {
                buf.put_u16(1);
                buf.put_u16(*code as u16);
            }
            Self::PerParam => {
                buf.put_u16(params.len() as u16);
                for p in params {
                    buf.put_u16(p.format_code() as u16);
                }
            }
        }
    }
}
