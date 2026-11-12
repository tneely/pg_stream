//! Messages sent by the client.
//!
//! [`Frontend`] has one method per protocol message. It is implemented for
//! [`Connection`](crate::Connection), which queues them to be sent, and for
//! building them anywhere else: in a byte slice, or in anything that is a
//! [`Buffer`], such as a `Vec<u8>` or a buffer of your own.
//!
//! Each message is written in a single pass: nothing is measured beforehand
//! and nothing is allocated beyond the buffer itself.
//!
//! Statement, portal and other names are sent as given. An empty name selects
//! the unnamed statement or portal. A name or query containing a NUL byte
//! makes the server reject the message.
//!
//! # Panics
//!
//! Encoding panics rather than write something the protocol cannot express:
//! a message of more than 2 GiB, or more than 65535 parameters, parameter
//! types or format codes. It also panics when a message does not fit a buffer
//! that cannot grow, such as a slice.

mod params;

use std::{convert::Infallible, io::Cursor};

pub use params::{
    Formats, IsNull, Param, ParamWriter, Params, ParamsWith, TryParamsWith, params_with,
    try_params_with,
};

use crate::{Format, Oid, ProtocolVersion};

/// Somewhere messages can be written.
///
/// Bytes are appended at the end, and the ones already there can be rewritten
/// or dropped again. Encoding needs both: the length of a message is filled
/// in once its body has been written, and a message that fails partway is
/// removed.
///
/// It is implemented for `Vec<u8>`, which grows as needed, and for a
/// [`Cursor`] over a byte slice, which holds as much as the slice has room
/// for. Every `Buffer` is a [`Frontend`].
pub trait Buffer {
    /// How many bytes have been written.
    fn written(&self) -> usize;

    /// Appends `bytes`.
    ///
    /// # Panics
    ///
    /// A buffer that cannot grow panics when `bytes` do not fit.
    fn put(&mut self, bytes: &[u8]);

    /// Overwrites bytes written earlier, starting with the one at `at`.
    fn patch(&mut self, at: usize, bytes: &[u8]);

    /// Drops everything written from `len` on.
    fn truncate(&mut self, len: usize);

    /// Makes room for `additional` more bytes, in a buffer that can grow.
    fn reserve(&mut self, additional: usize) {
        let _ = additional;
    }

    /// Appends whatever `write` puts in a `Vec`.
    ///
    /// Parameter values are written this way, because a `Vec` is what their
    /// encoders are given. A buffer that is not one receives them by way of a
    /// temporary, at the cost of an allocation. One that keeps a `Vec` for
    /// the purpose can override this and save it.
    #[inline]
    fn with_vec<R>(&mut self, write: impl FnOnce(&mut Vec<u8>) -> R) -> R {
        let mut staged = Vec::with_capacity(STAGED_CAPACITY);
        let result = write(&mut staged);
        self.put(&staged);
        result
    }
}

impl Buffer for Vec<u8> {
    #[inline]
    fn written(&self) -> usize {
        self.len()
    }

    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    #[inline]
    fn patch(&mut self, at: usize, bytes: &[u8]) {
        self[at..at + bytes.len()].copy_from_slice(bytes);
    }

    #[inline]
    fn truncate(&mut self, len: usize) {
        Vec::truncate(self, len);
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        Vec::reserve(self, additional);
    }

    #[inline]
    fn with_vec<R>(&mut self, write: impl FnOnce(&mut Vec<u8>) -> R) -> R {
        write(self)
    }
}

/// Writes from the cursor's position on, which moves to the end of what has
/// been written.
impl Buffer for Cursor<&mut [u8]> {
    #[inline]
    fn written(&self) -> usize {
        self.position() as usize
    }

    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        let at = self.written();
        let end = at + bytes.len();
        let room = self.get_mut().get_mut(at..end);
        let room = room.expect("the message does not fit the buffer");
        room.copy_from_slice(bytes);
        self.set_position(end as u64);
    }

    #[inline]
    fn patch(&mut self, at: usize, bytes: &[u8]) {
        self.get_mut()[at..at + bytes.len()].copy_from_slice(bytes);
    }

    #[inline]
    fn truncate(&mut self, len: usize) {
        self.set_position(len as u64);
    }
}

/// Writes frontend messages.
///
/// Methods return `&mut Self`, so a pipeline reads as one expression:
///
/// ```
/// use pg_stream::{Format, Frontend, params};
///
/// let mut buf = Vec::new();
/// buf.parse("", "SELECT $1::int4 + $2::int4", &[])
///     .bind("", "", params![40i32, 2i32], Format::Binary)
///     .execute("", 0)
///     .sync();
/// ```
///
/// A byte slice takes messages at its front and is shortened to what is left
/// of it, as it is by [`std::io::Write`]:
///
/// ```
/// use pg_stream::Frontend;
///
/// let mut storage = [0; 64];
/// let mut buf = &mut storage[..];
/// buf.query("SELECT 1").terminate();
/// let unused = buf.len();
///
/// let written = &storage[..64 - unused];
/// assert_eq!(written, b"Q\0\0\0\x0dSELECT 1\0X\0\0\0\x04");
/// ```
pub trait Frontend {
    /// What the messages are written to.
    type Buf: Buffer;

    /// Calls `write` with the buffer that messages are appended to.
    fn with_buf<R>(&mut self, write: impl FnOnce(&mut Self::Buf) -> R) -> R;

    /// Query: runs `sql` with the simple query protocol. The string may hold
    /// several statements; the response ends with one ReadyForQuery.
    fn query(&mut self, sql: &str) -> &mut Self {
        self.with_buf(|buf| put_cstr(begin(buf, b'Q', sql.len() + 1), sql));
        self
    }

    /// Parse: prepares `sql` as `statement`. Parameter types that are omitted
    /// or [`UNSPECIFIED`](crate::oid::UNSPECIFIED) are inferred by the server.
    fn parse(&mut self, statement: &str, sql: &str, param_types: &[Oid]) -> &mut Self {
        let types = u16::try_from(param_types.len()).expect("at most 65535 parameter types");
        let len = statement.len() + 1 + sql.len() + 1 + 2 + param_types.len() * 4;
        self.with_buf(|buf| {
            let buf = begin(buf, b'P', len);
            put_cstr(buf, statement);
            put_cstr(buf, sql);
            buf.put(&types.to_be_bytes());
            for oid in param_types {
                buf.put(&oid.to_be_bytes());
            }
        });
        self
    }

    /// Bind: creates `portal` from `statement` and its parameter values.
    /// `result_formats` chooses the format of the columns the portal returns.
    fn bind<P>(
        &mut self,
        portal: &str,
        statement: &str,
        params: P,
        result_formats: impl Formats,
    ) -> &mut Self
    where
        P: Params<Error = Infallible>,
    {
        match self.try_bind(portal, statement, params, result_formats) {
            Ok(this) => this,
            Err(never) => match never {},
        }
    }

    /// Like [`bind`](Self::bind), for parameters whose encoding can fail. On
    /// error nothing is appended.
    fn try_bind<P: Params>(
        &mut self,
        portal: &str,
        statement: &str,
        params: P,
        result_formats: impl Formats,
    ) -> Result<&mut Self, P::Error> {
        self.with_buf(|buf| {
            measured(buf, b'B', |buf| {
                put_cstr(buf, portal);
                put_cstr(buf, statement);
                params.encode(buf)?;
                result_formats.put(buf);
                Ok(())
            })
        })?;
        Ok(self)
    }

    /// Describe: asks for the parameter and result types of a statement.
    fn describe_statement(&mut self, statement: &str) -> &mut Self {
        named(self, b'D', b'S', statement)
    }

    /// Describe: asks for the result types of a portal.
    fn describe_portal(&mut self, portal: &str) -> &mut Self {
        named(self, b'D', b'P', portal)
    }

    /// Execute: runs `portal`, returning at most `max_rows` rows, or all of
    /// them if `max_rows` is zero.
    fn execute(&mut self, portal: &str, max_rows: u32) -> &mut Self {
        self.with_buf(|buf| {
            let buf = begin(buf, b'E', portal.len() + 1 + 4);
            put_cstr(buf, portal);
            buf.put(&max_rows.to_be_bytes());
        });
        self
    }

    /// Close: releases a prepared statement.
    fn close_statement(&mut self, statement: &str) -> &mut Self {
        named(self, b'C', b'S', statement)
    }

    /// Close: releases a portal.
    fn close_portal(&mut self, portal: &str) -> &mut Self {
        named(self, b'C', b'P', portal)
    }

    /// Sync: ends a cycle of extended-protocol messages. The server commits
    /// (or, after an error, rolls back) any implicit transaction and answers
    /// with ReadyForQuery.
    fn sync(&mut self) -> &mut Self {
        bare(self, b'S')
    }

    /// Flush: asks the server to send what it has so far, without ending the
    /// cycle. Not to be confused with flushing the connection's own buffer.
    fn flush_request(&mut self) -> &mut Self {
        bare(self, b'H')
    }

    /// FunctionCall: calls the function with the given OID directly.
    fn function_call<P>(&mut self, function: Oid, args: P, result_format: Format) -> &mut Self
    where
        P: Params<Error = Infallible>,
    {
        let call: Result<(), Infallible> = self.with_buf(|buf| {
            measured(buf, b'F', |buf| {
                buf.put(&function.to_be_bytes());
                args.encode(buf)?;
                buf.put(&(result_format as u16).to_be_bytes());
                Ok(())
            })
        });
        call.unwrap_or_else(|never| match never {});
        self
    }

    /// CopyData: a chunk of COPY data. Chunk boundaries carry no meaning.
    fn copy_data(&mut self, data: &[u8]) -> &mut Self {
        self.with_buf(|buf| begin(buf, b'd', data.len()).put(data));
        self
    }

    /// CopyDone: ends a COPY FROM STDIN successfully.
    fn copy_done(&mut self) -> &mut Self {
        bare(self, b'c')
    }

    /// CopyFail: aborts a COPY FROM STDIN; `reason` appears in the resulting
    /// error.
    fn copy_fail(&mut self, reason: &str) -> &mut Self {
        self.with_buf(|buf| put_cstr(begin(buf, b'f', reason.len() + 1), reason));
        self
    }

    /// Terminate: tells the server the client is going away.
    fn terminate(&mut self) -> &mut Self {
        bare(self, b'X')
    }

    /// StartupMessage: opens a session. `parameters` must include `user`.
    fn startup_message<'a>(
        &mut self,
        version: ProtocolVersion,
        parameters: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> &mut Self {
        self.with_buf(|buf| {
            let start = buf.written();
            buf.put(&[0; 4]);
            buf.put(&version.as_u32().to_be_bytes());
            for (name, value) in parameters {
                put_cstr(buf, name);
                put_cstr(buf, value);
            }
            buf.put(&[0]);
            patch_len(buf, start);
        });
        self
    }

    /// SSLRequest: asks to switch to TLS. See
    /// [`request_tls`](crate::request_tls).
    fn ssl_request(&mut self) -> &mut Self {
        self.with_buf(|buf| buf.put(&SSL_REQUEST));
        self
    }

    /// CancelRequest: asks to interrupt another session. See
    /// [`cancel`](crate::cancel).
    fn cancel_request(&mut self, process_id: u32, secret_key: &[u8]) -> &mut Self {
        self.with_buf(|buf| {
            buf.put(&len32(12 + secret_key.len()).to_be_bytes());
            buf.put(&CANCEL_REQUEST_CODE);
            buf.put(&process_id.to_be_bytes());
            buf.put(secret_key);
        });
        self
    }

    /// PasswordMessage: a cleartext or MD5-hashed password.
    fn password_message(&mut self, password: &str) -> &mut Self {
        self.with_buf(|buf| put_cstr(begin(buf, b'p', password.len() + 1), password));
        self
    }

    /// SASLInitialResponse: selects a SASL mechanism and sends its first
    /// message.
    fn sasl_initial_response(&mut self, mechanism: &str, response: &[u8]) -> &mut Self {
        let len = mechanism.len() + 1 + 4 + response.len();
        self.with_buf(|buf| {
            let buf = begin(buf, b'p', len);
            put_cstr(buf, mechanism);
            buf.put(&len32(response.len()).to_be_bytes());
            buf.put(response);
        });
        self
    }

    /// SASLResponse: a later message of a SASL exchange.
    fn sasl_response(&mut self, response: &[u8]) -> &mut Self {
        self.with_buf(|buf| begin(buf, b'p', response.len()).put(response));
        self
    }

    /// GSSResponse: a GSSAPI or SSPI token.
    fn gss_response(&mut self, token: &[u8]) -> &mut Self {
        self.sasl_response(token)
    }
}

impl<B: Buffer> Frontend for B {
    type Buf = B;

    #[inline]
    fn with_buf<R>(&mut self, write: impl FnOnce(&mut B) -> R) -> R {
        write(self)
    }
}

/// Messages are written to the front of the slice, which is then shortened to
/// the part that is still free.
impl<'a> Frontend for &'a mut [u8] {
    type Buf = Cursor<&'a mut [u8]>;

    fn with_buf<R>(&mut self, write: impl FnOnce(&mut Self::Buf) -> R) -> R {
        let mut cursor = Cursor::new(std::mem::take(self));
        let result = write(&mut cursor);
        let written = cursor.written();
        *self = cursor.into_inner().split_at_mut(written).1;
        result
    }
}

/// How much room a temporary `Vec` for parameter values starts with: enough
/// for most lists, so that staging them costs one allocation.
const STAGED_CAPACITY: usize = 256;

/// An SSLRequest: a length of eight and the request code 80877103, which no
/// protocol version number collides with.
const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];

/// The request code of a CancelRequest, 80877102.
const CANCEL_REQUEST_CODE: [u8; 4] = [0x04, 0xd2, 0x16, 0x2e];

// The functions below, and the pieces of a Bind in `params`, are `#[inline]`
// because they end up in the crate that calls them. Left to the compiler they
// are inlined there or not depending on what else that crate holds, and a
// message assembled from calls takes up to twice as long.

#[inline]
fn len32(len: usize) -> u32 {
    let len = i32::try_from(len).expect("a message is limited to 2 GiB");
    len as u32
}

#[inline]
fn put_cstr<B: Buffer>(buf: &mut B, s: &str) {
    buf.put(s.as_bytes());
    buf.put(&[0]);
}

/// Writes the header of a message whose body is `body_len` bytes, leaving the
/// body to the caller.
#[inline]
fn begin<B: Buffer>(buf: &mut B, code: u8, body_len: usize) -> &mut B {
    let [a, b, c, d] = len32(body_len + 4).to_be_bytes();
    buf.reserve(5 + body_len);
    buf.put(&[code, a, b, c, d]);
    buf
}

/// Writes a message that is nothing but its code.
fn bare<F: Frontend + ?Sized>(to: &mut F, code: u8) -> &mut F {
    to.with_buf(|buf| {
        begin(buf, code, 0);
    });
    to
}

/// Writes a Describe or Close, which name a statement or portal.
fn named<'f, F: Frontend + ?Sized>(to: &'f mut F, code: u8, kind: u8, name: &str) -> &'f mut F {
    to.with_buf(|buf| {
        let buf = begin(buf, code, 1 + name.len() + 1);
        buf.put(&[kind]);
        put_cstr(buf, name);
    });
    to
}

/// Writes a message whose length is only known once its body is written. If
/// the body fails, the message is removed again.
#[inline]
fn measured<B: Buffer, E>(
    buf: &mut B,
    code: u8,
    body: impl FnOnce(&mut B) -> Result<(), E>,
) -> Result<(), E> {
    let start = buf.written();
    buf.put(&[code, 0, 0, 0, 0]);
    match body(buf) {
        Ok(()) => patch_len(buf, start + 1),
        Err(e) => {
            buf.truncate(start);
            return Err(e);
        }
    }
    Ok(())
}

/// Fills in a length field at `at` that covers everything from there on.
#[inline]
fn patch_len<B: Buffer>(buf: &mut B, at: usize) {
    let len = len32(buf.written() - at);
    buf.patch(at, &len.to_be_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params;

    /// A message as the protocol documentation lays it out.
    fn message(code: u8, body: &[u8]) -> Vec<u8> {
        let mut message = vec![code];
        message.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
        message.extend_from_slice(body);
        message
    }

    fn encoded(encode: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut buf = Vec::new();
        encode(&mut buf);
        buf
    }

    #[test]
    fn query_cycle_messages() {
        let cases: [(Vec<u8>, Vec<u8>); 13] = [
            (
                encoded(|b| _ = b.query("SELECT 1")),
                message(b'Q', b"SELECT 1\0"),
            ),
            (
                encoded(|b| _ = b.parse("s", "SELECT $1", &[23, 0])),
                message(b'P', b"s\0SELECT $1\0\0\x02\0\0\0\x17\0\0\0\0"),
            ),
            (
                encoded(|b| _ = b.parse("", "", &[])),
                message(b'P', b"\0\0\0\0"),
            ),
            (
                encoded(|b| _ = b.describe_statement("s")),
                message(b'D', b"Ss\0"),
            ),
            (
                encoded(|b| _ = b.describe_portal("")),
                message(b'D', b"P\0"),
            ),
            (
                encoded(|b| _ = b.execute("p", 5)),
                message(b'E', b"p\0\0\0\0\x05"),
            ),
            (
                encoded(|b| _ = b.execute("", 0)),
                message(b'E', b"\0\0\0\0\0"),
            ),
            (
                encoded(|b| _ = b.close_statement("s")),
                message(b'C', b"Ss\0"),
            ),
            (encoded(|b| _ = b.close_portal("p")), message(b'C', b"Pp\0")),
            (encoded(|b| _ = b.sync()), message(b'S', b"")),
            (encoded(|b| _ = b.flush_request()), message(b'H', b"")),
            (encoded(|b| _ = b.terminate()), message(b'X', b"")),
            (
                encoded(|b| _ = b.function_call(1344, params![9i32], Format::Binary)),
                message(
                    b'F',
                    b"\0\0\x05\x40\0\x01\0\x01\0\x01\0\0\0\x04\0\0\0\x09\0\x01",
                ),
            ),
        ];
        for (encoded, expected) in cases {
            assert_eq!(encoded, expected, "{}", String::from_utf8_lossy(&expected));
        }
    }

    #[test]
    fn copy_messages() {
        assert_eq!(
            encoded(|b| _ = b.copy_data(b"1\t2\n")),
            message(b'd', b"1\t2\n")
        );
        assert_eq!(encoded(|b| _ = b.copy_data(b"")), message(b'd', b""));
        assert_eq!(encoded(|b| _ = b.copy_done()), message(b'c', b""));
        assert_eq!(encoded(|b| _ = b.copy_fail("no")), message(b'f', b"no\0"));
    }

    #[test]
    fn startup_and_authentication_messages() {
        let parameters = [("user", "u"), ("database", "d")];
        let startup = encoded(|b| _ = b.startup_message(ProtocolVersion::V3_2, parameters));
        assert_eq!(startup, b"\0\0\0\x1b\0\x03\0\x02user\0u\0database\0d\0\0");
        assert_eq!(startup.len(), 0x1b);

        assert_eq!(
            encoded(|b| _ = b.ssl_request()),
            [0, 0, 0, 8, 4, 210, 22, 47]
        );
        let cancel = encoded(|b| _ = b.cancel_request(1, &[9, 8, 7, 6, 5]));
        assert_eq!(
            cancel,
            [0, 0, 0, 17, 4, 210, 22, 46, 0, 0, 0, 1, 9, 8, 7, 6, 5]
        );

        assert_eq!(
            encoded(|b| _ = b.password_message("pw")),
            message(b'p', b"pw\0")
        );
        let initial = encoded(|b| _ = b.sasl_initial_response("SCRAM-SHA-256", b"n,,"));
        assert_eq!(initial, message(b'p', b"SCRAM-SHA-256\0\0\0\0\x03n,,"));
        assert_eq!(
            encoded(|b| _ = b.sasl_response(b"c=biws")),
            message(b'p', b"c=biws")
        );
        assert_eq!(
            encoded(|b| _ = b.gss_response(&[1, 2])),
            message(b'p', &[1, 2])
        );
    }

    /// Bind sends as few format codes as describe the values: none when all
    /// are text, one when all share a format, otherwise one each.
    #[test]
    fn bind_format_codes_are_compact() {
        let bind = |params: &[&dyn Param], results: &[Format]| {
            encoded(|b| _ = b.bind("p", "s", params, results))
        };

        let none = encoded(|b| _ = b.bind("p", "s", (), Format::Text));
        assert_eq!(none, message(b'B', b"p\0s\0\0\0\0\0\0\0"));

        let all_text = bind(params!["a", "bc"], &[]);
        assert_eq!(
            all_text,
            message(b'B', b"p\0s\0\0\0\0\x02\0\0\0\x01a\0\0\0\x02bc\0\0")
        );

        let all_binary = bind(params![1i16, 2i16], &[Format::Binary]);
        let expected = b"p\0s\0\0\x01\0\x01\0\x02\0\0\0\x02\0\x01\0\0\0\x02\0\x02\0\x01\0\x01";
        assert_eq!(all_binary, message(b'B', expected));

        let mixed = bind(params!["a", true], &[Format::Text, Format::Binary]);
        let expected = b"p\0s\0\0\x02\0\0\0\x01\0\x02\0\0\0\x01a\0\0\0\x01\x01\0\x02\0\0\0\x01";
        assert_eq!(mixed, message(b'B', expected));

        let null = bind(params![None::<i32>, Some(7i32)], &[]);
        let expected = b"p\0s\0\0\x02\0\0\0\x01\0\x02\xff\xff\xff\xff\0\0\0\x04\0\0\0\x07\0\0";
        assert_eq!(null, message(b'B', expected));
    }

    #[test]
    fn every_built_in_parameter_type_encodes() {
        let string = String::from("s");
        let (vec, bytes) = (vec![1u8, 2], bytes::Bytes::from_static(&[3]));
        let cases: [(&dyn Param, Format, &[u8]); 12] = [
            (&true, Format::Binary, &[1]),
            (&1i16, Format::Binary, &[0, 1]),
            (&2i32, Format::Binary, &[0, 0, 0, 2]),
            (&3i64, Format::Binary, &[0, 0, 0, 0, 0, 0, 0, 3]),
            (&1.5f32, Format::Binary, &1.5f32.to_be_bytes()),
            (&2.5f64, Format::Binary, &2.5f64.to_be_bytes()),
            (&"t", Format::Text, b"t"),
            (&string, Format::Text, b"s"),
            (&&[9u8][..], Format::Binary, &[9]),
            (&[4u8, 5], Format::Binary, &[4, 5]),
            (&vec, Format::Binary, &[1, 2]),
            (&bytes, Format::Binary, &[3]),
        ];
        for (value, format, expected) in cases {
            let mut encoded = Vec::new();
            assert_eq!(value.encode(&mut encoded), IsNull::No);
            assert_eq!((value.format(), &encoded[..]), (format, expected));
        }
    }

    #[test]
    fn streamed_parameters_are_counted_and_measured() {
        let params = params_with([Format::Binary, Format::Text, Format::Binary], |values| {
            values.write(|buf| buf.extend_from_slice(&[0xaa, 0xbb]));
            values.null();
            values.bytes(b"");
            values.param("text");
        });
        let bind = encoded(|b| _ = b.bind("", "", params, Format::Text));

        let mut expected = b"\0\0\0\x03\0\x01\0\0\0\x01\0\x04".to_vec();
        expected.extend_from_slice(b"\0\0\0\x02\xaa\xbb\xff\xff\xff\xff\0\0\0\0\0\0\0\x04text");
        expected.extend_from_slice(b"\0\0");
        assert_eq!(bind, message(b'B', &expected));
    }

    #[test]
    fn a_failed_bind_leaves_the_buffer_as_it_was() {
        let mut buf = Vec::new();
        buf.sync();
        let before = buf.clone();

        let failing = try_params_with(Format::Binary, |values| {
            values.write(|buf| buf.push(1));
            values.try_write(|buf| {
                buf.push(2);
                Err::<IsNull, _>("no")
            })
        });
        assert_eq!(
            buf.try_bind("p", "s", failing, Format::Text).err(),
            Some("no")
        );
        assert_eq!(buf, before);

        // What an encoder wrote before settling on NULL is taken back.
        let null = try_params_with(Format::Binary, |values| {
            values.try_write(|buf| {
                buf.extend_from_slice(b"discarded");
                Ok::<_, ()>(IsNull::Yes)
            })
        });
        buf.clear();
        buf.try_bind("", "", null, Format::Text).unwrap();
        assert_eq!(
            buf,
            message(b'B', b"\0\0\0\x01\0\x01\0\x01\xff\xff\xff\xff\0\0")
        );
    }

    /// One of every message, with each way of giving parameters.
    fn everything<F: Frontend>(to: &mut F) {
        let streamed = params_with([Format::Binary, Format::Text], |values| {
            values.write(|buf| buf.extend_from_slice(&[0xaa, 0xbb]));
            values.null();
        });
        to.startup_message(ProtocolVersion::V3_2, [("user", "u")]);
        to.ssl_request().cancel_request(1, &[9, 8, 7, 6]);
        to.password_message("pw").sasl_response(b"c=biws");
        to.sasl_initial_response("SCRAM-SHA-256", b"n,,");
        to.gss_response(&[1, 2]).query("SELECT 1");
        to.parse("s", "SELECT $1", &[23, 0]);
        to.bind("p", "s", (), Format::Text);
        let mixed = params!["a", true, None::<i32>, b"b"];
        to.bind("p", "s", mixed, Format::Binary);
        to.bind("p", "s", streamed, [Format::Text, Format::Binary]);
        to.describe_statement("s").describe_portal("p");
        to.execute("p", 5).close_statement("s").close_portal("p");
        to.function_call(1344, params![9i32], Format::Binary);
        to.copy_data(b"1\t2\n").copy_done().copy_fail("no");
        to.flush_request().sync().terminate();
    }

    /// A buffer that implements no more than it has to.
    #[derive(Default)]
    struct Ring(std::collections::VecDeque<u8>);

    impl Buffer for Ring {
        fn written(&self) -> usize {
            self.0.len()
        }

        fn put(&mut self, bytes: &[u8]) {
            self.0.extend(bytes);
        }

        fn patch(&mut self, at: usize, bytes: &[u8]) {
            let patched = self.0.range_mut(at..at + bytes.len());
            patched.zip(bytes).for_each(|(old, new)| *old = *new);
        }

        fn truncate(&mut self, len: usize) {
            self.0.truncate(len);
        }
    }

    #[test]
    fn every_buffer_is_written_the_same_bytes() {
        let mut vec = Vec::new();
        everything(&mut vec);

        // A slice is shortened to what is left of it, which is not touched.
        let mut storage = vec![0xee; vec.len() + 3];
        let mut slice = &mut storage[..];
        everything(&mut slice);
        assert_eq!(slice, [0xee; 3]);
        assert_eq!(storage[..vec.len()], vec[..]);

        // A cursor keeps its place instead, and a slice may be filled exactly.
        let mut storage = vec![0; vec.len()];
        let mut cursor = Cursor::new(&mut storage[..]);
        everything(&mut cursor);
        assert_eq!(cursor.position(), vec.len() as u64);
        assert_eq!(storage, vec);

        let mut ring = Ring::default();
        everything(&mut ring);
        assert_eq!(ring.0, vec);
    }

    #[test]
    #[should_panic(expected = "the message does not fit the buffer")]
    fn a_message_too_long_for_a_slice_panics() {
        let mut storage = [0; 13];
        let mut slice = &mut storage[..];
        slice.query("SELECT 1");
    }

    #[test]
    fn a_failed_bind_leaves_a_slice_as_long_as_it_was() {
        let mut storage = [0; 32];
        let mut slice = &mut storage[..];
        slice.sync();

        let failing = try_params_with(Format::Binary, |values| {
            values.write(|buf| buf.push(1));
            Err::<(), _>("no")
        });
        let result = slice.try_bind("p", "s", failing, Format::Text);
        assert_eq!(result.err(), Some("no"));
        assert_eq!(slice.len(), 32 - 5);

        slice.terminate();
        assert_eq!(storage[..10], *b"S\0\0\0\x04X\0\0\0\x04");
    }

    #[test]
    fn messages_are_appended_in_order() {
        let mut buf = Vec::new();
        buf.parse("", "SELECT 1", &[])
            .bind("", "", (), Format::Text)
            .execute("", 0)
            .sync();

        let codes: Vec<u8> = {
            let mut rest = &buf[..];
            let mut codes = Vec::new();
            while let Some((&[code, a, b, c, d], _)) = rest.split_first_chunk::<5>() {
                codes.push(code);
                rest = &rest[1 + u32::from_be_bytes([a, b, c, d]) as usize..];
            }
            codes
        };
        assert_eq!(codes, b"PBES");
    }
}
