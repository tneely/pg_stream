//! Parameter values for Bind and FunctionCall.

use std::{convert::Infallible, fmt};

use super::Buffer;
use crate::Format;

// `#[inline]` throughout, for the reason given where the parent module's
// helpers are.

mod sealed {
    pub trait Sealed {}
}
use sealed::Sealed;

/// Format codes for a list of parameters or result columns.
///
/// - a single [`Format`] applies to every value
/// - a slice or array gives one format per value
pub trait Formats: Sealed {
    #[doc(hidden)]
    fn put<B: Buffer>(self, buf: &mut B);
}

impl Sealed for Format {}
impl Formats for Format {
    #[inline]
    fn put<B: Buffer>(self, buf: &mut B) {
        // No codes at all means "all text"; one code applies to every value.
        match self {
            Format::Text => buf.put(&[0, 0]),
            Format::Binary => buf.put(&[0, 1, 0, 1]),
        }
    }
}

impl Sealed for &[Format] {}
impl Formats for &[Format] {
    #[inline]
    fn put<B: Buffer>(self, buf: &mut B) {
        put_formats(buf, self.len(), self.iter().copied());
    }
}

impl<const N: usize> Sealed for [Format; N] {}
impl<const N: usize> Formats for [Format; N] {
    #[inline]
    fn put<B: Buffer>(self, buf: &mut B) {
        self.as_slice().put(buf)
    }
}

#[inline]
fn put_formats<B: Buffer>(buf: &mut B, len: usize, formats: impl Iterator<Item = Format>) {
    let len = u16::try_from(len).expect("the protocol allows at most 65535 values");
    buf.put(&len.to_be_bytes());
    formats.for_each(|format| buf.put(&(format as u16).to_be_bytes()));
}

/// The parameters of a Bind or FunctionCall.
///
/// - `()` is no parameters
/// - [`params!`](crate::params) lists values that implement [`Param`]
/// - [`params_with`] and [`try_params_with`] write values directly, in any
///   encoding
pub trait Params: Sealed {
    /// What encoding a value can fail with.
    type Error;

    #[doc(hidden)]
    fn encode<B: Buffer>(self, buf: &mut B) -> Result<(), Self::Error>;
}

impl Sealed for () {}
impl Params for () {
    type Error = Infallible;

    #[inline]
    fn encode<B: Buffer>(self, buf: &mut B) -> Result<(), Infallible> {
        // Zero format codes, zero values.
        buf.put(&[0; 4]);
        Ok(())
    }
}

impl Sealed for &[&dyn Param] {}
impl Params for &[&dyn Param] {
    type Error = Infallible;

    #[inline]
    fn encode<B: Buffer>(self, buf: &mut B) -> Result<(), Infallible> {
        let mut formats = self.iter().map(|param| param.format());
        let first = formats.next().unwrap_or_default();
        match formats.all(|format| format == first) {
            true => first.put(buf),
            false => put_formats(buf, self.len(), self.iter().map(|param| param.format())),
        }

        buf.with_vec(|values| {
            ParamWriter::run(values, |writer| {
                self.iter().for_each(|param| writer.param(*param));
                Ok(())
            })
        })
    }
}

/// Parameters written by a closure. See [`params_with`].
pub struct ParamsWith<F, W>(F, W);

impl<F: fmt::Debug, W> fmt::Debug for ParamsWith<F, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ParamsWith");
        debug.field("formats", &self.0).finish_non_exhaustive()
    }
}

impl<F, W> Sealed for ParamsWith<F, W> {}
impl<F: Formats, W: FnOnce(&mut ParamWriter<'_>)> Params for ParamsWith<F, W> {
    type Error = Infallible;

    #[inline]
    fn encode<B: Buffer>(self, buf: &mut B) -> Result<(), Infallible> {
        self.0.put(buf);
        buf.with_vec(|values| {
            ParamWriter::run(values, |writer| {
                (self.1)(writer);
                Ok(())
            })
        })
    }
}

/// Parameters written by a fallible closure. See [`try_params_with`].
pub struct TryParamsWith<F, W>(F, W);

impl<F: fmt::Debug, W> fmt::Debug for TryParamsWith<F, W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("TryParamsWith");
        debug.field("formats", &self.0).finish_non_exhaustive()
    }
}

impl<F, W> Sealed for TryParamsWith<F, W> {}
impl<F: Formats, W: FnOnce(&mut ParamWriter<'_>) -> Result<(), E>, E> Params
    for TryParamsWith<F, W>
{
    type Error = E;

    #[inline]
    fn encode<B: Buffer>(self, buf: &mut B) -> Result<(), E> {
        self.0.put(buf);
        buf.with_vec(|values| ParamWriter::run(values, self.1))
    }
}

/// Parameters in the given `formats`, with values supplied by `write`.
///
/// ```
/// use pg_stream::{Format, Frontend, frontend::params_with};
///
/// let mut buf = Vec::new();
/// let ids = [7i32, 8, 9];
/// let params = params_with(Format::Binary, |params| {
///     for id in ids {
///         params.write(|value| value.extend_from_slice(&id.to_be_bytes()));
///     }
/// });
/// buf.bind("", "by_ids", params, Format::Binary);
/// ```
pub fn params_with<F, W>(formats: F, write: W) -> ParamsWith<F, W>
where
    F: Formats,
    W: FnOnce(&mut ParamWriter<'_>),
{
    ParamsWith(formats, write)
}

/// Like [`params_with`], for encoders that can fail. Pass the result to
/// [`try_bind`](crate::Frontend::try_bind), which leaves the buffer untouched
/// on error.
pub fn try_params_with<F, W, E>(formats: F, write: W) -> TryParamsWith<F, W>
where
    F: Formats,
    W: FnOnce(&mut ParamWriter<'_>) -> Result<(), E>,
{
    TryParamsWith(formats, write)
}

/// Whether an encoder produced SQL NULL instead of a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsNull {
    Yes,
    No,
}

/// Writes parameter values one at a time, in order.
pub struct ParamWriter<'a> {
    buf: &'a mut Vec<u8>,
    count: u16,
}

impl fmt::Debug for ParamWriter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("ParamWriter");
        debug.field("count", &self.count).finish_non_exhaustive()
    }
}

impl ParamWriter<'_> {
    /// Writes the value count and the values, in that order on the wire.
    #[inline]
    fn run<E>(
        buf: &mut Vec<u8>,
        write: impl FnOnce(&mut ParamWriter<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        let count_at = buf.len();
        buf.extend_from_slice(&[0; 2]);

        let mut writer = ParamWriter {
            buf: &mut *buf,
            count: 0,
        };
        write(&mut writer)?;
        let count = writer.count;
        buf[count_at..count_at + 2].copy_from_slice(&count.to_be_bytes());
        Ok(())
    }

    /// Writes SQL NULL.
    #[inline]
    pub fn null(&mut self) {
        self.begin();
    }

    /// Writes a value that is already encoded.
    #[inline]
    pub fn bytes(&mut self, value: &[u8]) {
        self.write(|buf| buf.extend_from_slice(value))
    }

    /// Writes a [`Param`]. Its format must match the one declared for it.
    #[inline]
    pub fn param(&mut self, value: &(impl Param + ?Sized)) {
        let at = self.begin();
        let null = value.encode(self.buf);
        self.finish(at, null);
    }

    /// Writes whatever `encode` appends to the buffer as one value.
    #[inline]
    pub fn write(&mut self, encode: impl FnOnce(&mut Vec<u8>)) {
        let at = self.begin();
        encode(self.buf);
        self.end(at);
    }

    /// Like [`write`](Self::write), for an encoder that can fail or produce
    /// NULL.
    #[inline]
    pub fn try_write<E>(
        &mut self,
        encode: impl FnOnce(&mut Vec<u8>) -> Result<IsNull, E>,
    ) -> Result<(), E> {
        let at = self.begin();
        let null = encode(self.buf)?;
        self.finish(at, null);
        Ok(())
    }

    /// Ends the value begun at `at`: what was written since, or NULL.
    #[inline]
    fn finish(&mut self, at: usize, null: IsNull) {
        match null {
            IsNull::No => self.end(at),
            IsNull::Yes => self.buf.truncate(at + 4),
        }
    }

    /// Starts a value as NULL, returning where its length lives.
    #[inline]
    fn begin(&mut self) -> usize {
        let count = self.count.checked_add(1);
        self.count = count.expect("the protocol allows at most 65535 parameters");
        let at = self.buf.len();
        self.buf.extend_from_slice(&(-1i32).to_be_bytes());
        at
    }

    /// Records the length of the value written since [`begin`](Self::begin).
    #[inline]
    fn end(&mut self, at: usize) {
        let len = self.buf.len() - at - 4;
        let len = i32::try_from(len).expect("a parameter value is limited to 2 GiB");
        self.buf[at..at + 4].copy_from_slice(&len.to_be_bytes());
    }
}

/// A value that knows how to send itself as a parameter.
///
/// It is implemented for the integer and float types, `bool`, strings, byte
/// strings and byte arrays, `Option` for NULL, and references to any of them.
/// Implement it for your own types, or write values without it using
/// [`params_with`].
pub trait Param {
    /// The format [`encode`](Self::encode) writes.
    fn format(&self) -> Format;

    /// Appends the value in its wire representation, without a length.
    fn encode(&self, buf: &mut Vec<u8>) -> IsNull;
}

/// Builds a parameter list from values that implement [`Param`].
///
/// ```
/// use pg_stream::{Format, Frontend, params};
///
/// let mut buf = Vec::new();
/// buf.bind("", "", params![42i32, "hello", None::<i64>], Format::Text);
/// ```
///
/// The list borrows its values, so build it in the call that uses it.
#[macro_export]
macro_rules! params {
    ($($value:expr),* $(,)?) => {
        &[$(&$value as &dyn $crate::frontend::Param),*] as &[&dyn $crate::frontend::Param]
    };
}

macro_rules! binary_param {
    ($($ty:ty),*) => {$(
        impl Param for $ty {
            fn format(&self) -> Format {
                Format::Binary
            }

            fn encode(&self, buf: &mut Vec<u8>) -> IsNull {
                buf.extend_from_slice(&self.to_be_bytes());
                IsNull::No
            }
        }
    )*};
}

binary_param!(i16, i32, i64, f32, f64);

impl Param for bool {
    fn format(&self) -> Format {
        Format::Binary
    }

    fn encode(&self, buf: &mut Vec<u8>) -> IsNull {
        buf.push(*self as u8);
        IsNull::No
    }
}

impl Param for str {
    fn format(&self) -> Format {
        Format::Text
    }

    fn encode(&self, buf: &mut Vec<u8>) -> IsNull {
        buf.extend_from_slice(self.as_bytes());
        IsNull::No
    }
}

impl Param for [u8] {
    fn format(&self) -> Format {
        Format::Binary
    }

    fn encode(&self, buf: &mut Vec<u8>) -> IsNull {
        buf.extend_from_slice(self);
        IsNull::No
    }
}

macro_rules! deref_param {
    ($($ty:ty => $target:ty),*) => {$(
        impl Param for $ty {
            fn format(&self) -> Format {
                <$target as Param>::format(self)
            }

            fn encode(&self, buf: &mut Vec<u8>) -> IsNull {
                <$target as Param>::encode(self, buf)
            }
        }
    )*};
}

deref_param!(String => str, Vec<u8> => [u8], bytes::Bytes => [u8]);

impl<const N: usize> Param for [u8; N] {
    fn format(&self) -> Format {
        self.as_slice().format()
    }

    fn encode(&self, buf: &mut Vec<u8>) -> IsNull {
        self.as_slice().encode(buf)
    }
}

impl<T: Param + ?Sized> Param for &T {
    fn format(&self) -> Format {
        (**self).format()
    }

    fn encode(&self, buf: &mut Vec<u8>) -> IsNull {
        (**self).encode(buf)
    }
}

impl<T: Param> Param for Option<T> {
    fn format(&self) -> Format {
        self.as_ref().map(Param::format).unwrap_or_default()
    }

    fn encode(&self, buf: &mut Vec<u8>) -> IsNull {
        self.as_ref().map_or(IsNull::Yes, |value| value.encode(buf))
    }
}
