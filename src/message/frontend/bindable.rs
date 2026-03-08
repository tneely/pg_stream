//! Bindable trait for encoding parameter values.

use bytes::BufMut;

use super::types::FormatCode;

/// Trait for types that can be bound as Postgres parameters.
pub trait Bindable {
    /// Returns the format code for this parameter type.
    fn format_code(&self) -> FormatCode;

    /// Returns encoded length including the 4-byte value length prefix.
    ///
    /// Override this for custom types to avoid fallback measurement cost.
    fn encoded_len(&self) -> usize {
        let mut counter = LenCounter::default();
        self.encode(&mut counter);
        counter.len
    }

    /// Encode this value to the buffer (including 4-byte length prefix).
    fn encode(&self, buf: &mut dyn BufMut);
}

impl Bindable for bool {
    fn format_code(&self) -> FormatCode {
        FormatCode::Binary
    }
    fn encoded_len(&self) -> usize {
        5
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
    fn encoded_len(&self) -> usize {
        6
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
    fn encoded_len(&self) -> usize {
        8
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
    fn encoded_len(&self) -> usize {
        12
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
    fn encoded_len(&self) -> usize {
        8
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
    fn encoded_len(&self) -> usize {
        12
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
    fn encoded_len(&self) -> usize {
        4 + self.len()
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
    fn encoded_len(&self) -> usize {
        4 + self.len()
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
    fn encoded_len(&self) -> usize {
        4 + self.len()
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
    fn encoded_len(&self) -> usize {
        4 + self.len()
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
    fn encoded_len(&self) -> usize {
        4 + self.len()
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
    fn encoded_len(&self) -> usize {
        4 + self.len()
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
    fn encoded_len(&self) -> usize {
        match self {
            Some(v) => v.encoded_len(),
            None => 4,
        }
    }
    fn encode(&self, buf: &mut dyn BufMut) {
        match self {
            Some(v) => v.encode(buf),
            None => buf.put_i32(-1),
        }
    }
}

/// Fallback byte counter used by `Bindable::encoded_len`.
struct LenCounter {
    len: usize,
    scratch: [u8; 64],
}

impl Default for LenCounter {
    fn default() -> Self {
        Self {
            len: 0,
            scratch: [0; 64],
        }
    }
}

unsafe impl BufMut for LenCounter {
    fn remaining_mut(&self) -> usize {
        usize::MAX - self.len
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.len = self.len.checked_add(cnt).expect("encoded length overflow");
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        bytes::buf::UninitSlice::new(&mut self.scratch)
    }
}
