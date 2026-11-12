//! A bounds-checked reader over a message body.

/// Reads big-endian fields off the front of a slice. Every method returns
/// `None` rather than reading past the end.
pub(super) struct Cursor<'a>(pub &'a [u8]);

impl<'a> Cursor<'a> {
    pub fn array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    pub fn u8(&mut self) -> Option<u8> {
        self.array::<1>().map(|[b]| b)
    }

    pub fn u16(&mut self) -> Option<u16> {
        self.array().map(u16::from_be_bytes)
    }

    pub fn i16(&mut self) -> Option<i16> {
        self.array().map(i16::from_be_bytes)
    }

    pub fn u32(&mut self) -> Option<u32> {
        self.array().map(u32::from_be_bytes)
    }

    pub fn i32(&mut self) -> Option<i32> {
        self.array().map(i32::from_be_bytes)
    }

    /// Reads up to a NUL terminator and consumes it.
    pub fn cstr(&mut self) -> Option<&'a [u8]> {
        let end = self.0.iter().position(|&b| b == 0)?;
        let (head, rest) = self.0.split_at(end);
        self.0 = &rest[1..];
        Some(head)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Decodes server text. Postgres sends text in the session's
/// `client_encoding`; anything from the first byte that is not valid UTF-8
/// onward is dropped.
pub(super) fn text(bytes: &[u8]) -> &str {
    match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => std::str::from_utf8(&bytes[..e.valid_up_to()]).unwrap_or_default(),
    }
}
