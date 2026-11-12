//! ErrorResponse and NoticeResponse.

use std::fmt;

use super::cursor::{Cursor, text};
use crate::ServerError;

/// The fields of an ErrorResponse or NoticeResponse.
///
/// The named accessors cover the fields documented in
/// <https://www.postgresql.org/docs/current/protocol-error-fields.html>;
/// [`get`](Self::get) reaches any others by their one-byte tag.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ErrorResponse<'a>(&'a [u8]);

impl<'a> ErrorResponse<'a> {
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        while cursor.u8()? != 0 {
            cursor.cstr()?;
        }
        cursor.is_empty().then_some(Self(body))
    }

    /// Wraps a body that already passed [`try_parse`](Self::try_parse).
    pub(crate) fn from_validated(body: &'a [u8]) -> Self {
        Self(body)
    }

    /// The raw message body: `tag, text, NUL` per field, then a final NUL.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.0
    }

    /// Every field as `(tag, text)`, in the order the server sent them.
    pub fn iter(&self) -> impl Iterator<Item = (u8, &'a str)> + use<'a> {
        let mut cursor = Cursor(self.0);
        std::iter::from_fn(move || match cursor.u8()? {
            0 => None,
            tag => Some((tag, text(cursor.cstr()?))),
        })
    }

    /// The field with the given tag.
    pub fn get(&self, tag: u8) -> Option<&'a str> {
        let mut fields = self.iter();
        fields.find_map(|(t, value)| (t == tag).then_some(value))
    }

    /// `ERROR`, `FATAL`, `PANIC`, `WARNING`, `NOTICE`, `DEBUG`, `INFO` or
    /// `LOG`. Never localized on PostgreSQL 9.6 and later.
    pub fn severity(&self) -> &'a str {
        self.get(b'V').or(self.get(b'S')).unwrap_or_default()
    }

    /// The SQLSTATE code, e.g. `"23505"`.
    pub fn code(&self) -> &'a str {
        self.get(b'C').unwrap_or_default()
    }

    /// The primary human-readable message.
    pub fn message(&self) -> &'a str {
        self.get(b'M').unwrap_or_default()
    }

    pub fn detail(&self) -> Option<&'a str> {
        self.get(b'D')
    }

    pub fn hint(&self) -> Option<&'a str> {
        self.get(b'H')
    }

    /// The 1-based character index into the query string where the error was
    /// detected.
    pub fn position(&self) -> Option<&'a str> {
        self.get(b'P')
    }

    /// Like [`position`](Self::position), but into
    /// [`internal_query`](Self::internal_query).
    pub fn internal_position(&self) -> Option<&'a str> {
        self.get(b'p')
    }

    /// The text of the internally generated command that failed.
    pub fn internal_query(&self) -> Option<&'a str> {
        self.get(b'q')
    }

    /// The call stack in which the error occurred.
    pub fn context(&self) -> Option<&'a str> {
        self.get(b'W')
    }

    pub fn schema(&self) -> Option<&'a str> {
        self.get(b's')
    }

    pub fn table(&self) -> Option<&'a str> {
        self.get(b't')
    }

    pub fn column(&self) -> Option<&'a str> {
        self.get(b'c')
    }

    pub fn data_type(&self) -> Option<&'a str> {
        self.get(b'd')
    }

    pub fn constraint(&self) -> Option<&'a str> {
        self.get(b'n')
    }

    /// The server source file that reported the error.
    pub fn file(&self) -> Option<&'a str> {
        self.get(b'F')
    }

    pub fn line(&self) -> Option<&'a str> {
        self.get(b'L')
    }

    pub fn routine(&self) -> Option<&'a str> {
        self.get(b'R')
    }

    /// Copies the fields into an error that can outlive the read buffer.
    pub fn to_error(&self) -> ServerError {
        ServerError::new(*self)
    }

    /// Whether the server closes the connection after this error.
    pub fn is_fatal(&self) -> bool {
        matches!(self.severity(), "FATAL" | "PANIC")
    }
}

impl fmt::Display for ErrorResponse<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.severity(), self.message())?;
        match self.code() {
            "" => Ok(()),
            code => write!(f, " (SQLSTATE {code})"),
        }
    }
}

impl fmt::Debug for ErrorResponse<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fields = self.iter().map(|(tag, value)| (tag as char, value));
        f.debug_map().entries(fields).finish()
    }
}
