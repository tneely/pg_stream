//! The smaller backend messages.

use std::fmt;

use super::cursor::{Cursor, text};
use crate::Format;

/// The transaction state reported by ReadyForQuery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionStatus {
    /// Not in a transaction block.
    Idle,
    /// In a transaction block.
    InTransaction,
    /// In a failed transaction block; queries are rejected until it ends.
    Failed,
}

impl TransactionStatus {
    pub(crate) fn try_parse(body: &[u8]) -> Option<Self> {
        match body {
            b"I" => Some(TransactionStatus::Idle),
            b"T" => Some(TransactionStatus::InTransaction),
            b"E" => Some(TransactionStatus::Failed),
            _ => None,
        }
    }
}

/// The outcome of one SQL command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandComplete<'a> {
    /// The command tag, e.g. `"SELECT 5"` or `"INSERT 0 1"`.
    pub tag: &'a str,
}

impl<'a> CommandComplete<'a> {
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let tag = text(cursor.cstr()?);
        cursor.is_empty().then_some(Self { tag })
    }

    /// The row count at the end of the tag: rows returned, inserted, updated,
    /// deleted, moved, fetched or copied. `None` for commands without one.
    pub fn rows(&self) -> Option<u64> {
        self.tag.rsplit(' ').next()?.parse().ok()
    }
}

/// A run-time parameter the client should know about, such as
/// `server_version` or `client_encoding`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParameterStatus<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

impl<'a> ParameterStatus<'a> {
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let status = Self {
            name: text(cursor.cstr()?),
            value: text(cursor.cstr()?),
        };
        cursor.is_empty().then_some(status)
    }
}

/// A NOTIFY delivered to a listening session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Notification<'a> {
    /// The backend that sent the notification.
    pub process_id: u32,
    pub channel: &'a str,
    pub payload: &'a str,
}

impl<'a> Notification<'a> {
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let notification = Self {
            process_id: cursor.u32()?,
            channel: text(cursor.cstr()?),
            payload: text(cursor.cstr()?),
        };
        cursor.is_empty().then_some(notification)
    }
}

/// The identity a CancelRequest must present to interrupt this session.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct BackendKeyData<'a> {
    pub process_id: u32,
    /// Four bytes before protocol 3.2, up to 256 from then on.
    pub secret_key: &'a [u8],
}

impl<'a> BackendKeyData<'a> {
    pub(super) const KEY_LEN: std::ops::RangeInclusive<usize> = 4..=256;

    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let key = Self {
            process_id: cursor.u32()?,
            secret_key: cursor.0,
        };
        Self::KEY_LEN.contains(&key.secret_key.len()).then_some(key)
    }

    pub fn to_owned(&self) -> BackendKey {
        BackendKey {
            process_id: self.process_id,
            secret_key: self.secret_key.into(),
        }
    }
}

impl fmt::Debug for BackendKeyData<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("BackendKeyData");
        debug.field("process_id", &self.process_id);
        debug.finish_non_exhaustive()
    }
}

/// An owned [`BackendKeyData`], kept for the life of a session so its queries
/// can be cancelled with [`cancel`](crate::cancel).
#[derive(Clone, PartialEq, Eq)]
pub struct BackendKey {
    pub process_id: u32,
    pub secret_key: Box<[u8]>,
}

impl fmt::Debug for BackendKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("BackendKey");
        debug.field("process_id", &self.process_id);
        debug.finish_non_exhaustive()
    }
}

/// The start of a COPY, in any direction.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CopyResponse<'a> {
    /// The format of the copy as a whole. Text copies are line-oriented.
    pub format: Format,
    column_formats: &'a [[u8; 2]],
}

impl<'a> CopyResponse<'a> {
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let format = Format::from_code(cursor.u8()?.into())?;
        let columns = cursor.u16()? as usize;
        let (column_formats, rest) = cursor.0.as_chunks::<2>();

        let mut codes = column_formats.iter();
        let valid = rest.is_empty()
            && column_formats.len() == columns
            && codes.all(|&code| column_format(code).is_some());
        let copy = Self {
            format,
            column_formats,
        };
        valid.then_some(copy)
    }

    /// The number of columns being copied.
    pub fn len(&self) -> usize {
        self.column_formats.len()
    }

    pub fn is_empty(&self) -> bool {
        self.column_formats.is_empty()
    }

    /// The format of each column in order.
    pub fn column_formats(&self) -> impl ExactSizeIterator<Item = Format> + use<'a> {
        let codes = self.column_formats.iter();
        codes.map(|&code| column_format(code).unwrap_or_default())
    }
}

fn column_format(code: [u8; 2]) -> Option<Format> {
    Format::from_code(u16::from_be_bytes(code))
}

impl fmt::Debug for CopyResponse<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column_formats: Vec<_> = self.column_formats().collect();
        let mut debug = f.debug_struct("CopyResponse");
        debug.field("format", &self.format);
        debug.field("column_formats", &column_formats);
        debug.finish()
    }
}

/// The server's answer to a startup packet asking for a protocol version or
/// protocol options it does not support.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct NegotiateProtocolVersion<'a> {
    /// The newest minor protocol version the server speaks.
    pub minor_version: u32,
    options: &'a [u8],
}

impl<'a> NegotiateProtocolVersion<'a> {
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let minor_version = cursor.u32()?;
        let count = cursor.u32()?;
        let options = cursor.0;

        for _ in 0..count {
            cursor.cstr()?;
        }
        cursor.is_empty().then_some(Self {
            minor_version,
            options,
        })
    }

    /// The protocol options from the startup packet that the server did not
    /// recognize.
    pub fn unrecognized_options(&self) -> impl Iterator<Item = &'a str> + use<'a> {
        let mut cursor = Cursor(self.options);
        std::iter::from_fn(move || cursor.cstr().map(text))
    }
}

impl fmt::Debug for NegotiateProtocolVersion<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("NegotiateProtocolVersion");
        debug.field("minor_version", &self.minor_version);
        let options = self.unrecognized_options().collect::<Vec<_>>();
        debug.field("unrecognized_options", &options).finish()
    }
}
