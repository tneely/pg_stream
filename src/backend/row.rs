//! Row data and row metadata.

use std::fmt;

use super::{
    code,
    cursor::{Cursor, text},
};
use crate::{Format, Oid, ProtocolError};

/// The length of the count that a DataRow or RowDescription begins with.
const COUNT_LEN: usize = 2;

/// One row of a result set.
///
/// The layout is checked when the row is parsed, so iteration cannot fail and
/// always yields exactly [`len`](Self::len) values.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DataRow<'a> {
    len: u16,
    body: &'a [u8],
}

impl<'a> DataRow<'a> {
    /// Parses a DataRow body, for rows carried around as an owned
    /// [`Frame`](super::Frame) or kept from [`as_bytes`](Self::as_bytes).
    #[inline]
    pub fn parse(body: &'a [u8]) -> Result<Self, ProtocolError> {
        const CODE: u8 = code::DATA_ROW;
        Self::try_parse(body).ok_or(ProtocolError::Malformed { code: CODE })
    }

    #[inline]
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let len = cursor.u16()?;
        for _ in 0..len {
            next_value(&mut cursor.0)?;
        }
        cursor.is_empty().then_some(Self { len, body })
    }

    /// The raw message body: the number of values, then each value behind
    /// its length. Copy it to keep a row for longer than the read buffer
    /// does.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.body
    }

    /// The number of columns.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The column values in order; `None` is SQL NULL.
    #[inline]
    pub fn iter(&self) -> Values<'a> {
        let (rest, remaining) = (&self.body[COUNT_LEN..], self.len);
        Values { rest, remaining }
    }

    /// The value of one column. This walks the row from the start, so prefer
    /// [`iter`](Self::iter) when reading more than one.
    pub fn get(&self, index: usize) -> Option<Option<&'a [u8]>> {
        self.iter().nth(index)
    }
}

impl<'a> IntoIterator for DataRow<'a> {
    type Item = Option<&'a [u8]>;
    type IntoIter = Values<'a>;

    fn into_iter(self) -> Values<'a> {
        self.iter()
    }
}

impl<'a> IntoIterator for &DataRow<'a> {
    type Item = Option<&'a [u8]>;
    type IntoIter = Values<'a>;

    fn into_iter(self) -> Values<'a> {
        self.iter()
    }
}

impl fmt::Debug for DataRow<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// Splits one length-prefixed value off the front of `rest`.
#[inline]
fn next_value<'a>(rest: &mut &'a [u8]) -> Option<Option<&'a [u8]>> {
    let (len, tail) = rest.split_first_chunk::<4>()?;
    let (value, tail) = match i32::from_be_bytes(*len) {
        -1 => (None, tail),
        len => {
            let (value, tail) = tail.split_at_checked(usize::try_from(len).ok()?)?;
            (Some(value), tail)
        }
    };
    *rest = tail;
    Some(value)
}

/// The column values of a [`DataRow`].
#[derive(Clone)]
pub struct Values<'a> {
    rest: &'a [u8],
    remaining: u16,
}

impl<'a> Iterator for Values<'a> {
    type Item = Option<&'a [u8]>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.remaining = self.remaining.checked_sub(1)?;
        next_value(&mut self.rest)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining as usize, Some(self.remaining as usize))
    }
}

impl ExactSizeIterator for Values<'_> {}

impl fmt::Debug for Values<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

/// The columns of a result set.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct RowDescription<'a> {
    len: u16,
    body: &'a [u8],
}

impl<'a> RowDescription<'a> {
    /// Parses a RowDescription body, such as one kept from
    /// [`as_bytes`](Self::as_bytes).
    pub fn parse(body: &'a [u8]) -> Result<Self, ProtocolError> {
        const CODE: u8 = code::ROW_DESCRIPTION;
        Self::try_parse(body).ok_or(ProtocolError::Malformed { code: CODE })
    }

    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let len = cursor.u16()?;
        // Only the layout is checked here. A name is decoded when its field
        // is read, which cannot fail.
        for _ in 0..len {
            cursor.cstr()?;
            let [.., a, b] = cursor.array::<FIELD_TAIL_LEN>()?;
            Format::from_code(u16::from_be_bytes([a, b]))?;
        }
        cursor.is_empty().then_some(Self { len, body })
    }

    /// The raw message body: the number of columns, then each column's name
    /// and attributes. Copy it to keep a description for longer than the
    /// read buffer does.
    pub fn as_bytes(&self) -> &'a [u8] {
        self.body
    }

    /// The number of columns.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The columns in order.
    pub fn iter(&self) -> Fields<'a> {
        let (rest, remaining) = (&self.body[COUNT_LEN..], self.len);
        Fields { rest, remaining }
    }
}

impl<'a> IntoIterator for RowDescription<'a> {
    type Item = Field<'a>;
    type IntoIter = Fields<'a>;

    fn into_iter(self) -> Fields<'a> {
        self.iter()
    }
}

impl fmt::Debug for RowDescription<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// One column of a [`RowDescription`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Field<'a> {
    pub name: &'a str,
    /// The table the column comes from, or 0.
    pub table_oid: Oid,
    /// The column's attribute number in that table, or 0.
    pub column_id: i16,
    pub type_oid: Oid,
    /// `pg_type.typlen`: negative for variable-width types.
    pub type_size: i16,
    /// `pg_attribute.atttypmod`.
    pub type_modifier: i32,
    /// Always [`Format::Text`] when describing a statement, whose result
    /// formats are not chosen until Bind.
    pub format: Format,
}

/// The length of what follows a field's name: the OID of its table and its
/// column number there, the OID, size and modifier of its type, and last its
/// format code.
const FIELD_TAIL_LEN: usize = 4 + 2 + 4 + 2 + 4 + 2;

fn next_field<'a>(rest: &mut &'a [u8]) -> Option<Field<'a>> {
    let mut cursor = Cursor(rest);
    let name = text(cursor.cstr()?);
    let tail = cursor.array::<FIELD_TAIL_LEN>()?;
    *rest = cursor.0;

    let mut tail = Cursor(&tail);
    Some(Field {
        name,
        table_oid: tail.u32()?,
        column_id: tail.i16()?,
        type_oid: tail.u32()?,
        type_size: tail.i16()?,
        type_modifier: tail.i32()?,
        format: Format::from_code(tail.u16()?)?,
    })
}

/// The columns of a [`RowDescription`].
#[derive(Clone)]
pub struct Fields<'a> {
    rest: &'a [u8],
    remaining: u16,
}

impl<'a> Iterator for Fields<'a> {
    type Item = Field<'a>;

    fn next(&mut self) -> Option<Field<'a>> {
        self.remaining = self.remaining.checked_sub(1)?;
        next_field(&mut self.rest)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining as usize, Some(self.remaining as usize))
    }
}

impl ExactSizeIterator for Fields<'_> {}

impl fmt::Debug for Fields<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.clone()).finish()
    }
}

/// The parameter types of a prepared statement.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ParameterDescription<'a> {
    oids: &'a [[u8; 4]],
}

impl<'a> ParameterDescription<'a> {
    pub(crate) fn try_parse(body: &'a [u8]) -> Option<Self> {
        let mut cursor = Cursor(body);
        let len = cursor.u16()? as usize;
        let (oids, rest) = cursor.0.as_chunks::<4>();
        (rest.is_empty() && oids.len() == len).then_some(Self { oids })
    }

    /// The number of parameters.
    pub fn len(&self) -> usize {
        self.oids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.oids.is_empty()
    }

    /// The type of each parameter in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Oid> + use<'a> {
        self.oids.iter().map(|&oid| u32::from_be_bytes(oid))
    }
}

impl fmt::Debug for ParameterDescription<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}
