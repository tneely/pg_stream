//! Zero-copy wrappers for backend message types.

use std::borrow::Cow;
use std::ops::Range;

use bytes::Bytes;

use crate::message::FormatCode;

/// A zero-copy wrapper for a DataRow message.
///
/// DataRow messages contain the values of a single row returned by a query.
/// This wrapper provides efficient access to column data without allocations.
#[derive(Debug, Clone)]
pub struct DataRow {
    pub(super) body: Bytes,
    pub(super) column_count: u16,
}

impl DataRow {
    /// Returns the number of columns in this row.
    #[inline]
    pub fn column_count(&self) -> u16 {
        self.column_count
    }

    /// Iterates the column values in a single O(n) pass, yielding `Some(bytes)`
    /// for a present value or `None` for SQL NULL. Prefer this over calling
    /// [`column`](Self::column) in a loop, which walks the row each time.
    #[inline]
    pub fn iter(&self) -> DataRowIter<'_> {
        DataRowIter {
            body: &self.body,
            offset: 2, // skip column count
            remaining: self.column_count,
        }
    }

    /// Returns the column at `idx`, or `None` if NULL or out of bounds. Walks
    /// from the start; use [`iter`](Self::iter) to read every column in one pass.
    #[inline]
    pub fn column(&self, idx: usize) -> Option<&[u8]> {
        self.iter().nth(idx).flatten()
    }

    /// Returns `true` if the column at the given index is NULL.
    ///
    /// Returns `false` if the index is out of bounds.
    #[inline]
    pub fn is_null(&self, idx: usize) -> bool {
        matches!(self.iter().nth(idx), Some(None))
    }
}

impl<'a> IntoIterator for &'a DataRow {
    type Item = Option<&'a [u8]>;
    type IntoIter = DataRowIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over the columns of a [`DataRow`], yielding `None` for SQL NULL.
#[derive(Debug, Clone)]
pub struct DataRowIter<'a> {
    body: &'a [u8],
    offset: usize,
    remaining: u16,
}

impl<'a> Iterator for DataRowIter<'a> {
    type Item = Option<&'a [u8]>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        // One slice-and-split per column: the length prefix and the value are
        // read through a single bounds check instead of five indexing ops.
        let Some((len_bytes, values)) = self
            .body
            .get(self.offset..)
            .and_then(|rest| rest.split_at_checked(4))
        else {
            self.remaining = 0;
            return None;
        };
        let len = i32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
        self.offset += 4;
        self.remaining -= 1;

        if len < 0 {
            return Some(None); // NULL
        }
        match values.get(..len as usize) {
            Some(col) => {
                self.offset += len as usize;
                Some(Some(col))
            }
            None => {
                self.remaining = 0;
                Some(None)
            }
        }
    }

    // A truncated body stops iteration early, so the declared column count is
    // only an upper bound.
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.remaining as usize))
    }
}

/// A zero-copy wrapper for a RowDescription message.
///
/// RowDescription messages describe the columns of a query result.
#[derive(Clone)]
pub struct RowDescription {
    pub(super) body: Bytes,
    /// Ranges for each column name (excluding null terminator).
    /// The fixed data starts at `range.end + 1`.
    pub(super) column_names: Vec<Range<usize>>,
}

impl std::fmt::Debug for RowDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowDescription")
            .field("column_count", &self.column_count())
            .finish_non_exhaustive()
    }
}

impl RowDescription {
    /// Returns the number of columns.
    #[inline]
    pub fn column_count(&self) -> u16 {
        self.column_names.len() as u16
    }

    /// Returns the column name at the given index.
    #[inline]
    pub fn column_name(&self, idx: usize) -> Option<Cow<'_, str>> {
        let range = self.column_names.get(idx)?;
        Some(String::from_utf8_lossy(&self.body[range.clone()]))
    }

    /// Returns the table OID for the column at the given index.
    #[inline]
    pub fn table_oid(&self, idx: usize) -> Option<u32> {
        let fixed_offset = self.fixed_data_offset(idx)?;
        Some(u32::from_be_bytes([
            self.body[fixed_offset],
            self.body[fixed_offset + 1],
            self.body[fixed_offset + 2],
            self.body[fixed_offset + 3],
        ]))
    }

    /// Returns the column ID (attribute number) at the given index.
    #[inline]
    pub fn column_id(&self, idx: usize) -> Option<u16> {
        let fixed_offset = self.fixed_data_offset(idx)?;
        Some(u16::from_be_bytes([
            self.body[fixed_offset + 4],
            self.body[fixed_offset + 5],
        ]))
    }

    /// Returns the type OID for the column at the given index.
    #[inline]
    pub fn type_oid(&self, idx: usize) -> Option<u32> {
        let fixed_offset = self.fixed_data_offset(idx)?;
        Some(u32::from_be_bytes([
            self.body[fixed_offset + 6],
            self.body[fixed_offset + 7],
            self.body[fixed_offset + 8],
            self.body[fixed_offset + 9],
        ]))
    }

    /// Returns the type size for the column at the given index.
    #[inline]
    pub fn type_size(&self, idx: usize) -> Option<i16> {
        let fixed_offset = self.fixed_data_offset(idx)?;
        Some(i16::from_be_bytes([
            self.body[fixed_offset + 10],
            self.body[fixed_offset + 11],
        ]))
    }

    /// Returns the type modifier for the column at the given index.
    #[inline]
    pub fn type_modifier(&self, idx: usize) -> Option<i32> {
        let fixed_offset = self.fixed_data_offset(idx)?;
        Some(i32::from_be_bytes([
            self.body[fixed_offset + 12],
            self.body[fixed_offset + 13],
            self.body[fixed_offset + 14],
            self.body[fixed_offset + 15],
        ]))
    }

    /// Returns the format code for the column at the given index.
    #[inline]
    pub fn format(&self, idx: usize) -> Option<FormatCode> {
        let fixed_offset = self.fixed_data_offset(idx)?;
        let code = u16::from_be_bytes([self.body[fixed_offset + 16], self.body[fixed_offset + 17]]);
        Some(if code == 0 {
            FormatCode::Text
        } else {
            FormatCode::Binary
        })
    }

    /// Returns the offset to the fixed-size data (after the null-terminated name)
    #[inline]
    fn fixed_data_offset(&self, idx: usize) -> Option<usize> {
        let range = self.column_names.get(idx)?;
        Some(range.end + 1) // +1 to skip the null terminator
    }
}

/// A zero-copy wrapper for a CommandComplete message.
///
/// CommandComplete messages indicate the completion of a SQL command and include
/// a tag describing the command and any affected row counts.
#[derive(Clone)]
pub struct CommandComplete {
    pub(super) body: Bytes,
    /// Length of the tag (excluding null terminator)
    pub(super) tag_len: usize,
}

impl std::fmt::Debug for CommandComplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandComplete")
            .field("tag", &self.tag())
            .finish()
    }
}

impl CommandComplete {
    /// Returns the command tag (e.g., "SELECT 5", "INSERT 0 1", "UPDATE 3").
    #[inline]
    pub fn tag(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[..self.tag_len])
    }

    /// Extracts the number of rows affected from the command tag.
    ///
    /// Returns `None` if the command doesn't have a row count (e.g., CREATE TABLE).
    ///
    /// # Examples
    ///
    /// - "SELECT 5" → `Some(5)`
    /// - "INSERT 0 1" → `Some(1)`
    /// - "UPDATE 3" → `Some(3)`
    /// - "DELETE 2" → `Some(2)`
    /// - "CREATE TABLE" → `None`
    #[inline]
    pub fn rows_affected(&self) -> Option<u64> {
        let tag = self.tag();
        tag.split_whitespace().last().and_then(|s| s.parse().ok())
    }
}

/// A zero-copy wrapper for a NotificationResponse message.
///
/// NotificationResponse messages are sent when a NOTIFY is triggered
/// on a channel the connection is listening to.
#[derive(Clone)]
pub struct NotificationResponse {
    pub(super) body: Bytes,
    pub(super) channel: Range<usize>,
    pub(super) payload: Range<usize>,
}

impl std::fmt::Debug for NotificationResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NotificationResponse")
            .field("process_id", &self.process_id())
            .field("channel", &self.channel())
            .field("payload", &self.payload())
            .finish()
    }
}

impl NotificationResponse {
    /// Returns the process ID of the notifying backend.
    #[inline]
    pub fn process_id(&self) -> u32 {
        u32::from_be_bytes([self.body[0], self.body[1], self.body[2], self.body[3]])
    }

    /// Returns the name of the notification channel.
    #[inline]
    pub fn channel(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[self.channel.clone()])
    }

    /// Returns the notification payload.
    #[inline]
    pub fn payload(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[self.payload.clone()])
    }
}

/// A zero-copy wrapper for a ParameterDescription message.
///
/// ParameterDescription messages describe the parameter types of a prepared statement.
#[derive(Debug, Clone)]
pub struct ParameterDescription {
    pub(super) body: Bytes,
    pub(super) param_count: u16,
}

impl ParameterDescription {
    /// Returns the number of parameters.
    #[inline]
    pub fn param_count(&self) -> u16 {
        self.param_count
    }

    /// Returns the OID of the parameter type at the given index.
    #[inline]
    pub fn param_oid(&self, idx: usize) -> Option<u32> {
        if idx >= self.param_count as usize {
            return None;
        }

        let offset = 2 + idx * 4;
        if offset + 4 > self.body.len() {
            return None;
        }

        Some(u32::from_be_bytes([
            self.body[offset],
            self.body[offset + 1],
            self.body[offset + 2],
            self.body[offset + 3],
        ]))
    }
}

/// A zero-copy wrapper for an ErrorResponse message.
///
/// ErrorResponse messages indicate an error occurred during query processing.
/// The message contains multiple fields describing the error.
///
/// Required fields (parsing fails if missing): severity (S), severity_non_localized (V),
/// code (C), and message (M).
#[derive(Clone)]
pub struct ErrorResponse {
    pub(super) body: Bytes,
    // Required fields
    pub(super) local_severity: Range<usize>, // S - always present
    pub(super) severity: Range<usize>,       // V - always present
    pub(super) code: Range<usize>,           // C - always present
    pub(super) message: Range<usize>,        // M - always present
    // Optional fields
    pub(super) detail: Option<Range<usize>>,            // D
    pub(super) hint: Option<Range<usize>>,              // H
    pub(super) position: Option<Range<usize>>,          // P
    pub(super) internal_position: Option<Range<usize>>, // p
    pub(super) internal_query: Option<Range<usize>>,    // q
    pub(super) r#where: Option<Range<usize>>,           // W
    pub(super) schema: Option<Range<usize>>,            // s
    pub(super) table: Option<Range<usize>>,             // t
    pub(super) column: Option<Range<usize>>,            // c
    pub(super) datatype: Option<Range<usize>>,          // d
    pub(super) constraint: Option<Range<usize>>,        // n
    pub(super) file: Option<Range<usize>>,              // F
    pub(super) line: Option<Range<usize>>,              // L
    pub(super) routine: Option<Range<usize>>,           // R
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}: {}",
            self.local_severity(),
            self.code(),
            self.message()
        )
    }
}

impl std::fmt::Debug for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorResponse")
            .field("local_severity", &self.local_severity())
            .field("severity", &self.severity())
            .field("code", &self.code())
            .field("message", &self.message())
            .field("detail", &self.detail())
            .field("hint", &self.hint())
            .field("position", &self.position())
            .field("where", &self.r#where())
            .field("file", &self.file())
            .field("line", &self.line())
            .field("routine", &self.routine())
            .finish_non_exhaustive()
    }
}

impl ErrorResponse {
    fn optional_field(&self, range: &Option<Range<usize>>) -> Option<Cow<'_, str>> {
        range
            .as_ref()
            .map(|r| String::from_utf8_lossy(&self.body[r.start..r.end]))
    }

    /// Returns the localized severity (e.g., "ERROR", "FATAL", "WARNING").
    #[inline]
    pub fn local_severity(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[self.local_severity.clone()])
    }

    /// Returns the severity in a non-localized form (e.g., "ERROR", "FATAL").
    #[inline]
    pub fn severity(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[self.severity.clone()])
    }

    /// Returns the SQLSTATE error code.
    #[inline]
    pub fn code(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[self.code.clone()])
    }

    /// Returns the primary human-readable error message.
    #[inline]
    pub fn message(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[self.message.clone()])
    }

    /// Returns additional detail about the error.
    #[inline]
    pub fn detail(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.detail)
    }

    /// Returns a hint for fixing the error.
    #[inline]
    pub fn hint(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.hint)
    }

    /// Returns the cursor position where the error occurred.
    #[inline]
    pub fn position(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.position)
    }

    /// Returns the cursor position in the internal query where the error occurred.
    #[inline]
    pub fn internal_position(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.internal_position)
    }

    /// Returns the internal query that caused the error.
    #[inline]
    pub fn internal_query(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.internal_query)
    }

    /// Returns the context/call stack where the error occurred.
    #[inline]
    pub fn r#where(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.r#where)
    }

    /// Returns the schema name related to the error.
    #[inline]
    pub fn schema(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.schema)
    }

    /// Returns the table name related to the error.
    #[inline]
    pub fn table(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.table)
    }

    /// Returns the column name related to the error.
    #[inline]
    pub fn column(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.column)
    }

    /// Returns the data type name related to the error.
    #[inline]
    pub fn datatype(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.datatype)
    }

    /// Returns the constraint name related to the error.
    #[inline]
    pub fn constraint(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.constraint)
    }

    /// Returns the source file where the error was reported.
    #[inline]
    pub fn file(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.file)
    }

    /// Returns the source line number where the error was reported.
    #[inline]
    pub fn line(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.line)
    }

    /// Returns the source routine name where the error was reported.
    #[inline]
    pub fn routine(&self) -> Option<Cow<'_, str>> {
        self.optional_field(&self.routine)
    }
}

/// A NoticeResponse has the same format as ErrorResponse.
pub type NoticeResponse = ErrorResponse;

/// A wrapper for a ReadyForQuery message.
///
/// ReadyForQuery indicates the backend is ready for a new query cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadyForQuery {
    pub(super) status: TransactionStatus,
}

impl ReadyForQuery {
    /// Returns the current transaction status.
    #[inline]
    pub fn status(&self) -> TransactionStatus {
        self.status
    }
}

/// The transaction status reported in ReadyForQuery messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    /// Not in a transaction block.
    Idle,
    /// In a transaction block.
    InTransaction,
    /// In a failed transaction block.
    Failed,
}

/// A zero-copy wrapper for a BackendKeyData message: the process ID and secret
/// key needed for cancellation requests. Protocol 3.2 widened the secret key
/// from four bytes to a variable-length field.
#[derive(Clone, PartialEq, Eq)]
pub struct BackendKeyData {
    pub(super) body: Bytes,
}

impl std::fmt::Debug for BackendKeyData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendKeyData")
            .field("process_id", &self.process_id())
            .field("secret_key_len", &self.secret_key().len())
            .finish_non_exhaustive()
    }
}

impl BackendKeyData {
    /// Shortest secret key the protocol allows (the fixed size before 3.2).
    pub const MIN_SECRET_KEY_LEN: usize = 4;

    /// Longest secret key the protocol allows. PostgreSQL sends at most 32
    /// bytes; the rest of the range is reserved for poolers and middleware.
    pub const MAX_SECRET_KEY_LEN: usize = 256;

    /// Returns the backend process ID.
    #[inline]
    pub fn process_id(&self) -> u32 {
        u32::from_be_bytes([self.body[0], self.body[1], self.body[2], self.body[3]])
    }

    /// Returns the secret key for cancellation.
    #[inline]
    pub fn secret_key(&self) -> &[u8] {
        &self.body[4..]
    }
}

/// A zero-copy wrapper for a ParameterStatus message.
///
/// ParameterStatus reports a runtime parameter value.
#[derive(Clone)]
pub struct ParameterStatus {
    pub(super) body: Bytes,
    pub(super) name: Range<usize>,
    pub(super) value: Range<usize>,
}

impl std::fmt::Debug for ParameterStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParameterStatus")
            .field("name", &self.name())
            .field("value", &self.value())
            .finish()
    }
}

impl ParameterStatus {
    /// Returns the parameter name.
    #[inline]
    pub fn name(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[self.name.clone()])
    }

    /// Returns the parameter value.
    #[inline]
    pub fn value(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.body[self.value.clone()])
    }
}

/// A typed Authentication request from the server. SASL/GSS variants carry the
/// raw mechanism payload for the caller or handshake to interpret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authentication {
    /// Authentication succeeded.
    Ok,
    /// Kerberos V5 authentication is required (obsolete).
    KerberosV5,
    /// A cleartext password is required.
    CleartextPassword,
    /// An MD5-hashed password is required, salted with these 4 bytes.
    Md5Password { salt: [u8; 4] },
    /// GSSAPI authentication is required.
    Gss,
    /// A GSSAPI/SSPI continuation with server data.
    GssContinue(Bytes),
    /// SSPI authentication is required.
    Sspi,
    /// SASL authentication is required; body is the nul-separated mechanism list.
    Sasl(Bytes),
    /// A SASL challenge with server data.
    SaslContinue(Bytes),
    /// The final SASL server message.
    SaslFinal(Bytes),
    /// An authentication request type not modeled here; raw code and body.
    Unknown { code: u32, body: Bytes },
}

impl Authentication {
    /// Returns `true` if this is [`Authentication::Ok`].
    #[inline]
    pub fn is_ok(&self) -> bool {
        matches!(self, Authentication::Ok)
    }

    /// For [`Authentication::Sasl`], iterates the offered mechanism names.
    #[inline]
    pub fn sasl_mechanisms(&self) -> impl Iterator<Item = &str> {
        let body = match self {
            Authentication::Sasl(body) => body.as_ref(),
            _ => &[][..],
        };
        body.split(|&b| b == 0)
            .filter(|m| !m.is_empty())
            .filter_map(|m| std::str::from_utf8(m).ok())
    }
}

/// A zero-copy wrapper for a FunctionCallResponse message.
#[derive(Clone)]
pub struct FunctionCallResponse {
    pub(super) body: Bytes,
}

impl std::fmt::Debug for FunctionCallResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionCallResponse")
            .field("value", &self.value())
            .finish()
    }
}

impl FunctionCallResponse {
    /// Returns the result value, or `None` if the function returned NULL.
    #[inline]
    pub fn value(&self) -> Option<&[u8]> {
        if self.body.len() < 4 {
            return None;
        }
        let len = i32::from_be_bytes([self.body[0], self.body[1], self.body[2], self.body[3]]);
        if len < 0 {
            return None;
        }
        let len = len as usize;
        self.body.get(4..4 + len)
    }
}

/// A zero-copy wrapper for a NegotiateProtocolVersion message.
///
/// Sent when the server supports a lower minor protocol version than requested,
/// or does not recognize some startup options.
#[derive(Clone)]
pub struct NegotiateProtocolVersion {
    pub(super) body: Bytes,
    pub(super) option_count: u32,
}

impl std::fmt::Debug for NegotiateProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NegotiateProtocolVersion")
            .field("minor_version", &self.minor_version())
            .field(
                "unrecognized_options",
                &self.unrecognized_options().collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl NegotiateProtocolVersion {
    /// The newest minor protocol version supported by the server.
    #[inline]
    pub fn minor_version(&self) -> u32 {
        u32::from_be_bytes([self.body[0], self.body[1], self.body[2], self.body[3]])
    }

    /// The number of startup options the server did not recognize.
    #[inline]
    pub fn option_count(&self) -> u32 {
        self.option_count
    }

    /// Iterates the names of startup options the server did not recognize.
    #[inline]
    pub fn unrecognized_options(&self) -> impl Iterator<Item = Cow<'_, str>> {
        self.body[8..]
            .split(|&b| b == 0)
            .filter(|s| !s.is_empty())
            .take(self.option_count as usize)
            .map(String::from_utf8_lossy)
    }
}

/// A zero-copy wrapper for Copy response messages (CopyInResponse, CopyOutResponse, CopyBothResponse).
#[derive(Debug, Clone)]
pub struct CopyResponse {
    pub(super) body: Bytes,
    pub(super) column_count: u16,
}

impl CopyResponse {
    /// Returns the overall copy format.
    #[inline]
    pub fn format(&self) -> FormatCode {
        if self.body[0] == 0 {
            FormatCode::Text
        } else {
            FormatCode::Binary
        }
    }

    /// Returns the number of columns.
    #[inline]
    pub fn column_count(&self) -> u16 {
        self.column_count
    }

    /// Returns the format code for a specific column.
    #[inline]
    pub fn column_format(&self, idx: usize) -> Option<FormatCode> {
        if idx >= self.column_count as usize {
            return None;
        }
        let offset = 3 + idx * 2;
        if offset + 2 > self.body.len() {
            return None;
        }
        let code = u16::from_be_bytes([self.body[offset], self.body[offset + 1]]);
        Some(if code == 0 {
            FormatCode::Text
        } else {
            FormatCode::Binary
        })
    }
}
