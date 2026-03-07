//! Logic for handling and representing Postgres backend messages.

use std::mem::size_of;

use bytes::{Bytes, BytesMut};

#[cfg(feature = "async")]
use tokio::io::{AsyncRead, AsyncReadExt};

#[cfg(feature = "sync")]
use std::io::Read;

/// Maximum allowed frame size from Postgres (1GiB).
///
/// This is an upper bound to prevent misbehaving servers from
/// allocating excessive memory or causing OOMs.
/// See: <https://github.com/postgres/postgres/blob/879c492480d0e9ad8155c4269f95c5e8add41901/src/include/utils/memutils.h#L40>
const MAX_FRAME_SIZE_BYTES: usize = 1 << 30; // 1GiB

/// Postgres backend messages are framed by a 1-byte message code,
/// followed by a u32 length for the rest of the message body.
///
/// The message code identifies the type of message and the structure
/// of its payload.
///
/// See: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageCode(u8);

impl MessageCode {
    pub const AUTHENTICATION: Self = Self(b'R');
    pub const BACKEND_KEY_DATA: Self = Self(b'K');
    pub const BIND_COMPLETE: Self = Self(b'2');
    pub const CLOSE_COMPLETE: Self = Self(b'3');
    pub const COMMAND_COMPLETE: Self = Self(b'C');
    pub const COPY_DATA: Self = Self(b'd');
    pub const COPY_DONE: Self = Self(b'c');
    pub const COPY_IN_RESPONSE: Self = Self(b'G');
    pub const COPY_OUT_RESPONSE: Self = Self(b'H');
    pub const COPY_BOTH_RESPONSE: Self = Self(b'W');
    pub const DATA_ROW: Self = Self(b'D');
    pub const EMPTY_QUERY_RESPONSE: Self = Self(b'I');
    pub const ERROR_RESPONSE: Self = Self(b'E');
    pub const FUNCTION_CALL_RESPONSE: Self = Self(b'V');
    pub const NEGOTIATE_PROTOCOL_VERSION: Self = Self(b'v');
    pub const NO_DATA: Self = Self(b'n');
    pub const NOTICE_RESPONSE: Self = Self(b'N');
    pub const NOTIFICATION_RESPONSE: Self = Self(b'A');
    pub const PARAMETER_DESCRIPTION: Self = Self(b't');
    pub const PARAMETER_STATUS: Self = Self(b'S');
    pub const PARSE_COMPLETE: Self = Self(b'1');
    pub const PORTAL_SUSPENDED: Self = Self(b's');
    pub const READY_FOR_QUERY: Self = Self(b'Z');
    pub const ROW_DESCRIPTION: Self = Self(b'T');
}

impl From<u8> for MessageCode {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<MessageCode> for u8 {
    fn from(value: MessageCode) -> Self {
        value.0
    }
}

impl PartialEq<u8> for MessageCode {
    fn eq(&self, other: &u8) -> bool {
        self.0 == *other
    }
}

impl PartialEq<MessageCode> for u8 {
    fn eq(&self, other: &MessageCode) -> bool {
        *self == other.0
    }
}

impl std::fmt::Display for MessageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match *self {
            MessageCode::AUTHENTICATION => "Authentication",
            MessageCode::BACKEND_KEY_DATA => "BackendKeyData",
            MessageCode::BIND_COMPLETE => "BindComplete",
            MessageCode::CLOSE_COMPLETE => "CloseComplete",
            MessageCode::COMMAND_COMPLETE => "CommandComplete",
            MessageCode::COPY_DATA => "CopyData",
            MessageCode::COPY_DONE => "CopyDone",
            MessageCode::COPY_IN_RESPONSE => "CopyInResponse",
            MessageCode::COPY_OUT_RESPONSE => "CopyOutResponse",
            MessageCode::COPY_BOTH_RESPONSE => "CopyBothResponse",
            MessageCode::DATA_ROW => "DataRow",
            MessageCode::EMPTY_QUERY_RESPONSE => "EmptyQueryResponse",
            MessageCode::ERROR_RESPONSE => "ErrorResponse",
            MessageCode::FUNCTION_CALL_RESPONSE => "FunctionCallResponse",
            MessageCode::NEGOTIATE_PROTOCOL_VERSION => "NegotiateProtocolVersion",
            MessageCode::NO_DATA => "NoData",
            MessageCode::NOTICE_RESPONSE => "NoticeResponse",
            MessageCode::NOTIFICATION_RESPONSE => "NotificationResponse",
            MessageCode::PARAMETER_DESCRIPTION => "ParameterDescription",
            MessageCode::PARAMETER_STATUS => "ParameterStatus",
            MessageCode::PARSE_COMPLETE => "ParseComplete",
            MessageCode::PORTAL_SUSPENDED => "PortalSuspended",
            MessageCode::READY_FOR_QUERY => "ReadyForQuery",
            MessageCode::ROW_DESCRIPTION => "RowDescription",
            _ => "Unknown",
        };
        write!(f, "{name}({})", self.0 as char)
    }
}

impl std::fmt::Debug for MessageCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MessageCode({})", self.0 as char)
    }
}

/// A single Postgres protocol frame, containing the message code and the message body.
#[derive(Debug, Clone)]
pub struct PgFrame {
    /// The type of the message
    pub code: MessageCode,
    /// The payload of the message
    pub body: Bytes,
}

impl PgFrame {
    /// Constructs a new `PgFrame` with the given message code and body.
    pub fn new(code: impl Into<MessageCode>, body: impl Into<Bytes>) -> Self {
        Self {
            code: code.into(),
            body: body.into(),
        }
    }
}

impl std::fmt::Display for PgFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {:?}", self.code, self.body)
    }
}

/// Reads a single Postgres frame from an asynchronous `AsyncRead` stream.
#[cfg(feature = "async")]
pub async fn read_frame(mut stream: impl AsyncRead + Unpin) -> std::io::Result<PgFrame> {
    let mut buf = [0; 1];
    stream.read_exact(&mut buf).await?;
    let code: MessageCode = u8::from_be_bytes(buf).into();

    let mut buf = [0; 4];
    stream.read_exact(&mut buf).await?;
    let len = u32::from_be_bytes(buf) as usize;

    if len > MAX_FRAME_SIZE_BYTES {
        let err_msg = format!("frame size exceeds {MAX_FRAME_SIZE_BYTES}B");
        return Err(std::io::Error::new(
            std::io::ErrorKind::QuotaExceeded,
            err_msg,
        ));
    }

    // SAFETY: The uninitialized bytes are never read
    let mut body = unsafe { init_body(len - size_of::<u32>())? };
    stream.read_exact(&mut body).await?;

    Ok(PgFrame::new(code, body))
}

/// Reads a single Postgres frame from a synchronous `Read` stream.
#[cfg(feature = "sync")]
pub fn read_frame_sync(mut stream: impl Read) -> std::io::Result<PgFrame> {
    let mut buf = [0; 1];
    stream.read_exact(&mut buf)?;
    let code: MessageCode = u8::from_be_bytes(buf).into();

    let mut buf = [0; 4];
    stream.read_exact(&mut buf)?;
    let len = u32::from_be_bytes(buf) as usize;

    if len > MAX_FRAME_SIZE_BYTES {
        let err_msg = format!("frame size exceeds {MAX_FRAME_SIZE_BYTES}B");
        return Err(std::io::Error::new(
            std::io::ErrorKind::QuotaExceeded,
            err_msg,
        ));
    }

    // SAFETY: The uninitialized bytes are never read
    let mut body = unsafe { init_body(len - size_of::<u32>())? };
    stream.read_exact(&mut body)?;

    Ok(PgFrame::new(code, body))
}

#[inline]
unsafe fn init_body(len: usize) -> std::io::Result<BytesMut> {
    let mut body = BytesMut::with_capacity(len);
    unsafe {
        body.set_len(len);
    }
    Ok(body)
}

/// Reads a null-terminated string from a `Bytes` buffer.
///
/// The returned string excludes the null terminator. Returns an error
/// if no null terminator is found or if the bytes are not valid UTF-8.
pub(crate) fn read_cstring(bytes: &mut Bytes) -> std::io::Result<String> {
    let Some(end) = bytes.iter().position(|&b| b == 0) else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "null terminator missing",
        ));
    };

    let bytes = bytes.split_to(end + 1);
    match String::from_utf8(bytes[..end].to_vec()) {
        Ok(string) => Ok(string),
        Err(err) => Err(std::io::Error::other(err)),
    }
}

// ============================================================================
// Backend Message Parsers (Zero-Copy)
// ============================================================================

use crate::message::FormatCode;

/// Error when parsing a backend message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    kind: ParseErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParseErrorKind {
    WrongMessageCode {
        expected: MessageCode,
        got: MessageCode,
    },
    InvalidData(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ParseErrorKind::WrongMessageCode { expected, got } => {
                write!(f, "wrong message code: expected {expected}, got {got}")
            }
            ParseErrorKind::InvalidData(msg) => write!(f, "invalid data: {msg}"),
        }
    }
}

impl std::error::Error for ParseError {}

impl ParseError {
    fn wrong_code(expected: MessageCode, got: MessageCode) -> Self {
        Self {
            kind: ParseErrorKind::WrongMessageCode { expected, got },
        }
    }

    fn invalid_data(msg: impl Into<String>) -> Self {
        Self {
            kind: ParseErrorKind::InvalidData(msg.into()),
        }
    }
}

/// A zero-copy wrapper for a DataRow message.
///
/// DataRow messages contain the values of a single row returned by a query.
/// This wrapper provides efficient access to column data without allocations.
///
/// # Example
///
/// ```rust,no_run
/// use pg_stream::messages::backend::{DataRow, PgFrame};
///
/// # fn example(frame: PgFrame) -> Result<(), Box<dyn std::error::Error>> {
/// let row = DataRow::try_from(frame)?;
/// println!("Row has {} columns", row.column_count());
///
/// for i in 0..row.column_count() {
///     if let Some(value) = row.column(i as usize) {
///         println!("Column {}: {:?}", i, value);
///     } else {
///         println!("Column {}: NULL", i);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DataRow {
    body: Bytes,
    column_count: u16,
}

impl DataRow {
    /// Returns the number of columns in this row.
    pub fn column_count(&self) -> u16 {
        self.column_count
    }

    /// Returns the value of the column at the given index, or `None` if the value is NULL.
    ///
    /// Returns `None` if the index is out of bounds.
    pub fn column(&self, idx: usize) -> Option<&[u8]> {
        if idx >= self.column_count as usize {
            return None;
        }

        let mut offset = 2; // Skip column count
        for i in 0..=idx {
            if offset + 4 > self.body.len() {
                return None;
            }
            let len = i32::from_be_bytes([
                self.body[offset],
                self.body[offset + 1],
                self.body[offset + 2],
                self.body[offset + 3],
            ]);
            offset += 4;

            if i == idx {
                if len < 0 {
                    return None; // NULL
                }
                let len = len as usize;
                if offset + len > self.body.len() {
                    return None;
                }
                return Some(&self.body[offset..offset + len]);
            }

            if len >= 0 {
                offset += len as usize;
            }
        }
        None
    }

    /// Returns `true` if the column at the given index is NULL.
    ///
    /// Returns `false` if the index is out of bounds (treat as not-NULL for safety).
    pub fn is_null(&self, idx: usize) -> bool {
        if idx >= self.column_count as usize {
            return false;
        }

        let mut offset = 2; // Skip column count
        for i in 0..=idx {
            if offset + 4 > self.body.len() {
                return false;
            }
            let len = i32::from_be_bytes([
                self.body[offset],
                self.body[offset + 1],
                self.body[offset + 2],
                self.body[offset + 3],
            ]);
            offset += 4;

            if i == idx {
                return len < 0;
            }

            if len >= 0 {
                offset += len as usize;
            }
        }
        false
    }
}

impl TryFrom<PgFrame> for DataRow {
    type Error = ParseError;

    fn try_from(frame: PgFrame) -> Result<Self, Self::Error> {
        if frame.code != MessageCode::DATA_ROW {
            return Err(ParseError::wrong_code(MessageCode::DATA_ROW, frame.code));
        }

        if frame.body.len() < 2 {
            return Err(ParseError::invalid_data("DataRow body too short"));
        }

        let column_count = u16::from_be_bytes([frame.body[0], frame.body[1]]);

        Ok(Self {
            body: frame.body,
            column_count,
        })
    }
}

/// A column descriptor from a RowDescription message.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    /// The column name.
    pub name: String,
    /// Object ID of the table (0 if not a table column).
    pub table_oid: u32,
    /// Attribute number of the column (0 if not a table column).
    pub column_id: u16,
    /// Object ID of the column's data type.
    pub type_oid: u32,
    /// Data type size (negative for variable-width types).
    pub type_size: i16,
    /// Type modifier.
    pub type_modifier: i32,
    /// Format code (text or binary).
    pub format: FormatCode,
}

/// A zero-copy wrapper for a RowDescription message.
///
/// RowDescription messages describe the columns of a query result.
///
/// # Example
///
/// ```rust,no_run
/// use pg_stream::messages::backend::{RowDescription, PgFrame};
///
/// # fn example(frame: PgFrame) -> Result<(), Box<dyn std::error::Error>> {
/// let desc = RowDescription::try_from(frame)?;
/// println!("Result has {} columns", desc.column_count());
///
/// for i in 0..desc.column_count() {
///     if let Some(col) = desc.column(i as usize) {
///         println!("Column {}: {} (type OID {})", i, col.name, col.type_oid);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RowDescription {
    columns: Vec<ColumnDesc>,
}

impl RowDescription {
    /// Returns the number of columns.
    pub fn column_count(&self) -> u16 {
        self.columns.len() as u16
    }

    /// Returns the column descriptor at the given index.
    pub fn column(&self, idx: usize) -> Option<&ColumnDesc> {
        self.columns.get(idx)
    }

    /// Returns an iterator over all column descriptors.
    pub fn columns(&self) -> impl Iterator<Item = &ColumnDesc> {
        self.columns.iter()
    }
}

impl TryFrom<PgFrame> for RowDescription {
    type Error = ParseError;

    fn try_from(frame: PgFrame) -> Result<Self, Self::Error> {
        if frame.code != MessageCode::ROW_DESCRIPTION {
            return Err(ParseError::wrong_code(
                MessageCode::ROW_DESCRIPTION,
                frame.code,
            ));
        }

        if frame.body.len() < 2 {
            return Err(ParseError::invalid_data("RowDescription body too short"));
        }

        let column_count = u16::from_be_bytes([frame.body[0], frame.body[1]]) as usize;
        let mut columns = Vec::with_capacity(column_count);
        let mut body = frame.body.slice(2..);

        for _ in 0..column_count {
            let name =
                read_cstring(&mut body).map_err(|e| ParseError::invalid_data(e.to_string()))?;

            if body.len() < 18 {
                return Err(ParseError::invalid_data(
                    "RowDescription column data too short",
                ));
            }

            let table_oid = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            let column_id = u16::from_be_bytes([body[4], body[5]]);
            let type_oid = u32::from_be_bytes([body[6], body[7], body[8], body[9]]);
            let type_size = i16::from_be_bytes([body[10], body[11]]);
            let type_modifier = i32::from_be_bytes([body[12], body[13], body[14], body[15]]);
            let format_code = u16::from_be_bytes([body[16], body[17]]);
            let format = if format_code == 0 {
                FormatCode::Text
            } else {
                FormatCode::Binary
            };

            body = body.slice(18..);

            columns.push(ColumnDesc {
                name,
                table_oid,
                column_id,
                type_oid,
                type_size,
                type_modifier,
                format,
            });
        }

        Ok(Self { columns })
    }
}

/// A zero-copy wrapper for a CommandComplete message.
///
/// CommandComplete messages indicate the completion of a SQL command and include
/// a tag describing the command and any affected row counts.
///
/// # Example
///
/// ```rust,no_run
/// use pg_stream::messages::backend::{CommandComplete, PgFrame};
///
/// # fn example(frame: PgFrame) -> Result<(), Box<dyn std::error::Error>> {
/// let complete = CommandComplete::try_from(frame)?;
/// println!("Command: {}", complete.tag());
/// if let Some(rows) = complete.rows_affected() {
///     println!("Rows affected: {}", rows);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct CommandComplete {
    tag: String,
}

impl CommandComplete {
    /// Returns the command tag (e.g., "SELECT 5", "INSERT 0 1", "UPDATE 3").
    pub fn tag(&self) -> &str {
        &self.tag
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
    pub fn rows_affected(&self) -> Option<u64> {
        // The format varies by command:
        // - SELECT: "SELECT {count}"
        // - INSERT: "INSERT {oid} {count}"
        // - UPDATE/DELETE: "{COMMAND} {count}"
        // - Other: just the command name

        let parts: Vec<&str> = self.tag.split_whitespace().collect();
        parts.last().and_then(|s| s.parse().ok())
    }
}

impl TryFrom<PgFrame> for CommandComplete {
    type Error = ParseError;

    fn try_from(frame: PgFrame) -> Result<Self, Self::Error> {
        if frame.code != MessageCode::COMMAND_COMPLETE {
            return Err(ParseError::wrong_code(
                MessageCode::COMMAND_COMPLETE,
                frame.code,
            ));
        }

        let mut body = frame.body;
        let tag = read_cstring(&mut body).map_err(|e| ParseError::invalid_data(e.to_string()))?;

        Ok(Self { tag })
    }
}

/// A zero-copy wrapper for a NotificationResponse message.
///
/// NotificationResponse messages are sent when a NOTIFY is triggered
/// on a channel the connection is listening to.
///
/// # Example
///
/// ```rust,no_run
/// use pg_stream::messages::backend::{NotificationResponse, PgFrame};
///
/// # fn example(frame: PgFrame) -> Result<(), Box<dyn std::error::Error>> {
/// let notification = NotificationResponse::try_from(frame)?;
/// println!(
///     "Notification from process {}: {} -> {}",
///     notification.process_id(),
///     notification.channel(),
///     notification.payload()
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct NotificationResponse {
    process_id: u32,
    channel: String,
    payload: String,
}

impl NotificationResponse {
    /// Returns the process ID of the notifying backend.
    pub fn process_id(&self) -> u32 {
        self.process_id
    }

    /// Returns the name of the notification channel.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// Returns the notification payload.
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

impl TryFrom<PgFrame> for NotificationResponse {
    type Error = ParseError;

    fn try_from(frame: PgFrame) -> Result<Self, Self::Error> {
        if frame.code != MessageCode::NOTIFICATION_RESPONSE {
            return Err(ParseError::wrong_code(
                MessageCode::NOTIFICATION_RESPONSE,
                frame.code,
            ));
        }

        if frame.body.len() < 4 {
            return Err(ParseError::invalid_data(
                "NotificationResponse body too short",
            ));
        }

        let process_id =
            u32::from_be_bytes([frame.body[0], frame.body[1], frame.body[2], frame.body[3]]);

        let mut body = frame.body.slice(4..);
        let channel =
            read_cstring(&mut body).map_err(|e| ParseError::invalid_data(e.to_string()))?;
        let payload =
            read_cstring(&mut body).map_err(|e| ParseError::invalid_data(e.to_string()))?;

        Ok(Self {
            process_id,
            channel,
            payload,
        })
    }
}

/// A zero-copy wrapper for a ParameterDescription message.
///
/// ParameterDescription messages describe the parameter types of a prepared statement.
///
/// # Example
///
/// ```rust,no_run
/// use pg_stream::messages::backend::{ParameterDescription, PgFrame};
///
/// # fn example(frame: PgFrame) -> Result<(), Box<dyn std::error::Error>> {
/// let desc = ParameterDescription::try_from(frame)?;
/// println!("Statement has {} parameters", desc.param_count());
///
/// for i in 0..desc.param_count() {
///     if let Some(oid) = desc.param_oid(i as usize) {
///         println!("Parameter {}: type OID {}", i, oid);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ParameterDescription {
    body: Bytes,
    param_count: u16,
}

impl ParameterDescription {
    /// Returns the number of parameters.
    pub fn param_count(&self) -> u16 {
        self.param_count
    }

    /// Returns the OID of the parameter type at the given index.
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

impl TryFrom<PgFrame> for ParameterDescription {
    type Error = ParseError;

    fn try_from(frame: PgFrame) -> Result<Self, Self::Error> {
        if frame.code != MessageCode::PARAMETER_DESCRIPTION {
            return Err(ParseError::wrong_code(
                MessageCode::PARAMETER_DESCRIPTION,
                frame.code,
            ));
        }

        if frame.body.len() < 2 {
            return Err(ParseError::invalid_data(
                "ParameterDescription body too short",
            ));
        }

        let param_count = u16::from_be_bytes([frame.body[0], frame.body[1]]);

        Ok(Self {
            body: frame.body,
            param_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use bytes::{BufMut, BytesMut};

    use super::*;

    #[cfg(feature = "async")]
    mod async_tests {
        use super::*;

        #[tokio::test]
        async fn can_read_max_size_frame() {
            let mut buf = BytesMut::new();
            buf.put_u8(42);
            buf.put_u32(MAX_FRAME_SIZE_BYTES as u32);
            let err = read_frame(buf.as_ref()).await.err().unwrap();
            // We only wrote 5 bytes but are trying to read 1 GiB so we'd expect an EoF
            assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        }

        #[tokio::test]
        async fn can_not_read_past_max_size_frame() {
            let mut buf = BytesMut::new();
            buf.put_u8(42);
            buf.put_u32(MAX_FRAME_SIZE_BYTES as u32 + 1);
            let err = read_frame(buf.as_ref()).await.err().unwrap();
            assert_eq!(err.kind(), ErrorKind::QuotaExceeded);
        }
    }

    #[cfg(feature = "sync")]
    mod sync_tests {
        use super::*;
        use crate::messages::backend::read_frame_sync;
        use std::io::Cursor;

        #[test]
        fn can_read_max_size_frame_sync() {
            let mut buf = BytesMut::new();
            buf.put_u8(42);
            buf.put_u32(MAX_FRAME_SIZE_BYTES as u32);
            let err = read_frame_sync(Cursor::new(buf.as_ref())).err().unwrap();
            // We only wrote 5 bytes but are trying to read 1 GiB so we'd expect an EoF
            assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
        }

        #[test]
        fn can_not_read_past_max_size_frame_sync() {
            let mut buf = BytesMut::new();
            buf.put_u8(42);
            buf.put_u32(MAX_FRAME_SIZE_BYTES as u32 + 1);
            let err = read_frame_sync(Cursor::new(buf.as_ref())).err().unwrap();
            assert_eq!(err.kind(), ErrorKind::QuotaExceeded);
        }
    }

    mod parser_tests {
        use super::*;

        #[test]
        fn test_data_row_parse() {
            // Create a DataRow with 2 columns: "hello" (5 bytes) and NULL
            let mut buf = BytesMut::new();
            buf.put_u16(2); // 2 columns
            buf.put_i32(5); // length of first column
            buf.put_slice(b"hello");
            buf.put_i32(-1); // NULL

            let frame = PgFrame::new(MessageCode::DATA_ROW, buf.freeze());
            let row = DataRow::try_from(frame).unwrap();

            assert_eq!(row.column_count(), 2);
            assert_eq!(row.column(0), Some(b"hello".as_slice()));
            assert!(row.column(1).is_none());
            assert!(!row.is_null(0));
            assert!(row.is_null(1));
        }

        #[test]
        fn test_data_row_wrong_code() {
            let frame = PgFrame::new(MessageCode::COMMAND_COMPLETE, Bytes::new());
            let err = DataRow::try_from(frame).unwrap_err();
            assert!(matches!(err.kind, ParseErrorKind::WrongMessageCode { .. }));
        }

        #[test]
        fn test_command_complete_select() {
            let mut buf = BytesMut::new();
            buf.put_slice(b"SELECT 5\0");

            let frame = PgFrame::new(MessageCode::COMMAND_COMPLETE, buf.freeze());
            let cmd = CommandComplete::try_from(frame).unwrap();

            assert_eq!(cmd.tag(), "SELECT 5");
            assert_eq!(cmd.rows_affected(), Some(5));
        }

        #[test]
        fn test_command_complete_insert() {
            let mut buf = BytesMut::new();
            buf.put_slice(b"INSERT 0 1\0");

            let frame = PgFrame::new(MessageCode::COMMAND_COMPLETE, buf.freeze());
            let cmd = CommandComplete::try_from(frame).unwrap();

            assert_eq!(cmd.tag(), "INSERT 0 1");
            assert_eq!(cmd.rows_affected(), Some(1));
        }

        #[test]
        fn test_command_complete_create_table() {
            let mut buf = BytesMut::new();
            buf.put_slice(b"CREATE TABLE\0");

            let frame = PgFrame::new(MessageCode::COMMAND_COMPLETE, buf.freeze());
            let cmd = CommandComplete::try_from(frame).unwrap();

            assert_eq!(cmd.tag(), "CREATE TABLE");
            assert_eq!(cmd.rows_affected(), None);
        }

        #[test]
        fn test_notification_response() {
            let mut buf = BytesMut::new();
            buf.put_u32(12345); // process_id
            buf.put_slice(b"my_channel\0");
            buf.put_slice(b"hello world\0");

            let frame = PgFrame::new(MessageCode::NOTIFICATION_RESPONSE, buf.freeze());
            let notif = NotificationResponse::try_from(frame).unwrap();

            assert_eq!(notif.process_id(), 12345);
            assert_eq!(notif.channel(), "my_channel");
            assert_eq!(notif.payload(), "hello world");
        }

        #[test]
        fn test_parameter_description() {
            let mut buf = BytesMut::new();
            buf.put_u16(2); // 2 parameters
            buf.put_u32(23); // INT4 OID
            buf.put_u32(25); // TEXT OID

            let frame = PgFrame::new(MessageCode::PARAMETER_DESCRIPTION, buf.freeze());
            let desc = ParameterDescription::try_from(frame).unwrap();

            assert_eq!(desc.param_count(), 2);
            assert_eq!(desc.param_oid(0), Some(23));
            assert_eq!(desc.param_oid(1), Some(25));
            assert_eq!(desc.param_oid(2), None);
        }

        #[test]
        fn test_row_description() {
            let mut buf = BytesMut::new();
            buf.put_u16(1); // 1 column
            buf.put_slice(b"id\0"); // column name
            buf.put_u32(0); // table OID
            buf.put_u16(0); // column ID
            buf.put_u32(23); // type OID (INT4)
            buf.put_i16(4); // type size
            buf.put_i32(-1); // type modifier
            buf.put_u16(0); // format code (text)

            let frame = PgFrame::new(MessageCode::ROW_DESCRIPTION, buf.freeze());
            let desc = RowDescription::try_from(frame).unwrap();

            assert_eq!(desc.column_count(), 1);
            let col = desc.column(0).unwrap();
            assert_eq!(col.name, "id");
            assert_eq!(col.type_oid, 23);
            assert_eq!(col.type_size, 4);
        }
    }
}
