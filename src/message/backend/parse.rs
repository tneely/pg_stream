//! Parsing functions for backend messages.

use bytes::Bytes;

use super::wrappers::*;
use super::{MessageCode, PgMessage};

/// Parses a message code and body into a PgMessage.
///
/// Returns an io::Error for parse failures on known message types.
/// Returns PgMessage::Unknown for unrecognized message codes.
pub(super) fn parse_message(code: MessageCode, body: Bytes) -> std::io::Result<PgMessage> {
    let invalid_data = |msg: String| std::io::Error::new(std::io::ErrorKind::InvalidData, msg);

    match code {
        MessageCode::DATA_ROW => parse_data_row(body)
            .map(PgMessage::DataRow)
            .map_err(invalid_data),
        MessageCode::ROW_DESCRIPTION => parse_row_description(body)
            .map(PgMessage::RowDescription)
            .map_err(invalid_data),
        MessageCode::COMMAND_COMPLETE => parse_command_complete(body)
            .map(PgMessage::CommandComplete)
            .map_err(invalid_data),
        MessageCode::EMPTY_QUERY_RESPONSE => Ok(PgMessage::EmptyQueryResponse),
        MessageCode::ERROR_RESPONSE => parse_error_response(body)
            .map(PgMessage::ErrorResponse)
            .map_err(invalid_data),
        MessageCode::NOTICE_RESPONSE => parse_error_response(body)
            .map(PgMessage::NoticeResponse)
            .map_err(invalid_data),
        MessageCode::READY_FOR_QUERY => parse_ready_for_query(body)
            .map(PgMessage::ReadyForQuery)
            .map_err(invalid_data),
        MessageCode::BACKEND_KEY_DATA => parse_backend_key_data(body)
            .map(PgMessage::BackendKeyData)
            .map_err(invalid_data),
        MessageCode::PARAMETER_STATUS => parse_parameter_status(body)
            .map(PgMessage::ParameterStatus)
            .map_err(invalid_data),
        MessageCode::PARSE_COMPLETE => Ok(PgMessage::ParseComplete),
        MessageCode::BIND_COMPLETE => Ok(PgMessage::BindComplete),
        MessageCode::CLOSE_COMPLETE => Ok(PgMessage::CloseComplete),
        MessageCode::PARAMETER_DESCRIPTION => parse_parameter_description(body)
            .map(PgMessage::ParameterDescription)
            .map_err(invalid_data),
        MessageCode::NO_DATA => Ok(PgMessage::NoData),
        MessageCode::PORTAL_SUSPENDED => Ok(PgMessage::PortalSuspended),
        MessageCode::NOTIFICATION_RESPONSE => parse_notification_response(body)
            .map(PgMessage::NotificationResponse)
            .map_err(invalid_data),
        MessageCode::COPY_DATA => Ok(PgMessage::CopyData(body)),
        MessageCode::COPY_DONE => Ok(PgMessage::CopyDone),
        MessageCode::COPY_IN_RESPONSE => parse_copy_response(body)
            .map(PgMessage::CopyInResponse)
            .map_err(invalid_data),
        MessageCode::COPY_OUT_RESPONSE => parse_copy_response(body)
            .map(PgMessage::CopyOutResponse)
            .map_err(invalid_data),
        MessageCode::COPY_BOTH_RESPONSE => parse_copy_response(body)
            .map(PgMessage::CopyBothResponse)
            .map_err(invalid_data),
        MessageCode::AUTHENTICATION => Ok(PgMessage::Authentication(body)),
        MessageCode::FUNCTION_CALL_RESPONSE => Ok(PgMessage::FunctionCallResponse(body)),
        MessageCode::NEGOTIATE_PROTOCOL_VERSION => Ok(PgMessage::NegotiateProtocolVersion(body)),
        _ => Ok(PgMessage::Unknown { code, body }),
    }
}

fn parse_data_row(body: Bytes) -> Result<DataRow, String> {
    if body.len() < 2 {
        return Err("DataRow body too short".into());
    }
    let column_count = u16::from_be_bytes([body[0], body[1]]);
    Ok(DataRow { body, column_count })
}

fn parse_row_description(body: Bytes) -> Result<RowDescription, String> {
    if body.len() < 2 {
        return Err("RowDescription body too short".into());
    }

    let column_count = u16::from_be_bytes([body[0], body[1]]);
    let mut column_names = Vec::with_capacity(column_count as usize);
    let mut offset = 2usize;

    for _ in 0..column_count {
        // Find end of null-terminated name
        let name_len = body[offset..]
            .iter()
            .position(|&b| b == 0)
            .ok_or("RowDescription column name missing null terminator")?;
        column_names.push(offset..offset + name_len);
        offset += name_len + 1; // Skip past null terminator

        // Skip fixed-size fields (18 bytes)
        if offset + 18 > body.len() {
            return Err("RowDescription column data too short".into());
        }
        offset += 18;
    }

    Ok(RowDescription { body, column_names })
}

fn parse_command_complete(body: Bytes) -> Result<CommandComplete, String> {
    let tag_len = body
        .iter()
        .position(|&b| b == 0)
        .ok_or("CommandComplete missing null terminator")?;
    Ok(CommandComplete { body, tag_len })
}

fn parse_ready_for_query(body: Bytes) -> Result<ReadyForQuery, String> {
    if body.is_empty() {
        return Err("ReadyForQuery body too short".into());
    }
    let status = match body[0] {
        b'I' => TransactionStatus::Idle,
        b'T' => TransactionStatus::InTransaction,
        b'E' => TransactionStatus::Failed,
        other => return Err(format!("unknown transaction status: {}", other as char)),
    };
    Ok(ReadyForQuery { status })
}

fn parse_backend_key_data(body: Bytes) -> Result<BackendKeyData, String> {
    if body.len() < 8 {
        return Err("BackendKeyData body too short".into());
    }
    let process_id = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let secret_key = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
    Ok(BackendKeyData {
        process_id,
        secret_key,
    })
}

fn parse_parameter_status(body: Bytes) -> Result<ParameterStatus, String> {
    let name_end = body
        .iter()
        .position(|&b| b == 0)
        .ok_or("ParameterStatus name missing null terminator")?;
    let value_start = name_end + 1;
    let value_end = body[value_start..]
        .iter()
        .position(|&b| b == 0)
        .ok_or("ParameterStatus value missing null terminator")?;
    Ok(ParameterStatus {
        body,
        name: 0..name_end,
        value: value_start..value_start + value_end,
    })
}

fn parse_parameter_description(body: Bytes) -> Result<ParameterDescription, String> {
    if body.len() < 2 {
        return Err("ParameterDescription body too short".into());
    }
    let param_count = u16::from_be_bytes([body[0], body[1]]);
    Ok(ParameterDescription { body, param_count })
}

fn parse_notification_response(body: Bytes) -> Result<NotificationResponse, String> {
    if body.len() < 4 {
        return Err("NotificationResponse body too short".into());
    }
    // process_id is at bytes 0..4
    let channel_start = 4;
    let channel_end = body[channel_start..]
        .iter()
        .position(|&b| b == 0)
        .ok_or("NotificationResponse channel missing null terminator")?;
    let payload_start = channel_start + channel_end + 1;
    let payload_end = body[payload_start..]
        .iter()
        .position(|&b| b == 0)
        .ok_or("NotificationResponse payload missing null terminator")?;
    Ok(NotificationResponse {
        body,
        channel: channel_start..channel_start + channel_end,
        payload: payload_start..payload_start + payload_end,
    })
}

fn parse_copy_response(body: Bytes) -> Result<CopyResponse, String> {
    if body.len() < 3 {
        return Err("CopyResponse body too short".into());
    }
    let column_count = u16::from_be_bytes([body[1], body[2]]);

    if body.len() < 3 + (column_count as usize) * 2 {
        return Err("CopyResponse column formats too short".into());
    }

    Ok(CopyResponse { body, column_count })
}

fn parse_error_response(body: Bytes) -> Result<ErrorResponse, String> {
    let mut local_severity = None;
    let mut severity = None;
    let mut code = None;
    let mut message = None;
    let mut detail = None;
    let mut hint = None;
    let mut position = None;
    let mut internal_position = None;
    let mut internal_query = None;
    let mut r#where = None;
    let mut schema = None;
    let mut table = None;
    let mut column = None;
    let mut datatype = None;
    let mut constraint = None;
    let mut file = None;
    let mut line = None;
    let mut routine = None;

    let mut offset = 0;
    let iter = body.split(|b| *b == 0);

    for field in iter {
        if field.is_empty() {
            break;
        }

        // field[0] = tag, field[1..] = value
        let tag = field[0];
        let start = offset + 1;
        let end = start + field.len() - 1; // minus tag

        let range = start..end;
        match tag {
            b'S' => local_severity = Some(range),
            b'V' => severity = Some(range),
            b'C' => code = Some(range),
            b'M' => message = Some(range),
            b'D' => detail = Some(range),
            b'H' => hint = Some(range),
            b'P' => position = Some(range),
            b'p' => internal_position = Some(range),
            b'q' => internal_query = Some(range),
            b'W' => r#where = Some(range),
            b's' => schema = Some(range),
            b't' => table = Some(range),
            b'c' => column = Some(range),
            b'd' => datatype = Some(range),
            b'n' => constraint = Some(range),
            b'F' => file = Some(range),
            b'L' => line = Some(range),
            b'R' => routine = Some(range),
            _ => {}
        }

        offset += field.len() + 1; // +1 for the null terminator
    }

    let local_severity = local_severity.ok_or("ErrorResponse missing local_severity (S)")?;
    let severity = severity.ok_or("ErrorResponse missing severity (V)")?;
    let code = code.ok_or("ErrorResponse missing code (C)")?;
    let message = message.ok_or("ErrorResponse missing message (M)")?;

    Ok(ErrorResponse {
        body,
        local_severity,
        severity,
        code,
        message,
        detail,
        hint,
        position,
        internal_position,
        internal_query,
        r#where,
        schema,
        table,
        column,
        datatype,
        constraint,
        file,
        line,
        routine,
    })
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};

    use super::*;

    #[test]
    fn test_data_row_parse() {
        let mut body = BytesMut::new();
        body.put_u16(2); // 2 columns
        body.put_i32(5); // length of first column
        body.put_slice(b"hello");
        body.put_i32(-1); // NULL

        let msg = parse_message(MessageCode::DATA_ROW, body.freeze()).unwrap();
        let PgMessage::DataRow(row) = msg else {
            panic!("expected DataRow");
        };

        assert_eq!(row.column_count(), 2);
        assert_eq!(row.column(0), Some(b"hello".as_slice()));
        assert!(row.column(1).is_none());
        assert!(!row.is_null(0));
        assert!(row.is_null(1));
    }

    #[test]
    fn test_unknown_message_code() {
        let msg = parse_message(MessageCode::from(0xFF), Bytes::new()).unwrap();
        assert!(matches!(msg, PgMessage::Unknown { .. }));
    }

    #[test]
    fn test_command_complete_select() {
        let mut body = BytesMut::new();
        body.put_slice(b"SELECT 5\0");

        let msg = parse_message(MessageCode::COMMAND_COMPLETE, body.freeze()).unwrap();
        let PgMessage::CommandComplete(cmd) = msg else {
            panic!("expected CommandComplete");
        };

        assert_eq!(cmd.tag(), "SELECT 5");
        assert_eq!(cmd.rows_affected(), Some(5));
    }

    #[test]
    fn test_command_complete_insert() {
        let mut body = BytesMut::new();
        body.put_slice(b"INSERT 0 1\0");

        let msg = parse_message(MessageCode::COMMAND_COMPLETE, body.freeze()).unwrap();
        let PgMessage::CommandComplete(cmd) = msg else {
            panic!("expected CommandComplete");
        };

        assert_eq!(cmd.tag(), "INSERT 0 1");
        assert_eq!(cmd.rows_affected(), Some(1));
    }

    #[test]
    fn test_command_complete_create_table() {
        let mut body = BytesMut::new();
        body.put_slice(b"CREATE TABLE\0");

        let msg = parse_message(MessageCode::COMMAND_COMPLETE, body.freeze()).unwrap();
        let PgMessage::CommandComplete(cmd) = msg else {
            panic!("expected CommandComplete");
        };

        assert_eq!(cmd.tag(), "CREATE TABLE");
        assert_eq!(cmd.rows_affected(), None);
    }

    #[test]
    fn test_notification_response() {
        let mut body = BytesMut::new();
        body.put_u32(12345); // process_id
        body.put_slice(b"my_channel\0");
        body.put_slice(b"hello world\0");

        let msg = parse_message(MessageCode::NOTIFICATION_RESPONSE, body.freeze()).unwrap();
        let PgMessage::NotificationResponse(notif) = msg else {
            panic!("expected NotificationResponse");
        };

        assert_eq!(notif.process_id(), 12345);
        assert_eq!(notif.channel(), "my_channel");
        assert_eq!(notif.payload(), "hello world");
    }

    #[test]
    fn test_parameter_description() {
        let mut body = BytesMut::new();
        body.put_u16(2); // 2 parameters
        body.put_u32(23); // INT4 OID
        body.put_u32(25); // TEXT OID

        let msg = parse_message(MessageCode::PARAMETER_DESCRIPTION, body.freeze()).unwrap();
        let PgMessage::ParameterDescription(desc) = msg else {
            panic!("expected ParameterDescription");
        };

        assert_eq!(desc.param_count(), 2);
        assert_eq!(desc.param_oid(0), Some(23));
        assert_eq!(desc.param_oid(1), Some(25));
        assert_eq!(desc.param_oid(2), None);
    }

    #[test]
    fn test_row_description() {
        let mut body = BytesMut::new();
        body.put_u16(1); // 1 column
        body.put_slice(b"id\0"); // column name
        body.put_u32(0); // table OID
        body.put_u16(0); // column ID
        body.put_u32(23); // type OID (INT4)
        body.put_i16(4); // type size
        body.put_i32(-1); // type modifier
        body.put_u16(0); // format code (text)

        let msg = parse_message(MessageCode::ROW_DESCRIPTION, body.freeze()).unwrap();
        let PgMessage::RowDescription(desc) = msg else {
            panic!("expected RowDescription");
        };

        assert_eq!(desc.column_count(), 1);
        assert_eq!(desc.column_name(0).unwrap(), "id");
        assert_eq!(desc.type_oid(0).unwrap(), 23);
        assert_eq!(desc.type_size(0).unwrap(), 4);
    }

    #[test]
    fn test_ready_for_query() {
        let msg = parse_message(MessageCode::READY_FOR_QUERY, Bytes::from_static(&[b'I'])).unwrap();
        let PgMessage::ReadyForQuery(rfq) = msg else {
            panic!("expected ReadyForQuery");
        };
        assert_eq!(rfq.status(), TransactionStatus::Idle);

        let msg = parse_message(MessageCode::READY_FOR_QUERY, Bytes::from_static(&[b'T'])).unwrap();
        let PgMessage::ReadyForQuery(rfq) = msg else {
            panic!("expected ReadyForQuery");
        };
        assert_eq!(rfq.status(), TransactionStatus::InTransaction);

        let msg = parse_message(MessageCode::READY_FOR_QUERY, Bytes::from_static(&[b'E'])).unwrap();
        let PgMessage::ReadyForQuery(rfq) = msg else {
            panic!("expected ReadyForQuery");
        };
        assert_eq!(rfq.status(), TransactionStatus::Failed);
    }

    #[test]
    fn test_backend_key_data() {
        let mut body = BytesMut::new();
        body.put_u32(12345); // process_id
        body.put_u32(67890); // secret_key

        let msg = parse_message(MessageCode::BACKEND_KEY_DATA, body.freeze()).unwrap();
        let PgMessage::BackendKeyData(bkd) = msg else {
            panic!("expected BackendKeyData");
        };

        assert_eq!(bkd.process_id(), 12345);
        assert_eq!(bkd.secret_key(), 67890);
    }

    #[test]
    fn test_parameter_status() {
        let mut body = BytesMut::new();
        body.put_slice(b"server_version\0");
        body.put_slice(b"15.0\0");

        let msg = parse_message(MessageCode::PARAMETER_STATUS, body.freeze()).unwrap();
        let PgMessage::ParameterStatus(ps) = msg else {
            panic!("expected ParameterStatus");
        };

        assert_eq!(ps.name(), "server_version");
        assert_eq!(ps.value(), "15.0");
    }

    #[test]
    fn test_unit_variants() {
        assert!(matches!(
            parse_message(MessageCode::PARSE_COMPLETE, Bytes::new()).unwrap(),
            PgMessage::ParseComplete
        ));
        assert!(matches!(
            parse_message(MessageCode::BIND_COMPLETE, Bytes::new()).unwrap(),
            PgMessage::BindComplete
        ));
        assert!(matches!(
            parse_message(MessageCode::CLOSE_COMPLETE, Bytes::new()).unwrap(),
            PgMessage::CloseComplete
        ));
        assert!(matches!(
            parse_message(MessageCode::NO_DATA, Bytes::new()).unwrap(),
            PgMessage::NoData
        ));
        assert!(matches!(
            parse_message(MessageCode::EMPTY_QUERY_RESPONSE, Bytes::new()).unwrap(),
            PgMessage::EmptyQueryResponse
        ));
        assert!(matches!(
            parse_message(MessageCode::PORTAL_SUSPENDED, Bytes::new()).unwrap(),
            PgMessage::PortalSuspended
        ));
        assert!(matches!(
            parse_message(MessageCode::COPY_DONE, Bytes::new()).unwrap(),
            PgMessage::CopyDone
        ));
    }
}
