//! Parsing for backend messages. Each wrapper type owns its own `parse`,
//! and [`parse_message`] dispatches on the message code.

use bytes::Bytes;

use super::wrappers::*;
use super::{MessageCode, PgMessage};

/// Parses a message code and body into a PgMessage.
///
/// Returns an io::Error for parse failures on known message types.
/// Returns PgMessage::Unknown for unrecognized message codes.
pub(super) fn parse_message(code: MessageCode, body: Bytes) -> std::io::Result<PgMessage> {
    parse(code, body).map_err(|msg| std::io::Error::new(std::io::ErrorKind::InvalidData, msg))
}

/// Parses messages that keep no reference to their body, letting the reader
/// skip the `Bytes` handle. `None` if the code's parsed form borrows the body.
pub(super) fn parse_borrowed(code: MessageCode, body: &[u8]) -> Option<Result<PgMessage, String>> {
    let msg = match code {
        MessageCode::READY_FOR_QUERY => {
            return Some(ReadyForQuery::parse(body).map(PgMessage::ReadyForQuery));
        }
        MessageCode::EMPTY_QUERY_RESPONSE => PgMessage::EmptyQueryResponse,
        MessageCode::PARSE_COMPLETE => PgMessage::ParseComplete,
        MessageCode::BIND_COMPLETE => PgMessage::BindComplete,
        MessageCode::CLOSE_COMPLETE => PgMessage::CloseComplete,
        MessageCode::NO_DATA => PgMessage::NoData,
        MessageCode::PORTAL_SUSPENDED => PgMessage::PortalSuspended,
        MessageCode::COPY_DONE => PgMessage::CopyDone,
        _ => return None,
    };
    Some(empty(body, msg))
}

fn parse(code: MessageCode, body: Bytes) -> Result<PgMessage, String> {
    if let Some(msg) = parse_borrowed(code, &body) {
        return msg;
    }

    Ok(match code {
        MessageCode::DATA_ROW => PgMessage::DataRow(DataRow::parse(body)?),
        MessageCode::ROW_DESCRIPTION => PgMessage::RowDescription(RowDescription::parse(body)?),
        MessageCode::COMMAND_COMPLETE => PgMessage::CommandComplete(CommandComplete::parse(body)?),
        MessageCode::ERROR_RESPONSE => {
            PgMessage::ErrorResponse(Box::new(ErrorResponse::parse(body)?))
        }
        MessageCode::NOTICE_RESPONSE => {
            PgMessage::NoticeResponse(Box::new(NoticeResponse::parse(body)?))
        }
        MessageCode::BACKEND_KEY_DATA => PgMessage::BackendKeyData(BackendKeyData::parse(body)?),
        MessageCode::PARAMETER_STATUS => PgMessage::ParameterStatus(ParameterStatus::parse(body)?),
        MessageCode::PARAMETER_DESCRIPTION => {
            PgMessage::ParameterDescription(ParameterDescription::parse(body)?)
        }
        MessageCode::NOTIFICATION_RESPONSE => {
            PgMessage::NotificationResponse(NotificationResponse::parse(body)?)
        }
        MessageCode::COPY_DATA => PgMessage::CopyData(body),
        MessageCode::COPY_IN_RESPONSE => PgMessage::CopyInResponse(CopyResponse::parse(body)?),
        MessageCode::COPY_OUT_RESPONSE => PgMessage::CopyOutResponse(CopyResponse::parse(body)?),
        MessageCode::COPY_BOTH_RESPONSE => PgMessage::CopyBothResponse(CopyResponse::parse(body)?),
        MessageCode::AUTHENTICATION => PgMessage::Authentication(Authentication::parse(body)?),
        MessageCode::FUNCTION_CALL_RESPONSE => {
            PgMessage::FunctionCallResponse(FunctionCallResponse::parse(body)?)
        }
        MessageCode::NEGOTIATE_PROTOCOL_VERSION => {
            PgMessage::NegotiateProtocolVersion(NegotiateProtocolVersion::parse(body)?)
        }
        _ => PgMessage::Unknown { code, body },
    })
}

/// Accepts a body-less message. Unit variants Debug-format as their own name.
fn empty(body: &[u8], msg: PgMessage) -> Result<PgMessage, String> {
    if body.is_empty() {
        Ok(msg)
    } else {
        Err(format!("{msg:?} body must be empty"))
    }
}

impl DataRow {
    fn parse(body: Bytes) -> Result<Self, String> {
        if body.len() < 2 {
            return Err("DataRow body too short".into());
        }
        let column_count = u16::from_be_bytes([body[0], body[1]]);
        Ok(DataRow { body, column_count })
    }
}

impl RowDescription {
    fn parse(body: Bytes) -> Result<Self, String> {
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
}

impl CommandComplete {
    fn parse(body: Bytes) -> Result<Self, String> {
        let tag_len = body
            .iter()
            .position(|&b| b == 0)
            .ok_or("CommandComplete missing null terminator")?;
        Ok(CommandComplete { body, tag_len })
    }
}

impl ReadyForQuery {
    fn parse(body: &[u8]) -> Result<Self, String> {
        if body.len() != 1 {
            return Err("ReadyForQuery body must be exactly 1 byte".into());
        }
        let status = match body[0] {
            b'I' => TransactionStatus::Idle,
            b'T' => TransactionStatus::InTransaction,
            b'E' => TransactionStatus::Failed,
            other => return Err(format!("unknown transaction status: {}", other as char)),
        };
        Ok(ReadyForQuery { status })
    }
}

impl BackendKeyData {
    fn parse(body: Bytes) -> Result<Self, String> {
        // 4-byte process ID, then a secret key running to the end of the body.
        let key_len = body.len().saturating_sub(4);
        if !(Self::MIN_SECRET_KEY_LEN..=Self::MAX_SECRET_KEY_LEN).contains(&key_len) {
            return Err(format!(
                "BackendKeyData secret key length {key_len} is outside {}..={}",
                Self::MIN_SECRET_KEY_LEN,
                Self::MAX_SECRET_KEY_LEN
            ));
        }
        Ok(BackendKeyData { body })
    }
}

impl ParameterStatus {
    fn parse(body: Bytes) -> Result<Self, String> {
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
}

impl ParameterDescription {
    fn parse(body: Bytes) -> Result<Self, String> {
        if body.len() < 2 {
            return Err("ParameterDescription body too short".into());
        }
        let param_count = u16::from_be_bytes([body[0], body[1]]);
        Ok(ParameterDescription { body, param_count })
    }
}

impl NotificationResponse {
    fn parse(body: Bytes) -> Result<Self, String> {
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
}

impl CopyResponse {
    fn parse(body: Bytes) -> Result<Self, String> {
        if body.len() < 3 {
            return Err("CopyResponse body too short".into());
        }
        let column_count = u16::from_be_bytes([body[1], body[2]]);

        if body.len() < 3 + (column_count as usize) * 2 {
            return Err("CopyResponse column formats too short".into());
        }

        Ok(CopyResponse { body, column_count })
    }
}

impl FunctionCallResponse {
    fn parse(body: Bytes) -> Result<Self, String> {
        if body.len() < 4 {
            return Err("FunctionCallResponse body too short".into());
        }
        Ok(FunctionCallResponse { body })
    }
}

impl Authentication {
    fn parse(body: Bytes) -> Result<Self, String> {
        if body.len() < 4 {
            return Err("Authentication body too short".into());
        }
        let code = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        let rest = body.slice(4..);
        let auth = match code {
            0 => Authentication::Ok,
            2 => Authentication::KerberosV5,
            3 => Authentication::CleartextPassword,
            5 => {
                let salt: [u8; 4] = rest
                    .as_ref()
                    .try_into()
                    .map_err(|_| "MD5 challenge salt must be 4 bytes".to_string())?;
                Authentication::Md5Password { salt }
            }
            7 => Authentication::Gss,
            8 => Authentication::GssContinue(rest),
            9 => Authentication::Sspi,
            10 => Authentication::Sasl(rest),
            11 => Authentication::SaslContinue(rest),
            12 => Authentication::SaslFinal(rest),
            code => Authentication::Unknown { code, body: rest },
        };
        Ok(auth)
    }
}

impl NegotiateProtocolVersion {
    fn parse(body: Bytes) -> Result<Self, String> {
        if body.len() < 8 {
            return Err("NegotiateProtocolVersion body too short".into());
        }
        let option_count = u32::from_be_bytes([body[4], body[5], body[6], body[7]]);
        Ok(NegotiateProtocolVersion { body, option_count })
    }
}

impl ErrorResponse {
    fn parse(body: Bytes) -> Result<Self, String> {
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
        // Out-of-bounds index is None / not-null.
        assert_eq!(row.column(2), None);
        assert!(!row.is_null(2));

        // Iterator yields one item per column, single pass.
        let cols: Vec<Option<&[u8]>> = row.iter().collect();
        assert_eq!(cols, vec![Some(b"hello".as_slice()), None]);
        assert_eq!(row.iter().size_hint(), (0, Some(2)));
    }

    #[test]
    fn test_data_row_iter_matches_column() {
        let mut body = BytesMut::new();
        body.put_u16(3);
        body.put_i32(1);
        body.put_slice(b"a");
        body.put_i32(-1); // NULL
        body.put_i32(3);
        body.put_slice(b"ccc");

        let PgMessage::DataRow(row) = parse_message(MessageCode::DATA_ROW, body.freeze()).unwrap()
        else {
            panic!("expected DataRow");
        };

        let via_iter: Vec<_> = row.iter().collect();
        let via_index: Vec<_> = (0..row.column_count() as usize)
            .map(|i| row.column(i))
            .collect();
        assert_eq!(via_iter, via_index);
        assert_eq!(
            via_iter,
            vec![Some(b"a".as_slice()), None, Some(b"ccc".as_slice())]
        );
    }

    /// A body carrying fewer columns than it declares stops iteration early, so
    /// `size_hint` must not promise the declared count as a lower bound.
    #[test]
    fn test_data_row_truncated_body_size_hint_is_not_exact() {
        let mut body = BytesMut::new();
        body.put_u16(3); // declares 3 columns
        body.put_i32(1);
        body.put_slice(b"a"); // but only carries 1

        let PgMessage::DataRow(row) = parse_message(MessageCode::DATA_ROW, body.freeze()).unwrap()
        else {
            panic!("expected DataRow");
        };

        assert_eq!(row.iter().size_hint(), (0, Some(3)));
        assert_eq!(row.iter().count(), 1);
    }

    /// A column whose declared length runs past the body end yields NULL and
    /// stops iteration, rather than panicking or reading out of bounds.
    #[test]
    fn test_data_row_column_value_truncated() {
        let mut body = BytesMut::new();
        body.put_u16(2);
        body.put_i32(1);
        body.put_slice(b"a");
        body.put_i32(99); // claims 99 bytes, none follow

        let PgMessage::DataRow(row) = parse_message(MessageCode::DATA_ROW, body.freeze()).unwrap()
        else {
            panic!("expected DataRow");
        };

        let cols: Vec<_> = row.iter().collect();
        assert_eq!(cols, vec![Some(b"a".as_slice()), None]);
        assert_eq!(row.column(1), None);
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
        let msg = parse_message(MessageCode::READY_FOR_QUERY, Bytes::from_static(b"I")).unwrap();
        let PgMessage::ReadyForQuery(rfq) = msg else {
            panic!("expected ReadyForQuery");
        };
        assert_eq!(rfq.status(), TransactionStatus::Idle);

        let msg = parse_message(MessageCode::READY_FOR_QUERY, Bytes::from_static(b"T")).unwrap();
        let PgMessage::ReadyForQuery(rfq) = msg else {
            panic!("expected ReadyForQuery");
        };
        assert_eq!(rfq.status(), TransactionStatus::InTransaction);

        let msg = parse_message(MessageCode::READY_FOR_QUERY, Bytes::from_static(b"E")).unwrap();
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
        assert_eq!(bkd.secret_key(), 67890u32.to_be_bytes());
    }

    /// Protocol 3.2 keys are variable-length, up to 256 bytes.
    #[test]
    fn test_backend_key_data_long_key() {
        for key_len in [
            BackendKeyData::MIN_SECRET_KEY_LEN,
            32,
            BackendKeyData::MAX_SECRET_KEY_LEN,
        ] {
            let mut body = BytesMut::new();
            body.put_u32(4242);
            body.put_slice(&vec![0xAB; key_len]);

            let PgMessage::BackendKeyData(bkd) =
                parse_message(MessageCode::BACKEND_KEY_DATA, body.freeze()).unwrap()
            else {
                panic!("expected BackendKeyData");
            };
            assert_eq!(bkd.process_id(), 4242);
            assert_eq!(bkd.secret_key(), vec![0xAB; key_len]);
        }
    }

    #[test]
    fn test_backend_key_data_rejects_out_of_range_key() {
        for key_len in [0, 3, BackendKeyData::MAX_SECRET_KEY_LEN + 1] {
            let mut body = BytesMut::new();
            body.put_u32(1);
            body.put_slice(&vec![0; key_len]);
            let err = parse_message(MessageCode::BACKEND_KEY_DATA, body.freeze()).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        }
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
    fn test_authentication_ok_and_sasl() {
        let mut body = BytesMut::new();
        body.put_u32(0);
        let PgMessage::Authentication(auth) =
            parse_message(MessageCode::AUTHENTICATION, body.freeze()).unwrap()
        else {
            panic!("expected Authentication");
        };
        assert!(auth.is_ok());

        let mut body = BytesMut::new();
        body.put_u32(10); // SASL
        body.put_slice(b"SCRAM-SHA-256\0SCRAM-SHA-256-PLUS\0");
        let PgMessage::Authentication(auth) =
            parse_message(MessageCode::AUTHENTICATION, body.freeze()).unwrap()
        else {
            panic!("expected Authentication");
        };
        let mechs: Vec<_> = auth.sasl_mechanisms().collect();
        assert_eq!(mechs, vec!["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]);
    }

    #[test]
    fn test_authentication_md5_salt() {
        let mut body = BytesMut::new();
        body.put_u32(5);
        body.put_slice(&[0xde, 0xad, 0xbe, 0xef]);
        let PgMessage::Authentication(auth) =
            parse_message(MessageCode::AUTHENTICATION, body.freeze()).unwrap()
        else {
            panic!("expected Authentication");
        };
        assert_eq!(
            auth,
            crate::message::Authentication::Md5Password {
                salt: [0xde, 0xad, 0xbe, 0xef]
            }
        );
    }

    #[test]
    fn test_authentication_short_body_errors() {
        let err =
            parse_message(MessageCode::AUTHENTICATION, Bytes::from_static(&[0, 0])).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_function_call_response() {
        let mut body = BytesMut::new();
        body.put_i32(3);
        body.put_slice(b"abc");
        let PgMessage::FunctionCallResponse(resp) =
            parse_message(MessageCode::FUNCTION_CALL_RESPONSE, body.freeze()).unwrap()
        else {
            panic!("expected FunctionCallResponse");
        };
        assert_eq!(resp.value(), Some(b"abc".as_slice()));

        let mut null_body = BytesMut::new();
        null_body.put_i32(-1);
        let PgMessage::FunctionCallResponse(resp) =
            parse_message(MessageCode::FUNCTION_CALL_RESPONSE, null_body.freeze()).unwrap()
        else {
            panic!("expected FunctionCallResponse");
        };
        assert_eq!(resp.value(), None);

        let err = parse_message(
            MessageCode::FUNCTION_CALL_RESPONSE,
            Bytes::from_static(&[0, 0]),
        )
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_negotiate_protocol_version() {
        let mut body = BytesMut::new();
        body.put_u32(0); // newest minor version
        body.put_u32(2); // 2 unrecognized options
        body.put_slice(b"opt_a\0opt_b\0");
        let PgMessage::NegotiateProtocolVersion(neg) =
            parse_message(MessageCode::NEGOTIATE_PROTOCOL_VERSION, body.freeze()).unwrap()
        else {
            panic!("expected NegotiateProtocolVersion");
        };
        assert_eq!(neg.minor_version(), 0);
        assert_eq!(neg.option_count(), 2);
        let opts: Vec<_> = neg.unrecognized_options().collect();
        assert_eq!(opts, vec!["opt_a", "opt_b"]);
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

        for code in [
            MessageCode::PARSE_COMPLETE,
            MessageCode::BIND_COMPLETE,
            MessageCode::CLOSE_COMPLETE,
            MessageCode::NO_DATA,
            MessageCode::EMPTY_QUERY_RESPONSE,
            MessageCode::PORTAL_SUSPENDED,
            MessageCode::COPY_DONE,
        ] {
            assert_eq!(
                parse_message(code, Bytes::from_static(b"x"))
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::InvalidData
            );
        }
    }
}
