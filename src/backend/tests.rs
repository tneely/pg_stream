use super::*;
use crate::{Format, oid};

fn parse(code: u8, body: &[u8]) -> Message<'_> {
    Message::parse(code, body).unwrap()
}

fn malformed(code: u8, body: &[u8]) {
    let result = Message::parse(code, body);
    let rejected = matches!(result, Err(ProtocolError::Malformed { code: c }) if c == code);
    assert!(
        rejected,
        "{} {body:?} parsed as {result:?}",
        code::name(code)
    );
}

/// A RowDescription field: name, table, column, type, size, modifier, format.
fn field(name: &str, type_oid: u32, format: u16) -> Vec<u8> {
    let mut field = format!("{name}\0").into_bytes();
    field.extend_from_slice(&[0, 0, 0x40, 0x01, 0, 3]);
    field.extend_from_slice(&type_oid.to_be_bytes());
    field.extend_from_slice(&[0xff, 0xff, 0, 0, 0, 20]);
    field.extend_from_slice(&format.to_be_bytes());
    field
}

#[test]
fn messages_without_a_body() {
    let cases = [
        (b'1', "ParseComplete"),
        (b'2', "BindComplete"),
        (b'3', "CloseComplete"),
        (b'n', "NoData"),
        (b's', "PortalSuspended"),
        (b'I', "EmptyQueryResponse"),
        (b'c', "CopyDone"),
    ];
    for (code, name) in cases {
        let message = parse(code, b"");
        assert_eq!(format!("{message:?}"), name);
        assert_eq!((message.code(), code::name(code)), (code, name));
        malformed(code, b"x");
    }
}

#[test]
fn ready_for_query() {
    let statuses = [
        (b"I", TransactionStatus::Idle),
        (b"T", TransactionStatus::InTransaction),
        (b"E", TransactionStatus::Failed),
    ];
    for (body, status) in statuses {
        assert!(matches!(parse(b'Z', body), Message::ReadyForQuery(s) if s == status));
    }
    malformed(b'Z', b"");
    malformed(b'Z', b"X");
    malformed(b'Z', b"II");
}

#[test]
fn data_row() {
    let Message::DataRow(row) = parse(b'D', b"\0\x03\0\0\0\x02hi\xff\xff\xff\xff\0\0\0\0") else {
        panic!("not a DataRow");
    };
    let values = [Some(&b"hi"[..]), None, Some(&b""[..])];
    assert_eq!(row.len(), 3);
    assert_eq!(row.iter().len(), 3);
    assert_eq!(row.iter().collect::<Vec<_>>(), values);
    assert_eq!(row.into_iter().collect::<Vec<_>>(), values);
    assert_eq!(
        (row.get(0), row.get(1), row.get(3)),
        (Some(values[0]), Some(None), None)
    );
    assert_eq!(format!("{row:?}"), "[Some([104, 105]), None, Some([])]");
    assert_eq!(format!("{:?}", row.iter()), format!("{row:?}"));
    // A row kept as bytes parses back into the row it was.
    assert_eq!(DataRow::parse(row.as_bytes()), Ok(row));

    let Message::DataRow(empty) = parse(b'D', b"\0\0") else {
        panic!("not a DataRow")
    };
    assert!(empty.is_empty());

    malformed(b'D', b"");
    malformed(b'D', b"\0");
    // Fewer values than declared, more than declared, and a value cut short.
    malformed(b'D', b"\0\x02\0\0\0\x01a");
    malformed(b'D', b"\0\x01\0\0\0\x01a\0\0\0\x01b");
    malformed(b'D', b"\0\x01\0\0\0\x05abc");
    // Only -1 is NULL; any other negative length is nonsense.
    malformed(b'D', b"\0\x01\xff\xff\xff\xfe");
    malformed(b'D', b"\0\x01\x80\0\0\0");
}

#[test]
fn row_description() {
    let mut body = vec![0, 2];
    body.extend(field("id", oid::INT4, 1));
    body.extend(field("name", oid::TEXT, 0));
    let Message::RowDescription(columns) = parse(b'T', &body) else {
        panic!("not a RowDescription");
    };

    assert_eq!((columns.len(), columns.iter().len()), (2, 2));
    let id = Field {
        name: "id",
        table_oid: 0x4001,
        column_id: 3,
        type_oid: oid::INT4,
        type_size: -1,
        type_modifier: 20,
        format: Format::Binary,
    };
    let name = Field {
        name: "name",
        type_oid: oid::TEXT,
        format: Format::Text,
        ..id
    };
    assert_eq!(columns.iter().collect::<Vec<_>>(), [id, name]);
    assert_eq!(columns.into_iter().count(), 2);
    assert_eq!(format!("{:?}", columns.iter()), format!("{columns:?}"));
    assert_eq!(RowDescription::parse(columns.as_bytes()), Ok(columns));

    let Message::RowDescription(none) = parse(b'T', b"\0\0") else {
        panic!()
    };
    assert!(none.is_empty());

    malformed(b'T', b"\0");
    malformed(b'T', &body[..body.len() - 1]);
    malformed(b'T', &[&body[..], b"x"].concat());
    malformed(
        b'T',
        &[&[0, 1][..], &field("bad format", oid::INT4, 2)].concat(),
    );
    malformed(b'T', b"\0\x01no terminator");
}

#[test]
fn parameter_description() {
    let Message::ParameterDescription(types) = parse(b't', b"\0\x02\0\0\0\x17\0\0\0\x19") else {
        panic!("not a ParameterDescription");
    };
    assert_eq!((types.len(), types.is_empty()), (2, false));
    assert_eq!(types.iter().collect::<Vec<_>>(), [oid::INT4, oid::TEXT]);
    assert_eq!(format!("{types:?}"), "[23, 25]");

    malformed(b't', b"\0");
    malformed(b't', b"\0\x02\0\0\0\x17");
    malformed(b't', b"\0\x01\0\0\0\x17\0");
}

#[test]
fn command_complete() {
    let rows = |tag: &str| match parse(b'C', format!("{tag}\0").as_bytes()) {
        Message::CommandComplete(command) => (command.tag.to_owned(), command.rows()),
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(rows("SELECT 5"), ("SELECT 5".to_owned(), Some(5)));
    assert_eq!(rows("INSERT 0 12"), ("INSERT 0 12".to_owned(), Some(12)));
    assert_eq!(rows("COPY 18446744073709551615").1, Some(u64::MAX));
    assert_eq!(rows("CREATE TABLE").1, None);
    assert_eq!(rows("BEGIN").1, None);
    assert_eq!(rows("").1, None);

    malformed(b'C', b"SELECT 1");
    malformed(b'C', b"SELECT 1\0x");
}

#[test]
fn error_and_notice_fields() {
    let body = b"SERREUR\0VERROR\0C23505\0Mduplicate key\0Dkey (id)=(1)\0Hhint\0P7\0\
        p3\0qinternal\0Wwhere\0spublic\0tusers\0cid\0dint4\0nusers_pkey\0Ffile.c\0L42\0Rroutine\0\
        Zunknown tag\0\0";
    let Message::ErrorResponse(error) = parse(b'E', body) else {
        panic!("not an ErrorResponse")
    };

    assert_eq!(
        (error.severity(), error.code(), error.message()),
        ("ERROR", "23505", "duplicate key")
    );
    let optional = [
        (error.detail(), "key (id)=(1)"),
        (error.hint(), "hint"),
        (error.position(), "7"),
        (error.internal_position(), "3"),
        (error.internal_query(), "internal"),
        (error.context(), "where"),
        (error.schema(), "public"),
        (error.table(), "users"),
        (error.column(), "id"),
        (error.data_type(), "int4"),
        (error.constraint(), "users_pkey"),
        (error.file(), "file.c"),
        (error.line(), "42"),
        (error.routine(), "routine"),
        (error.get(b'Z'), "unknown tag"),
    ];
    for (value, expected) in optional {
        assert_eq!(value, Some(expected));
    }
    assert_eq!(error.iter().count(), 19);
    assert_eq!(error.as_bytes(), body);
    assert_eq!(error.to_string(), "ERROR: duplicate key (SQLSTATE 23505)");
    assert!(!error.is_fatal());

    let owned = error.to_error();
    assert_eq!((owned.code(), owned.message()), ("23505", "duplicate key"));
    assert_eq!(owned.fields(), error);
    assert_eq!(format!("{owned:?}"), format!("{error:?}"));

    // Servers before 9.6 send only the localized severity.
    let Message::NoticeResponse(notice) = parse(b'N', b"SFATAL\0Mold server\0\0") else {
        panic!()
    };
    assert_eq!(
        (notice.severity(), notice.code(), notice.detail()),
        ("FATAL", "", None)
    );
    assert_eq!(notice.to_string(), "FATAL: old server");
    assert!(notice.is_fatal());

    let Message::ErrorResponse(bare) = parse(b'E', b"\0") else {
        panic!()
    };
    assert_eq!((bare.severity(), bare.code(), bare.message()), ("", "", ""));

    malformed(b'E', b"");
    malformed(b'E', b"SERROR\0");
    malformed(b'E', b"SERROR");
    malformed(b'N', b"SNOTICE\0\0trailing");
}

#[test]
fn text_that_is_not_utf8_is_cut_at_the_first_bad_byte() {
    let Message::ErrorResponse(error) = parse(b'E', b"Mcaf\xe9 ferm\xe9\0C42601\0\0") else {
        panic!()
    };
    assert_eq!((error.message(), error.code()), ("caf", "42601"));
    // The owned copy keeps what it can of the rest.
    assert_eq!(error.to_error().message(), "caf\u{fffd} ferm\u{fffd}");

    let Message::CommandComplete(command) = parse(b'C', b"\xffSELECT 1\0") else {
        panic!()
    };
    assert_eq!(command.tag, "");
}

#[test]
fn parameter_status_and_notification() {
    let Message::ParameterStatus(status) = parse(b'S', b"TimeZone\0UTC\0") else {
        panic!()
    };
    assert_eq!((status.name, status.value), ("TimeZone", "UTC"));
    malformed(b'S', b"TimeZone\0UTC");
    malformed(b'S', b"TimeZone\0");
    malformed(b'S', b"TimeZone\0UTC\0x");

    let Message::NotificationResponse(notify) = parse(b'A', b"\0\0\x01\0chan\0payload\0") else {
        panic!("not a NotificationResponse");
    };
    assert_eq!(
        (notify.process_id, notify.channel, notify.payload),
        (256, "chan", "payload")
    );
    malformed(b'A', b"\0\0\x01");
    malformed(b'A', b"\0\0\x01\0chan\0");
    malformed(b'A', b"\0\0\x01\0chan\0payload\0x");
}

#[test]
fn backend_key_data() {
    for key_len in [4, 32, 256] {
        let body = [&[0, 0, 0, 7][..], &vec![0xab; key_len]].concat();
        let Message::BackendKeyData(key) = parse(b'K', &body) else {
            panic!()
        };
        assert_eq!((key.process_id, key.secret_key.len()), (7, key_len));
        let owned = key.to_owned();
        assert_eq!(
            (owned.process_id, &owned.secret_key[..]),
            (7, key.secret_key)
        );
        // The key is a credential and stays out of logs.
        assert_eq!(format!("{key:?}"), "BackendKeyData { process_id: 7, .. }");
        assert_eq!(format!("{owned:?}"), "BackendKey { process_id: 7, .. }");
    }
    for key_len in [0, 3, 257] {
        malformed(b'K', &[&[0, 0, 0, 7][..], &vec![0; key_len]].concat());
    }
    malformed(b'K', b"\0\0");
}

#[test]
fn copy_responses() {
    for code in *b"GHW" {
        let message = parse(code, b"\x01\0\x03\0\0\0\x01\0\x01");
        let (Message::CopyInResponse(copy)
        | Message::CopyOutResponse(copy)
        | Message::CopyBothResponse(copy)) = message
        else {
            panic!("unexpected {message:?}");
        };
        assert_eq!(
            (copy.format, copy.len(), copy.is_empty()),
            (Format::Binary, 3, false)
        );
        let formats = [Format::Text, Format::Binary, Format::Binary];
        assert_eq!(copy.column_formats().collect::<Vec<_>>(), formats);

        malformed(code, b"");
        malformed(code, b"\x02\0\0");
        malformed(code, b"\0\0\x02\0\0");
        malformed(code, b"\0\0\x01\0\x02");
        malformed(code, b"\0\0\x01\0\0\0");
    }
    assert!(matches!(
        parse(b'd', b"any bytes"),
        Message::CopyData(b"any bytes")
    ));
}

#[test]
fn function_call_response() {
    let value = |body| match parse(b'V', body) {
        Message::FunctionCallResponse(value) => value,
        other => panic!("unexpected {other:?}"),
    };
    assert_eq!(value(b"\0\0\0\x02ok"), Some(&b"ok"[..]));
    assert_eq!(value(b"\0\0\0\0"), Some(&b""[..]));
    assert_eq!(value(b"\xff\xff\xff\xff"), None);

    malformed(b'V', b"\0\0\0");
    malformed(b'V', b"\0\0\0\x03ok");
    malformed(b'V', b"\0\0\0\x01ok");
    malformed(b'V', b"\xff\xff\xff\xffx");
    malformed(b'V', b"\xff\xff\xff\xfe");
}

#[test]
fn authentication_requests() {
    let request = |body| match parse(b'R', body) {
        Message::Authentication(request) => request,
        other => panic!("unexpected {other:?}"),
    };
    let cases = [
        (&b"\0\0\0\0"[..], Authentication::Ok),
        (b"\0\0\0\x02", Authentication::KerberosV5),
        (b"\0\0\0\x03", Authentication::CleartextPassword),
        (
            b"\0\0\0\x05salt",
            Authentication::Md5Password { salt: *b"salt" },
        ),
        (b"\0\0\0\x07", Authentication::Gss),
        (b"\0\0\0\x08token", Authentication::GssContinue(b"token")),
        (b"\0\0\0\x09", Authentication::Sspi),
        (
            b"\0\0\0\x0bchallenge",
            Authentication::SaslContinue(b"challenge"),
        ),
        (b"\0\0\0\x0cfinal", Authentication::SaslFinal(b"final")),
        (
            b"\0\0\0\x63?",
            Authentication::Unknown {
                code: 99,
                body: b"?",
            },
        ),
    ];
    for (body, expected) in cases {
        let request = request(body);
        assert_eq!(request, expected);
        assert_eq!(
            request.code(),
            u32::from_be_bytes(body[..4].try_into().unwrap())
        );
    }

    let Authentication::Sasl(offered) = request(b"\0\0\0\x0aSCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0")
    else {
        panic!("not a SASL request");
    };
    assert_eq!(
        offered.iter().collect::<Vec<_>>(),
        ["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"]
    );
    assert!(offered.contains("SCRAM-SHA-256") && !offered.contains("SCRAM-SHA"));
    assert_eq!(
        format!("{offered:?}"),
        r#"["SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"]"#
    );

    malformed(b'R', b"\0\0\0");
    malformed(b'R', b"\0\0\0\x05sal");
    malformed(b'R', b"\0\0\0\x05salty");
}

#[test]
fn negotiate_protocol_version() {
    let Message::NegotiateProtocolVersion(offer) =
        parse(b'v', b"\0\0\0\0\0\0\0\x02_pq_.a\0_pq_.b\0")
    else {
        panic!("not a NegotiateProtocolVersion");
    };
    assert_eq!(offer.minor_version, 0);
    assert_eq!(
        offer.unrecognized_options().collect::<Vec<_>>(),
        ["_pq_.a", "_pq_.b"]
    );

    malformed(b'v', b"\0\0\0\0\0\0\0");
    malformed(b'v', b"\0\0\0\0\0\0\0\x02_pq_.a\0");
    malformed(b'v', b"\0\0\0\0\0\0\0\x01_pq_.a\0_pq_.b\0");
}

#[test]
fn unknown_codes_pass_through_and_are_never_asynchronous() {
    for code in (0..=255).filter(|&code| code::name(code) == "an unknown message") {
        assert!(
            matches!(parse(code, b"body"), Message::Unknown { code: c, body: b"body" } if c == code)
        );
        assert!(!code::is_async(code));
    }
    let asynchronous: Vec<u8> = (0..=255).filter(|&code| code::is_async(code)).collect();
    assert_eq!(asynchronous, b"ANS");
    assert!(AsyncMessage::try_parse(b'D', b"\0\0").is_none());
}

#[test]
fn fixed_length_messages_are_known_from_their_header() {
    let fixed = |code| length_bounds(code).map(|bounds| (*bounds.start(), *bounds.end()));
    for code in *b"123nsIc" {
        assert_eq!(fixed(code), Some((4, 4)), "{}", code::name(code));
    }
    assert_eq!(fixed(b'Z'), Some((5, 5)));
    assert_eq!(fixed(b'K'), Some((12, 264)));
    assert_eq!(fixed(b'D'), None);

    // A message is held to four bytes exactly when its parser takes no body.
    for code in 0..=255 {
        let known = code::name(code) != "an unknown message";
        let empty = Message::parse(code, b"").is_ok();
        let bodiless = known && empty && Message::parse(code, b"\0").is_err();
        assert_eq!(
            fixed(code) == Some((4, 4)),
            bodiless,
            "{}",
            code::name(code)
        );
    }
}

/// A small deterministic generator: enough to vary inputs, and reproducible.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Uses a message the way a caller would, touching everything it exposes,
/// and checks that it is the message its code says it is.
fn exercise(code: u8, message: &Message<'_>) {
    let expected = match code::name(code) {
        "an unknown message" => "Unknown",
        name => name,
    };
    let shown = format!("{message:?}");
    let variant = shown.split(['(', ' ']).next().unwrap();
    assert_eq!((message.code(), variant), (code, expected));

    match message {
        Message::DataRow(row) => assert_eq!(row.iter().count(), row.len()),
        Message::RowDescription(columns) => assert_eq!(columns.iter().count(), columns.len()),
        Message::ParameterDescription(types) => assert_eq!(types.iter().count(), types.len()),
        Message::ErrorResponse(fields) | Message::NoticeResponse(fields) => {
            let _ = (fields.to_string(), fields.to_error(), fields.is_fatal());
        }
        Message::CopyInResponse(copy)
        | Message::CopyOutResponse(copy)
        | Message::CopyBothResponse(copy) => assert_eq!(copy.column_formats().count(), copy.len()),
        _ => {}
    }
}

/// Whatever a server sends, parsing it either succeeds with a message that is
/// safe to use or fails cleanly.
#[test]
fn no_input_makes_parsing_panic() {
    let mut row = vec![0, 2, 0, 0, 0, 3, b'a', b'b', b'c', 0xff, 0xff, 0xff, 0xff];
    row.extend_from_slice(&[0; 3]);
    let description = [&[0, 2][..], &field("id", 23, 1), &field("name", 25, 0)].concat();
    let valid: [(u8, &[u8]); 12] = [
        (b'D', &row[..13]),
        (b'T', &description),
        (b't', b"\0\x02\0\0\0\x17\0\0\0\x19"),
        (b'C', b"INSERT 0 1\0"),
        (b'E', b"SERROR\0VERROR\0C23505\0Mduplicate key\0\0"),
        (b'S', b"TimeZone\0UTC\0"),
        (b'A', b"\0\0\x01\0chan\0payload\0"),
        (b'K', b"\0\0\0\x07\x01\x02\x03\x04"),
        (b'G', b"\0\0\x02\0\0\0\x01"),
        (b'V', b"\0\0\0\x02ok"),
        (b'R', b"\0\0\0\x0aSCRAM-SHA-256\0\0"),
        (b'v', b"\0\0\0\0\0\0\0\x01_pq_.a\0"),
    ];

    let mut random = Random(0x9e37_79b9_7f4a_7c15);
    for (code, body) in valid {
        exercise(code, &parse(code, body));

        // Every prefix, and every single-byte corruption.
        for len in 0..body.len() {
            let parsed = Message::parse(code, &body[..len]);
            parsed.iter().for_each(|message| exercise(code, message));
        }
        for at in 0..body.len() {
            for _ in 0..16 {
                let mut corrupt = body.to_vec();
                corrupt[at] = random.next() as u8;
                let parsed = Message::parse(code, &corrupt);
                parsed.iter().for_each(|message| exercise(code, message));
            }
        }
    }

    // Arbitrary bytes under every code.
    for code in 0..=255 {
        for _ in 0..200 {
            let len = random.next() as usize % 48;
            let body: Vec<u8> = (0..len).map(|_| random.next() as u8).collect();
            let parsed = Message::parse(code, &body);
            parsed.iter().for_each(|message| exercise(code, message));
        }
    }
}
