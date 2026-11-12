//! Tests against a real Postgres server.
//!
//! These run only when `PGSTREAM_TEST_PORT` names a server on localhost with
//! a trusted `postgres` superuser and the roles `md5_user`, `sha_user` and
//! `pw_user` (passwords `md5`, `sha` and `pw`), each restricted to the
//! authentication method in its name. Set `PGSTREAM_TEST_TLS=1` if the server
//! also accepts TLS. `tests/server.sh` starts one in Docker.
#![cfg(all(feature = "startup", feature = "tokio"))]

mod mock;

use std::{sync::Arc, time::Duration};

use pg_stream::{
    AuthError, AuthMethod, Blocking, Completion, Connection, Error, Format, Frontend, Message,
    ProtocolError, ProtocolVersion, Response, Transport,
    backend::TransactionStatus,
    block_on, cancel,
    frontend::{IsNull, params_with, try_params_with},
    oid, params, request_tls,
    startup::{Session, Startup},
};
use tokio::net::TcpStream;

fn port() -> Option<u16> {
    let port = std::env::var("PGSTREAM_TEST_PORT").ok()?.parse().ok();
    if port.is_none() {
        eprintln!("skipping: PGSTREAM_TEST_PORT is not set");
    }
    port
}

async fn tcp(port: u16) -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    stream.set_nodelay(true).unwrap();
    stream
}

async fn connect_as(startup: &mut Startup<'_>) -> Option<(Connection<TcpStream>, Session)> {
    let stream = tcp(port()?).await;
    Some(startup.database("postgres").connect(stream).await.unwrap())
}

/// A user that authenticates with SCRAM.
fn sha_user<'a>() -> Startup<'a> {
    let mut startup = Startup::new("sha_user");
    startup.password("sha").database("postgres");
    startup
}

async fn connect() -> Option<Connection<TcpStream>> {
    Some(connect_as(&mut Startup::new("postgres")).await?.0)
}

/// Fails a test that would otherwise hang.
async fn within<T>(seconds: u64, future: impl Future<Output = T>) -> T {
    let limit = Duration::from_secs(seconds);
    let finished = tokio::time::timeout(limit, future).await;
    finished.expect("timed out")
}

/// Reads the rest of a result set as text.
async fn text_rows<S: Transport>(response: &mut Response<'_, S>) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    while let Some(row) = response.row().await.unwrap() {
        let text = |value: &[u8]| String::from_utf8(value.to_vec()).unwrap();
        rows.push(row.iter().map(|value| value.map(text)).collect());
    }
    rows
}

/// Runs a statement for its effect.
async fn run<S: Transport>(conn: &mut Connection<S>, sql: &str) {
    conn.query(sql);
    conn.response().finish().await.unwrap();
}

fn sqlstate(error: Error) -> String {
    match error {
        Error::Server(error) => error.code().to_owned(),
        other => panic!("expected a server error, got {other:?}"),
    }
}

#[tokio::test]
async fn startup_reports_the_session() {
    let mut startup = Startup::new("postgres");
    startup.parameter("application_name", "live");
    let Some((conn, session)) = connect_as(&mut startup).await else {
        return;
    };

    assert_eq!(session.protocol, ProtocolVersion::V3_0);
    assert_eq!(session.parameter("application_name"), Some("live"));
    assert_eq!(session.parameter("client_encoding"), Some("UTF8"));
    assert!(session.parameter("server_version").is_some());
    assert_eq!(session.backend_key.unwrap().secret_key.len(), 4);
    assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn protocol_3_2_widens_the_cancel_key() {
    let mut startup = Startup::new("postgres");
    let Some((_, session)) = connect_as(startup.protocol(ProtocolVersion::V3_2)).await else {
        return;
    };

    let version = session.parameter("server_version").unwrap();
    let major: u32 = version.split(['.', ' ']).next().unwrap().parse().unwrap();
    match major >= 18 {
        true => {
            assert_eq!(session.protocol, ProtocolVersion::V3_2);
            assert_eq!(session.backend_key.unwrap().secret_key.len(), 32);
        }
        false => assert_eq!(session.protocol, ProtocolVersion::V3_0),
    }
}

#[tokio::test]
async fn every_password_method_authenticates() {
    for (user, password) in [("pw_user", "pw"), ("md5_user", "md5"), ("sha_user", "sha")] {
        let mut startup = Startup::new(user);
        let Some((mut conn, _)) = connect_as(startup.password(password)).await else {
            return;
        };

        conn.query("SELECT current_user");
        let mut response = conn.response();
        response.row_description().await.unwrap();
        assert_eq!(text_rows(&mut response).await, [[Some(user.to_owned())]]);
    }
}

/// Proves each method really checks the password, rather than falling back
/// to trust.
#[tokio::test]
async fn wrong_and_missing_passwords_are_rejected() {
    let Some(port) = port() else { return };
    for user in ["pw_user", "md5_user", "sha_user"] {
        let mut startup = Startup::new(user);
        startup.database("postgres");
        let error = startup.connect(tcp(port).await).await.err().unwrap();
        assert!(
            matches!(error, Error::Auth(pg_stream::AuthError::PasswordRequired)),
            "{user}"
        );

        startup.password("wrong");
        let error = startup.connect(tcp(port).await).await.err().unwrap();
        assert_eq!(sqlstate(error), "28P01", "{user}");
    }
}

#[tokio::test]
async fn a_method_that_is_not_allowed_is_not_answered() {
    let Some(port) = port() else { return };
    let allowed = [AuthMethod::ScramSha256, AuthMethod::ScramSha256Plus];

    // The server asks this user for the password itself.
    let mut startup = Startup::new("pw_user");
    startup.database("postgres").password("pw");
    startup.allow_only(&allowed);
    let error = startup.connect(tcp(port).await).await.err();
    let refused = AuthError::MethodNotAllowed(AuthMethod::Cleartext);
    assert!(matches!(&error, Some(Error::Auth(e)) if e.to_string() == refused.to_string()));

    let mut startup = sha_user();
    startup.allow_only(&allowed);
    startup.connect(tcp(port).await).await.unwrap();
}

#[tokio::test]
async fn simple_query_with_typed_readers() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query("SELECT 1 AS one, NULL::text AS nothing, 'x' AS letter");
    let mut response = conn.response();

    let columns = response.row_description().await.unwrap().unwrap();
    let types: Vec<_> = columns.iter().map(|column| column.type_oid).collect();
    assert_eq!(mock::column_names(columns), ["one", "nothing", "letter"]);
    assert_eq!(types, [oid::INT4, oid::TEXT, oid::TEXT]);

    let rows = text_rows(&mut response).await;
    assert_eq!(rows, [[Some("1".to_owned()), None, Some("x".to_owned())]]);

    let completion = response.complete().await.unwrap();
    assert_eq!(completion.rows(), Some(1));
    assert_eq!(response.finish().await.unwrap(), TransactionStatus::Idle);
}

#[tokio::test]
async fn the_rows_of_a_simple_query_come_after_their_description() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query("SELECT 1");
    let mut response = conn.response();
    let error = response.row().await.unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol(ProtocolError::Unexpected { code: b'T', .. })
    ));

    response.row_description().await.unwrap();
    assert_eq!(text_rows(&mut response).await, [[Some("1".to_owned())]]);
    response.finish().await.unwrap();
}

#[tokio::test]
async fn multiple_statements_are_iterated_with_next() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query("SELECT 1; SELECT 2, 3; ; BEGIN");
    let mut response = conn.response();
    let mut seen = Vec::new();
    while let Some(message) = response.next().await.unwrap() {
        seen.push(match message {
            Message::RowDescription(columns) => format!("columns:{}", columns.len()),
            Message::DataRow(row) => format!("row:{}", row.len()),
            Message::CommandComplete(done) => done.tag.to_owned(),
            Message::ReadyForQuery(status) => format!("{status:?}"),
            other => panic!("unexpected {other:?}"),
        });
    }

    let expected = [
        "columns:1",
        "row:1",
        "SELECT 1",
        "columns:2",
        "row:2",
        "SELECT 1",
        "BEGIN",
    ];
    assert_eq!(seen[..7], expected);
    assert_eq!(seen[7], "InTransaction");
    drop(response);
    assert_eq!(conn.transaction_status(), TransactionStatus::InTransaction);
}

#[tokio::test]
async fn extended_query_round_trip() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.parse(
        "add",
        "SELECT $1::int4 + $2::int4, $3::text",
        &[oid::INT4, oid::INT4],
    )
    .describe_statement("add")
    .bind("", "add", params![40i32, 2i32, None::<&str>], Format::Text)
    .describe_portal("")
    .execute("", 0)
    .close_statement("add")
    .sync();

    let mut response = conn.response();
    response.parse_complete().await.unwrap();

    let parameters = response.parameter_description().await.unwrap();
    assert_eq!(
        parameters.iter().collect::<Vec<_>>(),
        [oid::INT4, oid::INT4, oid::TEXT]
    );
    assert_eq!(response.row_description().await.unwrap().unwrap().len(), 2);

    response.bind_complete().await.unwrap();
    assert_eq!(response.row_description().await.unwrap().unwrap().len(), 2);
    assert_eq!(
        text_rows(&mut response).await,
        [[Some("42".to_owned()), None]]
    );
    assert_eq!(response.complete().await.unwrap().rows(), Some(1));
    response.close_complete().await.unwrap();
    assert_eq!(response.finish().await.unwrap(), TransactionStatus::Idle);
}

#[tokio::test]
async fn binary_parameters_and_results() {
    let Some(mut conn) = connect().await else {
        return;
    };

    let ids = [7i64, -1, i64::MAX];
    let write = |values: &mut pg_stream::frontend::ParamWriter<'_>| {
        for id in ids {
            values.write(|buf| buf.extend_from_slice(&id.to_be_bytes()));
        }
        values.null();
        values.bytes(&[0xde, 0xad]);
    };
    conn.parse(
        "",
        "SELECT $1::int8, $2::int8, $3::int8, $4::int8, $5::bytea",
        &[],
    )
    .bind("", "", params_with(Format::Binary, write), Format::Binary)
    .execute("", 0)
    .sync();

    let mut response = conn.response();
    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    let row = response.row().await.unwrap().unwrap();
    let values: Vec<_> = row.iter().collect();
    assert_eq!(values[0], Some(&7i64.to_be_bytes()[..]));
    assert_eq!(values[1], Some(&(-1i64).to_be_bytes()[..]));
    assert_eq!(values[2], Some(&i64::MAX.to_be_bytes()[..]));
    assert_eq!(values[3], None);
    assert_eq!(values[4], Some(&[0xde, 0xad][..]));
    response.finish().await.unwrap();
}

#[tokio::test]
async fn byte_arrays_are_binary_parameters() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.parse("", "SELECT $1::uuid", &[])
        .bind("", "", params![[0x11u8; 16]], Format::Text)
        .execute("", 0)
        .sync();
    let mut response = conn.response();
    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    let uuid = "11111111-1111-1111-1111-111111111111".to_owned();
    assert_eq!(text_rows(&mut response).await, [[Some(uuid)]]);
    response.finish().await.unwrap();
}

#[tokio::test]
async fn a_failed_encoder_leaves_nothing_queued() {
    let Some(mut conn) = connect().await else {
        return;
    };

    let failing = try_params_with(Format::Binary, |values| {
        values.write(|buf| buf.push(1));
        values.try_write(|_| Err::<IsNull, _>("cannot encode"))
    });
    conn.parse("", "SELECT $1::int4, $2::int4", &[]);
    let queued = conn.buf().len();
    assert_eq!(
        conn.try_bind("", "", failing, Format::Text).err(),
        Some("cannot encode")
    );
    assert_eq!(conn.buf().len(), queued);

    // The connection carries on as if the Bind had never been attempted.
    let nulls = try_params_with(Format::Binary, |values| {
        values.try_write(|buf| {
            buf.extend_from_slice(&5i32.to_be_bytes());
            Ok::<_, &str>(IsNull::No)
        })?;
        // Whatever an encoder wrote before deciding on NULL is taken back.
        values.try_write(|buf| {
            buf.extend_from_slice(&9i32.to_be_bytes());
            Ok::<_, &str>(IsNull::Yes)
        })
    });
    let bound = conn.try_bind("", "", nulls, Format::Text).unwrap();
    bound.execute("", 0).sync();

    let mut response = conn.response();
    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    assert_eq!(
        text_rows(&mut response).await,
        [[Some("5".to_owned()), None]]
    );
    response.finish().await.unwrap();
}

#[tokio::test]
async fn an_error_aborts_the_rest_of_the_cycle() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.parse("", "SELECT * FROM no_such_table", &[])
        .bind("", "", (), Format::Text)
        .execute("", 0)
        .sync()
        .query("SELECT 'still alive'");

    let mut response = conn.response();
    assert_eq!(
        sqlstate(response.parse_complete().await.unwrap_err()),
        "42P01"
    );
    assert!(matches!(
        response.bind_complete().await,
        Err(Error::Aborted)
    ));
    assert!(matches!(response.row().await, Err(Error::Aborted)));
    assert!(matches!(response.complete().await, Err(Error::Aborted)));
    // The error has been reported once already.
    assert_eq!(response.finish().await.unwrap(), TransactionStatus::Idle);

    // The next cycle is unaffected.
    let mut response = conn.response();
    response.row_description().await.unwrap();
    assert_eq!(
        text_rows(&mut response).await,
        [[Some("still alive".to_owned())]]
    );
    response.finish().await.unwrap();
}

#[tokio::test]
async fn finish_reports_an_error_nobody_read() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query("SELECT 1; SELECT 1/0; SELECT 2");
    assert_eq!(
        sqlstate(conn.response().finish().await.unwrap_err()),
        "22012"
    );

    // An error raised by the Sync itself has no reply of its own to appear in.
    run(&mut conn, "CREATE TEMP TABLE parent (id int PRIMARY KEY)").await;
    run(
        &mut conn,
        "CREATE TEMP TABLE child (id int REFERENCES parent DEFERRABLE INITIALLY DEFERRED)",
    )
    .await;
    conn.parse("", "INSERT INTO child VALUES (1)", &[])
        .bind("", "", (), Format::Text)
        .execute("", 0)
        .sync();
    let mut response = conn.response();
    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    assert_eq!(response.complete().await.unwrap().rows(), Some(1));
    assert_eq!(sqlstate(response.finish().await.unwrap_err()), "23503");

    run(&mut conn, "SELECT 1").await;
}

/// An early return, a panic or a cancelled future all look like this.
#[tokio::test]
async fn a_dropped_response_is_skipped() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query("SELECT generate_series(1, 10000)")
        .query("SELECT 'next'");

    let mut response = conn.response();
    response.row_description().await.unwrap();
    assert_eq!(
        response.row().await.unwrap().unwrap().get(0),
        Some(Some(&b"1"[..]))
    );
    drop(response);

    let mut response = conn.response();
    response.row_description().await.unwrap();
    assert_eq!(text_rows(&mut response).await, [[Some("next".to_owned())]]);
    response.finish().await.unwrap();

    // Never looking at a response at all skips the whole cycle.
    conn.query("SELECT 'ignored'").query("SELECT 'wanted'");
    drop(conn.response());
    let mut response = conn.response();
    response.row_description().await.unwrap();
    assert_eq!(
        text_rows(&mut response).await,
        [[Some("wanted".to_owned())]]
    );
}

#[tokio::test]
async fn a_mismatched_reply_is_left_unread() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.parse("", "SELECT 1", &[])
        .bind("", "", (), Format::Text)
        .execute("", 0)
        .sync();
    let mut response = conn.response();

    let error = response.bind_complete().await.unwrap_err();
    let expected = ProtocolError::Unexpected {
        code: b'1',
        expected: "BindComplete",
    };
    assert!(matches!(error, Error::Protocol(e) if e == expected));

    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    assert_eq!(text_rows(&mut response).await, [[Some("1".to_owned())]]);
    assert_eq!(response.complete().await.unwrap().rows(), Some(1));
    response.finish().await.unwrap();

    // Once the cycle is over there is nothing left to ask for.
    let error = response.row().await.unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol(ProtocolError::Unexpected { code: b'Z', .. })
    ));
    assert!(response.next().await.unwrap().is_none());
}

#[tokio::test]
async fn portals_suspend_at_the_row_limit() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query("BEGIN")
        .parse("", "SELECT generate_series(1, 5)", &[])
        .bind("cursor", "", (), Format::Text)
        .execute("cursor", 2)
        .execute("cursor", 2)
        .execute("cursor", 2)
        .sync();
    conn.response().finish().await.unwrap();

    let mut response = conn.response();
    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    for expected in [2, 2] {
        assert_eq!(text_rows(&mut response).await.len(), expected);
        assert_eq!(response.complete().await.unwrap(), Completion::Suspended);
    }
    // The count is of the rows this Execute returned, not the portal's total.
    assert_eq!(text_rows(&mut response).await.len(), 1);
    assert_eq!(response.complete().await.unwrap().rows(), Some(1));
    assert_eq!(
        response.finish().await.unwrap(),
        TransactionStatus::InTransaction
    );
}

#[tokio::test]
async fn empty_queries_complete() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.parse("", "", &[])
        .bind("", "", (), Format::Text)
        .execute("", 0)
        .sync();
    let mut response = conn.response();
    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    assert_eq!(response.complete().await.unwrap(), Completion::EmptyQuery);
    response.finish().await.unwrap();
}

#[tokio::test]
async fn function_call_returns_a_value() {
    let Some(mut conn) = connect().await else {
        return;
    };

    // 1344 is dsqrt(float8).
    conn.function_call(1344, params![9f64], Format::Binary);
    let mut response = conn.response();
    assert_eq!(
        response.function_result().await.unwrap(),
        Some(&3f64.to_be_bytes()[..])
    );
    response.finish().await.unwrap();

    conn.function_call(1344, params![None::<f64>], Format::Binary);
    let mut response = conn.response();
    assert_eq!(response.function_result().await.unwrap(), None);
    response.finish().await.unwrap();
}

/// Enough traffic in both directions that neither side's socket buffers can
/// hold it. A client that only writes, or only reads, stalls here: the server
/// stops reading once it cannot send, and then the client cannot send either.
#[tokio::test]
async fn deep_pipelines_do_not_deadlock() {
    const QUERIES: usize = 600;
    const SIZE: usize = 64 * 1024;
    let Some(mut conn) = connect().await else {
        return;
    };

    let padding = "x".repeat(SIZE);
    for flush_first in [true, false] {
        for id in 0..QUERIES {
            conn.query(&format!("SELECT {id}, repeat('y', {SIZE}) /* {padding} */"));
        }
        if flush_first {
            within(60, conn.flush()).await.unwrap();
        }

        for id in 0..QUERIES {
            let mut response = conn.response();
            within(60, response.row_description()).await.unwrap();
            let row = within(60, response.row()).await.unwrap().unwrap();
            let values: Vec<_> = row.iter().collect();
            assert_eq!(values[0], Some(id.to_string().as_bytes()));
            assert_eq!(values[1].unwrap().len(), SIZE);
            within(60, response.finish()).await.unwrap();
        }
    }
}

/// One message larger than the socket buffers, sent by the read that waits
/// for its reply. Nothing arrives until all of it is written, so that read
/// has to be woken by the socket taking more.
#[tokio::test]
async fn a_read_sends_a_message_larger_than_the_socket_buffers() {
    const SIZE: usize = 32 * 1024 * 1024;
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query(&format!("SELECT 1 /* {} */", "x".repeat(SIZE)));
    let mut response = conn.response();
    within(60, response.row_description()).await.unwrap();
    assert_eq!(text_rows(&mut response).await, [[Some("1".to_owned())]]);
    within(60, response.finish()).await.unwrap();
}

/// A flush held up by a server that is itself held up sending a reply larger
/// than the socket buffers. That flush has to be woken by the reply arriving,
/// and take it.
#[tokio::test]
async fn a_flush_takes_a_reply_larger_than_the_socket_buffers() {
    const SIZE: usize = 32 * 1024 * 1024;
    const QUERIES: usize = 512;
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query(&format!("SELECT repeat('y', {SIZE})"));
    let padding = "x".repeat(SIZE / QUERIES);
    for _ in 0..QUERIES {
        conn.query(&format!("SELECT 1 /* {padding} */"));
    }
    within(60, conn.flush()).await.unwrap();

    let mut response = conn.response();
    response.row_description().await.unwrap();
    let row = response.row().await.unwrap().unwrap();
    assert_eq!(row.iter().next().flatten().unwrap().len(), SIZE);
    response.finish().await.unwrap();
    for _ in 0..QUERIES {
        conn.response().finish().await.unwrap();
    }
}

/// The room made for a large reply is kept for the next one, given up when
/// small replies follow, and made again when it is needed.
#[tokio::test]
async fn large_replies_come_and_go_among_small_ones() {
    let Some(mut conn) = connect().await else {
        return;
    };

    for cycle in 0..60 {
        let size = if cycle % 20 < 2 { 300_000 } else { 10 };
        conn.query(&format!("SELECT repeat('y', {size})"));
        let mut response = conn.response();
        response.row_description().await.unwrap();
        let row = response.row().await.unwrap().unwrap();
        assert_eq!(row.get(0).unwrap().unwrap().len(), size, "cycle {cycle}");
        response.finish().await.unwrap();
    }
}

#[tokio::test]
async fn copy_in_with_simple_and_extended_queries() {
    let Some(mut conn) = connect().await else {
        return;
    };
    run(&mut conn, "CREATE TEMP TABLE people (id int, name text)").await;

    conn.query("COPY people FROM STDIN");
    let mut response = conn.response();
    let copy = response.copy_in_response().await.unwrap();
    assert_eq!((copy.format, copy.len()), (Format::Text, 2));
    response
        .copy_data(b"1\talice\n")
        .copy_data(b"2\tbob\n")
        .copy_done();
    assert_eq!(response.complete().await.unwrap().rows(), Some(2));
    response.finish().await.unwrap();

    conn.parse("", "COPY people FROM STDIN", &[])
        .bind("", "", (), Format::Text)
        .execute("", 0)
        .sync();
    let mut response = conn.response();
    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    response.copy_in_response().await.unwrap();
    // The Sync above was swallowed by the COPY, so it takes another.
    response.copy_data(b"3\tcarol\n").copy_done().sync();
    assert_eq!(response.complete().await.unwrap().rows(), Some(1));
    response.finish().await.unwrap();

    conn.query("SELECT count(*) FROM people");
    let mut response = conn.response();
    response.row_description().await.unwrap();
    assert_eq!(text_rows(&mut response).await, [[Some("3".to_owned())]]);
}

#[tokio::test]
async fn copy_in_can_be_failed_by_the_client() {
    let Some(mut conn) = connect().await else {
        return;
    };
    run(&mut conn, "CREATE TEMP TABLE people (id int, name text)").await;

    conn.query("COPY people FROM STDIN");
    let mut response = conn.response();
    response.copy_in_response().await.unwrap();

    // The server is waiting on us, so there is no ReadyForQuery to skip to.
    let error = response.finish().await.unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol(ProtocolError::Unexpected { code: b'G', .. })
    ));

    response
        .copy_data(b"1\talice\n")
        .copy_fail("changed my mind");
    let error = response.complete().await.unwrap_err();
    assert_eq!(sqlstate(error), "57014");
    response.finish().await.unwrap();

    run(&mut conn, "SELECT 1").await;
}

#[tokio::test]
async fn abandoning_a_copy_in_desynchronizes() {
    let Some(mut conn) = connect().await else {
        return;
    };
    run(&mut conn, "CREATE TEMP TABLE people (id int, name text)").await;

    conn.query("COPY people FROM STDIN");
    let mut response = conn.response();
    response.copy_in_response().await.unwrap();
    drop(response);

    let error = conn.response().finish().await.unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol(ProtocolError::Desynchronized)
    ));
    let error = conn.recv().await.unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol(ProtocolError::Desynchronized)
    ));
}

#[tokio::test]
async fn copy_out_streams_data() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query("COPY (SELECT g, g * 2 FROM generate_series(1, 3) g) TO STDOUT");
    let mut response = conn.response();
    assert_eq!(response.copy_out_response().await.unwrap().len(), 2);
    let mut data = Vec::new();
    while let Some(chunk) = response.copy_out_data().await.unwrap() {
        data.extend_from_slice(chunk);
    }
    assert_eq!(data, b"1\t2\n2\t4\n3\t6\n");
    assert_eq!(response.complete().await.unwrap().rows(), Some(3));
    response.finish().await.unwrap();

    // Data nobody reads is skipped like rows are.
    conn.query("COPY (SELECT generate_series(1, 1000)) TO STDOUT");
    let mut response = conn.response();
    response.copy_out_response().await.unwrap();
    assert_eq!(response.complete().await.unwrap().rows(), Some(1000));
    response.finish().await.unwrap();
}

#[tokio::test]
async fn asynchronous_messages_are_set_aside() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query(
        "LISTEN news; NOTIFY news, 'hello'; SET application_name = 'renamed'; \
         DO $$ BEGIN RAISE NOTICE 'look out'; END $$; SELECT 1",
    );
    let mut response = conn.response();
    for _ in 0..4 {
        response.complete().await.unwrap();
    }
    response.row_description().await.unwrap();
    assert_eq!(text_rows(&mut response).await, [[Some("1".to_owned())]]);
    response.finish().await.unwrap();

    let mut seen: Vec<_> = conn.async_messages().map(mock::describe).collect();
    seen.sort();
    let expected = [
        "notice look out",
        "notify news hello",
        "parameter application_name=renamed",
    ];
    assert_eq!(seen, expected);

    // They are handed out once.
    assert_eq!(conn.async_messages().count(), 0);
}

#[tokio::test]
async fn recv_waits_for_notifications() {
    let Some(mut listener) = connect().await else {
        return;
    };
    let Some(mut notifier) = connect().await else {
        return;
    };

    run(&mut listener, "LISTEN wakeup").await;
    run(&mut notifier, "NOTIFY wakeup, 'now'").await;

    match within(10, listener.recv()).await.unwrap() {
        Message::NotificationResponse(notification) => {
            assert_eq!(
                (notification.channel, notification.payload),
                ("wakeup", "now")
            );
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn frames_can_be_handled_on_another_task() {
    let Some(mut conn) = connect().await else {
        return;
    };

    conn.query("SELECT g, repeat('z', 100) FROM generate_series(1, 500) g");
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let decoder = tokio::spawn(async move {
        let mut sum = 0u64;
        while let Some(frame) = receiver.recv().await {
            let frame: pg_stream::backend::Frame = frame;
            if let Message::DataRow(row) = frame.parse().unwrap() {
                let id = std::str::from_utf8(row.get(0).unwrap().unwrap()).unwrap();
                sum += id.parse::<u64>().unwrap();
            }
        }
        sum
    });

    loop {
        let frame = conn.recv_frame().await.unwrap();
        let done = frame.code == b'Z';
        sender.send(frame).unwrap();
        if done {
            break;
        }
    }
    drop(sender);
    assert_eq!(decoder.await.unwrap(), 500 * 501 / 2);
    assert_eq!(conn.transaction_status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn frames_hold_many_rows_each() {
    const ROWS: u64 = 5000;
    let Some(mut conn) = connect().await else {
        return;
    };

    let sql = format!("BEGIN; SELECT g, repeat('z', 100) FROM generate_series(1, {ROWS}) g");
    conn.query(&sql);
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let decoder = tokio::spawn(async move {
        let mut sum = 0u64;
        while let Some(frames) = receiver.recv().await {
            let frames: pg_stream::backend::Frames = frames;
            for message in frames.messages() {
                if let Message::DataRow(row) = message.unwrap() {
                    let id = std::str::from_utf8(row.get(0).unwrap().unwrap()).unwrap();
                    sum += id.parse::<u64>().unwrap();
                }
            }
        }
        sum
    });

    loop {
        let frames = conn.recv_frames().await.unwrap();
        let done = frames.ends_cycle();
        sender.send(frames).unwrap();
        if done {
            break;
        }
    }
    drop(sender);
    assert_eq!(decoder.await.unwrap(), ROWS * (ROWS + 1) / 2);
    assert_eq!(conn.transaction_status(), TransactionStatus::InTransaction);
}

#[tokio::test]
async fn messages_encoded_elsewhere_can_be_queued() {
    let Some(mut conn) = connect().await else {
        return;
    };

    let mut request = Vec::new();
    request
        .parse("", "SELECT $1::text", &[])
        .bind("", "", params!["prebuilt"], Format::Text);
    request.execute("", 0).sync();
    conn.buf().extend_from_slice(&request);

    let mut response = conn.response();
    response.parse_complete().await.unwrap();
    response.bind_complete().await.unwrap();
    assert_eq!(
        text_rows(&mut response).await,
        [[Some("prebuilt".to_owned())]]
    );
    response.finish().await.unwrap();
}

#[tokio::test]
async fn cancel_interrupts_a_running_query() {
    let Some(port) = port() else { return };
    let (mut conn, session) = connect_as(&mut Startup::new("postgres")).await.unwrap();

    conn.query("SELECT pg_sleep(60)");
    assert_ne!(conn.unsent(), 0);
    conn.flush().await.unwrap();
    assert_eq!(conn.unsent(), 0);
    // Give the query time to start, or there is nothing to cancel.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let key = session.backend_key.unwrap();
    within(10, cancel(tcp(port).await, &key)).await.unwrap();

    let error = within(10, conn.response().finish()).await.unwrap_err();
    assert_eq!(sqlstate(error), "57014");
    run(&mut conn, "SELECT 1").await;
}

#[tokio::test]
async fn the_message_limit_is_enforced() {
    let Some(mut conn) = connect().await else {
        return;
    };
    conn.set_max_message_len(1024);

    conn.query("SELECT repeat('x', 10000)");
    let mut response = conn.response();
    response.row_description().await.unwrap();
    let error = response.row().await.unwrap_err();
    assert!(matches!(
        error,
        Error::Protocol(ProtocolError::TooLarge { code: b'D', .. })
    ));
}

#[tokio::test]
async fn a_server_that_hangs_up_says_why_first() {
    let Some(mut conn) = connect().await else {
        return;
    };
    let Some(mut admin) = connect().await else {
        return;
    };

    conn.query("SELECT pg_backend_pid()");
    let mut response = conn.response();
    response.row_description().await.unwrap();
    let pid = text_rows(&mut response).await[0][0].clone().unwrap();
    response.finish().await.unwrap();
    run(&mut admin, &format!("SELECT pg_terminate_backend({pid})")).await;

    // The query is written into a connection the server has already closed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    conn.query("SELECT 1");
    let error = within(10, conn.response().finish()).await.unwrap_err();
    assert_eq!(sqlstate(error), "57P01");

    let error = within(10, conn.recv()).await.unwrap_err();
    assert!(matches!(error, Error::Io(_)), "{error:?}");
}

#[test]
fn blocking_streams_work_without_a_runtime() {
    let Some(port) = port() else { return };
    let stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();

    let (mut conn, session) = block_on(sha_user().connect(Blocking(stream))).unwrap();
    assert!(session.backend_key.is_some());

    conn.parse("", "SELECT $1::int4 * 2", &[])
        .bind("", "", params![21i32], Format::Text)
        .execute("", 0)
        .sync();
    let rows = block_on(async {
        let mut response = conn.response();
        response.parse_complete().await?;
        response.bind_complete().await?;
        let rows = text_rows(&mut response).await;
        response.finish().await?;
        Ok::<_, Error>(rows)
    });
    assert_eq!(rows.unwrap(), [[Some("42".to_owned())]]);

    conn.terminate();
    block_on(conn.flush()).unwrap();
}

/// A blocking stream cannot read while it waits to write, so it is given the
/// traffic of `deep_pipelines_do_not_deadlock` a batch at a time.
#[test]
fn blocking_streams_pipeline_in_batches() {
    const QUERIES: usize = 600;
    const BATCH: usize = 16;
    const SIZE: usize = 64 * 1024;
    let Some(port) = port() else { return };
    let stream = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let (mut conn, _) = block_on(sha_user().connect(Blocking(stream))).unwrap();

    let padding = "x".repeat(SIZE);
    for batch in 0..QUERIES / BATCH {
        for _ in 0..BATCH {
            conn.query(&format!("SELECT repeat('y', {SIZE}) /* {padding} */"));
        }
        for query in 0..BATCH {
            let mut response = conn.response();
            block_on(response.row_description()).unwrap();
            let row = block_on(response.row()).unwrap().unwrap();
            let value = row.iter().next().flatten().unwrap();
            assert_eq!(value.len(), SIZE, "batch {batch}, query {query}");
            block_on(response.finish()).unwrap();
        }
    }
}

mod tls {
    use tokio_rustls::{
        TlsConnector,
        client::TlsStream,
        rustls::{
            self, ClientConfig, DigitallySignedStruct, SignatureScheme,
            client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
            pki_types::{CertificateDer, ServerName, UnixTime},
        },
    };

    use super::*;

    /// Upgrades a new connection to TLS, returning the stream and the
    /// server's certificate.
    async fn upgrade(port: u16) -> (TlsStream<TcpStream>, Vec<u8>) {
        let mut stream = tcp(port).await;
        let accepted = request_tls(&mut stream).await.unwrap();
        assert!(accepted, "the server refused TLS");

        let provider = rustls::crypto::ring::default_provider();
        provider.install_default().ok();
        let config = ClientConfig::builder().dangerous();
        let config = config.with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate));
        let connector = TlsConnector::from(Arc::new(config.with_no_client_auth()));
        let name = ServerName::try_from("localhost").unwrap();
        let stream = connector.connect(name, stream).await.unwrap();

        let certificates = stream.get_ref().1.peer_certificates().unwrap();
        let certificate = certificates[0].to_vec();
        (stream, certificate)
    }

    fn enabled() -> Option<u16> {
        let port = port()?;
        if std::env::var_os("PGSTREAM_TEST_TLS").is_none() {
            eprintln!("skipping: PGSTREAM_TEST_TLS is not set");
            return None;
        }
        Some(port)
    }

    #[tokio::test]
    async fn scram_binds_to_the_tls_channel() {
        let Some(port) = enabled() else { return };
        let (stream, certificate) = upgrade(port).await;

        let mut startup = sha_user();
        startup.server_certificate(&certificate);
        let (mut conn, _) = startup.connect(stream).await.unwrap();

        conn.query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()");
        let mut response = conn.response();
        response.row_description().await.unwrap();
        assert_eq!(text_rows(&mut response).await, [[Some("t".to_owned())]]);
        response.finish().await.unwrap();
    }

    /// The server checks the binding, so a client bound to some other
    /// certificate, as it would be with an impostor in the middle, is refused.
    #[tokio::test]
    async fn a_binding_to_the_wrong_certificate_is_refused() {
        let Some(port) = enabled() else { return };
        let (stream, _) = upgrade(port).await;

        let other = mock::pem_to_der(include_str!("data/certs/ca.crt"));
        let mut startup = sha_user();
        startup.server_certificate(&other);
        let error = startup.connect(stream).await.err().unwrap();
        assert_eq!(sqlstate(error), "28000");
    }

    /// What libpq calls `channel_binding=require`.
    #[tokio::test]
    async fn channel_binding_can_be_required() {
        let Some(port) = enabled() else { return };
        let required = [AuthMethod::ScramSha256Plus];

        let (stream, certificate) = upgrade(port).await;
        let mut startup = sha_user();
        startup.server_certificate(&certificate);
        startup.allow_only(&required);
        startup.connect(stream).await.unwrap();

        // Without the certificate there is nothing to bind to.
        let (stream, _) = upgrade(port).await;
        let error = sha_user().allow_only(&required).connect(stream).await.err();
        let refused = AuthError::MethodNotAllowed(AuthMethod::ScramSha256);
        assert!(matches!(&error, Some(Error::Auth(e)) if e.to_string() == refused.to_string()));
    }

    #[tokio::test]
    async fn scram_works_over_tls_without_binding() {
        let Some(port) = enabled() else { return };
        let (stream, _) = upgrade(port).await;

        let (mut conn, _) = sha_user().connect(stream).await.unwrap();
        run(&mut conn, "SELECT 1").await;
    }

    /// A deep pipeline over TLS, where a write can need a read and the other
    /// way around.
    #[tokio::test]
    async fn pipelines_work_over_tls() {
        let Some(port) = enabled() else { return };
        let (stream, _) = upgrade(port).await;
        let mut startup = Startup::new("postgres");
        let (mut conn, _) = startup.database("postgres").connect(stream).await.unwrap();

        let padding = "x".repeat(32 * 1024);
        for id in 0..300 {
            conn.query(&format!("SELECT {id}, repeat('y', 32768) /* {padding} */"));
        }
        for id in 0..300 {
            let mut response = conn.response();
            within(60, response.row_description()).await.unwrap();
            let row = within(60, response.row()).await.unwrap().unwrap();
            assert_eq!(row.get(0), Some(Some(id.to_string().as_bytes())));
            within(60, response.finish()).await.unwrap();
        }
    }

    /// These tests are about what happens after the TLS handshake, not about
    /// whether the test certificate chains to anything.
    #[derive(Debug)]
    struct AcceptAnyCertificate;

    impl ServerCertVerifier for AcceptAnyCertificate {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _: &[u8],
            _: &CertificateDer<'_>,
            _: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            let algorithms = rustls::crypto::ring::default_provider();
            let algorithms = algorithms.signature_verification_algorithms;
            algorithms.supported_schemes()
        }
    }
}
