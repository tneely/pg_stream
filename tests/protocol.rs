//! The connection against a scripted server: the behaviour that needs more
//! control over the wire than a real server gives.

mod mock;

use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    pin::pin,
    task::Poll,
};

use mock::*;
use pg_stream::{
    Blocking, Completion, Connection, Error, Format, Frontend, Message, ProtocolError, Response,
    backend::{BackendKey, DataRow, TransactionStatus},
    block_on, cancel, params, request_tls,
};

fn connection() -> (Server, Connection<Stream>) {
    let (server, stream) = pair();
    (server, Connection::new(stream))
}

fn text(row: DataRow<'_>) -> Vec<Option<String>> {
    let text = |value: &[u8]| String::from_utf8(value.to_vec()).unwrap();
    row.iter().map(|value| value.map(text)).collect()
}

fn rows(response: &mut Response<'_, Stream>) -> Vec<Vec<Option<String>>> {
    let mut rows = Vec::new();
    while let Some(row) = now(response.row()).unwrap() {
        rows.push(text(row));
    }
    rows
}

fn sqlstate(error: Error) -> String {
    match error {
        Error::Server(error) => error.code().to_owned(),
        other => panic!("expected a server error, got {other:?}"),
    }
}

fn protocol_error<T: std::fmt::Debug>(result: Result<T, Error>) -> ProtocolError {
    match result {
        Err(Error::Protocol(error)) => error,
        other => panic!("expected a protocol error, got {other:?}"),
    }
}

fn io_error<T: std::fmt::Debug>(result: Result<T, Error>) -> io::ErrorKind {
    match result {
        Err(Error::Io(error)) => error.kind(),
        other => panic!("expected an I/O error, got {other:?}"),
    }
}

/// Runs the future that `$make` builds to completion while the wire moves a
/// few bytes at a time, dropping it at random and building it again.
macro_rules! despite_cancellation {
    ($server:expr, $random:expr, $make:expr) => {{
        let mut attempts = 0;
        loop {
            attempts += 1;
            assert!(attempts < 100_000, "the future makes no progress");
            let mut future = pin!($make);
            let mut outcome = None;
            while outcome.is_none() {
                $server.read_room($random.below(9));
                $server.write_room($random.below(9));
                match poll(future.as_mut()) {
                    Poll::Ready(output) => outcome = Some(output),
                    Poll::Pending if $random.below(3) == 0 => break,
                    Poll::Pending => {}
                }
            }
            if let Some(output) = outcome {
                break output;
            }
        }
    }};
}

#[test]
fn messages_survive_any_fragmentation() {
    for max_read in [1, 2, 3, 5, 7, 64] {
        let (server, mut conn) = connection();
        server.max_read(max_read);
        server.send(&row_description(&["id", "name"]));
        server.send(&data_row(&[Some("1"), None]));
        server.send(&data_row(&[Some(""), Some("a longer value")]));
        server.send(&command_complete("SELECT 2"));
        server.send(&ready(b'T'));

        let mut response = conn.response();
        let columns = now(response.row_description()).unwrap().unwrap();
        assert_eq!(column_names(columns), ["id", "name"]);
        let expected = [
            vec![Some("1".to_owned()), None],
            vec![Some(String::new()), Some("a longer value".to_owned())],
        ];
        assert_eq!(rows(&mut response), expected);
        assert_eq!(now(response.complete()).unwrap().rows(), Some(2));
        assert_eq!(
            now(response.finish()).unwrap(),
            TransactionStatus::InTransaction
        );
        assert_eq!(conn.transaction_status(), TransactionStatus::InTransaction);
    }
}

#[test]
fn a_cancelled_read_loses_nothing() {
    let (server, mut conn) = connection();
    let wire = [data_row(&[Some("hello")]), ready(b'I')].concat();

    for split in 1..wire.len() {
        server.send(&wire[..split]);
        {
            // Read until the server has no more to give, then give up.
            let mut response = conn.response();
            let mut reading = pin!(response.finish());
            assert!(poll(reading.as_mut()).is_pending());
        }
        server.send(&wire[split..]);

        // The abandoned response is skipped, and nothing after it is lost.
        server.send(&select("next", b'I'));
        let mut response = conn.response();
        now(response.row_description()).unwrap();
        assert_eq!(
            rows(&mut response),
            [[Some("next".to_owned())]],
            "split at {split}"
        );
        now(response.finish()).unwrap();
    }
}

#[test]
fn a_cancelled_recv_resumes_mid_message() {
    let (server, mut conn) = connection();
    let wire = data_row(&[Some("hello"), None]);

    for split in 0..wire.len() {
        server.send(&wire[..split]);
        {
            let mut receiving = pin!(conn.recv());
            assert!(poll(receiving.as_mut()).is_pending());
        }
        server.send(&wire[split..]);
        match now(conn.recv()).unwrap() {
            Message::DataRow(row) => assert_eq!(text(row), [Some("hello".to_owned()), None]),
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[test]
fn a_cancelled_flush_sends_every_byte_once() {
    let (server, mut conn) = connection();
    let mut expected = Vec::new();

    conn.query("SELECT 1").sync();
    expected.query("SELECT 1").sync();
    for room in [3, 1, 4] {
        server.write_room(room);
        let mut flushing = pin!(conn.flush());
        assert!(poll(flushing.as_mut()).is_pending());
    }
    // The write under way has taken the queue with it.
    assert!(conn.buf().is_empty());
    assert_eq!(conn.unsent(), expected.len() - 8);

    // Messages queued while a write is under way go out after it.
    conn.query("SELECT 2");
    expected.query("SELECT 2");
    assert_eq!(conn.unsent(), expected.len() - 8);
    server.write_room(usize::MAX).max_write(2);
    now(conn.flush()).unwrap();

    assert_eq!(server.received(), expected);
    assert_eq!(conn.unsent(), 0);
}

#[test]
fn reading_sends_what_is_queued() {
    let (server, mut conn) = connection();
    server.reply(b'Q', &select("1", b'I'));

    conn.query("SELECT 1");
    let mut response = conn.response();
    now(response.row_description()).unwrap();
    assert_eq!(rows(&mut response), [[Some("1".to_owned())]]);
    now(response.finish()).unwrap();

    let mut expected = Vec::new();
    expected.query("SELECT 1");
    assert_eq!(server.received(), expected);
    assert_eq!(server.flushes(), 1);
}

#[test]
fn flush_is_not_repeated_without_new_output() {
    let (server, mut conn) = connection();
    conn.sync();
    now(conn.flush()).unwrap();
    now(conn.flush()).unwrap();
    assert_eq!(server.flushes(), 1);
}

/// A server that cannot send its replies stops reading requests. Unless the
/// client takes the replies while it writes, both sides wait forever.
#[test]
fn flushing_takes_replies_off_the_wire() {
    const CYCLES: usize = 400;
    let (server, mut conn) = connection();

    let wide = "x".repeat(1000);
    for id in 0..CYCLES {
        conn.query("SELECT repeat('x', 1000)");
        server.send(&select(&format!("{id}{wide}"), b'I'));
    }
    let queued = conn.buf().len();

    // The server takes part of the pipeline and then nothing more.
    server.write_room(100);
    {
        let mut flushing = pin!(conn.flush());
        assert!(poll(flushing.as_mut()).is_pending());
        assert_eq!(server.unread(), 0, "the replies were left on the wire");

        server.write_room(usize::MAX);
        assert!(poll(flushing.as_mut()).is_ready());
    }
    assert_eq!(server.received().len(), queued);

    // The replies taken meanwhile are all there, and cost no further reads.
    let reads = server.reads();
    for id in 0..CYCLES {
        let mut response = conn.response();
        now(response.row_description()).unwrap();
        assert_eq!(rows(&mut response), [[Some(format!("{id}{wide}"))]]);
        now(response.finish()).unwrap();
    }
    assert_eq!(server.reads(), reads);
}

#[test]
fn a_failed_write_does_not_hide_what_the_server_said() {
    let (server, mut conn) = connection();
    server.fail_writes(io::ErrorKind::BrokenPipe);
    server.send(&fatal(
        "57P01",
        "terminating connection due to administrator command",
    ));
    server.close();

    conn.query("SELECT 1");
    assert_eq!(
        sqlstate(now(conn.response().finish()).unwrap_err()),
        "57P01"
    );
    assert_eq!(io_error(now(conn.recv())), io::ErrorKind::UnexpectedEof);
    assert_eq!(io_error(now(conn.flush())), io::ErrorKind::BrokenPipe);
}

#[test]
fn end_of_file_is_reported_after_everything_before_it() {
    let (server, mut conn) = connection();
    let partial = data_row(&[Some("cut short")]);
    server
        .send(&command_complete("SELECT 0"))
        .send(&partial[..partial.len() - 1])
        .close();

    assert!(matches!(
        now(conn.recv()).unwrap(),
        Message::CommandComplete(_)
    ));
    assert_eq!(io_error(now(conn.recv())), io::ErrorKind::UnexpectedEof);
    assert_eq!(io_error(now(conn.recv())), io::ErrorKind::UnexpectedEof);
    assert_eq!(
        io_error(now(conn.response().finish())),
        io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn asynchronous_messages_are_set_aside_in_order() {
    let (server, mut conn) = connection();
    server.send(&notice("first"));
    server.send(&row_description(&["value"]));
    server.send(&notification(7, "news", "second"));
    server.send(&data_row(&[Some("1")]));
    server.send(&parameter_status("TimeZone", "UTC"));
    server.send(&command_complete("SELECT 1"));
    server.send(&notice("fourth"));
    server.send(&ready(b'I'));

    let mut response = conn.response();
    now(response.row_description()).unwrap();
    assert_eq!(rows(&mut response), [[Some("1".to_owned())]]);
    now(response.complete()).unwrap();
    now(response.finish()).unwrap();

    let seen: Vec<_> = conn.async_messages().map(describe).collect();
    let expected = [
        "notice first",
        "notify news second",
        "parameter TimeZone=UTC",
        "notice fourth",
    ];
    assert_eq!(seen, expected);
    assert_eq!(conn.async_messages().count(), 0);

    // Read directly, they come as they are.
    server.send(&notice("in place"));
    assert!(matches!(
        now(conn.recv()).unwrap(),
        Message::NoticeResponse(_)
    ));
    assert_eq!(conn.async_messages().count(), 0);
}

#[test]
fn the_backlog_of_asynchronous_messages_is_bounded() {
    const NOTICES: usize = 20_000;
    let (server, mut conn) = connection();
    let one = notice("a notice nobody is going to look at any time soon");
    (0..NOTICES).for_each(|_| _ = server.send(&one));
    server.send(&ready(b'I'));

    now(conn.response().finish()).unwrap();
    let kept = conn.async_messages().count();
    assert!(
        kept > 0 && kept * one.len() <= 300 * 1024,
        "kept {kept} notices"
    );

    // Draining makes room again.
    server.send(&one).send(&ready(b'I'));
    now(conn.response().finish()).unwrap();
    assert_eq!(conn.async_messages().count(), 1);
}

/// Postgres stops reading requests while it cannot send replies. A client
/// that finishes writing before it reads then waits forever, once a pipeline
/// and its replies outgrow the buffers between the two.
#[test]
fn a_server_that_stops_reading_cannot_stall_a_pipeline() {
    const CYCLES: usize = 300;
    for flush_first in [true, false] {
        let (server, mut conn) = connection();
        // Requests are taken in small pieces, and not at all while 200 bytes
        // of replies are waiting to be read.
        server.max_write(64).max_read(48).backlog(200);
        for id in 0..CYCLES {
            conn.query(&format!("SELECT {id}"));
            server.reply(b'Q', &select(&id.to_string(), b'I'));
        }

        if flush_first {
            now(conn.flush()).unwrap();
            assert_eq!(conn.unsent(), 0);
        }
        for id in 0..CYCLES {
            let mut response = conn.response();
            now(response.row_description()).unwrap();
            assert_eq!(rows(&mut response), [[Some(id.to_string())]]);
            now(response.finish()).unwrap();
        }
        assert!(server.script_finished());
    }
}

/// The futures of a connection keep nothing to themselves, so one dropped
/// while it waits can be replaced by another, and no byte is lost, repeated
/// or reordered in either direction.
#[test]
fn futures_dropped_at_random_lose_nothing_and_send_nothing_twice() {
    for seed in 1..=50 {
        let mut random = Random(seed);
        let (server, mut conn) = connection();
        let (mut requests, mut replies) = (Vec::new(), Vec::new());
        for id in 0..20 {
            let sql = format!("SELECT {id}");
            conn.query(&sql);
            requests.query(&sql);
            let reply = match id % 4 {
                0 => [notice("between cycles"), select(&sql, b'I')].concat(),
                1 => [error("42601", "syntax error"), ready(b'I')].concat(),
                _ => select(&sql, b'T'),
            };
            server.reply(b'Q', &reply);
            replies.extend_from_slice(&reply);
        }

        if random.below(2) == 0 {
            despite_cancellation!(server, random, conn.flush()).unwrap();
        }
        let mut expected = VecDeque::from(server_messages(&replies));
        while !expected.is_empty() {
            let mut next = || expected.pop_front().expect("more messages than were sent");
            match random.below(3) {
                0 => {
                    let frame = despite_cancellation!(server, random, conn.recv_frame()).unwrap();
                    assert_eq!((frame.code, frame.body.to_vec()), next(), "seed {seed}");
                }
                1 => {
                    let message = despite_cancellation!(server, random, conn.recv()).unwrap();
                    let (code, body) = next();
                    assert_eq!(message, Message::parse(code, &body).unwrap(), "seed {seed}");
                }
                _ => {
                    let frames = despite_cancellation!(server, random, conn.recv_frames()).unwrap();
                    let mut last = 0;
                    for message in frames.messages() {
                        let (code, body) = next();
                        assert_eq!(message, Message::parse(code, &body), "seed {seed}");
                        last = code;
                    }
                    assert_eq!(frames.ends_cycle(), last == b'Z', "seed {seed}");
                }
            }
        }
        assert_eq!(server.received(), requests, "seed {seed}");
    }
}

/// A response can be walked away from at any point, whether by dropping one
/// of its futures or the response itself, and the next one starts where it
/// should.
#[test]
fn responses_dropped_at_random_leave_the_next_one_aligned() {
    const CYCLES: usize = 20;
    for seed in 1..=50 {
        let mut random = Random(seed);
        let (server, mut conn) = connection();
        let fails = |id: usize| id % 5 == 4;
        for id in 0..=CYCLES {
            conn.query(&format!("SELECT {id}"));
            let rows = [data_row(&[Some(&id.to_string())]), data_row(&[None])];
            let description = row_description(&["value"]);
            let end = [command_complete("SELECT 2"), ready(b'I')].concat();
            let refusal = [error("42601", "syntax error"), ready(b'I')].concat();
            let reply = match fails(id) {
                true => refusal,
                false => [description, rows.concat(), end].concat(),
            };
            server.reply(b'Q', &reply);
        }

        for id in 0..CYCLES {
            let mut response = conn.response();
            // How much of the cycle is read before it is left behind.
            let steps = random.below(4);
            if steps == 0 {
                continue;
            }
            if steps == 3 && random.below(2) == 0 {
                // The untyped way of reading a cycle through to its end.
                let mut codes = Vec::new();
                loop {
                    let next = despite_cancellation!(server, random, response.next());
                    let Some(message) = next.unwrap() else { break };
                    codes.push(message.code());
                }
                let expected: &[u8] = if fails(id) { b"EZ" } else { b"TDDCZ" };
                assert_eq!(codes, expected, "seed {seed}");
                continue;
            }
            if fails(id) {
                // Whichever method comes across the error reports it.
                let error = match steps {
                    1 => despite_cancellation!(server, random, response.row_description()).err(),
                    _ => despite_cancellation!(server, random, response.finish()).err(),
                };
                assert_eq!(error.map(sqlstate).as_deref(), Some("42601"), "seed {seed}");
                continue;
            }
            despite_cancellation!(server, random, response.row_description()).unwrap();
            if steps == 1 {
                continue;
            }
            let row = despite_cancellation!(server, random, response.row()).unwrap();
            assert_eq!(text(row.unwrap()), [Some(id.to_string())], "seed {seed}");
            if steps == 2 {
                continue;
            }
            despite_cancellation!(server, random, response.finish()).unwrap();
        }

        server.read_room(usize::MAX).write_room(usize::MAX);
        let mut response = conn.response();
        now(response.row_description()).unwrap();
        let expected = [vec![Some(CYCLES.to_string())], vec![None]];
        assert_eq!(rows(&mut response), expected, "seed {seed}");
        now(response.finish()).unwrap();
        assert!(server.script_finished(), "seed {seed}");
    }
}

#[test]
fn an_abandoned_response_is_skipped() {
    let (server, mut conn) = connection();
    server.send(&row_description(&["value"]));
    server.send(&data_row(&[Some("1")]));
    server.send(&data_row(&[Some("2")]));
    server.send(&notice("in the part that is skipped"));
    server.send(&command_complete("SELECT 2"));
    server.send(&ready(b'T'));
    server.send(&select("second", b'I'));
    server.send(&select("third", b'I'));

    let mut response = conn.response();
    now(response.row_description()).unwrap();
    assert_eq!(
        text(now(response.row()).unwrap().unwrap()),
        [Some("1".to_owned())]
    );

    // Starting the next response is what abandons the first.
    let mut response = conn.response();
    now(response.row_description()).unwrap();
    assert_eq!(rows(&mut response), [[Some("second".to_owned())]]);

    // Reading the connection directly does too.
    match now(conn.recv()).unwrap() {
        Message::RowDescription(columns) => assert_eq!(columns.len(), 1),
        other => panic!("unexpected {other:?}"),
    }
    let seen: Vec<_> = conn.async_messages().map(describe).collect();
    assert_eq!(seen, ["notice in the part that is skipped"]);
}

#[test]
fn a_response_never_opened_claims_nothing() {
    let (server, mut conn) = connection();
    server.send(&select("only", b'I'));

    // A finished response does not count against the next one.
    let mut response = conn.response();
    now(response.finish()).unwrap();
    server.send(&select("next", b'I'));
    let mut response = conn.response();
    now(response.row_description()).unwrap();
    assert_eq!(rows(&mut response), [[Some("next".to_owned())]]);
}

#[test]
fn an_error_ends_the_cycle_for_typed_readers() {
    let (server, mut conn) = connection();
    server.send(&message(b'1', b""));
    server.send(&error("42P01", "relation does not exist"));
    server.send(&ready(b'E'));

    let mut response = conn.response();
    now(response.parse_complete()).unwrap();
    assert_eq!(
        sqlstate(now(response.bind_complete()).unwrap_err()),
        "42P01"
    );
    assert!(matches!(now(response.row()), Err(Error::Aborted)));
    assert!(matches!(now(response.complete()), Err(Error::Aborted)));
    assert!(matches!(
        now(response.close_complete()),
        Err(Error::Aborted)
    ));
    assert!(matches!(
        now(response.row_description()),
        Err(Error::Aborted)
    ));

    // Asking for the skipped replies read nothing: ReadyForQuery is still next.
    assert_eq!(now(response.finish()).unwrap(), TransactionStatus::Failed);
}

#[test]
fn an_error_among_rows_is_returned_by_the_row_reader() {
    let (server, mut conn) = connection();
    server.send(&row_description(&["value"]));
    server.send(&data_row(&[Some("1")]));
    server.send(&error("22012", "division by zero"));
    server.send(&ready(b'I'));

    let mut response = conn.response();
    now(response.row_description()).unwrap();
    assert!(now(response.row()).unwrap().is_some());
    assert_eq!(sqlstate(now(response.row()).unwrap_err()), "22012");
    now(response.finish()).unwrap();
}

#[test]
fn rows_are_not_read_past_a_reply_that_is_not_one() {
    let (server, mut conn) = connection();
    server.send(&select("1", b'I'));
    let expected = "DataRow";
    let unexpected = |code| ProtocolError::Unexpected { code, expected };

    // A simple Query describes its rows before it sends them.
    let mut response = conn.response();
    assert_eq!(protocol_error(now(response.row())), unexpected(b'T'));
    now(response.row_description()).unwrap();
    assert_eq!(rows(&mut response), [[Some("1".to_owned())]]);

    // What ends the rows is left for `complete`, and nothing after it is one.
    assert_eq!(now(response.complete()).unwrap().rows(), Some(1));
    assert_eq!(protocol_error(now(response.row())), unexpected(b'Z'));
    assert_eq!(now(response.finish()).unwrap(), TransactionStatus::Idle);
}

#[test]
fn finish_reports_the_first_error_nobody_read() {
    let (server, mut conn) = connection();
    server.send(&command_complete("INSERT 0 1"));
    server.send(&error("23505", "duplicate key"));
    server.send(&error("XX000", "a second error"));
    server.send(&ready(b'I'));

    let mut response = conn.response();
    assert_eq!(sqlstate(now(response.finish()).unwrap_err()), "23505");
    // The cycle has ended all the same, and says so if asked again.
    assert_eq!(now(response.finish()).unwrap(), TransactionStatus::Idle);

    // An error with no reply of its own, as when a commit fails at Sync.
    server
        .send(&message(b'2', b""))
        .send(&command_complete("INSERT 0 1"));
    server
        .send(&error("23503", "foreign key violation"))
        .send(&ready(b'I'));
    let mut response = conn.response();
    now(response.bind_complete()).unwrap();
    assert_eq!(now(response.complete()).unwrap().rows(), Some(1));
    assert_eq!(sqlstate(now(response.finish()).unwrap_err()), "23503");
}

/// `finish` holds on to an error it has read until it can return it, even if
/// the future that read it is dropped before the cycle ends.
#[test]
fn a_cancelled_finish_still_reports_the_error() {
    let (server, mut conn) = connection();
    server.send(&error("40001", "serialization failure"));

    let mut response = conn.response();
    {
        let mut finishing = pin!(response.finish());
        assert!(poll(finishing.as_mut()).is_pending());
    }
    server.send(&ready(b'I'));
    assert_eq!(sqlstate(now(response.finish()).unwrap_err()), "40001");
    assert_eq!(now(response.finish()).unwrap(), TransactionStatus::Idle);
}

#[test]
fn a_reply_that_does_not_match_is_left_unread() {
    let (server, mut conn) = connection();
    server.send(&message(b'1', b"")).send(&message(b'2', b""));
    server.send(&message(b'n', b"")).send(&message(b's', b""));
    server.send(&ready(b'I'));

    let mut response = conn.response();
    let unexpected = |code, expected| ProtocolError::Unexpected { code, expected };
    let error = protocol_error(now(response.bind_complete()));
    assert_eq!(error, unexpected(b'1', "BindComplete"));
    assert_eq!(
        error.to_string(),
        "expected BindComplete, got ParseComplete"
    );

    now(response.parse_complete()).unwrap();
    let error = protocol_error(now(response.row_description()));
    assert_eq!(error, unexpected(b'2', "RowDescription or NoData"));
    now(response.bind_complete()).unwrap();

    assert!(now(response.row_description()).unwrap().is_none());
    assert!(now(response.row()).unwrap().is_none());
    assert_eq!(now(response.complete()).unwrap(), Completion::Suspended);

    // ReadyForQuery is not consumed by a reader that wants something else.
    assert_eq!(
        protocol_error(now(response.parse_complete())),
        unexpected(b'Z', "ParseComplete")
    );
    assert_eq!(now(response.finish()).unwrap(), TransactionStatus::Idle);
    assert_eq!(
        protocol_error(now(response.parse_complete())),
        unexpected(b'Z', "ParseComplete")
    );
}

#[test]
fn next_yields_every_reply_through_ready_for_query() {
    let (server, mut conn) = connection();
    server.send(&select("1", b'I'));
    server.send(&notice("between cycles"));
    server
        .send(&error("42601", "syntax error"))
        .send(&ready(b'I'));

    let mut response = conn.response();
    let mut codes = Vec::new();
    while let Some(message) = now(response.next()).unwrap() {
        codes.push(message.code());
    }
    assert_eq!(codes, b"TDCZ");
    assert!(now(response.next()).unwrap().is_none());

    // An error is a message like any other here, and stops typed readers.
    let mut response = conn.response();
    match now(response.next()).unwrap() {
        Some(Message::ErrorResponse(error)) => assert_eq!(error.code(), "42601"),
        other => panic!("unexpected {other:?}"),
    }
    assert!(matches!(now(response.row()), Err(Error::Aborted)));
    assert!(matches!(
        now(response.next()).unwrap(),
        Some(Message::ReadyForQuery(_))
    ));
    assert_eq!(conn.async_messages().count(), 1);
}

#[test]
fn malformed_messages_are_errors_not_panics() {
    let (server, mut conn) = connection();

    // Two columns declared, one present.
    let truncated = message(b'D', &[0, 2, 0, 0, 0, 1, b'x']);
    server.send(&truncated).send(&ready(b'I'));
    assert_eq!(
        protocol_error(now(conn.recv())),
        ProtocolError::Malformed { code: b'D' }
    );
    // The stream is still framed, so what follows can be read.
    assert!(matches!(
        now(conn.recv()).unwrap(),
        Message::ReadyForQuery(_)
    ));

    server
        .send(&message(b'1', b"unexpected body"))
        .send(&ready(b'I'));
    let mut response = conn.response();
    let error = protocol_error(now(response.parse_complete()));
    assert_eq!(error, ProtocolError::Malformed { code: b'1' });
    now(response.finish()).unwrap();

    server.send(&truncated).send(&ready(b'I'));
    let mut response = conn.response();
    assert_eq!(
        protocol_error(now(response.row())),
        ProtocolError::Malformed { code: b'D' }
    );
    now(response.finish()).unwrap();
}

#[test]
fn an_impossible_length_stops_the_connection() {
    let (server, mut conn) = connection();
    server.send(&[b'Z', 0, 0, 0, 2]);

    let invalid = ProtocolError::InvalidLength { code: b'Z', len: 2 };
    assert_eq!(protocol_error(now(conn.recv())), invalid);
    // Nothing after a broken frame can be trusted to be a frame.
    assert_eq!(protocol_error(now(conn.recv())), invalid);
    assert_eq!(protocol_error(now(conn.response().finish())), invalid);
}

#[test]
fn unknown_messages_pass_through() {
    let (server, mut conn) = connection();
    server.send(&message(b'~', b"from the future"));

    match now(conn.recv()).unwrap() {
        Message::Unknown { code, body } => {
            assert_eq!((code, body), (b'~', &b"from the future"[..]))
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn owned_frames_outlive_later_reads() {
    let (server, mut conn) = connection();
    server.max_read(11);
    let values: Vec<String> = (0..200).map(|id| format!("value number {id}")).collect();
    for value in &values {
        server.send(&data_row(&[Some(value)]));
    }
    server.send(&ready(b'T'));

    let mut frames = Vec::new();
    loop {
        let frame = now(conn.recv_frame()).unwrap();
        if frame.code == b'Z' {
            break;
        }
        frames.push(frame);
    }
    assert_eq!(conn.transaction_status(), TransactionStatus::InTransaction);

    let parsed = frames.iter().map(|frame| match frame.parse().unwrap() {
        Message::DataRow(row) => text(row)[0].clone().unwrap(),
        other => panic!("unexpected {other:?}"),
    });
    assert_eq!(parsed.collect::<Vec<_>>(), values);
}

#[test]
fn frames_hold_what_has_arrived_up_to_the_end_of_a_cycle() {
    let (server, mut conn) = connection();
    let first = select("1", b'T');
    let second = [notice("between cycles"), select("2", b'I')].concat();
    server.send(&first).send(&second);

    // Both cycles are there to be read, and each comes on its own.
    let frames = now(conn.recv_frames()).unwrap();
    assert_eq!(frames.as_bytes()[..], first[..]);
    assert!(frames.ends_cycle());
    let codes = frames.messages().map(|message| message.unwrap().code());
    assert_eq!(codes.collect::<Vec<_>>(), b"TDCZ");
    assert_eq!(conn.transaction_status(), TransactionStatus::InTransaction);

    let frames = now(conn.recv_frames()).unwrap();
    assert_eq!(frames.as_bytes()[..], second[..]);
    assert!(frames.ends_cycle());
    assert_eq!(conn.transaction_status(), TransactionStatus::Idle);

    // A cycle that arrives in pieces comes in as many, of whole messages each.
    let third = select("a value that is cut across several reads", b'I');
    server.max_read(11).send(&third);
    let mut received = Vec::new();
    loop {
        let frames = now(conn.recv_frames()).unwrap();
        received.extend(server_messages(frames.as_bytes()));
        if frames.ends_cycle() {
            break;
        }
    }
    assert_eq!(received, server_messages(&third));

    // A response left unread is skipped first, as it is by any other read.
    server.max_read(usize::MAX).send(&select("skipped", b'I'));
    server.send(&select("wanted", b'I'));
    drop(conn.response());
    let frames = now(conn.recv_frames()).unwrap();
    assert_eq!(frames.as_bytes()[..], select("wanted", b'I')[..]);
}

fn copy_in_response() -> Vec<u8> {
    message(b'G', &[0, 0, 1, 0, 0])
}

#[test]
fn a_copy_can_be_ended_before_it_is_seen_to_start() {
    let (server, mut conn) = connection();

    // The whole COPY is pipelined, twice over, before any reply is read.
    for _ in 0..2 {
        conn.query("COPY t FROM STDIN")
            .copy_data(b"1\n")
            .copy_done();
        server.send(&copy_in_response());
        server.send(&command_complete("COPY 1")).send(&ready(b'I'));
    }
    for _ in 0..2 {
        let mut response = conn.response();
        assert_eq!(now(response.copy_in_response()).unwrap().len(), 1);
        // Nothing is being waited for, so the rest can be skipped.
        assert_eq!(now(response.finish()).unwrap(), TransactionStatus::Idle);
    }
}

#[test]
fn finish_refuses_to_wait_on_a_copy_nobody_is_ending() {
    let (server, mut conn) = connection();
    conn.query("COPY t FROM STDIN");
    server.send(&copy_in_response());

    let mut response = conn.response();
    now(response.copy_in_response()).unwrap();
    let stuck = ProtocolError::Unexpected {
        code: b'G',
        expected: "ReadyForQuery",
    };
    assert_eq!(protocol_error(now(response.finish())), stuck);

    // Ending it, even with bytes encoded elsewhere, lets the cycle finish.
    let mut encoded = Vec::new();
    encoded.copy_fail("never mind");
    response.buf().extend_from_slice(&encoded);
    server.reply(b'Q', b"").reply(
        b'f',
        &[error("57014", "COPY aborted"), ready(b'I')].concat(),
    );
    assert_eq!(sqlstate(now(response.finish()).unwrap_err()), "57014");
}

#[test]
fn finish_does_not_skip_past_a_copy_it_has_not_read() {
    let (server, mut conn) = connection();
    conn.query("COPY t FROM STDIN");
    server.send(&copy_in_response());

    let mut response = conn.response();
    let stuck = ProtocolError::Unexpected {
        code: b'G',
        expected: "ReadyForQuery",
    };
    assert_eq!(protocol_error(now(response.finish())), stuck);
    now(response.copy_in_response()).unwrap();
}

#[test]
fn abandoning_an_open_copy_desynchronizes() {
    for read_start in [true, false] {
        let (server, mut conn) = connection();
        conn.query("COPY t FROM STDIN");
        server.send(&copy_in_response());

        let mut response = conn.response();
        if read_start {
            now(response.copy_in_response()).unwrap();
        }
        assert_eq!(
            protocol_error(now(conn.recv())),
            ProtocolError::Desynchronized
        );
        assert_eq!(
            protocol_error(now(conn.response().finish())),
            ProtocolError::Desynchronized
        );
        assert_eq!(
            protocol_error(now(conn.recv_frame())),
            ProtocolError::Desynchronized
        );
    }
}

#[test]
fn abandoning_an_ended_copy_does_not() {
    let (server, mut conn) = connection();
    conn.query("COPY t FROM STDIN");
    server.send(&copy_in_response());

    let mut response = conn.response();
    now(response.copy_in_response()).unwrap();
    response.copy_data(b"1\n").copy_done();
    server.send(&command_complete("COPY 1")).send(&ready(b'I'));
    server.send(&select("after the copy", b'I'));

    let mut response = conn.response();
    now(response.row_description()).unwrap();
    assert_eq!(rows(&mut response), [[Some("after the copy".to_owned())]]);
}

/// A response can be dropped without ever reading the CopyInResponse, and if
/// the COPY was pipelined whole there is nothing left for the server to wait
/// on.
#[test]
fn abandoning_a_pipelined_copy_does_not() {
    let (server, mut conn) = connection();
    conn.query("COPY t FROM STDIN")
        .copy_data(b"1\n")
        .copy_done();
    server.send(&copy_in_response());
    server.send(&command_complete("COPY 1")).send(&ready(b'I'));
    server.send(&select("after the copy", b'I'));

    let _ = conn.response();
    let mut response = conn.response();
    now(response.row_description()).unwrap();
    assert_eq!(rows(&mut response), [[Some("after the copy".to_owned())]]);
}

#[test]
fn copy_out_data_ends_with_copy_done() {
    let (server, mut conn) = connection();
    server.send(&message(b'H', &[0, 0, 2, 0, 0, 0, 0]));
    server
        .send(&message(b'd', b"1\t2\n"))
        .send(&message(b'd', b"3\t4\n"));
    server
        .send(&message(b'c', b""))
        .send(&command_complete("COPY 2"))
        .send(&ready(b'I'));

    let mut response = conn.response();
    let copy = now(response.copy_out_response()).unwrap();
    assert_eq!(
        copy.column_formats().collect::<Vec<_>>(),
        [Format::Text, Format::Text]
    );
    let mut data = Vec::new();
    while let Some(chunk) = now(response.copy_out_data()).unwrap() {
        data.extend_from_slice(chunk);
    }
    assert_eq!(data, b"1\t2\n3\t4\n");
    assert_eq!(now(response.complete()).unwrap().rows(), Some(2));
    now(response.finish()).unwrap();
}

#[test]
fn a_copy_in_both_directions_is_read_and_written() {
    let (server, mut conn) = connection();
    conn.query("START_REPLICATION 0/0");
    server.send(&message(b'W', &[0, 0, 0]));
    server.send(&message(b'd', b"from the server"));
    server.send(&message(b'c', b""));

    let mut response = conn.response();
    let copy = now(response.copy_both_response()).unwrap();
    assert_eq!((copy.format, copy.len()), (Format::Text, 0));
    let data = now(response.copy_out_data()).unwrap();
    assert_eq!(data, Some(&b"from the server"[..]));
    assert_eq!(now(response.copy_out_data()).unwrap(), None);

    response.copy_data(b"from the client").copy_done();
    server.send(&command_complete("START_REPLICATION"));
    server.send(&ready(b'I'));
    now(response.complete()).unwrap();
    assert_eq!(now(response.finish()).unwrap(), TransactionStatus::Idle);

    let mut expected = Vec::new();
    expected.query("START_REPLICATION 0/0");
    expected.copy_data(b"from the client").copy_done();
    assert_eq!(server.received(), expected);
}

#[test]
fn request_tls_reads_exactly_one_byte() {
    for (reply, accepted) in [(b'S', true), (b'N', false)] {
        let (server, mut stream) = pair();
        server.send(&[reply]).send(b"not for us");

        assert_eq!(now(request_tls(&mut stream)).unwrap(), accepted);
        assert_eq!(server.received(), [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]);
        assert_eq!(server.unread(), b"not for us".len());
    }

    // A server too old to know the request answers with an error.
    let (server, mut stream) = pair();
    server.send(&error("08P01", "unsupported frontend protocol"));
    let unexpected = ProtocolError::Unexpected {
        code: b'E',
        expected: "an SSLRequest reply",
    };
    assert_eq!(protocol_error(now(request_tls(&mut stream))), unexpected);

    let (server, mut stream) = pair();
    server.close();
    assert_eq!(
        io_error(now(request_tls(&mut stream))),
        io::ErrorKind::UnexpectedEof
    );
}

#[test]
fn cancel_sends_the_key_and_waits_for_the_server_to_hang_up() {
    let (server, stream) = pair();
    let key = BackendKey {
        process_id: 0x0102_0304,
        secret_key: (0..32).collect(),
    };

    let mut cancelling = pin!(cancel(stream, &key));
    assert!(poll(cancelling.as_mut()).is_pending());
    server.close();
    assert!(poll(cancelling.as_mut()).is_ready());

    let mut expected = vec![0, 0, 0, 44, 0x04, 0xd2, 0x16, 0x2e, 1, 2, 3, 4];
    expected.extend(0..32);
    assert_eq!(server.received(), expected);
}

/// A blocking stream that is interrupted before every operation.
struct Interrupted<S> {
    stream: S,
    interrupt: bool,
}

impl<S> Interrupted<S> {
    fn interrupted(&mut self) -> io::Result<()> {
        self.interrupt = !self.interrupt;
        match self.interrupt {
            true => Err(io::ErrorKind::Interrupted.into()),
            false => Ok(()),
        }
    }
}

impl<S: Read> Read for Interrupted<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.interrupted()?;
        self.stream.read(buf)
    }
}

impl<S: Read> Write for Interrupted<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.interrupted()?;
        Ok(buf.len().min(3))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.interrupted()
    }
}

#[test]
fn blocking_streams_retry_when_interrupted() {
    let wire = select("blocking", b'I');
    let stream = Interrupted {
        stream: &wire[..],
        interrupt: false,
    };
    let mut conn = Connection::new(Blocking(stream));

    conn.parse("", "SELECT $1", &[])
        .bind("", "", params!["blocking"], Format::Text)
        .sync();
    let status = block_on(async {
        let mut response = conn.response();
        response.row_description().await?;
        let row = response.row().await?.unwrap();
        assert_eq!(text(row), [Some("blocking".to_owned())]);
        response.finish().await
    });
    assert_eq!(status.unwrap(), TransactionStatus::Idle);
    assert_eq!(conn.unsent(), 0);
}

#[test]
#[should_panic(expected = "block_on requires a future that only awaits Blocking transports")]
fn block_on_refuses_a_future_that_would_wait() {
    let (_server, mut conn) = connection();
    let _ = block_on(conn.recv());
}

/// Spawning a connection onto a runtime needs these.
#[cfg(feature = "tokio")]
#[test]
fn futures_over_a_send_transport_are_send() {
    fn assert_send<T: Send>(_: &T) {}
    let (mut stream, _server) = tokio::io::duplex(64);
    assert_send(&request_tls(&mut stream));

    let mut conn = Connection::new(stream);
    assert_send(&conn);
    assert_send(&conn.recv());
    assert_send(&conn.recv_frame());
    assert_send(&conn.recv_frames());
    assert_send(&conn.flush());

    let mut response = conn.response();
    assert_send(&response.parse_complete());
    assert_send(&response.row_description());
    assert_send(&response.row());
    assert_send(&response.complete());
    assert_send(&response.copy_out_data());
    assert_send(&response.next());
    assert_send(&response.finish());

    let key = BackendKey {
        process_id: 1,
        secret_key: Box::new([0; 4]),
    };
    assert_send(&cancel(conn.into_inner(), &key));
}
