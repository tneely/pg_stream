//! The connection over tokio's in-memory pipe.
//!
//! The scripted server of the other tests decides what every read and write
//! does, but takes no notice of wakers: those tests poll again by hand. Here
//! the pipe has a size, the server is a task of its own, and a future runs
//! again only when something wakes it. The clock is paused, so a future that
//! is never woken ends its test at the timeout at once, instead of hanging.
#![cfg(feature = "tokio")]

mod mock;

use std::{collections::VecDeque, io, time::Duration};

use mock::*;
use pg_stream::{
    Connection, Error, Frontend,
    backend::{DataRow, Frame, code},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex},
    sync::mpsc,
    time::timeout,
};

/// A stand-in for a server, which answers each Query with what `reply` makes
/// of its text. Like Postgres, it reads nothing while it has a reply to send.
async fn serve(mut stream: DuplexStream, reply: impl Fn(&str) -> Vec<u8>) -> io::Result<()> {
    let mut header = [0; 5];
    while stream.read_exact(&mut header).await.is_ok() {
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        let mut body = vec![0; len as usize - 4];
        stream.read_exact(&mut body).await?;
        if let [b'Q', ..] = header {
            let sql = body.strip_suffix(&[0]).unwrap();
            let reply = reply(std::str::from_utf8(sql).unwrap());
            stream.write_all(&reply).await?;
        }
    }
    Ok(())
}

/// A server that sends back the text of each query as its one row.
fn echo(capacity: usize) -> Connection<DuplexStream> {
    let (client, server) = duplex(capacity);
    tokio::spawn(serve(server, |sql| select(sql, b'I')));
    Connection::new(client)
}

/// Waits for `future`, for longer than anything here takes. With the clock
/// paused, a wakeup that never comes costs no time to find out about.
async fn woken<T>(future: impl Future<Output = T>) -> T {
    let outcome = timeout(Duration::from_secs(60), future).await;
    outcome.expect("nothing woke the future")
}

/// The value of the single column of a row.
fn value(row: DataRow<'_>) -> String {
    let value = row.iter().next().flatten().unwrap();
    String::from_utf8(value.to_vec()).unwrap()
}

/// One message that the pipe cannot hold, sent by the read that waits for its
/// reply. Nothing arrives until all of it is written, so only the pipe
/// emptying can wake that read.
#[tokio::test(start_paused = true)]
async fn a_read_is_woken_to_write_the_rest_of_a_message() {
    let mut conn = echo(256);
    let sql = "x".repeat(16 * 1024);

    conn.query(&sql);
    let mut response = conn.response();
    woken(response.row_description()).await.unwrap();
    let row = woken(response.row()).await.unwrap();
    assert_eq!(value(row.unwrap()), sql);
    woken(response.finish()).await.unwrap();
}

/// A flush held up by a server that is itself held up sending a reply. Only
/// the reply arriving can wake that flush, which then has to take it.
#[tokio::test(start_paused = true)]
async fn a_flush_is_woken_to_take_a_reply() {
    const QUERIES: usize = 200;
    let (client, server) = duplex(256);
    let wide = "y".repeat(16 * 1024);
    tokio::spawn(serve(server, move |sql| match sql {
        "wide" => select(&wide, b'I'),
        _ => select("narrow", b'I'),
    }));
    let mut conn = Connection::new(client);

    conn.query("wide");
    let padding = "x".repeat(100);
    for _ in 0..QUERIES {
        conn.query(&padding);
    }
    woken(conn.flush()).await.unwrap();
    assert_eq!(conn.unsent(), 0);
    for _ in 0..=QUERIES {
        woken(conn.response().finish()).await.unwrap();
    }
}

/// Far more in each direction than the pipe holds, whichever of writing and
/// reading is started first.
#[tokio::test(start_paused = true)]
async fn a_pipeline_much_deeper_than_the_pipe_goes_through() {
    const QUERIES: usize = 500;
    for flush_first in [true, false] {
        let mut conn = echo(64);
        for id in 0..QUERIES {
            conn.query(&format!("SELECT {id}"));
        }

        if flush_first {
            woken(conn.flush()).await.unwrap();
        }
        for id in 0..QUERIES {
            let mut response = conn.response();
            woken(response.row_description()).await.unwrap();
            let row = woken(response.row()).await.unwrap();
            assert_eq!(value(row.unwrap()), format!("SELECT {id}"));
            woken(response.finish()).await.unwrap();
        }
    }
}

/// What a caller asks the task that owns the connection for.
struct Request {
    encoded: Vec<u8>,
    replies: mpsc::UnboundedSender<Frame>,
}

/// Owns a connection on behalf of many callers: writes what they ask for and
/// hands each reply to whoever is waiting for it.
async fn route(
    mut conn: Connection<DuplexStream>,
    mut requests: mpsc::UnboundedReceiver<Request>,
) -> pg_stream::Result<()> {
    let mut waiting = VecDeque::new();
    loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else { return Ok(()) };
                conn.buf().extend_from_slice(&request.encoded);
                waiting.push_back(request.replies);
            }
            frame = conn.recv_frame() => {
                let frame = frame?;
                let last = frame.code == code::READY_FOR_QUERY;
                if let Some(replies) = waiting.front() {
                    let _ = replies.send(frame);
                }
                if last {
                    waiting.pop_front();
                }
            }
        }
    }
}

/// Sends one query through the routing task and returns its row.
async fn ask(requests: &mpsc::UnboundedSender<Request>, sql: &str) -> String {
    let (replies, mut frames) = mpsc::unbounded_channel();
    let mut encoded = Vec::new();
    encoded.query(sql);
    requests.send(Request { encoded, replies }).unwrap();

    let mut answer = None;
    loop {
        let frame = frames.recv().await.expect("the connection went away");
        match frame.code {
            code::DATA_ROW => answer = Some(value(DataRow::parse(&frame.body).unwrap())),
            code::READY_FOR_QUERY => return answer.expect("no row came back"),
            _ => {}
        }
    }
}

/// Many tasks pipelining through one connection. Each request that comes in
/// makes `select!` drop the read under way, so this is cancellation as it
/// happens in use, with replies and requests both backed up behind the pipe.
#[tokio::test(start_paused = true)]
async fn a_connection_shared_between_tasks_gives_each_its_own_replies() {
    let (requests, receiver) = mpsc::unbounded_channel();
    let routing = tokio::spawn(route(echo(128), receiver));

    let callers: Vec<_> = (0..20).map(|caller| (caller, requests.clone())).collect();
    let callers = callers.into_iter().map(|(caller, requests)| async move {
        for round in 0..10 {
            let sql = format!("SELECT {caller}, {round}");
            assert_eq!(ask(&requests, &sql).await, sql);
        }
    });
    let callers: Vec<_> = callers.map(tokio::spawn).collect();
    for caller in callers {
        woken(caller).await.unwrap();
    }

    drop(requests);
    woken(routing).await.unwrap().unwrap();
}

/// A server that says why and hangs up, without taking what was sent to it.
/// What it said is reported, not the write that could not be finished.
#[tokio::test(start_paused = true)]
async fn a_server_that_hangs_up_is_heard_first() {
    let (client, mut server) = duplex(64);
    let goodbye = fatal("57P01", "terminating connection");
    tokio::spawn(async move { server.write_all(&goodbye).await });
    let mut conn = Connection::new(client);

    conn.query(&"x".repeat(1024));
    let error = woken(conn.response().finish()).await.unwrap_err();
    assert_eq!(error.as_server().unwrap().code(), "57P01");

    let error = woken(conn.recv()).await.unwrap_err();
    let hung_up = matches!(&error, Error::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof);
    assert!(hung_up, "{error:?}");
}
