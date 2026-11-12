use std::{
    hint::black_box,
    io::{self, Read, Write},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use pg_stream::{
    Blocking, Connection, Format, Frontend, Message, backend::RowDescription, block_on,
    frontend::params_with, oid, params,
};

/// A server that has already said everything it is going to say.
struct Replay<'a>(&'a [u8]);

impl Read for Replay<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for Replay<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A server that says nothing until it is written to, and then gives the
/// same reply every time.
struct Answering<'a> {
    reply: &'a [u8],
    unread: &'a [u8],
}

impl<'a> Answering<'a> {
    fn new(reply: &'a [u8]) -> Self {
        Self { reply, unread: &[] }
    }
}

impl Read for Answering<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.unread.read(buf)
    }
}

impl Write for Answering<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.unread = self.reply;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn frame(wire: &mut Vec<u8>, code: u8, body: &[u8]) {
    wire.push(code);
    wire.extend_from_slice(&(body.len() as u32 + 4).to_be_bytes());
    wire.extend_from_slice(body);
}

/// The body of a RowDescription of `columns` text columns.
fn description(columns: u16) -> Vec<u8> {
    let mut body = columns.to_be_bytes().to_vec();
    for column in 0..columns {
        body.extend_from_slice(format!("column_{column}\0").as_bytes());
        body.extend_from_slice(&[0; 18]);
    }
    body
}

/// The body of a DataRow of `columns` values, each of them `value`.
fn row(columns: u16, value: &[u8]) -> Vec<u8> {
    let mut body = columns.to_be_bytes().to_vec();
    for _ in 0..columns {
        body.extend_from_slice(&(value.len() as u32).to_be_bytes());
        body.extend_from_slice(value);
    }
    body
}

/// The reply to a simple query returning `rows` rows of `columns` short text
/// columns.
fn result_set(rows: usize, columns: u16) -> Vec<u8> {
    let row = row(columns, b"a value!");
    let mut wire = Vec::new();
    frame(&mut wire, b'T', &description(columns));
    (0..rows).for_each(|_| frame(&mut wire, b'D', &row));
    frame(&mut wire, b'C', format!("SELECT {rows}\0").as_bytes());
    frame(&mut wire, b'Z', b"I");
    wire
}

/// Queues one cycle of the extended protocol: a row looked up by its id.
fn lookup(to: &mut impl Frontend, id: i32) {
    to.bind("", "by_id", params![id], Format::Binary);
    to.execute("", 0).sync();
}

fn encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("encode");
    let mut buf = Vec::with_capacity(4096);

    group.bench_function("query", |b| {
        b.iter(|| {
            buf.clear();
            buf.query(black_box(
                "SELECT * FROM users WHERE id = 1 AND status = 'active'",
            ));
        })
    });

    group.bench_function("parse_bind_execute_sync", |b| {
        b.iter(|| {
            buf.clear();
            buf.parse(black_box(""), black_box("SELECT $1"), &[])
                .bind("", "", params!["42"], Format::Text)
                .execute("", 0)
                .sync();
        })
    });

    group.bench_function("bind_eight_mixed_params", |b| {
        let blob: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8];
        let values = params![
            "1",
            2i32,
            blob,
            "test",
            5i64,
            "more data",
            None::<i32>,
            true
        ];
        b.iter(|| {
            buf.clear();
            buf.bind(
                black_box("portal"),
                black_box("statement"),
                values,
                Format::Binary,
            );
        })
    });

    group.bench_function("bind_eight_binary_params_streamed", |b| {
        let ids = [1i64, 2, 3, 4, 5, 6, 7, 8];
        b.iter(|| {
            buf.clear();
            let values = params_with(Format::Binary, |values| {
                for id in black_box(ids) {
                    values.write(|buf| buf.extend_from_slice(&id.to_be_bytes()));
                }
            });
            buf.bind("portal", "statement", values, Format::Binary);
        })
    });

    // The same messages written into a slice, which cannot grow.
    let mut storage = vec![0; 4096];

    group.bench_function("query_into_slice", |b| {
        b.iter(|| {
            let mut buf = &mut storage[..];
            buf.query(black_box(
                "SELECT * FROM users WHERE id = 1 AND status = 'active'",
            ));
            black_box(buf.len());
        })
    });

    group.bench_function("parse_bind_execute_sync_into_slice", |b| {
        b.iter(|| {
            let mut buf = &mut storage[..];
            buf.parse(black_box(""), black_box("SELECT $1"), &[])
                .bind("", "", params!["42"], Format::Text)
                .execute("", 0)
                .sync();
            black_box(buf.len());
        })
    });

    group.bench_function("bind_eight_mixed_params_into_slice", |b| {
        let blob: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8];
        let values = params![
            "1",
            2i32,
            blob,
            "test",
            5i64,
            "more data",
            None::<i32>,
            true
        ];
        b.iter(|| {
            let mut buf = &mut storage[..];
            buf.bind(
                black_box("portal"),
                black_box("statement"),
                values,
                Format::Binary,
            );
            black_box(buf.len());
        })
    });

    group.bench_function("prepare_describe_execute_close", |b| {
        b.iter(|| {
            buf.clear();
            buf.parse(
                "stmt",
                black_box("SELECT * FROM t WHERE a = $1 AND b = $2"),
                &[oid::TEXT],
            )
            .describe_statement("stmt")
            .bind("portal", "stmt", params!["value", 7i32], Format::Binary)
            .describe_portal("portal")
            .execute("portal", 50)
            .close_portal("portal")
            .sync();
        })
    });

    group.finish();
}

fn receive(c: &mut Criterion) {
    let mut group = c.benchmark_group("receive");

    for (name, rows, columns) in [("100_rows_x_1", 100, 1), ("1000_rows_x_10", 1000, 10)] {
        let wire = result_set(rows, columns);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_function(format!("recv/{name}"), |b| {
            b.iter(|| {
                let mut conn = Connection::new(Blocking(Replay(black_box(&wire))));
                let mut values = 0;
                loop {
                    match block_on(conn.recv()).unwrap() {
                        Message::DataRow(row) => values += row.iter().flatten().count(),
                        Message::ReadyForQuery(_) => break values,
                        _ => {}
                    }
                }
            })
        });

        group.bench_function(format!("response/{name}"), |b| {
            b.iter(|| {
                let mut conn = Connection::new(Blocking(Replay(black_box(&wire))));
                block_on(async {
                    let mut response = conn.response();
                    response.row_description().await.unwrap();
                    let mut values = 0;
                    while let Some(row) = response.row().await.unwrap() {
                        values += row.iter().flatten().count();
                    }
                    response.finish().await.unwrap();
                    values
                })
            })
        });

        // The rows skipped rather than read.
        group.bench_function(format!("finish/{name}"), |b| {
            b.iter(|| {
                let mut conn = Connection::new(Blocking(Replay(black_box(&wire))));
                block_on(conn.response().finish()).unwrap()
            })
        });

        group.bench_function(format!("recv_frame/{name}"), |b| {
            b.iter(|| {
                let mut conn = Connection::new(Blocking(Replay(black_box(&wire))));
                let mut values = 0;
                loop {
                    let frame = block_on(conn.recv_frame()).unwrap();
                    match frame.parse().unwrap() {
                        Message::DataRow(row) => values += row.iter().flatten().count(),
                        Message::ReadyForQuery(_) => break values,
                        _ => {}
                    }
                }
            })
        });

        group.bench_function(format!("recv_frames/{name}"), |b| {
            b.iter(|| {
                let mut conn = Connection::new(Blocking(Replay(black_box(&wire))));
                let mut values = 0;
                loop {
                    let frames = block_on(conn.recv_frames()).unwrap();
                    for message in frames.messages() {
                        if let Message::DataRow(row) = message.unwrap() {
                            values += row.iter().flatten().count();
                        }
                    }
                    if frames.ends_cycle() {
                        break values;
                    }
                }
            })
        });
    }

    let body = description(10);
    group.throughput(Throughput::Elements(10));
    group.bench_function("row_description/10_columns", |b| {
        b.iter(|| {
            let columns = RowDescription::parse(black_box(&body)).unwrap();
            let names = columns.iter().map(|column| column.name.len());
            names.sum::<usize>()
        })
    });

    group.finish();
}

/// Queueing messages on a connection and writing them out, to a stream that
/// takes everything it is given.
fn send(c: &mut Criterion) {
    let mut group = c.benchmark_group("send");
    let mut conn = Connection::new(Blocking(Replay(&[])));

    group.bench_function("bind_execute_sync", |b| {
        b.iter(|| {
            lookup(&mut conn, black_box(7));
            block_on(conn.flush()).unwrap();
        })
    });

    group.throughput(Throughput::Elements(100));
    group.bench_function("pipeline_of_100_cycles", |b| {
        b.iter(|| {
            (0..100).for_each(|id| lookup(&mut conn, black_box(id)));
            block_on(conn.flush()).unwrap();
        })
    });

    group.bench_function("copy_of_100_rows", |b| {
        b.iter(|| {
            for _ in 0..100 {
                conn.copy_data(black_box(b"1\tsome text for a row\t2026-11-12\n"));
            }
            block_on(conn.flush()).unwrap();
        })
    });

    group.finish();
}

/// One request and its reply at a time, as a client sees them that does not
/// pipeline.
fn round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("round_trip");

    for (name, value_len) in [("row_of_8_bytes", 8), ("row_of_100KiB", 100 * 1024)] {
        let mut reply = Vec::new();
        frame(&mut reply, b'2', b"");
        frame(&mut reply, b'D', &row(1, &vec![b'x'; value_len]));
        frame(&mut reply, b'C', b"SELECT 1\0");
        frame(&mut reply, b'Z', b"I");
        let mut conn = Connection::new(Blocking(Answering::new(&reply)));

        group.bench_function(name, |b| {
            b.iter(|| {
                lookup(&mut conn, black_box(7));
                block_on(async {
                    let mut response = conn.response();
                    response.bind_complete().await.unwrap();
                    let mut bytes = 0;
                    while let Some(row) = response.row().await.unwrap() {
                        bytes += row.iter().flatten().map(<[u8]>::len).sum::<usize>();
                    }
                    response.finish().await.unwrap();
                    bytes
                })
            })
        });
    }

    group.finish();
}

criterion_group!(benches, encode, receive, send, round_trip);
criterion_main!(benches);
