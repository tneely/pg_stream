use std::hint::black_box;

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use pg_stream::{
    PgProtocol,
    message::{FormatCode, MessageReader, PgMessage, oid},
    params,
};

fn bench_put_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_query");

    let queries = vec![
        ("short", "SELECT 1"),
        (
            "medium",
            "SELECT * FROM users WHERE id = 1 AND status = 'active'",
        ),
        (
            "long",
            "SELECT u.id, u.name, u.email, o.order_id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.created_at > '2024-01-01' AND o.status IN ('pending', 'completed') ORDER BY o.created_at DESC LIMIT 100",
        ),
    ];

    for (name, query) in queries {
        group.bench_with_input(BenchmarkId::from_parameter(name), &query, |b, &query| {
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(256);
                buf.query(black_box(query));
            });
        });
    }

    group.finish();
}

fn bench_put_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_parse");

    let param_types_small: &[u32] = &[];
    let param_types_medium: &[u32] = &[oid::INT4, oid::TEXT, oid::TIMESTAMP];
    let param_types_large: &[u32] = &[
        oid::INT4,
        oid::INT8,
        oid::TEXT,
        oid::VARCHAR,
        oid::TIMESTAMP,
        oid::BOOL,
        oid::FLOAT4,
        oid::FLOAT8,
        oid::NUMERIC,
        oid::BYTEA,
    ];

    group.bench_function("no_params", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            buf.parse(Some(black_box("stmt1")))
                .query(black_box("SELECT * FROM users WHERE id = $1"))
                .param_types(black_box(param_types_small))
                .finish();
        });
    });

    group.bench_function("three_params", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            buf.parse(Some(black_box("stmt2")))
                .query(black_box(
                    "SELECT * FROM users WHERE id = $1 AND name = $2 AND created_at > $3",
                ))
                .param_types(black_box(param_types_medium))
                .finish();
        });
    });

    group.bench_function("ten_params", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            buf.parse(Some(black_box("stmt3")))
                .query(black_box(
                    "INSERT INTO large_table VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                ))
                .param_types(black_box(param_types_large))
                .finish();
        });
    });

    group.finish();
}

fn bench_put_bind(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_bind");

    group.bench_function("one_param_text_result", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            buf.bind(None)
                .statement(black_box(""))
                .result_format(FormatCode::Text)
                .finish(params!["42"]);
        });
    });

    group.bench_function("three_params_binary_result", |b| {
        let binary_data: &[u8] = &[1, 2, 3, 4];
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            buf.bind(Some(black_box("portal1")))
                .statement(black_box("stmt1"))
                .result_format(FormatCode::Binary)
                .finish(params!["42", "John Doe", binary_data]);
        });
    });

    group.bench_function("eight_params_mixed", |b| {
        let binary1: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8];
        let binary2: &[u8] = &[9, 10, 11, 12];
        let none: Option<i32> = None;
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            buf.bind(Some(black_box("portal2")))
                .statement(black_box("stmt2"))
                .finish(params![
                    "1",
                    "2",
                    binary1,
                    "test",
                    binary2,
                    "more data",
                    none,
                    "final"
                ]);
        });
    });

    group.finish();
}

fn bench_put_describe(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_describe");

    group.bench_function("portal", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            buf.describe_portal(Some(black_box("my_portal")));
        });
    });

    group.bench_function("statement", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            buf.describe_statement(Some(black_box("my_stmt")));
        });
    });

    group.finish();
}

fn bench_put_execute(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_execute");

    group.bench_function("unlimited_rows", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            buf.execute(Some(black_box("portal1")), black_box(0));
        });
    });

    group.bench_function("limited_rows", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            buf.execute(Some(black_box("portal2")), black_box(100));
        });
    });

    group.finish();
}

fn bench_put_fn_call(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_fn_call");

    group.bench_function("one_arg_text_result", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            buf.fn_call(black_box(12345))
                .result_format(FormatCode::Text)
                .finish(params!["arg1"]);
        });
    });

    group.bench_function("three_args_binary_result", |b| {
        let binary_data: &[u8] = &[1, 2, 3, 4];
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            buf.fn_call(black_box(67890))
                .result_format(FormatCode::Binary)
                .finish(params!["arg1", binary_data, "arg3"]);
        });
    });

    group.finish();
}

fn bench_chained_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("chained_operations");

    group.bench_function("parse_bind_execute_sync", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            buf.parse(Some(black_box("stmt")))
                .query(black_box("SELECT $1"))
                .finish()
                .bind(Some(black_box("stmt")))
                .finish(params!["42"])
                .execute(None, black_box(0))
                .sync();
        });
    });

    group.bench_function("complex_extended_query", |b| {
        let binary_data: &[u8] = &[1, 2, 3, 4];
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(512);
            buf.parse(Some(black_box("complex_stmt")))
                .query(black_box(
                    "SELECT * FROM table WHERE col1 = $1 AND col2 = $2",
                ))
                .param_types(&[oid::TEXT, oid::BYTEA])
                .finish()
                .describe_statement(Some(black_box("complex_stmt")))
                .bind(Some(black_box("my_portal")))
                .statement(black_box("complex_stmt"))
                .result_format(FormatCode::Binary)
                .finish(params!["value1", binary_data])
                .describe_portal(Some(black_box("my_portal")))
                .execute(Some(black_box("my_portal")), black_box(50))
                .close_portal(Some(black_box("my_portal")))
                .sync();
        });
    });

    group.finish();
}

fn bench_read_message(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_message");
    let rt = tokio::runtime::Runtime::new().unwrap();

    fn create_frame(code: u8, body: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(code);
        buf.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        buf.extend_from_slice(body);
        buf
    }

    async fn read_one(reader: &mut MessageReader, frame: &[u8]) -> PgMessage {
        let mut stream = frame;
        reader.read_message(&mut stream).await.unwrap()
    }

    let ready_for_query = create_frame(b'Z', b"I");
    group.bench_function("ready_for_query_idle", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(ready_for_query.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    let command_complete = create_frame(b'C', b"SELECT 1\0");
    group.bench_function("command_complete_select", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(command_complete.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    let mut parameter_status_body = Vec::new();
    parameter_status_body.extend_from_slice(b"server_version");
    parameter_status_body.push(0);
    parameter_status_body.extend_from_slice(b"16.0");
    parameter_status_body.push(0);
    let parameter_status = create_frame(b'S', &parameter_status_body);
    group.bench_function("parameter_status", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(parameter_status.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    let mut backend_key_data_body = Vec::new();
    backend_key_data_body.extend_from_slice(&12345_u32.to_be_bytes());
    backend_key_data_body.extend_from_slice(&[0xAB; 32]); // 3.2-length secret key
    let backend_key_data = create_frame(b'K', &backend_key_data_body);
    group.bench_function("backend_key_data", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(backend_key_data.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    let mut row_description_body = Vec::new();
    row_description_body.extend_from_slice(&1_u16.to_be_bytes());
    row_description_body.extend_from_slice(b"id");
    row_description_body.push(0);
    row_description_body.extend_from_slice(&0_u32.to_be_bytes());
    row_description_body.extend_from_slice(&0_u16.to_be_bytes());
    row_description_body.extend_from_slice(&23_u32.to_be_bytes());
    row_description_body.extend_from_slice(&4_i16.to_be_bytes());
    row_description_body.extend_from_slice(&(-1_i32).to_be_bytes());
    row_description_body.extend_from_slice(&0_u16.to_be_bytes());
    let row_description = create_frame(b'T', &row_description_body);
    group.bench_function("row_description_single_column", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(row_description.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    let mut data_row_body = Vec::new();
    data_row_body.extend_from_slice(&2_u16.to_be_bytes());
    data_row_body.extend_from_slice(&5_i32.to_be_bytes());
    data_row_body.extend_from_slice(b"hello");
    data_row_body.extend_from_slice(&(-1_i32).to_be_bytes());
    let data_row = create_frame(b'D', &data_row_body);
    group.bench_function("data_row_two_columns", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(data_row.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    let mut error_response_body = Vec::new();
    error_response_body.extend_from_slice(b"SERROR\0");
    error_response_body.extend_from_slice(b"VERROR\0");
    error_response_body.extend_from_slice(b"C42601\0");
    error_response_body.extend_from_slice(b"Msyntax error at or near \"SELECT\"\0");
    error_response_body.push(0);
    let error_response = create_frame(b'E', &error_response_body);
    group.bench_function("error_response", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(error_response.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    let copy_data_1kb = create_frame(b'd', &vec![b'x'; 1024]);
    group.bench_function("copy_data_1kb", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(copy_data_1kb.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    let copy_data_100kb = create_frame(b'd', &vec![b'x'; 100 * 1024]);
    group.bench_function("copy_data_100kb", |b| {
        let mut reader = MessageReader::new();
        b.iter(|| {
            rt.block_on(async {
                let stream = black_box(copy_data_100kb.as_slice());
                read_one(&mut reader, stream).await
            })
        })
    });

    group.finish();
}

/// Measures framing a stream of small messages with one persistent reader.
fn bench_stream_reader(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_reader");
    let rt = tokio::runtime::Runtime::new().unwrap();

    fn create_frame(code: u8, body: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(code);
        buf.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        buf.extend_from_slice(body);
        buf
    }

    // A typical result stream: RowDescription, many DataRows, CommandComplete,
    // ReadyForQuery, all delivered in one buffer.
    let mut stream_bytes = Vec::new();
    stream_bytes.extend_from_slice(&create_frame(b'T', &[0; 32]));
    for _ in 0..100 {
        stream_bytes.extend_from_slice(&create_frame(b'D', &[0, 1, 0, 0, 0, 2, b'4', b'2']));
    }
    stream_bytes.extend_from_slice(&create_frame(b'C', b"SELECT 100\0"));
    stream_bytes.extend_from_slice(&create_frame(b'Z', b"I"));
    let message_count = 103;

    group.bench_function("buffered_reader_103_messages", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut stream = black_box(stream_bytes.as_slice());
                let mut reader = MessageReader::new();
                for _ in 0..message_count {
                    black_box(reader.read_message(&mut stream).await.unwrap());
                }
            })
        })
    });

    group.finish();
}

fn bench_stream_reader_sync(c: &mut Criterion) {
    #[cfg(feature = "sync")]
    {
        use std::io::Read;

        let mut group = c.benchmark_group("stream_reader_sync");

        fn create_frame(code: u8, body: &[u8]) -> Vec<u8> {
            let mut buf = Vec::new();
            buf.push(code);
            buf.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
            buf.extend_from_slice(body);
            buf
        }

        let mut stream_bytes = Vec::new();
        stream_bytes.extend_from_slice(&create_frame(b'T', &[0; 32]));
        for _ in 0..100 {
            stream_bytes.extend_from_slice(&create_frame(b'D', &[0, 1, 0, 0, 0, 2, b'4', b'2']));
        }
        stream_bytes.extend_from_slice(&create_frame(b'C', b"SELECT 100\0"));
        stream_bytes.extend_from_slice(&create_frame(b'Z', b"I"));

        struct ChunkedRead<'a> {
            bytes: &'a [u8],
            position: usize,
            chunk_size: usize,
        }

        impl Read for ChunkedRead<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                let remaining = &self.bytes[self.position..];
                let read = remaining.len().min(buf.len()).min(self.chunk_size);
                buf[..read].copy_from_slice(&remaining[..read]);
                self.position += read;
                Ok(read)
            }
        }

        group.bench_function("buffered_reader_103_messages", |b| {
            b.iter(|| {
                let mut stream = black_box(stream_bytes.as_slice());
                let mut reader = MessageReader::new();
                for _ in 0..103 {
                    black_box(reader.read_message_sync(&mut stream).unwrap());
                }
            })
        });

        group.bench_function("64_byte_reads_103_messages", |b| {
            b.iter(|| {
                let mut stream = ChunkedRead {
                    bytes: black_box(&stream_bytes),
                    position: 0,
                    chunk_size: 64,
                };
                let mut reader = MessageReader::new();
                for _ in 0..103 {
                    black_box(reader.read_message_sync(&mut stream).unwrap());
                }
            })
        });

        group.finish();
    }

    #[cfg(not(feature = "sync"))]
    let _ = c;
}

criterion_group!(
    benches,
    bench_put_query,
    bench_put_parse,
    bench_put_bind,
    bench_put_describe,
    bench_put_execute,
    bench_put_fn_call,
    bench_chained_operations,
    bench_read_message,
    bench_stream_reader,
    bench_stream_reader_sync,
);
criterion_main!(benches);
