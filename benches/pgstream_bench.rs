use std::hint::black_box;

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use pg_stream::{
    FrontendMessage,
    message::{Bindable, FormatCode, backend, oid},
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
                .finish(&[&"42" as &dyn Bindable]);
        });
    });

    group.bench_function("three_params_binary_result", |b| {
        let binary_data: &[u8] = &[1, 2, 3, 4];
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(256);
            buf.bind(Some(black_box("portal1")))
                .statement(black_box("stmt1"))
                .result_format(FormatCode::Binary)
                .finish(&[
                    &"42" as &dyn Bindable,
                    &"John Doe" as &dyn Bindable,
                    &binary_data as &dyn Bindable,
                ]);
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
                .finish(&[
                    &"1" as &dyn Bindable,
                    &"2" as &dyn Bindable,
                    &binary1 as &dyn Bindable,
                    &"test" as &dyn Bindable,
                    &binary2 as &dyn Bindable,
                    &"more data" as &dyn Bindable,
                    &none as &dyn Bindable,
                    &"final" as &dyn Bindable,
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
                .finish(&[&"arg1" as &dyn Bindable]);
        });
    });

    group.bench_function("three_args_binary_result", |b| {
        let binary_data: &[u8] = &[1, 2, 3, 4];
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            buf.fn_call(black_box(67890))
                .result_format(FormatCode::Binary)
                .finish(&[
                    &"arg1" as &dyn Bindable,
                    &binary_data as &dyn Bindable,
                    &"arg3" as &dyn Bindable,
                ]);
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
                .finish(&[&"42" as &dyn Bindable])
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
                .finish(&[&"value1" as &dyn Bindable, &binary_data as &dyn Bindable])
                .describe_portal(Some(black_box("my_portal")))
                .execute(Some(black_box("my_portal")), black_box(50))
                .close_portal(Some(black_box("my_portal")))
                .sync();
        });
    });

    group.finish();
}

fn bench_read_frame(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_frame");

    // Helper function to create a frame buffer
    fn create_frame(code: u8, body: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(code);
        buf.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        buf.extend_from_slice(body);
        buf
    }

    group.bench_function("empty_body", |b| {
        let frame = create_frame(b'Z', b"");
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let stream = black_box(frame.as_slice());
                backend::read_frame(stream).await.unwrap()
            })
        });
    });

    group.bench_function("small_body_5_bytes", |b| {
        let frame = create_frame(b'Z', b"READY");
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let stream = black_box(frame.as_slice());
                backend::read_frame(stream).await.unwrap()
            })
        });
    });

    group.bench_function("medium_body_100_bytes", |b| {
        let body = vec![b'x'; 100];
        let frame = create_frame(b'D', &body);
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let stream = black_box(frame.as_slice());
                backend::read_frame(stream).await.unwrap()
            })
        });
    });

    group.bench_function("large_body_1kb", |b| {
        let body = vec![b'x'; 1024];
        let frame = create_frame(b'D', &body);
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let stream = black_box(frame.as_slice());
                backend::read_frame(stream).await.unwrap()
            })
        });
    });

    group.bench_function("large_body_10kb", |b| {
        let body = vec![b'x'; 10 * 1024];
        let frame = create_frame(b'D', &body);
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let stream = black_box(frame.as_slice());
                backend::read_frame(stream).await.unwrap()
            })
        });
    });

    group.bench_function("large_body_100kb", |b| {
        let body = vec![b'x'; 100 * 1024];
        let frame = create_frame(b'D', &body);
        b.iter(|| {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let stream = black_box(frame.as_slice());
                backend::read_frame(stream).await.unwrap()
            })
        });
    });

    group.finish();
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
    bench_read_frame,
);
criterion_main!(benches);
