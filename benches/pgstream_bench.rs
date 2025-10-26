use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use pg_stream::{
    PgStream,
    messages::{
        backend,
        frontend::{
            BindParameter, FormatCode, FunctionArg, ParameterKind, ResultFormat, TargetKind,
        },
    },
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
                let stream = Vec::<u8>::new();
                let mut pg_stream = PgStream::from_stream(stream);
                pg_stream.put_query(black_box(query));
            });
        });
    }

    group.finish();
}

fn bench_put_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_parse");

    let param_types_small = vec![];
    let param_types_medium = vec![
        ParameterKind::Int4,
        ParameterKind::Text,
        ParameterKind::Timestamp,
    ];
    let param_types_large = vec![
        ParameterKind::Int4,
        ParameterKind::Int8,
        ParameterKind::Text,
        ParameterKind::Varchar,
        ParameterKind::Timestamp,
        ParameterKind::Bool,
        ParameterKind::Float4,
        ParameterKind::Float8,
        ParameterKind::Numeric,
        ParameterKind::Bytea,
    ];

    group.bench_function("no_params", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_parse(
                black_box("stmt1"),
                black_box("SELECT * FROM users WHERE id = $1"),
                black_box(&param_types_small),
            );
        });
    });

    group.bench_function("three_params", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_parse(
                black_box("stmt2"),
                black_box("SELECT * FROM users WHERE id = $1 AND name = $2 AND created_at > $3"),
                black_box(&param_types_medium),
            );
        });
    });

    group.bench_function("ten_params", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_parse(
                black_box("stmt3"),
                black_box(
                    "INSERT INTO large_table VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                ),
                black_box(&param_types_large),
            );
        });
    });

    group.finish();
}

fn bench_put_bind(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_bind");

    // Create different parameter sets
    let params_small = vec![BindParameter::Text("42".to_string())];

    let params_medium = vec![
        BindParameter::Text("42".to_string()),
        BindParameter::Text("John Doe".to_string()),
        BindParameter::RawBinary(vec![1, 2, 3, 4].into()),
    ];

    let params_large = vec![
        BindParameter::Text("1".to_string()),
        BindParameter::Text("2".to_string()),
        BindParameter::RawBinary(vec![1, 2, 3, 4, 5, 6, 7, 8].into()),
        BindParameter::Text("test".to_string()),
        BindParameter::RawBinary(vec![9, 10, 11, 12].into()),
        BindParameter::Text("more data".to_string()),
        BindParameter::Null,
        BindParameter::Text("final".to_string()),
    ];

    group.bench_function("one_param_text_result", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_bind(
                black_box(""),
                black_box("stmt1"),
                black_box(&params_small),
                black_box(ResultFormat::Text),
            );
        });
    });

    group.bench_function("three_params_RawBinary_result", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_bind(
                black_box("portal1"),
                black_box("stmt2"),
                black_box(&params_medium),
                black_box(ResultFormat::Binary),
            );
        });
    });

    group.bench_function("eight_params_mixed_result", |b| {
        let mixed_formats = vec![
            FormatCode::Text,
            FormatCode::Binary,
            FormatCode::Text,
            FormatCode::Binary,
        ];
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_bind(
                black_box("portal2"),
                black_box("stmt3"),
                black_box(&params_large),
                black_box(ResultFormat::Mixed(&mixed_formats)),
            );
        });
    });

    group.finish();
}

fn bench_put_describe(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_describe");

    group.bench_function("portal", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_describe(black_box(TargetKind::new_portal("my_portal")));
        });
    });

    group.bench_function("statement", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_describe(black_box(TargetKind::new_stmt("my_stmt")));
        });
    });

    group.finish();
}

fn bench_put_execute(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_execute");

    group.bench_function("unlimited_rows", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_execute(black_box("portal1"), black_box(None));
        });
    });

    group.bench_function("limited_rows", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_execute(black_box("portal2"), black_box(Some(100)));
        });
    });

    group.finish();
}

fn bench_put_fn_call(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_fn_call");

    let args_small = vec![FunctionArg::Text("arg1".to_string())];
    let args_medium = vec![
        FunctionArg::Text("arg1".to_string()),
        FunctionArg::RawBinary(vec![1, 2, 3, 4].into()),
        FunctionArg::Text("arg3".to_string()),
    ];

    group.bench_function("one_arg_text_result", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_fn_call(
                black_box(12345),
                black_box(&args_small),
                black_box(FormatCode::Text),
            );
        });
    });

    group.bench_function("three_args_binary_result", |b| {
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream.put_fn_call(
                black_box(67890),
                black_box(&args_medium),
                black_box(FormatCode::Binary),
            );
        });
    });

    group.finish();
}

fn bench_chained_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("chained_operations");

    group.bench_function("parse_bind_execute_sync", |b| {
        let params = vec![BindParameter::Text("42".to_string())];
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream
                .put_parse(black_box("stmt"), black_box("SELECT $1"), black_box(&[]))
                .put_bind(
                    black_box(""),
                    black_box("stmt"),
                    black_box(&params),
                    black_box(ResultFormat::Text),
                )
                .put_execute(black_box(""), black_box(None))
                .put_sync();
        });
    });

    group.bench_function("complex_extended_query", |b| {
        let params = vec![
            BindParameter::Text("value1".to_string()),
            BindParameter::RawBinary(vec![1, 2, 3, 4].into()),
        ];
        b.iter(|| {
            let stream = Vec::<u8>::new();
            let mut pg_stream = PgStream::from_stream(stream);
            pg_stream
                .put_parse(
                    black_box("complex_stmt"),
                    black_box("SELECT * FROM table WHERE col1 = $1 AND col2 = $2"),
                    black_box(&[ParameterKind::Text, ParameterKind::Bytea]),
                )
                .put_describe(black_box(TargetKind::new_stmt("complex_stmt")))
                .put_bind(
                    black_box("my_portal"),
                    black_box("complex_stmt"),
                    black_box(&params),
                    black_box(ResultFormat::Binary),
                )
                .put_describe(black_box(TargetKind::new_portal("my_portal")))
                .put_execute(black_box("my_portal"), black_box(Some(50)))
                .put_close(black_box(TargetKind::new_portal("my_portal")))
                .put_sync();
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
