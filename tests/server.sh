#!/bin/sh
# Starts or stops a throwaway Postgres in Docker for tests/live.rs.
#
#   tests/server.sh start
#   PGSTREAM_TEST_PORT=54329 PGSTREAM_TEST_TLS=1 cargo test --test live
#   tests/server.sh stop
set -eu

name=pg-stream-test
port=${PGSTREAM_TEST_PORT:-54329}
image=${PGSTREAM_TEST_IMAGE:-postgres:18}
data=$(cd "$(dirname "$0")/data" && pwd)

case "${1:-}" in
start)
    docker rm -f "$name" >/dev/null 2>&1 || true
    # Docker would create a missing mount source as a directory owned by root.
    test -f "$data/pg/init.sh"
    docker run -d --name "$name" -p "127.0.0.1:$port:5432" \
        -e POSTGRES_HOST_AUTH_METHOD=trust \
        -v "$data/certs:/certs:ro" \
        -v "$data/pg/init.sh:/docker-entrypoint-initdb.d/init.sh:ro" \
        "$image" >/dev/null

    # The image runs a temporary server while it initializes, so wait for the
    # real one.
    for _ in $(seq 60); do
        if docker logs "$name" 2>&1 | grep -q 'init process complete' &&
            docker exec "$name" pg_isready -q -h 127.0.0.1 -U postgres; then
            echo "Postgres is listening on 127.0.0.1:$port"
            exit 0
        fi
        sleep 1
    done
    docker logs "$name" >&2
    exit 1
    ;;
stop)
    docker rm -f "$name" >/dev/null
    ;;
*)
    echo "usage: $0 start|stop" >&2
    exit 2
    ;;
esac
