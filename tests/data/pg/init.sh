#!/bin/sh
# Run by the Postgres image while it initializes a new data directory. Sets up
# what tests/live.rs expects: TLS, and one role per authentication method.
set -e

cp /certs/server.crt /certs/server.key "$PGDATA"/
chmod 600 "$PGDATA/server.key"
cat >> "$PGDATA/postgresql.conf" <<CONF
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
CONF

psql -v ON_ERROR_STOP=1 -U postgres <<SQL
SET password_encryption = 'md5';
CREATE ROLE md5_user WITH LOGIN PASSWORD 'md5';
SET password_encryption = 'scram-sha-256';
CREATE ROLE sha_user WITH LOGIN PASSWORD 'sha';
CREATE ROLE pw_user WITH LOGIN PASSWORD 'pw';
SQL

# The first matching line wins, and the last one keeps anything unlisted from
# falling back to trust.
cat > "$PGDATA/pg_hba.conf" <<HBA
local   all  all                 trust
host    all  sha_user  all       scram-sha-256
host    all  md5_user  all       md5
host    all  pw_user   all       password
host    all  postgres  all       trust
host    all  all       all       reject
HBA
