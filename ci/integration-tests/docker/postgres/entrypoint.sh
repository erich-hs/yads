#!/bin/bash
set -e

echo "▶ Starting PostgreSQL..."

# Initialize PostgreSQL data directory if it doesn't exist
PGDATA="/var/lib/postgresql/data"
if [ ! -d "$PGDATA" ]; then
    mkdir -p "$PGDATA"
    chown -R postgres:postgres "$PGDATA"
    sudo -u postgres /usr/lib/postgresql/*/bin/initdb -D "$PGDATA"
fi

# Start PostgreSQL
sudo -u postgres /usr/lib/postgresql/*/bin/pg_ctl -D "$PGDATA" -l /var/log/postgresql/postgresql.log start

# Wait for PostgreSQL to be ready
echo "▶ Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if sudo -u postgres psql -c "SELECT 1" >/dev/null 2>&1; then
        echo "✓ PostgreSQL is ready"
        break
    fi
    sleep 1
done

# Create test user and database
echo "▶ Creating test user and database..."
sudo -u postgres psql <<-EOSQL
    CREATE USER ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}' SUPERUSER;
    CREATE DATABASE ${POSTGRES_DB} OWNER ${POSTGRES_USER};
EOSQL
echo "✓ Test database created"

# Run the command passed to the container
echo "▶ Running tests..."
exec "$@"
