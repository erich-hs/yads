#!/bin/bash
set -e

echo "> Starting SQL Server..."

# Start SQL Server in the background
/opt/mssql/bin/sqlservr &

# Wait for SQL Server to be ready
echo "> Waiting for SQL Server to be ready..."
for i in {1..60}; do
    if /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -C -Q "SELECT 1" >/dev/null 2>&1; then
        echo "+ SQL Server is ready"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "X Timeout waiting for SQL Server to start"
        exit 1
    fi
    sleep 1
done

# Create test database
echo "> Creating test database..."
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -C -Q "
IF DB_ID('yads_test') IS NULL
    CREATE DATABASE yads_test;
"
echo "+ Test database created"

# Run the command passed to the container
echo "> Running tests..."
exec "$@"
