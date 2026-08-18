#!/bin/bash
set -e

# Create extensions in the order_order database
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
EOSQL

# Apply Order Service migrations
for f in /docker-entrypoint-initdb.d/migrations/order/*.sql; do
    echo "Applying order migration: $f"
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f"
done