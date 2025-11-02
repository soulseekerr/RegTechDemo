-- init.sql
-- Initialize the database schema

-- Stop execution on first error
\set ON_ERROR_STOP on

BEGIN;

\echo "Initializing database schema..."

-- Create tables
\i /docker-entrypoint-initdb.d/tables/create_samples.sql
\i /docker-entrypoint-initdb.d/tables/create_cpties.sql
\i /docker-entrypoint-initdb.d/tables/create_trades.sql
\i /docker-entrypoint-initdb.d/tables/create_users.sql

-- Seed initial data
\echo "Seeding initial data..."

\i /docker-entrypoint-initdb.d/seeds/seed_samples.sql
\i /docker-entrypoint-initdb.d/seeds/seed_users.sql

COMMIT;

\echo "Database initialization complete."