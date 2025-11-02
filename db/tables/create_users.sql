-- create_users.sql

BEGIN;

CREATE TABLE IF NOT EXISTS users (
  user_id         SERIAL PRIMARY KEY,
  username        VARCHAR(50) UNIQUE NOT NULL,
  password_hash   VARCHAR(255) NOT NULL,
  full_name       VARCHAR(100),
  email           VARCHAR(100) UNIQUE,
  created_at      TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

COMMIT;
