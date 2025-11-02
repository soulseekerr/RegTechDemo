-- Seed users table with initial data

INSERT INTO users (username, password_hash, full_name, email) VALUES
  ('admin', 'pass', 'Admin User', 'admin@sparky.com', CURRENT_TIMESTAMP)
ON CONFLICT (username) DO NOTHING;
INSERT INTO users (username, password_hash, full_name, email) VALUES
  ('user', 'pass', 'Basic User', 'user@sparky.com', CURRENT_TIMESTAMP)
ON CONFLICT (username) DO NOTHING;
