
-- seed a few rows:
INSERT INTO samples(value)
SELECT generate_series(1, 10)
ON CONFLICT DO NOTHING;
