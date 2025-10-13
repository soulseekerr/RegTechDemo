CREATE TABLE IF NOT EXISTS samples (
  id SERIAL PRIMARY KEY,
  value DOUBLE PRECISION NOT NULL
);

-- seed a few rows:
INSERT INTO samples(value)
SELECT generate_series(1, 10)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS spk_trades (
  cob             TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY,
  trade_id        BIGINT PRIMARY KEY,
  fo_trade_id     BIGINT NOT NULL PRIMARY KEY,
  book            TEXT NOT NULL,
  product         TEXT NOT NULL,
  product_type    TEXT,
  notional1       DOUBLE PRECISION,
  currency1       TEXT,
  notional2       DOUBLE PRECISION,
  currency2       TEXT,
  counterparty    TEXT NOT NULL,
  traded_at       TIMESTAMP WITHOUT TIME ZONE,
  matured_at      TIMESTAMP WITHOUT TIME ZONE,
  strike          DOUBLE PRECISION,
  rate            DOUBLE PRECISION,
  quantity        INTEGER,
  strategy_type   TEXT,
  strategy        TEXT,
  is_fx               SMALLINT,
  is_commodity        SMALLINT,
  is_equity           SMALLINT,
  is_interest_rate    SMALLINT,
  is_credit           SMALLINT
);
CREATE INDEX IF NOT EXISTS idx_trades_book ON spk_trades(book);
CREATE INDEX IF NOT EXISTS idx_trades_product ON spk_trades(product);