-- Create trades table

BEGIN;

CREATE TABLE IF NOT EXISTS trades (
  cob_dt          TIMESTAMP WITHOUT TIME ZONE,
  trade_id        BIGINT NOT NULL,
  fo_trade_id     BIGINT NOT NULL,
  book            TEXT NOT NULL,
  product         TEXT NOT NULL,
  product_type    TEXT,
  notional1       DOUBLE PRECISION,
  currency1       TEXT,
  notional2       DOUBLE PRECISION,
  currency2       TEXT,
  cpty_id         BIGINT NOT NULL,
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
  is_credit           SMALLINT,
  PRIMARY KEY (cob_dt, fo_trade_id),
  FOREIGN KEY (cob_dt, cpty_id)
      REFERENCES counterparties (cob_dt, cpty_id)
      ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_trades_cob_foid ON trades (cob_dt, fo_trade_id);

CREATE INDEX IF NOT EXISTS idx_trades_cob_cptyid ON trades (cob_dt, cpty_id);

COMMIT;
