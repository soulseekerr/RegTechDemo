-- Create counterparties table

BEGIN;

CREATE TABLE IF NOT EXISTS counterparties (
    cob_dt      TIMESTAMP WITHOUT TIME ZONE,
    cpty_id     BIGINT UNIQUE,
    cpty_code   VARCHAR(10) UNIQUE,
    cpty_name   VARCHAR(100),
    country     CHAR(2),
    cva_method  VARCHAR(50),
    ext_rating  VARCHAR(10),
    grr         FLOAT,
    PRIMARY KEY (cob_dt, cpty_id)
);
CREATE INDEX IF NOT EXISTS idx_counterparties_cpty_id ON counterparties(cob_dt, cpty_id);

COMMIT;
