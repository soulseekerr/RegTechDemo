SELECT t.cob_dt, t.trade_id, t.fo_trade_id, t.book, t.product, t.cpty_id, c.cpty_code, c.cpty_name
FROM trades t
INNER JOIN counterparties c ON c.cpty_id=t.cpty_id;