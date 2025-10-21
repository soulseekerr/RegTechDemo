import os
import sys
import io
import uuid
import json
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path
import pyarrow.dataset as ds
import pyarrow.csv as pacsv
from psycopg import connect
from typing import List, Dict
import pandas as pd
from pandas.tseries.offsets import BDay
import duckdb
import psycopg
from urllib.parse import urlparse
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel
from upload_playwright import rpa_submit

from fastapi import FastAPI, Query, HTTPException, Depends, BackgroundTasks, Request
from fastapi import UploadFile, File
from fastapi.responses import HTMLResponse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    print("Adding project root to sys.path:", ROOT)
    sys.path.insert(0, str(ROOT))

from api.tradebuilder import TradeBuilder
from api.cptybuilder import CptyBuilder
from api.config import Config

class MakeTradesRequest(BaseModel):
    cob_dt: datetime = datetime.now() - BDay(1)  # default to last business day
    rows: int = 10_000  # default
    chunk_size: int = 10_000  # default

class MakeCptiesRequest(BaseModel):
    cob_dt: datetime = datetime.now() - BDay(1)  # default to last business day
    rows: int = 10_000  # default
    chunk_size: int = 10_000  # default

app = FastAPI(title="RegTech API")

# We'll attach the pool to app.state
pool: ConnectionPool | None = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = ConnectionPool(
        os.getenv("DATABASE_URL"),
        min_size=1,
        max_size=4,  # tune with postgres max_connections & workers
        kwargs={"autocommit": False},
    )
    # # apply init.sql
    # not needed and incorrect path: done by docker once only if db data volume is empty
    # with open("./db/init.sql") as f, app.state.pool.connection() as conn, conn.cursor() as cur:
    #     cur.execute(f.read())
    #     conn.commit()
    try:
        yield
    finally:
        app.state.pool.close()
        app.state.pool.wait()  # wait for graceful close

# Recreate the app with lifespan enabled
app = FastAPI(title="RegTech API", lifespan=lifespan)

def get_conn(request: Request):
    with request.app.state.pool.connection() as conn:
        yield conn

@app.get("/v1/health")
def health(conn: psycopg.Connection = Depends(get_conn)):
    try:
        with conn.cursor() as cur:
            # keep things snappy; avoid hanging health checks
            cur.execute("SET LOCAL statement_timeout = 1500;")
            cur.execute("SELECT 1;")
            cur.fetchone()
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"db unhealthy: {e}")

@app.get("/v1/dbparams")
def get_dbparams():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        return {"dbparams": None}
    parsed = urlparse(url)
    redacted = f"{parsed.scheme}://***:***@{parsed.hostname}:{parsed.port}/{parsed.path.lstrip('/')}"
    return {"dbparams": redacted}

@app.get("/v1/testvalues")
def list_values(
    limit: int = Query(20, ge=1, le=1000),
    conn: psycopg.Connection = Depends(get_conn),
) -> List[Dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SET LOCAL statement_timeout = 3000;")
        cur.execute(
            "SELECT id, value FROM samples ORDER BY id DESC LIMIT %s;",
            (limit,),
        )
        rows = cur.fetchall()
    # rows already dicts thanks to dict_row
    # cast value to float to be JSON-friendly / consistent
    for r in rows:
        r["value"] = float(r["value"])
    return rows

@app.get("/v1/testaverage")
def compute_average(conn: psycopg.Connection = Depends(get_conn)):
    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = 3000;")
        cur.execute("SELECT AVG(value) FROM samples;")
        (avg,) = cur.fetchone()
    return {"average_value": float(avg) if avg is not None else None}

@app.post("/v1/maketrades", status_code=202)
def make_trades(req: MakeTradesRequest, bg: BackgroundTasks):
    """Generates a parquet file with mock trade data in the background."""
    if req.rows < 1 or req.rows > 100_000_000:
        raise HTTPException(status_code=400, detail="rows must be between 1 and 100,000,000")

    if req.chunk_size < 1 or req.chunk_size > 500_000:
        raise HTTPException(status_code=400, detail="chunk_size must be between 1 and 500,000")

    run_id = str(uuid.uuid4())
    st_cob_dt = req.cob_dt.strftime('%Y-%m-%d')
    
    print(f"Building {req.rows} trades in chunks of {req.chunk_size} at COB {st_cob_dt}")
    print(f"with run_id {run_id} ...")

    tradebuilder = TradeBuilder(req.cob_dt, run_id)

    def make_parquet(trade_builder: TradeBuilder, rows: int, chunk_size: int):
        """Background task to build the parquet file."""
        manifest = trade_builder.build(rows, chunk_size)
        if manifest.get("status") != "succeeded":
            print(f"Error: {manifest.get('error')}")
        else:
            print(f"Completed in {manifest['duration_s']}s. Wrote {manifest['rows']} rows (mb: {manifest['file_size_mb']}).")
        return manifest

    # Schedule the heavy work after returning the response
    bg.add_task(make_parquet, tradebuilder, req.rows, req.chunk_size)
    return {"run_id": run_id, "status": "accepted", "out_path": str(tradebuilder.parquet)}

@app.post("/v1/makecpties", status_code=202)
def make_cpties(req: MakeCptiesRequest, bg: BackgroundTasks):
    """Generates a parquet file with mock cpty data in the background."""
    if req.rows < 1 or req.rows > 100_000:
        raise HTTPException(status_code=400, detail="rows must be between 1 and 100,000")

    if req.chunk_size < 1 or req.chunk_size > 100_000:
        raise HTTPException(status_code=400, detail="chunk_size must be between 1 and 100,000")

    run_id = str(uuid.uuid4())
    st_cob_dt = req.cob_dt.strftime('%Y-%m-%d')
    
    print(f"Building {req.rows} cpties in chunks of {req.chunk_size} at COB {st_cob_dt}")
    print(f"with run_id {run_id} ...")

    cptybuilder = CptyBuilder(req.cob_dt, run_id)

    def make_parquet(cpty_builder: CptyBuilder, rows: int, chunk_size: int):
        """Background task to build the parquet file."""
        manifest = cpty_builder.build(rows, chunk_size)
        if manifest.get("status") != "succeeded":
            print(f"Error: {manifest.get('error')}")
        else:
            print(f"Completed in {manifest['duration_s']}s. Wrote {manifest['rows']} rows (mb: {manifest['file_size_mb']}).")
        return manifest

    # Schedule the heavy work after returning the response
    bg.add_task(make_parquet, cptybuilder, req.rows, req.chunk_size)
    return {"run_id": run_id, "status": "accepted", "out_path": str(cptybuilder.parquet)}

@app.get("/v1/runs/trades/{st_cob_dt}/{run_id}")
def run_trade_status(st_cob_dt: str, run_id: str):
    """Check status of a /make_trades run."""
    cob_dt = datetime.strptime(st_cob_dt, "%Y-%m-%d")
    manifest = TradeBuilder(cob_dt, run_id).manifest
    if not manifest.exists():
        raise HTTPException(404, "run not found")
    return json.loads(manifest.read_text())

@app.get("/v1/runs/cpties/{st_cob_dt}/{run_id}")
def run_cpty_status(st_cob_dt: str, run_id: str):
    """Check status of a /make_cpties run."""
    cob_dt = datetime.strptime(st_cob_dt, "%Y-%m-%d")
    manifest = CptyBuilder(cob_dt, run_id).manifest
    if not manifest.exists():
        raise HTTPException(404, "run not found")
    return json.loads(manifest.read_text())

# Columns (order must match Postgres table)
COLUMNS = [
    "trade_id","fo_trade_id","book","product","product_type",
    "notional1","currency1","notional2","currency2","counterparty",
    "traded_at","maturity","strike","rate","quantity",
    "strategy_type","strategy",
    "is_fx","is_commodity","is_equity","is_interest_rate","is_credit",
]

def _write_json(path: Path, obj: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))

# add this helper
def _resolve_parquet_files(run_id: str) -> list[Path]:
    run_dir = Config.RUNS_DIR / run_id
    if not run_dir.exists():
        raise FileNotFoundError(f"run folder not found: {run_dir}")
    files = sorted(p for p in run_dir.glob("*.parquet") if p.is_file())
    if not files:
        raise FileNotFoundError(f"No .parquet files in {run_dir}")
    return files

def _copy_parquet_to_pg(files: list[Path], table: str, batch_size: int, manifest: Path):
    _write_json(manifest, {
        "status": "running", "inserted": 0,
        "parquet_files": [str(p) for p in files], "table": table
    })

    inserted = 0
    try:
        # Build a dataset from only the .parquet files
        dataset   = ds.dataset([str(p) for p in files], format="parquet")
        writeopts = pacsv.WriteOptions(include_header=False)
        copy_sql  = f"COPY {table} ({', '.join(COLUMNS)}) FROM STDIN WITH (FORMAT csv)"

        with connect(os.getenv("DATABASE_URL")) as conn:
            with conn.cursor() as cur, cur.copy(copy_sql) as cp:
                for batch in dataset.to_batches(columns=COLUMNS, batch_size=batch_size):
                    buf = io.BytesIO()
                    pacsv.write_csv(batch, buf, write_options=writeopts)
                    cp.write(buf.getvalue())
                    inserted += len(batch)
                    _write_json(manifest, {
                        "status": "running", "inserted": inserted,
                        "parquet_files": [str(p) for p in files], "table": table
                    })
            conn.commit()

        _write_json(manifest, {"status": "succeeded", "inserted": inserted,
                               "parquet_files": [str(p) for p in files], "table": table})
    except Exception as e:
        _write_json(manifest, {"status": "failed", "inserted": inserted,
                               "parquet_files": [str(p) for p in files], "table": table,
                               "error": str(e)})

@app.post("/v1/loadtrades", status_code=202)
def load_trades(
    bg: BackgroundTasks,
    run_id: str | None = None,            # e.g. from /make_trades
    parquet_path: str | None = None,      # OR pass explicit path
    batch_size: int = 200_000,
    table: str = 'spk_trades',
):
    if not run_id and not parquet_path:
        raise HTTPException(400, "Provide run_id or parquet_path")
    
    if run_id:
        files = _resolve_parquet_files(run_id)               # <-- only *.parquet in that folder
    else:
        p = Path(parquet_path)
        if p.is_file() and p.suffix.lower() == ".parquet":
            files = [p]
        elif p.is_dir():
            files = sorted(pp for pp in p.glob("*.parquet") if pp.is_file())
            if not files:
                raise HTTPException(404, f"No parquet files in {p}")
        else:
            raise HTTPException(404, f"Not a file/dir: {p}")
        
    path = Path(parquet_path) if parquet_path else (Config.RUNS_DIR / run_id / "trades.parquet")
    if not path.exists():
        raise HTTPException(404, f"Parquet not found: {path}")

    load_id  = str(uuid.uuid4())
    manifest = Config.LOADS_DIR / load_id / "manifest.json"
    _write_json(manifest, {
        "status": "accepted",
        "parquet_files": [str(f) for f in files],
        "table": table,
        "batch_size": batch_size
    })

    bg.add_task(_copy_parquet_to_pg, files, table, batch_size, manifest)
    return {"load_id": load_id, "status": "accepted", "parquet_files": [str(f) for f in files], "table": table}

@app.get("/v1/loads/{load_id}")
def load_status(load_id: str):
    m = Config.LOADS_DIR / load_id / "manifest.json"
    if not m.exists():
        raise HTTPException(404, "load not found")
    return json.loads(m.read_text())

@app.get("/", response_class=HTMLResponse)
def home():
    return '<p>POST /run (multipart file)</p><p><a href="/portal/login">Mock portal</a></p>'

# --- mock portal (so Playwright has something to drive) ---
@app.get("/portal/login", response_class=HTMLResponse)
def portal_login():
    return """<form method="post" action="/portal/login">
      <input id="user" name="user"/><input id="pass" name="pass" type="password"/>
      <button>Sign in</button></form>"""

@app.post("/portal/login", response_class=HTMLResponse)
def portal_login_post():
    return """<h4>Upload Filing</h4>
      <form method="post" action="/portal/upload" enctype="multipart/form-data">
        <input type="file" name="file"/><button id="submit-btn">Submit</button></form>"""

@app.post("/portal/upload", response_class=HTMLResponse)
def portal_upload(file: UploadFile = File(...)):
    Path("evidence/uploads").mkdir(parents=True, exist_ok=True)
    Path("evidence/uploads", file.filename).write_bytes(file.file.read())
    return "<h3>Submission Success</h3> Ack: 12345"

def _parse_pg_for_duckdb_attach(db_url: str) -> str:
    # Build a DuckDB POSTGRES attach string: dbname=... user=... password=... host=... port=...
    u = urlparse(db_url)
    return (
        f"dbname={u.path.lstrip('/')}"
        f" user={u.username}"
        f" password={u.password}"
        f" host={u.hostname}"
        f" port={u.port or 5432}"
    )

def _trades_duckdb_from_postgres() -> "pd.DataFrame":
    con = duckdb.connect()
    # Try Postgres scanner; fallback to querying Postgres directly if extension unavailable
    try:
        con.execute("INSTALL postgres; LOAD postgres;")
        db_url = os.getenv("DATABASE_URL")
        attach_str = _parse_pg_for_duckdb_attach(db_url)
        con.execute(f"ATTACH '{attach_str}' AS pg (TYPE POSTGRES)")
        table = "spk_trades"
        group_cols = ["book", "currency1"]
        sum_col = "notional1"
        cols = ", ".join(group_cols)
        q = f"""
          SELECT {cols},
                 SUM({sum_col})::DOUBLE AS total_{sum_col},
                 COUNT(*)::BIGINT      AS n_rows
          FROM pg.public.{table}
          GROUP BY {cols}
          ORDER BY total_{sum_col} DESC
        """
        return con.execute(q).df()
    except Exception as e:
        # Fallback: do it in Postgres and return a small frame
        import psycopg
        cols = ", ".join(group_cols)
        with psycopg.connect(db_url) as conn, conn.cursor() as cur:
            cur.execute(f"""
              SELECT {cols},
                     SUM({sum_col})::DOUBLE PRECISION AS total_{sum_col},
                     COUNT(*)::BIGINT                 AS n_rows
              FROM {table}
              GROUP BY {cols}
              ORDER BY total_{sum_col} DESC
            """)
            import pandas as pd
            df = pd.DataFrame(cur.fetchall(), columns=group_cols + [f"total_{sum_col}", "n_rows"])
        return df

def _evidence_job(evidence_id: str):
    run_dir = Config.EVIDENCE_BASE / evidence_id
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = run_dir / "manifest.json"

    def write(state: dict):
        (run_dir / "manifest.json").write_text(json.dumps(state, indent=2))

    state = {
        "evidence_id": evidence_id,
        "status": "running",
        "source": 'db',  # or could be "parquet"
        "table": 'spk_trades',
    }
    write(state)

    try:
        # 1) Aggregate with DuckDB from database trades table
        df = _trades_duckdb_from_postgres()

        summary_csv = run_dir / "summary.csv"
        df.to_csv(summary_csv, index=False)

        # 2) Upload evidence via Playwright (reuses your RPA function)
        rpa = rpa_submit(str(summary_csv))  # returns manifest + screenshots

        state.update({
            "status": "succeeded",
            "summary_csv": str(summary_csv),
            "rpa_manifest": rpa.get("manifest"),
            "screenshots": rpa.get("evidence", []),
            "rows": int(df["n_rows"].sum()) if "n_rows" in df.columns else len(df),
        })
        write(state)

    except Exception as e:
        state.update({"status": "failed", "error": str(e)})
        write(state)

@app.post("/v1/evidencerun", status_code=202)
def evidence_run(
    bg: BackgroundTasks,
    load_id: str | None = None,
):
    """
    Build an evidence summary (DuckDB aggregation) and upload it via Playwright.
    - source of data aggregate from Postgres table
    """
    evidence_id = str(uuid.uuid4())
    bg.add_task(
        _evidence_job,
        evidence_id,
    )
    return {"evidence_id": evidence_id, "status": "accepted"}

@app.get("/v1/evidence/{evidence_id}")
def evidence_status(evidence_id: str):
    m = (Config.EVIDENCE_BASE / evidence_id / "manifest.json")
    if not m.exists():
        raise HTTPException(404, "evidence run not found")
    return json.loads(m.read_text())