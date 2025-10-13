from pathlib import Path
import numpy as np
import pandas as pd
import json, uuid, time
from pydantic import BaseModel
import pyarrow as pa
import pyarrow.parquet as pq
import traceback

DATA_DIR = Path("./.data")
RUNS_DIR = DATA_DIR / "runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

LOADS_DIR = DATA_DIR / "loads"         # manifests for load jobs
LOADS_DIR.mkdir(parents=True, exist_ok=True)

EVIDENCE_BASE = DATA_DIR / "evidence_runs"
EVIDENCE_BASE.mkdir(parents=True, exist_ok=True)



class MakeTradesRequest(BaseModel):
    rows: int = 10_000  # default
    chunk_size: int = 10_000  # default
    out_path: str | None = None

def _write_manifest(path: Path, obj: dict):
    # ensure directory exists before writing
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))

def _generate_trades_chunk(n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generates a chunk of n trades as a DataFrame."""
    books = np.array(["XVA","CVA","FVA","MM","EQD","FXD"])
    prods = np.array(["IRS","CDS","FXO","EQO","XCCY"])
    ccys  = np.array(["USD","EUR","GBP","JPY","AUD"])
    cps   = np.array(["A","B","C","D","E","F","G","H"])
    prodtypes = np.array(["Swap","Option","Cap/Floor","Forward","Future"])
    strattypes = np.array(["Curve","Basis","Vol","Dispersion","Carry","Roll"])
    strats = np.array(["DOFHOFS","DSCJOSF","DSFSDEE","ERTOOQW","FDSFNBV","FDSFJAA","FDSJDER"]) 

    df = pd.DataFrame({
        "trade_id": np.arange(1, n_rows+1, dtype=np.int64),
        "fo_trade_id": np.arange(1, n_rows+1, dtype=np.int64),
        "book": rng.choice(books, n_rows),
        "product": rng.choice(prods, n_rows),
        "product_type": rng.choice(prodtypes, n_rows),
        "notional1": rng.lognormal(mean=13, sigma=1.0, size=n_rows).astype("float64"),
        "currency1": rng.choice(ccys, n_rows),
        "notional2": rng.lognormal(mean=13, sigma=1.0, size=n_rows).astype("float64"),
        "currency2": rng.choice(ccys, n_rows),
        "counterparty": rng.choice(cps, n_rows),
        "traded_at": pd.to_datetime("2024-01-01") + pd.to_timedelta(rng.integers(0, 365, n_rows), unit="D"),
        "maturity": pd.to_datetime("2025-10-01") + pd.to_timedelta(rng.integers(0, 365, n_rows), unit="D"),
        "strike": rng.uniform(0.8, 1.2, n_rows).astype("float64"),
        "rate": rng.uniform(0.01, 0.1, n_rows).astype("float64"),
        "quantity": rng.integers(1, 100, n_rows).astype("int32"),
        "strategy_type": rng.choice(strattypes, n_rows),
        "strategy": rng.choice(strats, n_rows),
        "is_fx": rng.choice([0,1], n_rows, p=[0.7,0.3]).astype("int8"),
        "is_commodity": rng.choice([0,1], n_rows, p=[0.9,0.1]).astype("int8"),
        "is_equity": rng.choice([0,1], n_rows, p=[0.85,0.15]).astype("int8"),
        "is_interest_rate": rng.choice([0,1], n_rows, p=[0.6,0.4]).astype("int8"),
        "is_credit": rng.choice([0,1], n_rows, p=[0.95,0.05]).astype("int8"),
    })
    return df
    
def _make_parquet(run_id: str, rows: int, chunk_size: int, out_path: Path):
    """Runs AFTER HTTP response (BackgroundTasks). Writes in chunks to avoid OOM/timeouts."""
    run_dir = RUNS_DIR / run_id
    manifest = run_dir / "manifest.json"
    started = time.time()

    _write_manifest(manifest, {
        "run_id": run_id, 
        "status": "running", 
        "rows": rows, 
        "written": 0, 
        "out_path": str(out_path)
    })

    writer = None
    written = 0
    start_id = 1
    rng = np.random.default_rng(42)

    try:
        # create parent dir
        out_path.parent.mkdir(parents=True, exist_ok=True)

        while written < rows:
            n = min(chunk_size, rows - written)
            df = _generate_trades_chunk(n, start_id, rng)
            table = pa.Table.from_pandas(df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(str(out_path), table.schema, compression="zstd")  # or "snappy"
            
            writer.write_table(table)
            written += n
            start_id += n

            _write_manifest(manifest, {
                "run_id": run_id, 
                "status": "running",
                "rows": rows, 
                "written": written,
                "out_path": str(out_path)
            })
        
        if writer: 
            writer.close()
        
        _write_manifest(manifest, {
            "run_id": run_id, 
            "status": "succeeded",
            "rows": rows, 
            "written": written,
            "out_path": str(out_path),
            "duration_s": round(time.time() - started, 2)
        })

    except Exception:
        if writer: 
            writer.close()

        _write_manifest(manifest, {
            "run_id": run_id, 
            "status": "failed",
            "rows": rows, 
            "written": written,
            "out_path": str(out_path),
            # write full traceback for easier debugging next time
            "error": traceback.format_exc()})