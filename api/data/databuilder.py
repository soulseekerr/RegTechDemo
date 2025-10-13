
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import json
import time
import traceback
from typing import Callable, Dict, Any

from .config import Config


class DataBuilder:
    """ Generic Parquet data builder class. """
    def __init__(self, name: str, generator: Callable[[datetime, int, np.random.Generator], pd.DataFrame]):
        self.name = name
        self.generator = generator

    def build(self, cob_dt: datetime, run_id: str, rows: int, chunk_size: int, out_path: Path):
        self._make_parquet(cob_dt, run_id, rows, chunk_size, out_path)
        return self._get_manifest(run_id)

    def _write_manifest(self, path: Path, obj: Dict[str, Any]):
        """Ensure directory exists before writing"""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, indent=2))
    
    def _get_manifest(self, run_id: str) -> Dict[str, Any]:
        """Reads the manifest file if it exists, else returns empty dict."""
        run_dir = Config.RUNS_DIR / run_id
        manifest_file = run_dir / Config.MANIFEST_FILE
        if manifest_file.exists():
            return json.loads(manifest_file.read_text())
        return {}
    
    def _make_parquet(self, cob_dt: datetime, run_id: str, rows: int, chunk_size: int, out_path: Path):
        run_dir = Config.RUNS_DIR / run_id
        manifest = run_dir / Config.MANIFEST_FILE
        started = time.time()

        self._write_manifest(manifest, {
            "dataset": self.name,
            "cob": cob_dt.strftime("%Y-%m-%d"),
            "run_id": run_id,
            "status": "running",
            "rows": rows,
            "written": 0,
            "out_path": str(out_path)
        })

        writer, written = None, 0
        rng = np.random.default_rng(42)

        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)

            while written < rows:
                n = min(chunk_size, rows - written)
                df = self.generator(cob_dt, n, rng)
                table = pa.Table.from_pandas(df, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(str(out_path), table.schema, compression="zstd")

                writer.write_table(table)
                written += n

                self._write_manifest(manifest, {
                    "dataset": self.name,
                    "run_id": run_id,
                    "status": "running",
                    "rows": rows,
                    "written": written,
                    "out_path": str(out_path)
                })

            if writer:
                writer.close()
            
            file_size_bytes = out_path.stat().st_size
            file_size_mb = round(file_size_bytes / (1024*1024), 2)
        
            self._write_manifest(manifest, {
                "dataset": self.name,
                "run_id": run_id,
                "status": "succeeded",
                "rows": rows,
                "written": written,
                "out_path": str(out_path),
                "duration_s": round(time.time() - started, 2),
                "file_size_bytes": file_size_bytes,
                "file_size_mb": file_size_mb
            })

        except Exception:
            if writer:
                writer.close()

            self._write_manifest(manifest, {
                "dataset": self.name,
                "run_id": run_id,
                "status": "failed",
                "rows": rows,
                "written": written,
                "out_path": str(out_path),
                "error": traceback.format_exc()
            })
