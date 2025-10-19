
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

from api.config import Config


class DataBuilder:
    """ Generic Parquet data builder class. """
    def __init__(self, cob_dt: datetime, run_id: str, name: str, generator: Callable[[datetime, int, np.random.Generator], pd.DataFrame]):
        self.cob_dt = cob_dt
        st_cob_dt = self.cob_dt.strftime("%Y-%m-%d")
        self.run_id = run_id
        self.name = name
        self.generator = generator
        self.manifest_file = f"{self.name}-{Config.MANIFEST_FILE}"
        self.parquet_file = f"{self.name}-{Config.PARQUET_FILE}"
        self.manifest = Path(Config.RUNS_DIR) / st_cob_dt / self.run_id / self.manifest_file
        self.parquet = Path(Config.RUNS_DIR) / st_cob_dt / self.run_id / self.parquet_file

    def build(self, rows: int, chunk_size: int):
        self._make_parquet(rows, chunk_size)
        return self._get_manifest()

    def _write_manifest(self, obj: Dict[str, Any]):
        """Ensure directory exists before writing"""
        self.manifest.parent.mkdir(parents=True, exist_ok=True)
        self.manifest.write_text(json.dumps(obj, indent=2))
    
    def _get_manifest(self) -> Dict[str, Any]:
        """Reads the manifest file if it exists, else returns empty dict."""
        if self.manifest.exists():
            return json.loads(self.manifest.read_text())
        return {}
    
    def _make_parquet(self, rows: int, chunk_size: int):
        """Generates a Parquet file in chunks and writes a manifest file with status updates."""
        st_cob_dt = self.cob_dt.strftime("%Y-%m-%d")
        started = time.time()

        self._write_manifest({
            "dataset": self.name,
            "cob": st_cob_dt,
            "run_id": self.run_id,
            "status": "running",
            "rows": rows,
            "written": 0,
            "out_path": str(self.parquet)
        })

        writer, written = None, 0
        rng = np.random.default_rng(42)

        try:
            self.parquet.parent.mkdir(parents=True, exist_ok=True)

            while written < rows:
                n = min(chunk_size, rows - written)
                df = self.generator(self.cob_dt, n, rng)
                table = pa.Table.from_pandas(df, preserve_index=False)

                if writer is None:
                    writer = pq.ParquetWriter(str(self.parquet), table.schema, compression="zstd")

                writer.write_table(table)
                written += n

                self._write_manifest({
                    "dataset": self.name,
                    "cob": self.cob_dt.strftime("%Y-%m-%d"),
                    "run_id": self.run_id,
                    "status": "running",
                    "rows": rows,
                    "written": written,
                    "out_path": str(self.parquet)
                })

            if writer:
                writer.close()
            
            file_size_bytes = self.parquet.stat().st_size
            file_size_mb = round(file_size_bytes / (1024*1024), 2)
        
            self._write_manifest({
                "dataset": self.name,
                "cob": self.cob_dt.strftime("%Y-%m-%d"),
                "run_id": self.run_id,
                "status": "succeeded",
                "rows": rows,
                "written": written,
                "out_path": str(self.parquet),
                "duration_s": round(time.time() - started, 2),
                "file_size_bytes": file_size_bytes,
                "file_size_mb": file_size_mb
            })

        except Exception:
            if writer:
                writer.close()

            self._write_manifest({
                "dataset": self.name,
                "cob": self.cob_dt.strftime("%Y-%m-%d"),
                "run_id": self.run_id,
                "status": "failed",
                "rows": rows,
                "written": written,
                "out_path": str(self.parquet),
                "error": traceback.format_exc()
            })
