from pathlib import Path

class Config:
    """Configuration settings."""
    DATA_DIR = "data"
    DATA_DIR = Path(".data")
    RUNS_DIR = DATA_DIR / "runs"
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_FILE = "manifest.json"
    