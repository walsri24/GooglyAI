# googlyai/data_pipeline/db.py
import duckdb
from pathlib import Path

DB_PATH = Path("data/duckdb/googlyai.duckdb")

def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))
