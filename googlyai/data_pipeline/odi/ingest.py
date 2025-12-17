# googlyai/data_pipeline/odi/loader.py
import json
from pathlib import Path

def load_match_json(file_path: Path) -> dict | None:
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        if data.get("info", {}).get("match_type") != "ODI":
            return None

        return data

    except Exception as e:
        print(f"[WARN] Failed to load {file_path.name}: {e}")
        return None
