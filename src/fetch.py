"""
fetch.py
Fetch Guam 2020 Decennial (dpgu dataset) data in 50‑variable chunks.
Accepts optional API key.
"""

import re
import csv
from collections import defaultdict
from pathlib import Path
from typing import List

import pandas as pd
import requests

CHUNK_SIZE = 50
DATASET_URL = "https://api.census.gov/data/2020/dec/dpgu"

VAR_RE = re.compile(r"\b[A-Z]{1,3}\d{0,3}_[0-9]{4}[A-Z]?\b")

# ----------------------------------------------------------------------
# Helper: extract raw variable codes from expression strings – match only things that look like DP4_0125C, B16005_007E, etc.
# ----------------------------------------------------------------------
def _extract_codes(expr: str) -> List[str]:
    return VAR_RE.findall(expr)

# ----------------------------------------------------------------------
# Read config CSV → list of unique variable codes
# ----------------------------------------------------------------------
def load_variable_codes(csv_path: Path) -> List[str]:
    with csv_path.open(newline="", encoding="utf8") as fh:
        reader = csv.DictReader(fh)
        if "variable" not in reader.fieldnames:
            raise RuntimeError(f"CSV {csv_path} must have a 'variable' column.")
        codes = []
        for row in reader:
            # collapse all whitespace/newlines into single spaces
            raw = row["variable"]
            expr = " ".join(raw.replace("\r", " ").split())
            codes.extend(_extract_codes(expr))
        # Drop duplicates while preserving order
        seen = set()
        unique = []
        for c in codes:
            if c not in seen:
                seen.add(c)
                unique.append(c)
        return unique

# ----------------------------------------------------------------------
# Download data for every tract in Guam
# ----------------------------------------------------------------------
def download_guam(var_codes: List[str], api_key: str | None = None) -> pd.DataFrame:
    frames = []
    for i in range(0, len(var_codes), CHUNK_SIZE):
        chunk = var_codes[i : i + CHUNK_SIZE]
        params = {
            "get": ",".join(chunk + ["NAME"]),
            "for": "place:*",  # <-- geographic level 160
            "in": "state:66",
        }
        if api_key:
            params["key"] = api_key
        resp = requests.get(DATASET_URL, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        df_chunk = pd.DataFrame(data[1:], columns=data[0])
        frames.append(df_chunk)

    # Merge on geographic keys
    geokeys = ["state", "place"]  # <-- keys now returned by the API
    df = frames[0]
    for extra in frames[1:]:
            df = df.merge(extra, on=geokeys, how="left")

    # Convert numerics
    geo_cols = {"NAME", *geokeys}
    for col in df.columns.difference(geo_cols):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df