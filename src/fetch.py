"""
fetch.py
Helper utilities to download ACS data for any U.S. state/territory.
"""
import csv
from collections import defaultdict
from pathlib import Path
from typing import List, Dict

import pandas as pd
import requests

# Constants
BASE_URL = "https://api.census.gov/data/{year}/acs/acs5"
CHUNK_SIZE = 50   # max variables per call

def load_variable_list(csv_path: Path) -> List[str]:
    """
    Read your config CSV and return the list of ACS codes.
    Supports either a column named 'variable' or 'acs_code'.
    """
    with csv_path.open(newline="", encoding="utf8") as fh:
        reader = csv.DictReader(fh)
        if "variable" in reader.fieldnames:
            key = "variable"
        elif "acs_code" in reader.fieldnames:
            key = "acs_code"
        else:
            raise RuntimeError(
                f"CSV {csv_path} must have a header 'variable' or 'acs_code'"
            )
        return [row[key].strip() for row in reader]

def download_acs(year: int, var_codes: List[str], state_fips: str) -> pd.DataFrame:
    """
    Download all requested variables for every census tract in the given state.
    Variables are grouped by table prefix to minimize API calls.
    """
    # Group codes by table name (prefix before underscore)
    grouped: Dict[str, List[str]] = defaultdict(list)
    for code in var_codes:
        table = code.split("_")[0]
        grouped[table].append(code)

    frames: List[pd.DataFrame] = []

    for table, codes in grouped.items():
        # pick correct endpoint based on table type
        if table.startswith("DP"):
            url = f"https://api.census.gov/data/{year}/acs/acs5/profile"
        elif table.startswith("S"):
            url = f"https://api.census.gov/data/{year}/acs/acs5/subject"
        else:
            url = BASE_URL.format(year=year)

        for i in range(0, len(codes), CHUNK_SIZE):
            chunk = codes[i : i + CHUNK_SIZE]
            params = {
                "get": ",".join(chunk),
                "for": "tract:*",
                "in": f"state:{state_fips}",
            }
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            df_chunk = pd.DataFrame(data[1:], columns=data[0])

            if frames:
                frames[-1] = frames[-1].merge(
                    df_chunk, on=["state", "county", "tract"], how="outer"
                )
            else:
                frames.append(df_chunk)

    df = frames[0]
    geo_cols = {"state", "county", "tract"}
    for col in df.columns.difference(geo_cols):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df
