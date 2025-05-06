"""
fetch.py
~~~~~~~~
Pure‑Python helpers that download raw Census data and return it as a tidy
Dataframe.  No Guam‑specific logic lives here – pass any state, year, dataset, or geography 
supported by the Decennial API and the functions will work.

------------------------------------------------------------------------------
High‑level workflow
------------------------------------------------------------------------------
(1)  *variables.csv* → **{dataset_slug → [raw variable codes…]}**  
     A helper reads the project‑wide CSV, discovers **every** raw Census token
     used in a formula, and groups them by the dataset (product slug) where
     they live.

(2)  **download one dataset at a time**  
     Each API call is limited to ≤ 50 variables, so a second helper slices the
     list into manageable “chunks”, downloads them in series, and merges the
     partial frames on the geography keys.

(3)  **data cleanliness**  
     Every numeric column is coerced to a real float, and any of the official
     *sentinel* “no‑data” markers (‑888888888, ‑999999999) are mapped to NaN so
     that subsequent calculations ignore them automatically.

"""
from __future__ import annotations

###############################################################################
# ── Standard library imports ─────────────────────────────────────────────────
###############################################################################
import csv
import re
from pathlib import Path
from typing import Dict, List

###############################################################################
# ── Third‑party imports ──────────────────────────────────────────────────────
###############################################################################
import numpy as np
import pandas as pd
import requests

###############################################################################
# 1.  Regular‑expression that locates *raw* Census variable codes
#     ----------------------------------------------------------
# A genuine code always looks like one of these:
#
#   • DP1_0001C   (Profile table, field 0001, **C** = percent)
#   • S1701_C01_040E (Subject table, C01 group, field 040, **E** = estimate)
#   • P1_0001N    (Public Law table, field 0001, **N** = number)
#
# Anatomy of the regex below:
#
#   \b              – word boundary, so we don't match inside longer strings
#   [A-Z]{1,4}      – table prefix  (DP, S, P, B, etc.)
#   \d{0,3}         – optional digits in the prefix  (e.g. B01001)
#   _               – literal underscore
#   [0-9]{4}        – 4‑digit field number
#   [A-Z]?          – optional one‑letter suffix  (E, C, M, N, etc.)
#   \b              – closing word boundary
#
# The pattern is intentionally strict so we never mistake something like
# “DP1_0001C_extra” for a real code.
###############################################################################
VAR_RE = re.compile(r"\b[A-Z]{1,4}\d{0,3}_[0-9]{4}[A-Z]?\b")

# The Census API refuses requests with >50 variables.  We therefore slice long
# lists into CHUNK_SIZE‑sized segments and fire multiple calls.
CHUNK_SIZE = 50

# Official “no‑data” sentinels used by several Census products.  Mapping them
# to NaN at ingestion guarantees that all later arithmetic and percentile
# calculations automatically skip them.
BAD_SENTINELS = {-888888888, -999999999}

###############################################################################
# 2.  Tiny helpers – kept private (underscore‑prefixed) because they are
#     implementation details rather than part of the public API.
###############################################################################
def _extract_codes(expr: str) -> List[str]:
    """
    Return **every** raw variable token inside *expr* **in order of appearance**.

    The order matters because we want the first crash or missing column to
    point developers back to the first offending row in the CSV, not a random
    later one.
    """
    return VAR_RE.findall(expr)


def _dedupe_preserve_order(seq: List[str]) -> List[str]:
    """
    Remove duplicates from *seq* while preserving the first‑seen order.

    A straight `set(seq)` would scramble the order, making debugging harder.
    """
    seen: set[str] = set()
    unique: List[str] = []
    for item in seq:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


###############################################################################
# 3.  PUBLIC HELPER:  variables.csv → {dataset_slug: [raw_codes…]}
###############################################################################
def group_variable_codes_by_dataset(csv_path: Path) -> Dict[str, List[str]]:
    """
    Scan *variables.csv* and return a mapping “dataset → unique token list”.

    The CSV **must** have *exactly* these three headers in this order::

        alias,dataset,variable

    Example row::

        E_TOTPOP,dpgu,S0601_C01_001E

    Any free‑form formula placed in the *variable* column is scanned for raw
    codes; all of them are added to the bucket of the row’s *dataset*.
    """
    buckets: Dict[str, List[str]] = {}
    seen: Dict[str, set[str]] = {}  # per‑dataset duplicate tracker

    with csv_path.open(newline="", encoding="utf‑8") as fh:
        reader = csv.DictReader(fh)
        required = {"alias", "dataset", "variable"}

        if not required.issubset(reader.fieldnames or []):
            raise RuntimeError(
                f"{csv_path} must contain headers {', '.join(required)} "
                f"(found {reader.fieldnames})"
            )

        for row in reader:
            dataset = row["dataset"].strip()
            codes_in_expr = _extract_codes(row["variable"])

            bucket = buckets.setdefault(dataset, [])
            already = seen.setdefault(dataset, set())

            for code in codes_in_expr:
                if code not in already:
                    already.add(code)
                    bucket.append(code)

    # Final pass – ensure each bucket has no duplicates but order is predictable
    return {ds: _dedupe_preserve_order(codes) for ds, codes in buckets.items()}


###################################################################################
# 4.  Geography helpers – translate the API “for=” parameter into returned columns
###################################################################################
def build_dataset_url(year: int, product: str) -> str:
    """
    Return the root API endpoint for one decennial *product* (dataset slug).

    Examples
    --------
    >>> build_dataset_url(2020, "dpgu")
    'https://api.census.gov/data/2020/dec/dpgu'
    """
    return f"https://api.census.gov/data/{year}/dec/{product}"


def geokeys_for(geography: str) -> List[str]:
    """
    Map an API geography keyword to the list of columns that identify a row.

    These columns are always returned by the API and together serve as a
    composite primary key when merging multiple datasets.
    """
    lookup = {
        "place": ["state", "place"],
        "county": ["state", "county"],
        "county subdivision": ["state", "county", "county subdivision"],
        "tract": ["state", "county", "tract"],
        "state": ["state"],
    }
    try:
        return lookup[geography]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported geography '{geography}'.  "
            f"Choose one of: {', '.join(lookup)}."
        ) from exc


###############################################################################
# 5.  PUBLIC HELPER:  download_data  (one dataset, all variables)
###############################################################################
def download_data(
    var_codes: List[str],
    state: str,
    *,
    year: int,
    product: str,
    geography: str,
    api_key: str | None = None,
) -> pd.DataFrame:
    """
    Fetch **all** variables in *var_codes* for the given dataset (*product*).

    Steps performed internally
    --------------------------
    1. Split *var_codes* into ≤ 50‑item chunks (API hard limit).
    2. Download each chunk and convert the JSON into a small DataFrame.
    3. Merge all chunk frames on the geography keys.
    4. Convert numeric strings → floats, coercing errors to NaN.
    5. Replace sentinel “no‑data” values with NaN to keep calculations safe.

    Returns
    -------
    pandas.DataFrame
        • Geography columns first (state, place, …)  
        • A human‑readable “NAME” column  
        • One column per requested variable code
    """
    base_url = build_dataset_url(year, product)
    geokeys = geokeys_for(geography)

    partials: list[pd.DataFrame] = []

    # ------------------------------------------------------------------ loop over 50‑var chunks
    for i in range(0, len(var_codes), CHUNK_SIZE):
        chunk = var_codes[i : i + CHUNK_SIZE]

        params = {
            "get": ",".join(chunk + ["NAME"]),
            "for": f"{geography}:*",
            "in": f"state:{state}",
        }
        if api_key:
            params["key"] = api_key  # optional but unlocks higher rate limits

        r = requests.get(base_url, params=params, timeout=60)
        r.raise_for_status()  # fail on 4xx or 5xx errors

        json_rows = r.json()
        partials.append(pd.DataFrame(json_rows[1:], columns=json_rows[0]))

    # ------------------------------------------------------------------ merge all chunk frames
    df = partials[0]
    for extra in partials[1:]:
        df = df.merge(extra, on=geokeys, how="left")

    # ------------------------------------------------------------------ numeric coercion & NaN masking
    geo_cols = set(["NAME", *geokeys])
    for col in df.columns.difference(geo_cols):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.replace(BAD_SENTINELS, np.nan, inplace=True)
    return df
