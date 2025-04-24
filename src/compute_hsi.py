"""
compute_hsi.py
Dynamic HSI calculator for Guam 2020 Decennial data.
Reads alias/expression pairs from configs/variables.csv,
evaluates expressions (supports + and -),
and computes percentile ranks (RPL_) for each SPL_ series.
"""

import csv
import re
from pathlib import Path

import numpy as np
import pandas as pd

# Path to alias mapping
CONFIG_PATH = Path(__file__).parents[1] / "configs" / "variables.csv"

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")

def _load_alias_map():
    alias_map = {}
    with CONFIG_PATH.open(newline="", encoding="utf8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            alias = row["alias"].strip()
            expr  = row["variable"].strip()
            alias_map[alias] = expr
    return alias_map

def _evaluate_aliases(df: pd.DataFrame, alias_map: dict) -> pd.DataFrame:
    """Create new columns for each alias expression."""
    df = df.copy()
    for alias, expr in alias_map.items():
        if alias in df.columns:
            continue
        # Simple copy
        if _TOKEN_RE.fullmatch(expr):
            df[alias] = pd.to_numeric(df.get(expr, np.nan), errors="coerce")
            continue

        # Build Python expression referencing df columns
        py_expr = expr
        for token in _TOKEN_RE.findall(expr):
            py_expr = py_expr.replace(token, f"df['{token}']")
        try:
            df[alias] = eval(py_expr)
        except Exception:
            df[alias] = np.nan
    return df

def _add_percentiles(df: pd.DataFrame, alias_map: dict) -> pd.DataFrame:
    df = df.copy()
    for alias in alias_map:
        # Skip population denominator
        if alias.upper() in {"E_TOTPOP", "TOT_POP", "TOTPOP"}:
            continue
        spl_col = f"SPL_{alias}"
        rpl_col = f"RPL_{alias}"
        df[spl_col] = df[alias]
        df[rpl_col] = df[spl_col].rank(pct=True).round(4)
    return df

def hsi(df: pd.DataFrame) -> pd.DataFrame:
    alias_map = _load_alias_map()
    df = _evaluate_aliases(df, alias_map)
    df = _add_percentiles(df, alias_map)
    return df