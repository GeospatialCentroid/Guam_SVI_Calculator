"""
compute_hsi.py
Generic HSI calculator that dynamically reads aliases from the CSV and computes percentiles for each variable.
"""
import csv
from pathlib import Path

import pandas as pd

# Locate the config file relative to this script
CONFIG_PATH = Path(__file__).parents[1] / "configs" / "variables.csv"


def dummy_hsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dynamic hazard susceptibility demo:
    1. Load alias-to-ACS-variable mapping from the config CSV.
    2. Rename DataFrame columns from raw ACS codes to aliases.
    3. For each alias (except TOT_POP), treat its value as a series,
       compute its percentile rank, and add SPL_<alias> and RPL_<alias>.
    """
    df = df.copy()

    # 1. Read alias mapping
    alias_map = {}
    with CONFIG_PATH.open(newline="", encoding="utf8") as fh:
        reader = csv.DictReader(fh)
        if not {"alias", "variable"}.issubset(reader.fieldnames):
            raise RuntimeError(
                f"Config CSV must have 'alias' and 'variable' headers: {CONFIG_PATH}"
            )
        for row in reader:
            alias = row["alias"].strip()
            var   = row["variable"].strip()
            alias_map[alias] = var

    # 2. Rename raw ACS columns to alias names
    rename_map = {raw: alias for alias, raw in alias_map.items()}
    df = df.rename(columns=rename_map)

    # 3. Compute percentiles for each alias except TOT_POP
    for alias in alias_map:
        if alias == "TOT_POP":
            continue
        spl_col = f"SPL_{alias}"
        rpl_col = f"RPL_{alias}"
        # Use the alias column directly as the series
        df[spl_col] = df[alias]
        # Compute percentile rank
        df[rpl_col] = df[spl_col].rank(pct=True).round(4)

    return df
