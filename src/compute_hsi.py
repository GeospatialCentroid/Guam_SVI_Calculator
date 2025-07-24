"""
compute_hsi.py
~~~~~~~~~~~~~~
Take the wide *raw* Census dataframe returned by :pyfunc:`fetch.download_data`
(GEOGRAPHY columns + dozens/hundreds of raw numeric fields) and **add three
families of new columns**:

1. **Alias columns** – short, friendly names defined in *variables.csv*.
2. **SPL_*** – the *score/"simple"* value for each alias (here identical to the
   alias, but kept separate so future versions can apply weighting or caps).^†
3. **RPL_*** – *percentile* (0‒1) of each SPL_ column within the dataframe.

The module is intentionally generic:
* There is **no fixed list of variables** in the code.  All mappings come from
  a CSV that can be swapped out for Guam, other areas.
* Expressions in the CSV can reference raw Census columns **and/or** other
  aliases using standard math symbols (+ – * / () ).  The minimal expression
  parser below converts those tokens into *pandas‑aware* expressions so we can
  evaluate them safely with :pymeth:`pandas.eval`.

CDC/ATSDR SVI nomenclature uses *SPL* (Scaled Percentile) for a value that is
  later re‑ranked – we keep the same prefix to stay familiar.
"""
from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import math

###############################################################################
# 1. Configuration ------------------------------------------------------------
###############################################################################
# Path to the default variables.csv (…/configs/variables.csv relative to repo)
DEFAULT_CONFIG = Path(__file__).parents[1] / "configs" / "variables.csv"

# In the alias expression, *tokens* (variable names) are the substrings that look
# like   ABC123   or   TOTAL_POP   – i.e. letters, numbers and underscores only.
# We use this regex to find every such token so we can wrap it in df["TOKEN"].
TOKEN_RE = re.compile(r"\b[A-Z]{1,4}\d{0,3}_[0-9]{4}[A-Z]?\b")

###############################################################################
# 2. Internal helpers ----------------------------------------------------------
###############################################################################
# These helpers are *private* (underscore‑prefixed) – only used inside this
# module – but documented anyway so a non‑Python reader can still follow along.


def _load_alias_map(config_path: Path) -> Dict[str, str]:
    """Read *variables.csv* and return a ``dict`` mapping **alias → expression**.

    The CSV **must** have at least two columns named *alias* and *variable*.
    Example row:
    ``EP_POV150, (E_POV150 / E_TOTPOP) * 100``
    """
    with config_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return {row["alias"].strip(): row["variable"].strip() for row in reader}


def _evaluate_aliases(df: pd.DataFrame, alias_map: Dict[str, str]) -> pd.DataFrame:
    """Create *one new column per alias* using the expression in ``alias_map``.

    Steps for each alias:
      1. **Trivial copy** – if the expression is just a raw column name,
         `df[alias] = df[column].astype(float)`.
      2. **General expression** – replace every *token* ``T`` with ``df['T']`` so
         that :pyfunc:`pandas.eval` sees actual *Series* objects and can broadcast
         maths row‑wise.  Example:

             (E_POV150 / E_TOTPOP) * 100     →
             (df['E_POV150'] / df['E_TOTPOP']) * 100

         Using ``engine='python'`` sacrifices speed but *disallows* arbitrary
         Python – only arithmetic & NumPy functions are permitted, which is far
         safer when the content comes from a CSV.

    Any error (missing field, divide‑by‑zero, etc.) falls back to ``NaN`` so the
    pipeline keeps running – a conscious trade‑off for robustness.
    """
    df = df.copy()  # work on a copy so we never mutate caller dataframe

    for alias, expr in alias_map.items():

        # 1️ Skip if alias already exists (sometimes the raw column == alias)
        if alias in df.columns:
            continue

        # 2️ If the expression is *just* one token – simplest/fastest path
        if TOKEN_RE.fullmatch(expr):
            df[alias] = pd.to_numeric(df.get(expr, np.nan), errors="coerce")
            df = _add_percentiles(df, [alias])
            continue

        # 3️ Build a *safe* expression by wrapping tokens → df['TOKEN']
        safe_expr = expr
        for token in TOKEN_RE.findall(expr):
            safe_expr = safe_expr.replace(token, f"df['{token}']")

        # Local namespace visible to pandas.eval – we expose only df & numpy
        safe_locals = {"df": df, "np": np}

        try:

            df[alias] = pd.eval(safe_expr, local_dict=safe_locals, engine="python")
            print(alias,"👍",safe_expr)
            print("alias",alias)
            df = _add_percentiles(df, [alias])

        except Exception:
            print(alias,"❌",safe_expr)
            # Any issue → mark as NaN so downstream ranking ignores it
            df[alias] = np.nan

    return df


def _add_percentiles(df: pd.DataFrame, alias_map: Dict[str, str]) -> pd.DataFrame:
    """For *each* alias create matching **SPL_*** and **RPL_*** columns.

    * **SPL_ALIAS** – copy of the raw alias value (simple score).  Having a
      distinct column keeps the door open for future transforms without modifying the original.
    * **RPL_ALIAS** – percentile rank of SPL_ALIAS within *the entire* dataframe
      (0 → lowest, 1 → highest).  Percentiles are rounded to 4 decimal places to
      match CDC/ATSDR SVI convention.

    Population denominators (aliases like ``TOT_POP``) are **excluded** because
    ranking those makes no conceptual sense in SVI/HSI context.
    """
    df = df.copy()

    # Aliases that represent total population – skip percentile logic
    skip = {"E_TOTPOP", "TOT_POP", "TOTPOP"}

    for alias in alias_map:
        if alias.upper() in skip:
            continue

        spl = f"SPL_{alias}"
        rpl = f"RPL_{alias}"
        print("Calculating ",spl,rpl)
        df[spl] = df[alias]
        # pandas.Series.rank(..., pct=True) → value / (n – 1)
        df[rpl] = df[spl].rank(pct=True).round(4)

    return df

###############################################################################
# 3. Public entry point --------------------------------------------------------
###############################################################################

def hsi(df: pd.DataFrame, *, config_path: Path | None = None) -> pd.DataFrame:
    """Return *df* enriched with all Alias, SPL_, and RPL_ columns.

    Parameters
    ----------
    df : DataFrame
        Raw dataframe straight from :pyfunc:`fetch.download_data`.
    config_path : Path | None, default *DEFAULT_CONFIG*
        Override if you want to point at a different CSV (e.g., Guam vs. CO).

    Workflow
    ---------
      1. Load alias→expression map from the CSV.
      2. Evaluate expressions → new alias columns.
      3. Derive SPL_ & RPL_ percentiles.
      4. Return a *new* dataframe (original is untouched).
    """
    config_path = config_path or DEFAULT_CONFIG
    alias_map = _load_alias_map(config_path)

    df = _evaluate_aliases(df, alias_map)

    return df
