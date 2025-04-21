"""
compute_hsi.py
Placeholder HSI calculator using the poverty variable.
"""
import pandas as pd


def dummy_hsi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Simple hazard susceptibility demo:
    1. Rename the raw ACS poverty estimate
    2. Use it directly as the 'score'
    3. Compute a percentile rank
    """
    df = df.copy()

    # Rename the ACS poverty column to a shorter name
    # (raw ACS subject table code S1701_C01_040E)
    if "S1701_C01_040E" in df.columns:
        df = df.rename(columns={"S1701_C01_040E": "EP_POV"})
    else:
        raise KeyError("Expected column S1701_C01_040E not found in DataFrame")

    # Use the poverty percentage directly as our demo 'series'
    df["SPL_DUMMY"] = df["EP_POV"]
    # Compute percentile rank (0 to 1)
    df["RPL_DUMMY"] = df["SPL_DUMMY"].rank(pct=True).round(4)
    return df
