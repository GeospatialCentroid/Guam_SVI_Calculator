#!/usr/bin/env python
"""
CLI for Guam HSI starter
"""
import argparse
from pathlib import Path

# ← relative imports: since this file is run as module src.main,
#    the dot tells Python “look for fetch and compute_hsi in the same package”
from .fetch import load_variable_codes, download_guam
from .compute_hsi import hsi


def parse_args():
    p = argparse.ArgumentParser(description="Compute a demo SVI/HSI")
    p.add_argument("--year",  type=int,   default=2022,
                   help="ACS 5‑year vintage (default: 2022)")
    p.add_argument("--state", type=str,   default="66",
                   help="State FIPS code (e.g. Colorado=08, Guam=66)")
    p.add_argument("--config",type=Path,  default=Path("configs/variables.csv"),
                   help="CSV listing raw Census variables")
    p.add_argument("--outfile",type=Path, default=Path("demo_svi.csv"),
                   help="Output CSV file name")
    p.add_argument("--api-key", type=str, default=None,
                   help="Census API key (optional but recommended)")
    return p.parse_args()


def main():
    args = parse_args()

    var_codes = load_variable_codes(args.config)
    print(f"Loaded {len(var_codes)} variables from {args.config}")

    df_raw = download_guam(var_codes, api_key=args.api_key)
    print(f"Fetched data for {len(df_raw)} places in state {args.state}")

    df_result = hsi(df_raw)

    # reorder: geography first
    geo_choices = ["state", "place", "county", "tract"]
    geo_cols = [c for c in geo_choices if c in df_result.columns]
    other = [c for c in df_result.columns if c not in geo_cols]
    df_result = df_result[geo_cols + other]

    df_result.to_csv(args.outfile, index=False)
    print(f"Saved demo results → {args.outfile}")


if __name__ == "__main__":
    main()
