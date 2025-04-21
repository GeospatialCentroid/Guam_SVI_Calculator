#!/usr/bin/env python
"""
CLI for Guam/Colorado SVI starter
"""
import argparse
from pathlib import Path

# ← relative imports: since this file is run as module src.main,
#    the dot tells Python “look for fetch and compute_hsi in the same package”
from .fetch import load_variable_list, download_acs
from .compute_hsi import dummy_hsi


def parse_args():
    p = argparse.ArgumentParser(description="Compute a demo SVI/HSI")
    p.add_argument("--year",  type=int,   default=2022,
                   help="ACS 5‑year vintage (default: 2022)")
    p.add_argument("--state", type=str,   default="66",
                   help="State FIPS code (e.g. Colorado=08, Guam=66)")
    p.add_argument("--config",type=Path,  default=Path("configs/variables.csv"),
                   help="CSV listing raw ACS variables")
    p.add_argument("--outfile",type=Path, default=Path("demo_svi.csv"),
                   help="Output CSV file name")
    return p.parse_args()


def main():
    args = parse_args()

    var_codes = load_variable_list(args.config)
    print(f"Loaded {len(var_codes)} variables from {args.config}")

    df_raw = download_acs(args.year, var_codes, args.state)
    print(f"Fetched data for {len(df_raw)} tracts in state {args.state}")

    df_result = dummy_hsi(df_raw)

    # reorder: geography first
    geo_cols = ["state", "county", "tract"]
    other = [c for c in df_result.columns if c not in geo_cols]
    df_result = df_result[geo_cols + other]

    df_result.to_csv(args.outfile, index=False)
    print(f"Saved demo results → {args.outfile}")


if __name__ == "__main__":
    main()
