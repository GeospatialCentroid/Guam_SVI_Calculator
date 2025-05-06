"""
main.py
~~~~~~~
Command‑line front‑end that strings the helper modules together and
transparently falls back to a local cache when the Census API is unavailable.

Typical invocation
------------------
::

    python -m src.main \
        --state 66 \
        --year 2020 \
        --geography place

What it does
------------
1.  Parse CLI flags.
2.  Read *variables.csv* and build **{dataset → variable list}**.
3.  For each dataset:
      • download the required variables *or* reuse a cached CSV,
      • ensure every requested variable is present.
4.  Merge the per‑dataset frames on the geography keys.
5.  Enrich the dataframe with alias, SPL_, and RPL_ columns via *compute_hsi*.
6.  Write a tidy CSV whose geography columns appear first for readability.
7.  Persist each raw dataset CSV to the cache after a successful API pull.
"""
from __future__ import annotations

###############################################################################
# ── Standard‑library imports ─────────────────────────────────────────────────
###############################################################################
import argparse
from pathlib import Path
import sys
import traceback

###############################################################################
# ── Third‑party imports ──────────────────────────────────────────────────────
###############################################################################
import pandas as pd  # required for cache I/O

###############################################################################
# ── Local (project) imports ─────────────────────────────────────────────────
###############################################################################
from . import fetch
from .compute_hsi import hsi

###############################################################################
# 1.  CLI parsing – every flag has a safe default so a first‑time user can
#     simply run “python -m src.main” and get a working demo.
###############################################################################
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pull Census data and calculate HSI/SVI scores."
    )

    # ── Census pull parameters ─────────────────────────────────────────────
    p.add_argument("--state", default="66", help="FIPS code of state/territory")
    p.add_argument("--year", type=int, default=2020, help="Decennial Census year")
    p.add_argument(
        "--geography",
        default="place",
        help="API geography keyword (place, county, tract, …)",
    )

    # ── File paths ─────────────────────────────────────────────────────────
    p.add_argument(
        "--config",
        type=Path,
        default=Path("configs/variables.csv"),
        help="CSV with headers: alias,dataset,variable",
    )
    p.add_argument(
        "--outfile",
        type=Path,
        default=Path("hsi_output.csv"),
        help="Destination CSV file",
    )
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("cache"),
        help="Directory that stores one CSV per dataset/year/state/geography",
    )

    # ── Optional token for higher API quotas ───────────────────────────────
    p.add_argument("--api-key", default=None, help="Optional Census API key")

    return p.parse_args()


###############################################################################
# 2.  Tiny helper – confirm we truly fetched every requested column
###############################################################################
def _assert_all_vars_present(df: pd.DataFrame, expected: set[str]) -> None:
    missing = expected.difference(df.columns)
    if missing:
        raise RuntimeError(f"missing {len(missing)} variables: {', '.join(sorted(missing))}")


###############################################################################
# 3.  Top‑level driver
###############################################################################
def main() -> None:
    args = _parse_args()
    args.cache_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ 3‑A. discover variables per dataset
    dataset_map = fetch.group_variable_codes_by_dataset(args.config)
    total_vars = sum(len(c) for c in dataset_map.values())
    print(f"Discovered {total_vars} variables across {len(dataset_map)} datasets")

    # ------------------------------------------------------------------ 3‑B. download each dataset (or use cache) and merge
    frames = []
    geokeys = fetch.geokeys_for(args.geography)

    for slug, codes in dataset_map.items():
        cache_file = (
            args.cache_dir
            / f"{args.year}_{args.state}_{args.geography}_{slug}.csv"
        )
        print(f"  • Processing dataset '{slug}' ({len(codes)} vars) …")

        try:
            # ---------- attempt live download
            df_ds = fetch.download_data(
                codes,
                state=args.state,
                year=args.year,
                product=slug,
                geography=args.geography,
                api_key=args.api_key,
            )
            _assert_all_vars_present(df_ds, set(codes))
            print("    ✓ fetched from API")
            # ---------- persist snapshot for future offline runs
            df_ds.to_csv(cache_file, index=False)
        except Exception as exc:
            print(f"    ⚠️  API unavailable or incomplete: {exc}")
            if cache_file.exists():
                print("      falling back to cached copy")
                df_ds = pd.read_csv(cache_file, low_memory=False)
                _assert_all_vars_present(df_ds, set(codes))
            else:
                traceback.print_exception(exc, file=sys.stderr)
                sys.exit(
                    f"Fatal: no cached copy for dataset '{slug}' and API fetch failed."
                )

        frames.append(df_ds)

    # ------------------------------------------------------------------ 3‑C. merge all dataset frames on geography keys
    df_raw = frames[0]
    for extra in frames[1:]:
        df_raw = df_raw.merge(extra, on=geokeys, how="left")

    print(f"Combined dataframe: {len(df_raw)} rows × {df_raw.shape[1]} columns")

    # ------------------------------------------------------------------ 3‑D. add alias / SPL_ / RPL_ fields
    df_enriched = hsi(df_raw, config_path=args.config)

    # ------------------------------------------------------------------ 3‑E. reorder columns (geo first, everything else after)
    other_cols = [col for col in df_enriched.columns if col not in geokeys]
    df_enriched = df_enriched[geokeys + other_cols]

    # ------------------------------------------------------------------ 3‑F. write final CSV
    df_enriched.to_csv(args.outfile, index=False)
    print(f"Saved results → {args.outfile}")


if __name__ == "__main__":
    main()
