**Guam HSI (Hazard Susceptibility Index) Calculator**

The Hazard Susceptibility Index (HSI) was created for the Pacific Region to document and display areas within islands and territories that, by the nature of their demographic makeup, infrastructure sitings, or topographical features, may require extra resources or earlier messaging in the event of hazardous weather conditions. 
The Center for Disease Control’s [Social Vulnerability Index (CDC SVI)](https://www.atsdr.cdc.gov/place-health/php/svi) for the continental United States, upon which the HSI is based, does not extend to many of the islands and territories in the Pacific Region, motivating the creation of this program.
A number of the variables used in the calculation of the HSI have been expressly selected to be reflective of the demographic makeup of our first test case: the island of Guam.

Expanding on the work of [Paulino et. al. (2021)](https://hazards.colorado.edu/public-health-disaster-research/calculating-the-social-vulnerability-index-for-guam), the HSI incorporates spatial layers representing critical infrastructure locations and topographic features in addition to demographic and socioeconomic layers. 

---

## 1 Project Purpose

The calculator pulls **Decennial Census** variables (2020 by default) and computes
**alias fields** to mirror the CDC/ATSDR Social‑Vulnerability Index (SVI).  
Because every transformation is driven by a CSV, **you can swap‑in a different
variable list** (e.g. Guam, US States) **without touching the Python code**.



---

## 2 Directory layout

```
project‑root/
│
├── configs/
│   └── variables.csv     ← One row per alias.  THIS drives all calculations.
│
├── src/
│   ├── fetch.py          ← Generic Census‑API downloader.
│   ├── compute_hsi.py    ← Adds Aliases and computes values based on calculations.
│   └── main.py           ← Command‑line driver & offline‑cache manager.
│
├── cache/                ← Auto‑created.  Holds raw CSV snapshots per dataset.
└── hsi_output.csv        ← Example final output (path is user‑selectable).
```

---

## 3 Quick‑start (30‑second demo)

```bash
python -m venv venv         # create isolated environment (optional)
source venv/bin/activate    # on Windows:source venv\Scripts\Activate
pip install -r requirements.txt   # pandas, requests, numpy only

python -m src.main --state 66 --year 2020 --geography place --outfile output/hsi_output.csv

python src/join_csv_to_shapefile.py cache/tl_2020_66_place.zip output/hsi_output.csv PLACEFP place --output output/hsi_output.shp

```

*First run* downloads data from the Census API and writes a copy under
`cache/…csv`. If the API is unreachable on later runs the program it
**re‑uses the cached copy automatically**.

---

## 4 Configuration – `variables.csv`

| Column         | Purpose                                                                           | Example                                    |
|----------------|-----------------------------------------------------------------------------------|--------------------------------------------|
| **alias**      | Short, human‑friendly name created in the output                                  | `EP_POV150`                                |
| **dataset**    | Census product slug (matches API URL segment)                                     | `dpgu` (2020 Guam Data Profile)            |
| **variable**   | ‑ A raw Census code **or**<br>‑ Any arithmetic *expression* referencing raw codes | `(E_POV150 / S1701_C01_001E) * 100`        |

*No limits*: use `+ – * / ( )`, and `**` for powers (e.g **0.5 for square root)

To reference a previously declared variables use: **df['{previously declared variable}']** and replace '{previously declared variable}' with the previously declared variable of your choosing.

To calculate ranks use: **df['{previously declared variable}'].rank(pct=True).round(4)**.

---

## 5 Program workflow in depth

### 5.1 `main.py` – orchestration & offline cache

| Step | Code fragment | What happens & why                                                                                                                                                                                          |
|------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | `_parse_args()` | Reads CLI flags; every flag has a **sensible default** so beginners can run the script “as‑is”.                                                                                                             |
| 2 | `fetch.group_variable_codes_by_dataset()` | Scans *variables.csv* to build `{dataset → [raw codes…]}`. This works for any CSV – no hard‑coding.                                                                                                         |
| 3 | Loop over datasets | For each slug:<br>• try live download via `fetch.download_data()`<br>• on failure **fall back to `cache/`** if a snapshot exists.<br>• verify every requested code is present (`_assert_all_vars_present`). |
| 4 | Merge frames | A left‑join on the geography keys (`state`, `place`, …) produces one wide frame `df_raw`.                                                                                                                   |
| 5 | `hsi()` | Delegates to *compute_hsi.py* to add Alias.                                                                                                                                            |
| 6 | Column reorder | Geography keys first → tidy output.                                                                                                                                                                         |
| 7 | `to_csv(args.outfile)` | Final flat‑file output for ArcGIS/QGIS.                                                                                                                                                                     |

### 5.2 `fetch.py` – Census API downloader

1. **Regex discovery** (`VAR_RE`)  
   Strict pattern ensures only valid Census tokens are captured from free‑form expressions.

2. **Bucket by dataset** (`group_variable_codes_by_dataset`)  
   Ensures that each API call hits *exactly one* product root
   (`…/dec/dpgu` vs `…/dec/sdgu`, etc.).

3. **Chunking** (`CHUNK_SIZE = 50`)  
   Census limits “get=…” to 50 variables. The code slices long lists and
   merges partial DataFrames.

4. **Geography helper** (`geokeys_for`)  
   Maintains the composite primary key (`state + county + tract`, etc.).
   Future geographies can be added in one lookup table.

5. **Data cleaning**  
   Numeric coercion converts `"1234"` → `1234.0`; sentinel
   ‑888888888/‑999999999 are mapped to `NaN` so downstream calculations are safe.

### 5.3 `compute_hsi.py` – from raw numbers to percentile ranks

| Phase | Function | Detail |
|-------|----------|--------|
| 1 | `_load_alias_map` | Reads *variables.csv* into `{alias → expression}`. |
| 2 | `_evaluate_aliases` | • If expression is a single token → *fast copy*.<br>• Else, wraps every token as `df['TOKEN']` and calls **`pandas.eval(engine='python')`** – arithmetic only, *no arbitrary code execution*.<br>• Errors (divide‑by‑zero, missing column) yield `NaN` so the pipeline never aborts mid‑run. |
| 3 | `hsi()` | Public entry point used by `main.py`. Returns a **new** DataFrame (original untouched). |

---

## 6 Extending / Modifying the Calculator

| Task | How‑to |
|------|--------|
| **Add a new variable** | Append a row to *variables.csv* with the correct `dataset` slug and either the raw code *or* an expression. |
| **Switch to a different territory or year** | CLI flags: `--state`, `--year`, `--geography`. |
| **Support new geographies** | Add an entry in `fetch.geokeys_for()` and pass the keyword on the CLI. |
| **Increase API rate limits** | Supply `--api-key <your‑Census‑key>` (free registration). |

---

## 7 Error handling & offline resilience

* **Network / API outage** – Any HTTP error triggers a fallback to the cached
  CSV so workflows continue uninterrupted.
* **Missing variables** – The script halts with a clear “missing X variables”
  message, pointing you to gaps in *variables.csv*.
* **Expression errors** – Problematic expressions resolve to `NaN`; the rest of
  the pipeline (percentiles, sums) still executes.
  
---
## 8 Mapping the data
To map the data, a places shapefile is needed.
A places shapefile for the state you are working with can be downloaded from https://www.census.gov/cgi-bin/geo/shapefiles/index.php?year=2020&layergroup=Places
This file has been downloaded for Guam and can be found in 'cache/tl_2020_66_place.zip'

With a places shape file you can call the following script to join the computed HSI values with the shapefile. 
This script will also remove any columns that start with "DP" that were downloaded earlier using the API. 
```bash
python src/join_csv_to_shapefile.py cache/tl_2020_66_place.zip output/hsi_output.csv PLACEFP place --output output/hsi_output.shp
```
Replacing the following parameters as appropriate:
* cache/tl_2020_66_place.zip: The path to the zipped or unzipped shapefile
* cache/2020_66_place_dpgu.csv: The path to the generated CSV file
* PLACEFP: The column name to be joined from the shapefile
* place: The column name to be joined from the CSV file
* --output cache/joined_places.shp: The output file to be created
* --remove_data (optional): Removes any columns that start with letters the "DP" but can be changed to any starting letters you'd like to exclude from the output.
---

## 10 Appendix A – Key regular expressions

* **`VAR_RE`** `r"\b[A-Z]{1,4}\d{0,3}_[0-9]{4}[A-Z]?\b"`  
  Captures *only* well‑formed Census codes, avoiding false matches like
  `DP1_0001C_extra`.

* **`TOKEN_RE`** `r"[A-Za-z0-9_]+"` changed to `\b[A-Z]{1,4}\d{0,3}_[0-9]{4}[A-Z]?\b`
  Finds tokens inside an alias expression so they can be wrapped as
  `df["TOKEN"]` for safe evaluation.

---

## 11 Appendix B – Sentinel values

| Value | Meaning | Action taken |
|-------|---------|--------------|
| ‑888 888 888 | “The estimate or margin of error cannot be displayed because there were an insufficient number of sample cases in the selected geographic area.” | Replaced with `NaN` |
| ‑999 999 999 | “The estimate or margin of error is not applicable or not available for the requested variable.” | Replaced with `NaN` |

---

## 12 Appendix C – CLI reference (all flags)

| Flag | Default | Description |
|------|---------|-------------|
| `--state` | `66` | FIPS code (two digits) – 66 = Guam |
| `--year` | `2020` | Decennial Census year |
| `--geography` | `place` | API keyword: `state`, `county`, `tract`, `place`, … |
| `--config` | `configs/variables.csv` | Path to the alias/variable mapping CSV |
| `--outfile` | `hsi_output.csv` | Destination CSV |
| `--cache-dir` | `cache` | Directory for raw dataset snapshots |
| `--api-key` | *None* | Optional Census key for higher rate limits |

## Credits and Acknowledgments

Anthony Berardi, Colorado State University

Kevin Worthington, Colorado State University

Jarrod Loerzel, NOAA

Liz Batty, NOAA

---
## References

OpenAI. (2025). ChatGPT (May 7th version) [Large language model]. https://chat.openai.com/chat

---