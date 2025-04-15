#!/usr/bin/env python
import argparse
import requests
import pandas as pd

# ---------------------------
# Set up argparse for flexibility
# ---------------------------
parser = argparse.ArgumentParser(
    description="Compute a proof‐of‐concept SVI from ACS data for a specified state."
)
# Use --state to specify the state FIPS code (default = 08 for Colorado)
parser.add_argument("-s", "--state", type=str, default="08",
                    help="State FIPS code (default: 08 for Colorado)")
# An optional output file name to save the CSV
parser.add_argument("-o", "--outfile", type=str, default="colorado_svi.csv",
                    help="Output CSV file name (optional)")

# Parse arguments
args = parser.parse_args()

# ---------------------------
# Define the Census API query parameters
# ---------------------------
# We use the following variables:
variables = [
    "B17001_001E", "B17001_002E",  # Poverty
    "B01001_001E",                # Total age population
    "B01001_020E", "B01001_021E", "B01001_022E", "B01001_023E", "B01001_024E", "B01001_025E",  # Males 65+
    "B01001_044E", "B01001_045E", "B01001_046E", "B01001_047E", "B01001_048E", "B01001_049E",  # Females 65+
    "B03002_001E", "B03002_003E"   # Race
]

params = {
    "get": ",".join(variables),
    "for": "tract:*",
    "in": f"state:{args.state} county:*"
}

# Base URL for the 2020 ACS 5-Year dataset
BASE_URL = "https://api.census.gov/data/2020/acs/acs5"

print("Querying Census API for state FIPS:", args.state)
response = requests.get(BASE_URL, params=params)
if response.status_code != 200:
    print("Status Code:", response.status_code)
    print("Response Text:", response.text)
    response.raise_for_status()

try:
    data = response.json()
except Exception as e:
    print("Error decoding JSON. Response text:")
    print(response.text)
    raise e

# ---------------------------
# Create DataFrame from API response
# ---------------------------
df = pd.DataFrame(data[1:], columns=data[0])

# Convert the expected numeric columns to numbers
numeric_columns = [
    "B17001_001E", "B17001_002E", "B01001_001E",
    "B01001_020E", "B01001_021E", "B01001_022E", "B01001_023E", "B01001_024E", "B01001_025E",
    "B01001_044E", "B01001_045E", "B01001_046E", "B01001_047E", "B01001_048E", "B01001_049E",
    "B03002_001E", "B03002_003E"
]
for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# ---------------------------
# Calculate the Rates and SVI
# ---------------------------
df["poverty_rate"] = df["B17001_002E"] / df["B17001_001E"]

male_elderly = ["B01001_020E", "B01001_021E", "B01001_022E", "B01001_023E", "B01001_024E", "B01001_025E"]
female_elderly = ["B01001_044E", "B01001_045E", "B01001_046E", "B01001_047E", "B01001_048E", "B01001_049E"]
df["elderly_total"] = df[male_elderly + female_elderly].sum(axis=1)
df["elderly_rate"] = df["elderly_total"] / df["B01001_001E"]

df["minority_rate"] = 1 - (df["B03002_003E"] / df["B03002_001E"])

# For the proof-of-concept SVI, take the average of the three rates
df["SVI"] = df[["poverty_rate", "elderly_rate", "minority_rate"]].mean(axis=1)

# Optional: Convert these to percentages
df["poverty_rate_pct"] = df["poverty_rate"] * 100
df["elderly_rate_pct"] = df["elderly_rate"] * 100
df["minority_rate_pct"] = df["minority_rate"] * 100
df["SVI_pct"] = df["SVI"] * 100

# ---------------------------
# Reorder Columns in the Output CSV
# ---------------------------
# We want the primary geographic identifiers first. For this example, we assume that
# 'state', 'county', and 'tract' (and if available, 'NAME') are our key identifiers.
geo_cols = [col for col in df.columns if col in ['NAME', 'state', 'county', 'tract']]
# Define the list of computed SVI columns
computed_cols = [
    "poverty_rate", "elderly_total", "elderly_rate", "minority_rate",
    "SVI", "poverty_rate_pct", "elderly_rate_pct", "minority_rate_pct", "SVI_pct"
]
# All remaining columns from the API that are not in the above lists
other_cols = [col for col in df.columns if col not in geo_cols + computed_cols]
# New column order: key geographic identifiers, then the raw census variables, then computed columns.
new_order = geo_cols + other_cols + computed_cols
df = df[new_order]

# ---------------------------
# Output the results
# ---------------------------
print("Preview of the computed SVI for the first few census tracts:")
print(df.head())

if args.outfile:
    df.to_csv(args.outfile, index=False)
    print(f"Output written to {args.outfile}")
else:
    # If no outfile is specified, print the CSV text to stdout
    print(df.to_csv(index=False))
