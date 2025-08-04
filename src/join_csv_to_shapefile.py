import geopandas as gpd
import pandas as pd

def join_csv_to_shapefile(shapefile_path, csv_path, shapefile_key, csv_key, output_path,remove_data):
    """
    Joins a CSV file to a shapefile based on a common key and writes the result to a new shapefile.

    Parameters:
        shapefile_path (str): Path to the input shapefile (.shp).
        csv_path (str): Path to the input CSV file.
        shapefile_key (str): Column name in the shapefile to join on.
        csv_key (str): Column name in the CSV file to join on.
        output_path (str): Path to save the output shapefile (.shp).
    """
    print(f"Loading shapefile: {shapefile_path}")
    gdf = gpd.read_file(shapefile_path , dtype={shapefile_key: str})

    print(f"Loading CSV file: {csv_path}")
    df = pd.read_csv(csv_path, dtype={csv_key: str})

    # Create a boolean array that start with the match
    cols_to_drop = df.columns[df.columns.str.startswith(remove_data)]

    # Drop the columns containing the remove_data
    df.drop(cols_to_drop, axis=1, inplace=True)

    if shapefile_key not in gdf.columns:
        raise KeyError(f"Column '{shapefile_key}' not found in shapefile.")
    if csv_key not in df.columns:
        raise KeyError(f"Column '{csv_key}' not found in CSV file.")


    print(f"Joining on shapefile column '{shapefile_key}' and CSV column '{csv_key}'")
    merged_gdf = gdf.merge(df, left_on=shapefile_key, right_on=csv_key, how='left')
    print(merged_gdf)

    print(f"Saving output to: {output_path}")
    merged_gdf.to_file(output_path)
    print("Join and export completed successfully.")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Join a CSV file to a shapefile based on common keys.")
    parser.add_argument("shapefile", help="Path to the input shapefile (.shp)")
    parser.add_argument("csv", help="Path to the input CSV file")
    parser.add_argument("shapefile_key", help="Column name in the shapefile to join on")
    parser.add_argument("csv_key", help="Column name in the CSV file to join on")
    parser.add_argument(
        "--output", "-o",
        default="joined_output.shp",
        help="Path to save the output shapefile (.shp). Default is 'joined_output.shp'"
    )
    parser.add_argument(
        "--remove_data", "-r",
        default="DP",
        help="Remove any columns that start with letters"
    )

    args = parser.parse_args()

    join_csv_to_shapefile(
        shapefile_path=args.shapefile,
        csv_path=args.csv,
        shapefile_key=args.shapefile_key,
        csv_key=args.csv_key,
        output_path=args.output,
        remove_data = args.remove_data
    )

if __name__ == "__main__":
    main()
