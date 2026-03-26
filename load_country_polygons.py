import json
import os
import tempfile
import requests
from google.cloud import bigquery

PROJECT  = "flights-490708"
DATASET  = "flight_data"
TABLE    = "country_polygons"
BQ_TABLE = f"{PROJECT}.{DATASET}.{TABLE}"

GEOJSON_URL = (
    "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
)

NAME_OVERRIDES = {
    "United States of America":         "United States",
    "Czechia":                           "Czech Republic",
    "Democratic Republic of the Congo":  "Congo (Kinshasa)",
    "Republic of the Congo":             "Congo (Brazzaville)",
    "Ivory Coast":                       "Ivory Coast",
    "eSwatini":                          "Swaziland",
    "Macedonia":                         "Macedonia",
    "Palestine":                         "Palestine",
    "Taiwan":                            "Taiwan",
    "Kosovo":                            "Kosovo",
}

# Custom region aliases that don't exist in Natural Earth
# Maps custom region name -> real country name in country_borders
REGION_ALIASES = [
    ("Russia (Western)",                           "Russia"),
    ("Pakistan (Northwest/Baluchistan)",            "Pakistan"),
    ("Democratic Republic of the Congo (Eastern)", "Congo (Kinshasa)"),
]


def download_geojson():
    print("Downloading country polygons from Natural Earth...")
    r = requests.get(GEOJSON_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    print(f"  Downloaded {len(data['features'])} features")
    return data


def build_rows(geojson):
    rows = []
    for feature in geojson["features"]:
        props = feature.get("properties", {})
        name  = props.get("ADMIN") or props.get("name", "")
        iso   = props.get("ISO_A3", "")
        geom  = feature.get("geometry")
        if not name or not geom:
            continue
        rows.append({
            "country_name":       NAME_OVERRIDES.get(name, name),
            "natural_earth_name": name,
            "iso_a3":             iso,
            "geometry_geojson":   json.dumps(geom),
        })
    print(f"  Built {len(rows)} rows")
    return rows


def load_to_bigquery(rows):
    client = bigquery.Client(project=PROJECT)

    schema = [
        bigquery.SchemaField("country_name",       "STRING", mode="REQUIRED"),
        bigquery.SchemaField("natural_earth_name",  "STRING", mode="NULLABLE"),
        bigquery.SchemaField("iso_a3",              "STRING", mode="NULLABLE"),
        bigquery.SchemaField("geometry_geojson",    "STRING", mode="REQUIRED"),
    ]

    table_ref = bigquery.Table(BQ_TABLE, schema=schema)
    client.delete_table(BQ_TABLE, not_found_ok=True)
    client.create_table(table_ref)
    print(f"  Created table {BQ_TABLE}")

    # Write rows to temp NDJSON file and load via batch job
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for row in rows:
            f.write(json.dumps(row) + '\n')
        tmp_path = f.name

    print(f"  Loading {len(rows)} rows via batch job...")
    with open(tmp_path, 'rb') as f:
        job = client.load_table_from_file(f, BQ_TABLE, job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
        ))
    job.result()
    os.unlink(tmp_path)
    print(f"  Loaded {client.get_table(BQ_TABLE).num_rows} rows")

    # Create geography table from raw polygons
    geo_table = f"{PROJECT}.{DATASET}.country_borders"
    print(f"  Creating country_borders geography table...")
    client.query(f"""
        CREATE OR REPLACE TABLE `{geo_table}` AS
        SELECT country_name, natural_earth_name, iso_a3,
            ST_GEOGFROMGEOJSON(geometry_geojson, make_valid => TRUE) AS border
        FROM `{BQ_TABLE}`
    """).result()
    print(f"  country_borders has {client.get_table(geo_table).num_rows} rows")

    # Add custom region aliases
    print("  Adding custom region aliases...")
    for alias_name, source_name in REGION_ALIASES:
        client.query(f"""
            INSERT INTO `{geo_table}`
            SELECT '{alias_name}', natural_earth_name, iso_a3, border
            FROM `{geo_table}`
            WHERE country_name = '{source_name}'
        """).result()
        print(f"    Added alias: {alias_name} -> {source_name}")

    print(f"  Final country_borders count: {client.get_table(geo_table).num_rows} rows")
    print("\nDone!")


if __name__ == "__main__":
    geojson = download_geojson()
    rows    = build_rows(geojson)
    load_to_bigquery(rows)