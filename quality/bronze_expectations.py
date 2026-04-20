"""
Bronze quality gate.

Reads the Parquet for a given ingest_date from MinIO, runs Great Expectations,
exits 0 on all-pass, 1 on any-fail. Airflow uses that exit code to block Silver.
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timezone

import duckdb                      # Reads Parquet from MinIO via S3 URL
import great_expectations as ge    # Quality framework


# ── Logging: stdout so Airflow captures it in the task log ────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bronze_expectations")


# ── Config ────────────────────────────────────────────────────────────────
# Cities we expect in Bronze. Update here if CITIES in bronze_ingest.py changes.
EXPECTED_CITIES = {"Karachi", "Lahore", "Islamabad", "Mumbai", "Dubai"}

# Columns Silver will depend on. If any are missing, Silver breaks.
REQUIRED_COLUMNS = {
    "time",
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "city",
    "latitude",
    "longitude",
    "_source_url",
    "_ingested_at",
}


# ── DuckDB loader ─────────────────────────────────────────────────────────
def load_bronze_parquet(ingest_date: str) -> "pd.DataFrame":
    """Read one day's bronze partition from MinIO into a pandas DataFrame."""
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    # Strip scheme from MINIO_ENDPOINT — DuckDB wants host:port only
    endpoint = os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{endpoint}';
        SET s3_access_key_id='{os.environ["MINIO_ACCESS_KEY"]}';
        SET s3_secret_access_key='{os.environ["MINIO_SECRET_KEY"]}';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)

    s3_path = f"s3://{bucket}/bronze/weather/ingest_date={ingest_date}/data.parquet"
    log.info("Reading %s", s3_path)
    df = con.execute(f"SELECT * FROM '{s3_path}'").fetchdf()
    log.info("Loaded %d rows, %d columns", len(df), len(df.columns))
    return df


# ── Expectation runner ────────────────────────────────────────────────────
def run_expectations(df) -> list:
    """Wrap df with GE, run every expectation, return list of results."""
    gdf = ge.from_pandas(df)         # Turns a DataFrame into a GE Dataset

    results = []

    # Row count — 5 cities x 7 days = 35, allow 25-50 for partial windows
    results.append(("row_count_between_25_and_50",
        gdf.expect_table_row_count_to_be_between(min_value=25, max_value=50)))

    # Schema — all required columns present
    results.append(("columns_to_match_set",
        gdf.expect_table_columns_to_match_set(column_set=list(REQUIRED_COLUMNS), exact_match=False)))

    # city — not null and only the cities we expect
    results.append(("city_not_null",
        gdf.expect_column_values_to_not_be_null("city")))
    results.append(("city_in_expected_set",
        gdf.expect_column_values_to_be_in_set("city", list(EXPECTED_CITIES))))

    # time — not null, ISO date format
    results.append(("time_not_null",
        gdf.expect_column_values_to_not_be_null("time")))
    results.append(("time_iso_date_format",
        gdf.expect_column_values_to_match_regex("time", r"^\d{4}-\d{2}-\d{2}$")))

    # Temperature sanity bounds (Celsius). Earth's record range ~ -90 to 60.
    results.append(("temp_mean_in_sane_range",
        gdf.expect_column_values_to_be_between("temperature_2m_mean", min_value=-50, max_value=60)))
    results.append(("temp_min_le_max",
        gdf.expect_column_pair_values_A_to_be_greater_than_B(
            column_A="temperature_2m_max",
            column_B="temperature_2m_min",
            or_equal=True,
        )))

    # Wind speed — non-negative and below tornado scale
    results.append(("wind_speed_non_negative",
        gdf.expect_column_values_to_be_between("wind_speed_10m_max", min_value=0, max_value=200)))

    # Precipitation — non-negative (never negative rain)
    results.append(("precip_non_negative",
        gdf.expect_column_values_to_be_between("precipitation_sum", min_value=0, max_value=6000)))

    # Metadata columns — sanity checks
    results.append(("ingested_at_not_null",
        gdf.expect_column_values_to_not_be_null("_ingested_at")))

    return results


# ── Main ──────────────────────────────────────────────────────────────────
def main(ingest_date: str) -> int:
    df = load_bronze_parquet(ingest_date)
    results = run_expectations(df)

    passed = 0
    failed = 0
    for name, result in results:
        if result.success:
            log.info("PASS  %s", name)
            passed += 1
        else:
            log.error("FAIL  %s  -> %s", name, result.result)
            failed += 1

    log.info("Summary: %d passed, %d failed (of %d)", passed, failed, len(results))

    if failed > 0:
        log.error("Bronze quality gate FAILED for ingest_date=%s. Silver will not run.", ingest_date)
        return 1

    log.info("Bronze quality gate PASSED for ingest_date=%s. Silver can proceed.", ingest_date)
    return 0


# ── CLI entrypoint ────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze GE gate")
    today = datetime.now(timezone.utc).date()
    parser.add_argument("--ingest-date", default=str(today))
    args = parser.parse_args()

    sys.exit(main(args.ingest_date))
