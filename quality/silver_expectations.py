"""
Silver quality gate.

Reads ALL Silver partitions from MinIO, runs stricter GE suite than Bronze,
exits 0 on all-pass, 1 on any-fail. Airflow uses that exit code to block Gold.
"""

import os
import sys
import logging
from datetime import date as _date_cls, datetime, timezone

import duckdb
import pandas as pd
import great_expectations as ge


# ── Logging: stdout so Airflow captures it in task logs ───────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("silver_expectations")


# ── Shared helper for custom (non-GE) expectation results ─────────────────
class _Result:
    """Duck-types a GE ValidationResult so our manual checks fit the same loop."""
    def __init__(self, success, detail):
        self.success = success
        self.result = detail


# ── Config ────────────────────────────────────────────────────────────────
EXPECTED_CITIES = {"Karachi", "Lahore", "Islamabad", "Mumbai", "Dubai"}

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
    "_ingested_at",
}

NON_NULL_METRICS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
]


# ── DuckDB loader — read ALL Silver partitions via Hive glob ──────────────
def load_silver_dataframe() -> pd.DataFrame:
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
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

    glob_path = f"s3://{bucket}/silver/weather/weather_date=*/data.parquet"
    log.info("Reading all Silver partitions: %s", glob_path)

    df = con.execute(f"""
        SELECT * EXCLUDE (weather_date)
        FROM read_parquet('{glob_path}', hive_partitioning=1)
    """).fetchdf()

    log.info("Loaded %d rows, %d columns", len(df), len(df.columns))
    return df


# ── Expectation runner ────────────────────────────────────────────────────
def run_expectations(df: pd.DataFrame) -> list:
    gdf = ge.from_pandas(df)
    results = []

    # Shape
    results.append(("row_count_between_25_and_100",
        gdf.expect_table_row_count_to_be_between(min_value=25, max_value=100)))
    results.append(("columns_to_match_set",
        gdf.expect_table_columns_to_match_set(column_set=list(REQUIRED_COLUMNS), exact_match=False)))

    # Compound uniqueness — dedup correctness
    results.append(("city_time_pair_unique",
        gdf.expect_compound_columns_to_be_unique(column_list=["city", "time"])))

    # No nulls
    for col in NON_NULL_METRICS + ["city", "time", "latitude", "longitude", "_ingested_at"]:
        results.append((f"{col}_not_null",
            gdf.expect_column_values_to_not_be_null(col)))

    # Type contract: time must be a datetime.date (custom check — GE strftime only works on strings)
    all_are_dates = df["time"].apply(lambda x: isinstance(x, _date_cls)).all()
    if all_are_dates:
        results.append(("time_is_date_type", _Result(True, {"checked_rows": len(df)})))
    else:
        bad_types = df["time"].apply(lambda x: type(x).__name__).value_counts().to_dict()
        results.append(("time_is_date_type",
            _Result(False, {"type_distribution": bad_types})))

    # Value ranges
    results.append(("temp_mean_in_range",
        gdf.expect_column_values_to_be_between("temperature_2m_mean", min_value=-50, max_value=60)))
    results.append(("temp_min_le_max",
        gdf.expect_column_pair_values_A_to_be_greater_than_B(
            column_A="temperature_2m_max",
            column_B="temperature_2m_min",
            or_equal=True,
        )))
    results.append(("wind_speed_in_range",
        gdf.expect_column_values_to_be_between("wind_speed_10m_max", min_value=0, max_value=150)))
    results.append(("precip_in_range",
        gdf.expect_column_values_to_be_between("precipitation_sum", min_value=0, max_value=500)))

    # City enum
    results.append(("city_in_expected_set",
        gdf.expect_column_values_to_be_in_set("city", list(EXPECTED_CITIES))))

    # Completeness: every date has all 5 cities (custom)
    counts = df.groupby("time")["city"].nunique()
    incomplete_dates = counts[counts != 5]
    if len(incomplete_dates) == 0:
        results.append(("every_date_has_all_5_cities", _Result(True, {"dates_checked": len(counts)})))
    else:
        results.append(("every_date_has_all_5_cities",
            _Result(False, {"incomplete_dates": incomplete_dates.to_dict()})))

    return results


# ── Main ──────────────────────────────────────────────────────────────────
def main() -> int:
    df = load_silver_dataframe()
    results = run_expectations(df)

    passed = failed = 0
    for name, result in results:
        if result.success:
            log.info("PASS  %s", name)
            passed += 1
        else:
            log.error("FAIL  %s  -> %s", name, result.result)
            failed += 1

    log.info("Summary: %d passed, %d failed (of %d)", passed, failed, len(results))

    if failed > 0:
        log.error("Silver quality gate FAILED. Gold will not run.")
        return 1

    log.info("Silver quality gate PASSED. Gold can proceed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
