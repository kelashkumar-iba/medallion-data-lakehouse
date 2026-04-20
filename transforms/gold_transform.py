"""
Gold transform: Silver -> 3 business marts -> MinIO gold/.

Marts produced:
  1. city_weekly_summary   — per (city, week): avg/min/max temp, total precip, rainy days
  2. regional_daily        — per day: regional avg, hottest/coldest city, total precip
  3. city_extremes         — per city all-time: hottest/coldest/wettest/windiest day

Each mart is a single Parquet file, overwritten on each run. No partitioning
— these are small consolidated tables optimized for dashboard queries.
"""

import io
import os
import sys
import logging
from datetime import datetime, timezone

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio


# ── Logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("gold_transform")


# ── MinIO client ──────────────────────────────────────────────────────────
def get_minio_client() -> Minio:
    endpoint = os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


# ── DuckDB connection primed to read Silver ───────────────────────────────
def get_duckdb() -> duckdb.DuckDBPyConnection:
    """Single DuckDB connection with S3 configured. Reused for all 3 mart queries."""
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

    # Register Silver as a view called `silver_weather` so every mart query
    # reads from the same source name — cleaner SQL, easier to reason about.
    silver_glob = f"s3://{bucket}/silver/weather/weather_date=*/data.parquet"
    con.execute(f"""
        CREATE OR REPLACE VIEW silver_weather AS
        SELECT * EXCLUDE (weather_date)
        FROM read_parquet('{silver_glob}', hive_partitioning=1)
    """)
    log.info("Silver view registered: %s", silver_glob)
    return con


# ── Mart 1: city_weekly_summary ───────────────────────────────────────────
# Teaches: DATE_TRUNC, conditional SUM CASE, classic GROUP BY aggregates
CITY_WEEKLY_SUMMARY_SQL = """
SELECT
    city,
    DATE_TRUNC('week', time)::DATE                            AS week_start_date,

    ROUND(AVG(temperature_2m_mean), 2)                        AS avg_temp,
    MIN(temperature_2m_min)                                   AS min_temp,
    MAX(temperature_2m_max)                                   AS max_temp,

    ROUND(AVG(wind_speed_10m_max), 2)                         AS avg_wind_max,

    ROUND(SUM(precipitation_sum), 2)                          AS total_precip,
    -- Conditional count: how many days that week had ANY rain
    SUM(CASE WHEN precipitation_sum > 0 THEN 1 ELSE 0 END)    AS days_with_rain,

    COUNT(*)                                                  AS days_in_window
FROM silver_weather
GROUP BY city, DATE_TRUNC('week', time)
ORDER BY city, week_start_date
"""


# ── Mart 2: regional_daily ────────────────────────────────────────────────
# Teaches: AVG() OVER, ROW_NUMBER() with ASC/DESC, pick-a-row-per-group trick
REGIONAL_DAILY_SQL = """
WITH ranked AS (
    SELECT
        time,
        city,
        temperature_2m_mean,
        precipitation_sum,
        -- Window aggregates: compute regional avg/total WITHOUT a GROUP BY,
        -- so we keep row-level detail for the hottest/coldest lookup below.
        AVG(temperature_2m_mean)  OVER (PARTITION BY time) AS regional_avg_temp,
        SUM(precipitation_sum)    OVER (PARTITION BY time) AS regional_total_precip,
        COUNT(*)                  OVER (PARTITION BY time) AS cities_reporting,
        -- Rank each city's mean temp within its date, hot first and cold first
        ROW_NUMBER() OVER (PARTITION BY time ORDER BY temperature_2m_mean DESC) AS hot_rank,
        ROW_NUMBER() OVER (PARTITION BY time ORDER BY temperature_2m_mean ASC)  AS cold_rank
    FROM silver_weather
)
SELECT
    time                                                       AS weather_date,
    ROUND(ANY_VALUE(regional_avg_temp), 2)                     AS regional_avg_temp,
    ROUND(ANY_VALUE(regional_total_precip), 2)                 AS regional_total_precip,
    ANY_VALUE(cities_reporting)                                AS cities_reporting,

    -- Classic trick: MAX(CASE WHEN rank = 1 ...) collapses one row per group
    MAX(CASE WHEN hot_rank  = 1 THEN city END)                 AS hottest_city,
    ROUND(MAX(CASE WHEN hot_rank  = 1 THEN temperature_2m_mean END), 2) AS hottest_temp,
    MAX(CASE WHEN cold_rank = 1 THEN city END)                 AS coldest_city,
    ROUND(MAX(CASE WHEN cold_rank = 1 THEN temperature_2m_mean END), 2) AS coldest_temp
FROM ranked
GROUP BY time
ORDER BY time
"""


# ── Mart 3: city_extremes ─────────────────────────────────────────────────
# Teaches: multi-window ranking + the same pick-a-row trick, but per city
CITY_EXTREMES_SQL = """
WITH ranked AS (
    SELECT
        city,
        time,
        temperature_2m_max,
        temperature_2m_min,
        precipitation_sum,
        wind_speed_10m_max,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY temperature_2m_max DESC) AS hot_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY temperature_2m_min ASC)  AS cold_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY precipitation_sum DESC)  AS wet_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY wind_speed_10m_max DESC) AS wind_rank
    FROM silver_weather
)
SELECT
    city,
    MAX(CASE WHEN hot_rank  = 1 THEN time END)                 AS hottest_day,
    MAX(CASE WHEN hot_rank  = 1 THEN temperature_2m_max END)   AS hottest_temp,
    MAX(CASE WHEN cold_rank = 1 THEN time END)                 AS coldest_day,
    MAX(CASE WHEN cold_rank = 1 THEN temperature_2m_min END)   AS coldest_temp,
    MAX(CASE WHEN wet_rank  = 1 THEN time END)                 AS wettest_day,
    MAX(CASE WHEN wet_rank  = 1 THEN precipitation_sum END)    AS wettest_precip,
    MAX(CASE WHEN wind_rank = 1 THEN time END)                 AS windiest_day,
    MAX(CASE WHEN wind_rank = 1 THEN wind_speed_10m_max END)   AS windiest_wind
FROM ranked
GROUP BY city
ORDER BY city
"""


MARTS = {
    "city_weekly_summary": CITY_WEEKLY_SUMMARY_SQL,
    "regional_daily":      REGIONAL_DAILY_SQL,
    "city_extremes":       CITY_EXTREMES_SQL,
}


# ── Parquet write helper (same pattern as Silver) ─────────────────────────
def write_parquet_to_minio(df: pd.DataFrame, client: Minio, object_name: str) -> None:
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)
    size = buffer.getbuffer().nbytes
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=buffer,
        length=size,
        content_type="application/octet-stream",
    )
    log.info("Wrote s3://%s/%s (%d rows, %d bytes)", bucket, object_name, len(df), size)


# ── Main ──────────────────────────────────────────────────────────────────
def main() -> int:
    client = get_minio_client()
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    if not client.bucket_exists(bucket):
        log.error("Bucket '%s' does not exist.", bucket)
        return 1

    con = get_duckdb()

    # Run each mart query, write the result as Parquet to gold/<name>/data.parquet
    for mart_name, sql in MARTS.items():
        log.info("Building mart: %s", mart_name)
        df = con.execute(sql).fetchdf()
        log.info("  -> %d rows, %d columns", len(df), len(df.columns))
        write_parquet_to_minio(df, client, f"gold/{mart_name}/data.parquet")

    log.info("Gold transform complete: %d marts written", len(MARTS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
