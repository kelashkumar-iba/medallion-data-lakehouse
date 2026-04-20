"""
Silver transform: all Bronze partitions -> dedup + type-cast -> Silver Parquet.

Rules:
  1. Read every Bronze Parquet via glob. Silver owns the consolidated view.
  2. Dedupe on (city, time) keeping the latest _ingested_at. SQL window function.
  3. Cast time string -> DATE. Keep weather metrics as doubles.
  4. Drop _source_url (Bronze audit only). Keep _ingested_at (lineage).
  5. Partition Silver by weather_date, not ingest_date.
  6. Idempotent: rerunning always overwrites affected partitions.
"""

import io
import os
import sys
import argparse
import logging
from datetime import datetime, timezone

import duckdb                      # SQL engine — reads Bronze, dedups via window function
import pandas as pd                # DataFrame handling + per-partition write loop
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio


# ── Logging: stdout so Airflow captures it in task logs ───────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("silver_transform")


# ── MinIO client factory ──────────────────────────────────────────────────
def get_minio_client() -> Minio:
    """Build MinIO client from env vars — same pattern as bronze_ingest."""
    endpoint = os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


# ── DuckDB: read ALL Bronze partitions + dedup in one query ───────────────
def read_and_dedup_bronze() -> pd.DataFrame:
    """Glob all Bronze Parquet files, dedup on (city, time), return a DataFrame."""
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

    # Glob pattern: match every Bronze partition's data.parquet at once.
    # DuckDB's httpfs extension handles the S3 LIST + read transparently.
    glob_path = f"s3://{bucket}/bronze/weather/ingest_date=*/data.parquet"
    log.info("Reading all Bronze partitions: %s", glob_path)

    # Window-function dedup:
    #   - PARTITION BY (city, time) groups rows per (city, day)
    #   - ORDER BY _ingested_at DESC puts the newest first in each group
    #   - ROW_NUMBER() labels them 1, 2, 3...
    #   - Outer WHERE rn = 1 keeps only the freshest row per key
    dedup_sql = f"""
    WITH ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY city, time
                ORDER BY _ingested_at DESC
            ) AS rn
        FROM read_parquet('{glob_path}')
    )
    SELECT * EXCLUDE (rn, _source_url)   -- drop the rank column + Bronze audit col
    FROM ranked
    WHERE rn = 1
    ORDER BY time, city
    """

    df = con.execute(dedup_sql).fetchdf()
    log.info("After dedup: %d rows, %d columns", len(df), len(df.columns))
    return df


# ── Type casting ──────────────────────────────────────────────────────────
def apply_silver_types(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce Silver's typed contract."""
    # time is a string like '2026-04-11' in Bronze; Silver needs DATE.
    df["time"] = pd.to_datetime(df["time"], errors="raise").dt.date

    # Enforce float64 on all weather metrics — safe cast, raises on bad values.
    numeric_cols = [
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "precipitation_sum",
        "wind_speed_10m_max",
        "latitude",
        "longitude",
    ]
    for col in numeric_cols:
        df[col] = df[col].astype("float64")

    # _ingested_at stays as ISO string — next layer (Gold) won't need it typed
    return df


# ── Write per-partition (one Parquet per weather_date) ────────────────────
def write_silver_partitions(df: pd.DataFrame, client: Minio) -> int:
    """For each unique weather_date, write a single Parquet file to MinIO."""
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    partitions_written = 0

    # groupby the natural business key — one partition per day
    for weather_date, group_df in df.groupby("time"):
        # Serialize group to Parquet in memory
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(group_df, preserve_index=False)
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)
        size = buffer.getbuffer().nbytes

        # Hive-style partition path — DuckDB, Spark, Trino all understand it
        object_name = f"silver/weather/weather_date={weather_date}/data.parquet"

        client.put_object(
            bucket_name=bucket,
            object_name=object_name,
            data=buffer,
            length=size,
            content_type="application/octet-stream",
        )
        log.info("Wrote s3://%s/%s (%d rows, %d bytes)", bucket, object_name, len(group_df), size)
        partitions_written += 1

    return partitions_written


# ── Main ──────────────────────────────────────────────────────────────────
def main() -> int:
    client = get_minio_client()
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    if not client.bucket_exists(bucket):
        log.error("Bucket '%s' does not exist.", bucket)
        return 1

    # 1. Read + dedup via DuckDB window function
    df = read_and_dedup_bronze()

    if df.empty:
        log.error("No Bronze data found. Cannot build Silver.")
        return 1

    # 2. Type cast
    df = apply_silver_types(df)
    log.info("Types applied. Schema: %s", {c: str(t) for c, t in df.dtypes.items()})

    # 3. Write per-partition
    partitions = write_silver_partitions(df, client)
    log.info("Silver transform complete: %d partitions written", partitions)
    return 0


# ── CLI entrypoint ────────────────────────────────────────────────────────
if __name__ == "__main__":
    # No args needed — Silver is a full rebuild from all available Bronze.
    # If you want to limit scope later, add --start-date / --end-date filters.
    sys.exit(main())
