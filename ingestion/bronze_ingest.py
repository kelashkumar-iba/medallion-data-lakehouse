"""
Bronze ingestion: Open-Meteo Historical Weather API -> Parquet -> MinIO bronze/.

Design rules:
  1. NEVER modify source data here. Bronze is raw.
  2. Add only ingestion metadata columns (_ingested_at, _source_url).
  3. Partition output by ingest_date so we can reprocess single days cheaply.
  4. Idempotent: same date = overwrite that partition.
"""

import io                          # In-memory bytes buffer for Parquet upload
import os                          # Read MinIO creds from env vars
import sys                         # Exit codes for Airflow task status
import argparse                    # CLI args so Airflow can inject dates
import logging                     # Structured logs visible in Airflow task UI
from datetime import datetime, timedelta, timezone

import requests                    # HTTP client for Open-Meteo
import pandas as pd                # DataFrame -> Parquet conversion
import pyarrow as pa               # Parquet engine under pandas
import pyarrow.parquet as pq       # Direct Parquet writer (more control than pandas)
from minio import Minio            # MinIO client — speaks S3 API


# ── Logging setup ──────────────────────────────────────────────────────────
# Stream to stdout so Airflow captures it in the task log automatically.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("bronze_ingest")


# ── Configuration ──────────────────────────────────────────────────────────
# 5 cities — name + lat/lon. Open-Meteo is geo-coordinate based, not city-name.
CITIES = [
    {"name": "Karachi",   "lat": 24.8607, "lon":  67.0011},
    {"name": "Lahore",    "lat": 31.5497, "lon":  74.3436},
    {"name": "Islamabad", "lat": 33.6844, "lon":  73.0479},
    {"name": "Mumbai",    "lat": 19.0760, "lon":  72.8777},
    {"name": "Dubai",     "lat": 25.2048, "lon":  55.2708},
]

# Open-Meteo historical endpoint — free, no API key required
API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Daily fields we pull for each city. Keep the list small + intentional —
# every column you ingest is a column you'll have to validate in Silver.
DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
]


# ── MinIO client factory ───────────────────────────────────────────────────
def get_minio_client() -> Minio:
    """Build a MinIO client from environment variables.

    Env vars are injected by docker-compose into Airflow containers.
    """
    endpoint = os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
    return Minio(
        endpoint,
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,                 # Local MinIO over HTTP, no TLS
    )


# ── Fetch one city ─────────────────────────────────────────────────────────
def fetch_city(city: dict, start_date: str, end_date: str) -> pd.DataFrame:
    """Hit Open-Meteo for one city, return a DataFrame. One row per day."""
    params = {
        "latitude":  city["lat"],
        "longitude": city["lon"],
        "start_date": start_date,
        "end_date":   end_date,
        "daily":      ",".join(DAILY_FIELDS),
        "timezone":   "UTC",
    }

    log.info("Fetching %s (%s to %s)", city["name"], start_date, end_date)
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()       # Raises on 4xx/5xx — Airflow marks task failed
    payload = response.json()

    # Open-Meteo returns daily as parallel arrays under payload["daily"]:
    #   { "time": [...], "temperature_2m_max": [...], ... }
    # Convert to DataFrame with one row per date.
    daily = payload.get("daily", {})
    if not daily or "time" not in daily:
        raise ValueError(f"Open-Meteo returned no daily data for {city['name']}: {payload}")

    df = pd.DataFrame(daily)

    # Tag every row with the city — Open-Meteo doesn't echo this back
    df["city"] = city["name"]
    df["latitude"]  = city["lat"]
    df["longitude"] = city["lon"]

    # Capture the exact request URL for audit — useful when debugging Bronze 6 months later
    df["_source_url"] = response.url

    return df


# ── Write Parquet to MinIO ─────────────────────────────────────────────────
def write_parquet_to_minio(df: pd.DataFrame, client: Minio, bucket: str, object_name: str) -> None:
    """Serialize DataFrame to Parquet in-memory, upload to MinIO."""
    buffer = io.BytesIO()

    # Convert pandas -> pyarrow Table for explicit schema control.
    # preserve_index=False keeps the file clean — no pandas row index column.
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Snappy compression: industry default for Parquet — fast + good ratio.
    pq.write_table(table, buffer, compression="snappy")

    # Reset cursor to start so put_object reads from byte 0
    buffer.seek(0)
    size = buffer.getbuffer().nbytes

    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=buffer,
        length=size,
        content_type="application/octet-stream",
    )
    log.info("Wrote s3://%s/%s (%d bytes)", bucket, object_name, size)


# ── Main pipeline ──────────────────────────────────────────────────────────
def main(start_date: str, end_date: str, ingest_date: str) -> int:
    bucket = os.environ.get("MINIO_BUCKET", "lakehouse")
    client = get_minio_client()

    # Sanity check — bucket should already exist (created by minio-init container)
    if not client.bucket_exists(bucket):
        log.error("Bucket '%s' does not exist. Did minio-init container run?", bucket)
        return 1

    # Fetch all cities sequentially — small dataset, no need for async
    frames = []
    for city in CITIES:
        try:
            frames.append(fetch_city(city, start_date, end_date))
        except Exception as exc:
            # Fail the whole task on any city error — Bronze must be complete or not at all
            log.exception("Failed to fetch %s: %s", city["name"], exc)
            return 2

    # Combine all cities into one DataFrame, add ingestion timestamp
    bronze_df = pd.concat(frames, ignore_index=True)
    bronze_df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    log.info("Combined Bronze frame: %d rows, %d cols", len(bronze_df), len(bronze_df.columns))

    # Hive-style partition path — DuckDB / Spark / Trino all understand this layout
    object_name = f"bronze/weather/ingest_date={ingest_date}/data.parquet"
    write_parquet_to_minio(bronze_df, client, bucket, object_name)

    log.info("Bronze ingestion complete for ingest_date=%s", ingest_date)
    return 0


# ── CLI entrypoint ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze ingest: Open-Meteo -> MinIO")
    # Default: pull last 7 days. Airflow will override with execution_date logic.
    today = datetime.now(timezone.utc).date()
    parser.add_argument("--start",       default=str(today - timedelta(days=7)))
    parser.add_argument("--end",         default=str(today - timedelta(days=1)))
    parser.add_argument("--ingest-date", default=str(today))
    args = parser.parse_args()

    sys.exit(main(args.start, args.end, args.ingest_date))
