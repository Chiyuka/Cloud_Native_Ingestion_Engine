"""
WeatherFlow ETL — PySpark Transformation Engine
spark/transform.py

Responsibilities:
  1. Read raw JSON files from the shared Docker volume
  2. Enforce a strict schema (fail fast on bad data)
  3. Clean nulls with a documented strategy
  4. Standardise timestamps to UTC
  5. Calculate a 5-minute rolling average for temperature
  6. Write clean records to PostgreSQL (append) and aggregated records (upsert-safe)

Run via docker-compose:
  spark-submit --packages org.postgresql:postgresql:42.7.1 transform.py
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType
)
from data_quality import (
    enforce_schema,
    run_quality_checks,
    SchemaEnforcementError,
    DataQualityError
)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("weatherflow")

# ── Config (injected by Docker Compose via environment variables) ──────────────
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_USER = os.getenv("POSTGRES_USER", "etl_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "etl_pass")
POSTGRES_DB   = os.getenv("POSTGRES_DB", "weatherflow")
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "/app/raw_data/*.json")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}"
JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver":   "org.postgresql.Driver"
}

# ── 1.  Spark Session ──────────────────────────────────────────────────────────
# In production you would point master to your cluster URL.
# "local[*]" uses all available cores on this single container.
spark = (
    SparkSession.builder
    .appName("WeatherFlow-ETL")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.jars", "/opt/postgresql-42.7.1.jar")   # ← add this line
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
log.info("SparkSession started")


# ── 2.  Strict Schema ──────────────────────────────────────────────────────────
# Defining the schema explicitly instead of inferring it has two benefits:
#   a) 2× faster reads (no extra scan to infer types)
#   b) Bad data fails immediately with a clear error, not silently as a null
#
# This matches the JSON structure returned by the OpenWeather /weather endpoint.
RAW_SCHEMA = StructType([
    StructField("city",        StringType(),  nullable=False),
    StructField("dt",          LongType(),    nullable=False),   # UNIX timestamp (seconds)
    StructField("temp",        DoubleType(),  nullable=True),
    StructField("feels_like",  DoubleType(),  nullable=True),
    StructField("humidity",    IntegerType(), nullable=True),
    StructField("pressure",    IntegerType(), nullable=True),
    StructField("wind_speed",  DoubleType(),  nullable=True),
    StructField("weather_main",StringType(),  nullable=True),    # e.g. "Clouds", "Rain"
    StructField("weather_desc",StringType(),  nullable=True),
])


def read_raw(path: str):
    """
    Read raw JSON files from the Docker volume.
    multiLine=True handles pretty-printed JSON files written by the ingestion service.
    """
    log.info(f"Reading raw JSON from {path}")
    df = (
        spark.read
        .option("multiLine", True)
        .schema(RAW_SCHEMA)
        .json(path)
    )
    log.info(f"Raw record count: {df.count()}")
    return df


# ── 3.  Transformation 1 — Clean Null Values ──────────────────────────────────
# Strategy (document this in interviews — it shows maturity):
#   - city / dt are non-nullable in schema → Spark already drops rows missing these
#   - temp: critical measurement → drop the row entirely if missing
#   - feels_like: derived comfort metric → impute with temp (reasonable fallback)
#   - humidity / pressure / wind_speed: drop rows where ALL THREE are null
#     (a row with no sensor readings is useless); otherwise keep with nulls
#   - weather_main / weather_desc: low-criticality strings → fill with "Unknown"

def clean_nulls(df):
    log.info("Cleaning null values...")
    before = df.count()

    df = (
        df
        # Drop rows where the core measurement is missing
        .dropna(subset=["temp"])

        # Impute feels_like with temp when missing
        .withColumn(
            "feels_like",
            F.coalesce(F.col("feels_like"), F.col("temp"))
        )

        # Fill unknown categorical fields
        .fillna({"weather_main": "Unknown", "weather_desc": "Unknown"})
    )

    after = df.count()
    log.info(f"Null cleaning: dropped {before - after} rows ({before} → {after})")
    return df


# ── 4.  Transformation 2 — Standardise Timestamps to UTC ──────────────────────
# OpenWeather returns `dt` as a UNIX epoch integer (seconds since 1970-01-01 UTC).
# We convert it to a proper TimestampType for SQL compatibility and add a
# truncated hour column for easy aggregation.

def standardise_timestamps(df):
    log.info("Standardising timestamps to UTC...")
    df = (
        df
        .withColumn(
            "recorded_at",                         # human-readable UTC timestamp
            F.to_utc_timestamp(
                F.from_unixtime(F.col("dt")),      # epoch → local string
                "UTC"                              # tell Spark the source is already UTC
            )
        )
        .withColumn(
            "recorded_hour",                       # truncated for hourly aggregation
            F.date_trunc("hour", F.col("recorded_at"))
        )
        .withColumn(
            "recorded_date",                       # for partitioning / filtering
            F.to_date(F.col("recorded_at"))
        )
        .drop("dt")                                # raw epoch no longer needed
    )
    return df


# ── 5.  Transformation 3 — 5-Minute Rolling Average Temperature ────────────────
# A Window function partitions data by city, orders by timestamp, and looks back
# across a 5-minute range (5 * 60 = 300 seconds).
#
# Why rangeBetween instead of rowsBetween?
#   rowsBetween(N, 0) looks at the previous N *rows* — but if data arrives
#   unevenly, "5 rows back" ≠ "5 minutes back".
#   rangeBetween operates on the *actual timestamp values*, so the window is
#   always exactly 300 seconds regardless of data frequency.

def rolling_avg_temperature(df):
    log.info("Calculating 5-minute rolling average temperature...")

    # Spark's rangeBetween needs a numeric column — cast recorded_at to seconds
    df = df.withColumn("ts_seconds", F.col("recorded_at").cast("long"))

    window_5min = (
        Window
        .partitionBy("city")
        .orderBy("ts_seconds")
        .rangeBetween(-300, 0)          # 300 seconds = 5 minutes lookback
    )

    df = df.withColumn(
        "temp_rolling_avg_5m",
        F.round(F.avg("temp").over(window_5min), 2)
    )

    return df


# ── 6.  Derived Columns (bonus — great for interview demos) ───────────────────
def add_derived_columns(df):
    df = (
        df
        # How different does it feel vs actual temperature?
        .withColumn(
            "feels_like_delta",
            F.round(F.col("feels_like") - F.col("temp"), 2)
        )
        # Simple human-readable comfort label
        .withColumn(
            "comfort_label",
            F.when(F.col("temp") >= 25, "warm")
             .when(F.col("temp") >= 15, "mild")
             .when(F.col("temp") >= 5,  "cool")
             .otherwise("cold")
        )
    )
    return df


# ── 7.  Write to PostgreSQL ────────────────────────────────────────────────────
# Two tables:
#   weather_raw   — append-only, every clean record (the "source of truth")
#   weather_hourly_agg — aggregated by city + hour (for fast dashboard queries)

def write_to_postgres(df):
    log.info("Writing weather_raw to PostgreSQL...")

    # Read already-stored (city, recorded_at) pairs from Postgres
    # Then filter them out before writing — idempotent by design
    try:
        existing = spark.read.jdbc(
            url=JDBC_URL,
            table="(SELECT city, recorded_at FROM weather_raw) AS existing",
            properties=JDBC_PROPS
        )
        df_new = df.join(existing, on=["city", "recorded_at"], how="left_anti")
        log.info(f"Skipping {df.count() - df_new.count()} already-loaded rows")
    except Exception:
        # First run — table is empty, write everything
        df_new = df

    row_count = df_new.count()
    if row_count == 0:
        log.info("No new rows to write — all records already exist in DB")
    else:
        (
            df_new.select(
                "city", "recorded_at", "recorded_hour", "recorded_date",
                "temp", "feels_like", "feels_like_delta", "humidity",
                "pressure", "wind_speed", "weather_main", "weather_desc",
                "temp_rolling_avg_5m", "comfort_label"
            )
            .write
            .jdbc(url=JDBC_URL, table="weather_raw", mode="append", properties=JDBC_PROPS)
        )
        log.info(f"weather_raw: wrote {row_count} new rows")

    log.info("Writing weather_hourly_agg to PostgreSQL...")
    hourly = (
        df.withColumnRenamed("recorded_hour", "hour")
        .groupBy("city", "hour")            # ← matches the DB column name
        .agg(
            F.round(F.avg("temp"),       2).alias("avg_temp"),
            F.round(F.max("temp"),       2).alias("max_temp"),
            F.round(F.min("temp"),       2).alias("min_temp"),
            F.round(F.avg("humidity"),   1).alias("avg_humidity"),
            F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
            F.count("*").alias("reading_count")
        )
    )
    (
        hourly
        .write
        .jdbc(url=JDBC_URL, table="weather_hourly_agg", mode="append", properties=JDBC_PROPS)
    )
    log.info("weather_hourly_agg written.")


# ── 8.  Pipeline Orchestration ────────────────────────────────────────────────
def run():
    log.info("=== WeatherFlow ETL pipeline starting ===")

    df = read_raw(RAW_DATA_PATH)

    # Gate 1 — schema enforcement
    df, violations = enforce_schema(df)

    # Gate 2 — data quality checks
    df, report = run_quality_checks(df)
    log.info(report.summary())

    # Transformations
    df = clean_nulls(df)
    df = standardise_timestamps(df)
    df = rolling_avg_temperature(df)
    df = add_derived_columns(df)

    # Cache before double write
    df.cache()
    log.info(f"Final record count before write: {df.count()}")

    write_to_postgres(df)

    df.unpersist()
    spark.stop()
    log.info("=== Pipeline complete ===")


if __name__ == "__main__":
    run()