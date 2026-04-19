"""
WeatherFlow ETL — Data Quality & Schema Enforcement Layer
spark/data_quality.py

This module sits between the raw ingestion and the PySpark transformation.
It acts as a strict gate: data that fails quality checks never reaches PostgreSQL.

Two responsibilities:
  1. Schema Enforcement  — validate structure/types BEFORE processing
  2. Data Quality Checks — validate content/values AFTER parsing

Design principle: FAIL LOUD, FAIL EARLY.
  A silent bad record in your database is far worse than a loud pipeline crash.
  Every check here either quarantines the bad row or raises immediately —
  nothing is silently swallowed.

Usage (called from transform.py before any transformation):
  from data_quality import enforce_schema, run_quality_checks, QualityReport
"""

import logging
from dataclasses import dataclass, field
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType
)
from pyspark.sql.utils import AnalysisException

log = logging.getLogger("weatherflow.quality")


# ==============================================================================
# PART 1 - SCHEMA ENFORCEMENT
# ==============================================================================
#
# What is schema enforcement?
# ---------------------------
# APIs change. One day OpenWeather renames "temp" to "temperature", or adds a
# new required field, or changes humidity from int to float. Without enforcement,
# your pipeline silently produces nulls or crashes deep inside a transformation
# with a confusing error.
#
# Schema enforcement catches this AT THE DOOR - before a single transformation
# runs - with a clear, human-readable error message.
#
# Two layers of enforcement used here:
#   Layer 1 - Structural:  do all required columns exist?
#   Layer 2 - Type-level:  are column types castable to what we expect?

# The contract: this is what we expect from the OpenWeather API response.
# Any deviation from this is caught immediately.
EXPECTED_SCHEMA = StructType([
    StructField("city",         StringType(),  nullable=False),
    StructField("dt",           LongType(),    nullable=False),
    StructField("temp",         DoubleType(),  nullable=True),
    StructField("feels_like",   DoubleType(),  nullable=True),
    StructField("humidity",     IntegerType(), nullable=True),
    StructField("pressure",     IntegerType(), nullable=True),
    StructField("wind_speed",   DoubleType(),  nullable=True),
    StructField("weather_main", StringType(),  nullable=True),
    StructField("weather_desc", StringType(),  nullable=True),
])

# Columns that MUST be present for the pipeline to function at all
CRITICAL_COLUMNS = {"city", "dt", "temp"}

# Columns that are expected but non-fatal if missing (we can impute or skip)
OPTIONAL_COLUMNS = {"feels_like", "humidity", "pressure", "wind_speed",
                    "weather_main", "weather_desc"}


@dataclass
class SchemaViolation:
    """Represents a single schema contract violation."""
    column:   str
    issue:    str           # "missing_critical", "missing_optional", "type_mismatch"
    expected: Optional[str] = None
    actual:   Optional[str] = None
    is_fatal: bool = False


def enforce_schema(df: DataFrame) -> tuple:
    """
    Validate the incoming DataFrame against EXPECTED_SCHEMA.

    Returns:
        (df_safe, violations) where df_safe has optional missing columns
        filled with nulls so downstream code never hits AnalysisException.

    Raises:
        SchemaEnforcementError if any critical column is missing or
        has an incompatible type.
    """
    log.info("Running schema enforcement...")
    violations = []

    actual_columns   = {f.name: f.dataType for f in df.schema.fields}
    expected_columns = {f.name: f.dataType for f in EXPECTED_SCHEMA.fields}

    # Layer 1: Check for missing columns
    for col_name, expected_type in expected_columns.items():
        if col_name not in actual_columns:
            is_fatal = col_name in CRITICAL_COLUMNS
            v = SchemaViolation(
                column=col_name,
                issue="missing_critical" if is_fatal else "missing_optional",
                expected=str(expected_type),
                actual="ABSENT",
                is_fatal=is_fatal
            )
            violations.append(v)
            if is_fatal:
                log.error(f"FATAL schema violation: critical column '{col_name}' is missing!")
            else:
                log.warning(
                    f"Optional column '{col_name}' missing "
                    f"- adding null column as fallback"
                )
                # Gracefully add the missing optional column as null.
                # This prevents AnalysisException in downstream transformations.
                df = df.withColumn(col_name, F.lit(None).cast(expected_type))

    # Layer 2: Check for type mismatches on columns that ARE present
    for col_name, expected_type in expected_columns.items():
        if col_name in actual_columns:
            actual_type = actual_columns[col_name]

            # Allow numeric type interchangeability (Long/Int, Double/Float)
            type_compatible = (
                type(actual_type) == type(expected_type)
                or (str(actual_type) in ("LongType()", "IntegerType()") and
                    str(expected_type) in ("LongType()", "IntegerType()"))
                or (str(actual_type) in ("DoubleType()", "FloatType()") and
                    str(expected_type) in ("DoubleType()", "FloatType()"))
            )

            if not type_compatible:
                is_fatal = col_name in CRITICAL_COLUMNS
                v = SchemaViolation(
                    column=col_name,
                    issue="type_mismatch",
                    expected=str(expected_type),
                    actual=str(actual_type),
                    is_fatal=is_fatal
                )
                violations.append(v)
                if not is_fatal:
                    log.warning(
                        f"Type mismatch on '{col_name}': "
                        f"expected {expected_type}, got {actual_type}. "
                        f"Attempting safe cast."
                    )
                    try:
                        df = df.withColumn(
                            col_name, F.col(col_name).cast(expected_type)
                        )
                    except AnalysisException:
                        log.error(
                            f"Safe cast failed for '{col_name}' - nullifying column"
                        )
                        df = df.withColumn(
                            col_name, F.lit(None).cast(expected_type)
                        )

    # Raise immediately if any fatal violations exist
    fatal = [v for v in violations if v.is_fatal]
    if fatal:
        msg = "\n".join(
            f"  [{v.issue.upper()}] column='{v.column}' "
            f"expected={v.expected} actual={v.actual}"
            for v in fatal
        )
        raise SchemaEnforcementError(
            f"Pipeline halted - {len(fatal)} fatal schema violation(s):\n{msg}\n"
            f"This likely means the API response format has changed. "
            f"Check the OpenWeather API changelog and update EXPECTED_SCHEMA."
        )

    log.info(
        f"Schema enforcement complete: "
        f"{len(violations)} violation(s), 0 fatal. Pipeline may proceed."
    )
    return df, violations


class SchemaEnforcementError(Exception):
    """
    Raised when a fatal schema violation is detected.
    Caught at the top level of transform.py to trigger alerting/dead-letter.
    """
    pass


# ==============================================================================
# PART 2 - DATA QUALITY CHECKS
# ==============================================================================
#
# Schema enforcement checks STRUCTURE (do the right columns exist?).
# Data quality checks check CONTENT  (are the values sensible?).
#
# Each check follows the same pattern:
#   1. Identify bad rows with a filter expression
#   2. Count them
#   3. Either quarantine them (write to dead-letter) or raise - based on severity
#   4. Return only the clean rows

# Centralise all thresholds here so they're easy to adjust without touching logic.
QUALITY_RULES = {
    # Temperature in Celsius:
    # Below -90C (world record low) or above 60C is physically impossible.
    "temp_range": {
        "min": -90.0,
        "max":  60.0,
        "column": "temp",
        "severity": "quarantine",
    },
    "feels_like_range": {
        "min": -90.0,
        "max":  60.0,
        "column": "feels_like",
        "severity": "quarantine",
    },
    # Humidity is a percentage: must be 0-100
    "humidity_range": {
        "min":   0,
        "max": 100,
        "column": "humidity",
        "severity": "quarantine",
    },
    # Atmospheric pressure in hPa: 870-1085 covers all recorded extremes on Earth
    "pressure_range": {
        "min":  870,
        "max": 1085,
        "column": "pressure",
        "severity": "quarantine",
    },
    # Wind speed in m/s: 0-113 (world record ~113 m/s in a tornado)
    "wind_speed_range": {
        "min":   0.0,
        "max": 113.0,
        "column": "wind_speed",
        "severity": "quarantine",
    },
}

# If more than this fraction of records fail a single check, halt the pipeline.
# 30% failure rate means something is systemically wrong with the data source.
MAX_BAD_FRACTION = 0.30


@dataclass
class CheckResult:
    """Result of a single quality check."""
    check_name:   str
    column:       str
    total_rows:   int
    bad_rows:     int
    bad_fraction: float
    passed:       bool
    action_taken: str   # "none", "quarantined", "halted"
    details:      str = ""


@dataclass
class QualityReport:
    """Aggregated results of all quality checks for one pipeline run."""
    results:           list = field(default_factory=list)
    total_rows_in:     int = 0
    total_quarantined: int = 0

    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def summary(self) -> str:
        lines = [
            f"Quality Report - {self.total_rows_in} rows ingested, "
            f"{self.total_quarantined} quarantined\n",
            f"{'Check':<25} {'Column':<15} {'Bad rows':<10} {'Bad %':<8} {'Status'}",
            "-" * 75,
        ]
        for r in self.results:
            status = "PASS" if r.passed else "FAIL"
            lines.append(
                f"{r.check_name:<25} {r.column:<15} "
                f"{r.bad_rows:<10} {r.bad_fraction * 100:<7.2f}%  {status}"
            )
        return "\n".join(lines)


def _check_duplicates(df: DataFrame, report: QualityReport) -> tuple:
    """
    Check 1: Duplicate detection on (city, dt) - the natural composite key.

    A duplicate means the ingestion service fetched the same timestamp for the
    same city twice (e.g. a retry without deduplication). We keep the first
    occurrence and quarantine the rest.

    Why (city, dt) and not just dt?
    dt is a UNIX timestamp - two different cities can legitimately share
    the same dt value. The combination (city, dt) must be unique.
    """
    from pyspark.sql import Window as W

    total = df.count()
    log.info(f"Checking for duplicates on (city, dt) across {total} rows...")

    dedup_window = (
        W.partitionBy("city", "dt")
        .orderBy(F.monotonically_increasing_id())
    )

    df_ranked = df.withColumn("_row_rank", F.row_number().over(dedup_window))
    bad_df    = df_ranked.filter(F.col("_row_rank") > 1)
    good_df   = df_ranked.filter(F.col("_row_rank") == 1).drop("_row_rank")

    bad_count    = bad_df.count()
    bad_fraction = bad_count / total if total > 0 else 0.0

    report.results.append(CheckResult(
        check_name="duplicate_city_dt",
        column="city + dt",
        total_rows=total,
        bad_rows=bad_count,
        bad_fraction=bad_fraction,
        passed=True,
        action_taken="quarantined" if bad_count > 0 else "none",
        details="Kept first occurrence of each (city, dt) pair"
    ))
    report.total_quarantined += bad_count

    if bad_count > 0:
        log.warning(f"Duplicate check: {bad_count} duplicate rows quarantined")
        _write_quarantine(bad_df.drop("_row_rank"), check_name="duplicate_city_dt")
    else:
        log.info("Duplicate check: no duplicates found")

    return good_df, bad_count


def _check_value_range(
    df: DataFrame,
    rule_name: str,
    rule: dict,
    report: QualityReport
) -> DataFrame:
    """
    Check 2+: Value range validation for a single column.

    Rows outside the defined range are quarantined to a dead-letter partition.
    If the bad fraction exceeds MAX_BAD_FRACTION we halt the entire pipeline,
    because at that point something is systemically wrong with the data source.
    """
    col   = rule["column"]
    lo    = rule["min"]
    hi    = rule["max"]
    total = df.count()

    # Only flag rows that are non-null AND out of range.
    # Null rows are handled separately by clean_nulls() in transform.py.
    bad_condition = (
        F.col(col).isNotNull() &
        ((F.col(col) < lo) | (F.col(col) > hi))
    )

    bad_df  = df.filter(bad_condition)
    good_df = df.filter(~bad_condition)

    bad_count    = bad_df.count()
    bad_fraction = bad_count / total if total > 0 else 0.0
    is_fatal     = bad_fraction > MAX_BAD_FRACTION

    log.info(
        f"Range check '{rule_name}' ({col} in [{lo}, {hi}]): "
        f"{bad_count}/{total} bad rows ({bad_fraction * 100:.1f}%)"
    )

    if bad_count > 0:
        _write_quarantine(bad_df, check_name=rule_name)
        report.total_quarantined += bad_count

    action = "halted" if is_fatal else ("quarantined" if bad_count > 0 else "none")
    report.results.append(CheckResult(
        check_name=rule_name,
        column=col,
        total_rows=total,
        bad_rows=bad_count,
        bad_fraction=bad_fraction,
        passed=not is_fatal,
        action_taken=action,
        details=f"Valid range: [{lo}, {hi}]"
    ))

    if is_fatal:
        raise DataQualityError(
            f"Quality check '{rule_name}' FAILED: "
            f"{bad_fraction * 100:.1f}% of rows have '{col}' outside [{lo}, {hi}]. "
            f"Threshold is {MAX_BAD_FRACTION * 100:.0f}%. "
            f"The data source may be corrupted or the API may have changed units."
        )

    return good_df


def _write_quarantine(df: DataFrame, check_name: str):
    """
    Write bad rows to a quarantine partition on the Docker volume.

    In production this would be a dead-letter Kafka topic or an S3 prefix.
    Here we write to a local path so failures are inspectable without data loss.
    """
    from datetime import datetime
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"/app/raw_data/quarantine/{check_name}/{timestamp}"
    count = df.count()
    log.warning(f"Writing {count} quarantined rows to {path}")
    (
        df
        .withColumn("_quarantine_reason", F.lit(check_name))
        .withColumn("_quarantined_at",    F.current_timestamp())
        .coalesce(1)
        .write
        .mode("append")
        .json(path)
    )


class DataQualityError(Exception):
    """Raised when a quality check failure exceeds the acceptable threshold."""
    pass


# ==============================================================================
# PART 3 - PUBLIC API  (called from transform.py)
# ==============================================================================

def run_quality_checks(df: DataFrame) -> tuple:
    """
    Run all quality checks in sequence.

    Typical usage in transform.py:
        df, violations = enforce_schema(df)
        df, report     = run_quality_checks(df)
        log.info(report.summary())

    Returns:
        (clean_df, report)
        clean_df    — DataFrame with all bad rows removed/quarantined
        report      — QualityReport; call report.summary() for a log table

    Raises:
        DataQualityError       — if a range check fails beyond MAX_BAD_FRACTION
        SchemaEnforcementError — raised upstream by enforce_schema(), documented
                                 here for clarity
    """
    report = QualityReport(total_rows_in=df.count())
    log.info(f"Starting quality checks on {report.total_rows_in} rows")

    # Check 1: Duplicate rows
    df, _ = _check_duplicates(df, report)

    # Checks 2-N: Value range validation (config-driven, easy to extend)
    for rule_name, rule in QUALITY_RULES.items():
        df = _check_value_range(df, rule_name, rule, report)

    log.info("\n" + report.summary())
    return df, report