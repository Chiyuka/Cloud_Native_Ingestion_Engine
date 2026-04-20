-- =============================================================================
-- WeatherFlow ETL — PostgreSQL Schema
-- sql/init.sql
--
-- This file is auto-executed by the postgres Docker container on first start
-- (mounted at /docker-entrypoint-initdb.d/init.sql).
-- It is idempotent: safe to re-run, will not fail if objects already exist.
--
-- Schema design decisions:
--   - weather_raw       : append-only, one row per city per API call
--                         This is the "source of truth" — never update, never delete
--   - weather_hourly_agg: pre-aggregated by city + hour for fast dashboard queries
--                         Written by PySpark after each transformation run
--   - pipeline_runs     : audit log — one row per pipeline execution
--                         Lets you answer "when did the last run happen, did it pass?"
-- =============================================================================

-- =============================================================================
-- EXTENSIONS
-- =============================================================================

-- pgcrypto: provides gen_random_uuid() for surrogate primary keys
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =============================================================================
-- TABLE: weather_raw
-- Append-only fact table. Every clean reading from every city lands here.
-- =============================================================================

CREATE TABLE IF NOT EXISTS weather_raw (
    -- Surrogate key — stable identifier for each row regardless of source changes
    id              UUID        NOT NULL DEFAULT gen_random_uuid(),

    -- Business key — the natural unique identifier
    -- UNIQUE constraint enforces deduplication at the DB level as a safety net
    -- (primary deduplication happens in data_quality.py)
    city            TEXT        NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL,

    -- Core measurements
    temp            NUMERIC(5,2),           -- °C, e.g. 18.40
    feels_like      NUMERIC(5,2),           -- °C
    feels_like_delta NUMERIC(5,2),          -- feels_like - temp (derived)
    humidity        SMALLINT,               -- % 0-100
    pressure        SMALLINT,               -- hPa
    wind_speed      NUMERIC(6,2),           -- m/s

    -- Categorical
    weather_main    TEXT,                   -- e.g. "Clouds"
    weather_desc    TEXT,                   -- e.g. "overcast clouds"

    -- Derived / enriched columns added by PySpark
    temp_rolling_avg_5m  NUMERIC(5,2),      -- 5-min rolling avg temp
    comfort_label        TEXT,              -- "warm" | "mild" | "cool" | "cold"

    -- Partitioning helpers (redundant with recorded_at but faster for range scans)
    recorded_hour   TIMESTAMPTZ NOT NULL,   -- DATE_TRUNC('hour', recorded_at)
    recorded_date   DATE        NOT NULL,

    -- Audit
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT weather_raw_pkey PRIMARY KEY (id),
    CONSTRAINT weather_raw_city_dt_unique UNIQUE (city, recorded_at),

    -- Data integrity — enforce the same bounds as data_quality.py
    -- The DB is the last line of defence
    CONSTRAINT chk_temp_range
        CHECK (temp IS NULL OR (temp BETWEEN -90 AND 60)),
    CONSTRAINT chk_feels_like_range
        CHECK (feels_like IS NULL OR (feels_like BETWEEN -90 AND 60)),
    CONSTRAINT chk_humidity_range
        CHECK (humidity IS NULL OR (humidity BETWEEN 0 AND 100)),
    CONSTRAINT chk_pressure_range
        CHECK (pressure IS NULL OR (pressure BETWEEN 870 AND 1085)),
    CONSTRAINT chk_wind_speed_range
        CHECK (wind_speed IS NULL OR (wind_speed BETWEEN 0 AND 113))
);

-- =============================================================================
-- INDEXES on weather_raw
-- Design rationale: index what you query, not everything
-- =============================================================================

-- Most common query pattern: "give me all readings for city X in time range Y"
CREATE INDEX IF NOT EXISTS idx_raw_city_recorded_at
    ON weather_raw (city, recorded_at DESC);

-- Dashboard queries often filter by date only
CREATE INDEX IF NOT EXISTS idx_raw_recorded_date
    ON weather_raw (recorded_date DESC);

-- Comfort label filtering (low cardinality — good candidate for partial index)
CREATE INDEX IF NOT EXISTS idx_raw_comfort_label
    ON weather_raw (comfort_label)
    WHERE comfort_label IS NOT NULL;

-- =============================================================================
-- TABLE: weather_hourly_agg
-- Pre-aggregated by city + hour. Written by PySpark after each run.
-- Designed for fast dashboard reads — avoids GROUP BY on the raw table.
-- =============================================================================

CREATE TABLE IF NOT EXISTS weather_hourly_agg (
    id              UUID        NOT NULL DEFAULT gen_random_uuid(),

    -- Composite business key
    city            TEXT        NOT NULL,
    hour            TIMESTAMPTZ NOT NULL,

    -- Aggregated metrics
    avg_temp        NUMERIC(5,2),
    max_temp        NUMERIC(5,2),
    min_temp        NUMERIC(5,2),
    avg_humidity    NUMERIC(5,1),
    avg_wind_speed  NUMERIC(6,2),
    reading_count   SMALLINT,              -- how many raw readings this is based on

    -- Audit
    aggregated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT weather_hourly_agg_pkey PRIMARY KEY (id),
    CONSTRAINT weather_hourly_agg_city_hour_unique UNIQUE (city, hour)
);

CREATE INDEX IF NOT EXISTS idx_hourly_city_hour
    ON weather_hourly_agg (city, hour DESC);

-- =============================================================================
-- TABLE: pipeline_runs
-- One row per pipeline execution. Answers operational questions:
--   "When did the pipeline last run?"
--   "How many rows were quarantined yesterday?"
--   "Which runs failed?"
-- =============================================================================

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID        NOT NULL DEFAULT gen_random_uuid(),

    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          TEXT        NOT NULL    -- "running" | "success" | "failed"
                    CHECK (status IN ('running', 'success', 'failed')),

    rows_ingested   INTEGER,
    rows_quarantined INTEGER,
    rows_written    INTEGER,

    -- JSON blob of the QualityReport for full audit trail
    quality_report  JSONB,

    -- If failed, the error class and message
    error_type      TEXT,
    error_message   TEXT,

    CONSTRAINT pipeline_runs_pkey PRIMARY KEY (run_id)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON pipeline_runs (started_at DESC);

-- =============================================================================
-- VIEWS — convenience queries for dashboards or quick debugging
-- =============================================================================

-- Latest reading per city (useful for a "current conditions" dashboard tile)
CREATE OR REPLACE VIEW v_latest_readings AS
SELECT DISTINCT ON (city)
    city,
    recorded_at,
    temp,
    feels_like,
    humidity,
    wind_speed,
    weather_desc,
    comfort_label
FROM weather_raw
ORDER BY city, recorded_at DESC;

-- Last 24 hours of hourly aggregates
CREATE OR REPLACE VIEW v_last_24h_hourly AS
SELECT
    city,
    hour,
    avg_temp,
    max_temp,
    min_temp,
    avg_humidity,
    reading_count
FROM weather_hourly_agg
WHERE hour >= NOW() - INTERVAL '24 hours'
ORDER BY city, hour DESC;

-- =============================================================================
-- INITIAL DATA CHECK (runs at container start — visible in docker logs)
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================';
    RAISE NOTICE 'WeatherFlow schema initialised successfully';
    RAISE NOTICE 'Tables: weather_raw, weather_hourly_agg, pipeline_runs';
    RAISE NOTICE 'Views:  v_latest_readings, v_last_24h_hourly';
    RAISE NOTICE '============================================';
END $$;