"""
WeatherFlow ETL — Ingestion Service
ingestion/fetch.py

Responsibilities:
  - Poll the OpenWeather Current Weather API every 15 minutes
  - Fetch data for all configured cities in parallel
  - Normalise the raw API response into a flat JSON structure
  - Write one JSON file per city per fetch to the shared Docker volume
  - Never crash on a single city failure (log and continue)

Environment variables (injected by Docker Compose via .env):
  OPENWEATHER_API_KEY  — your API key
  CITIES               — comma-separated city names, e.g. Budapest,Vienna,Munich
  FETCH_INTERVAL_MIN   — how often to fetch, default 15
  RAW_DATA_PATH        — where to write JSON files, default /app/raw_data
"""

import os
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import schedule
from dotenv import load_dotenv

# ── Bootstrap ─────────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S"
)
log = logging.getLogger("weatherflow.ingestion")

# ── Config ────────────────────────────────────────────────────────────────────
API_KEY       = os.getenv("OPENWEATHER_API_KEY", "")
CITIES        = [c.strip() for c in os.getenv("CITIES", "Budapest").split(",")]
INTERVAL_MIN  = int(os.getenv("FETCH_INTERVAL_MIN", "15"))
RAW_DATA_PATH = Path(os.getenv("RAW_DATA_PATH", "/app/raw_data"))
BASE_URL      = "https://api.openweathermap.org/data/2.5/weather"

# Timeout for each individual API call (connect timeout, read timeout)
REQUEST_TIMEOUT = (5, 10)

# How many cities to fetch in parallel
MAX_WORKERS = 5


def _validate_config():
    """Fail fast on startup if critical config is missing."""
    if not API_KEY:
        raise EnvironmentError(
            "OPENWEATHER_API_KEY is not set. "
            "Add it to your .env file and restart."
        )
    if not CITIES:
        raise EnvironmentError("CITIES must contain at least one city name.")
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    log.info(f"Config OK — cities: {CITIES}, interval: {INTERVAL_MIN}m, output: {RAW_DATA_PATH}")


# ── API call ──────────────────────────────────────────────────────────────────

def fetch_city(city: str) -> dict | None:
    """
    Fetch current weather for a single city.

    Returns a flat dict ready to write as JSON, or None on failure.
    We flatten the nested OpenWeather response here so PySpark gets
    a simple, predictable structure — no nested JSON parsing needed later.
    """
    params = {
        "q":     city,
        "appid": API_KEY,
        "units": "metric",   # Celsius, m/s — document this choice explicitly
    }

    try:
        resp = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        raw = resp.json()

    except requests.exceptions.Timeout:
        log.warning(f"[{city}] Request timed out — skipping this cycle")
        return None
    except requests.exceptions.HTTPError as e:
        log.error(f"[{city}] HTTP {e.response.status_code}: {e.response.text[:200]}")
        return None
    except requests.exceptions.RequestException as e:
        log.error(f"[{city}] Network error: {e}")
        return None

    # Flatten the response into the exact schema PySpark expects.
    # This mapping is the "API contract" between ingestion and transformation.
    # If OpenWeather changes their response shape, this is the only place to fix it.
    try:
        record = {
            "city":         city,
            "dt":           raw["dt"],                          # UNIX epoch seconds UTC
            "temp":         raw["main"]["temp"],                # °C
            "feels_like":   raw["main"]["feels_like"],          # °C
            "humidity":     raw["main"]["humidity"],            # %
            "pressure":     raw["main"]["pressure"],            # hPa
            "wind_speed":   raw.get("wind", {}).get("speed"),  # m/s (optional)
            "weather_main": raw["weather"][0]["main"],          # e.g. "Clouds"
            "weather_desc": raw["weather"][0]["description"],   # e.g. "overcast clouds"
            # Metadata — not stored in PostgreSQL but useful for debugging raw files
            "_fetched_at":  datetime.now(timezone.utc).isoformat(),
            "_api_version": "2.5",
        }
    except (KeyError, IndexError) as e:
        log.error(
            f"[{city}] Unexpected API response shape — "
            f"missing key: {e}. Raw response: {str(raw)[:300]}"
        )
        return None

    log.info(
        f"[{city}] fetched — "
        f"temp={record['temp']}°C, "
        f"humidity={record['humidity']}%, "
        f"dt={record['dt']}"
    )
    return record


def write_record(record: dict):
    """
    Write a single record to a JSON file on the Docker volume.

    File naming: {city}_{dt}.json
      - city  : makes files easy to filter by city in PySpark
      - dt    : the UNIX timestamp from OpenWeather (not wall-clock time)
                ensures idempotent writes — re-fetching the same window
                produces the same filename and overwrites cleanly
    """
    city    = record["city"].lower().replace(" ", "_")
    dt      = record["dt"]
    outfile = RAW_DATA_PATH / f"{city}_{dt}.json"

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)

    log.debug(f"Written: {outfile}")


# ── Fetch cycle ───────────────────────────────────────────────────────────────

def run_fetch_cycle():
    """
    Fetch all cities in parallel and write results.

    Uses ThreadPoolExecutor because the bottleneck is I/O (HTTP requests),
    not CPU — threads are the right tool here, not multiprocessing.
    """
    log.info(f"=== Fetch cycle starting — {len(CITIES)} cities ===")
    start = time.monotonic()
    success, failed = 0, 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_to_city = {pool.submit(fetch_city, city): city for city in CITIES}

        for future in as_completed(future_to_city):
            city   = future_to_city[future]
            record = future.result()

            if record:
                write_record(record)
                success += 1
            else:
                failed += 1

    elapsed = time.monotonic() - start
    log.info(
        f"=== Fetch cycle complete — "
        f"{success} succeeded, {failed} failed, "
        f"{elapsed:.2f}s elapsed ==="
    )


# ── Scheduler ─────────────────────────────────────────────────────────────────

def main():
    _validate_config()
    log.info(f"Ingestion service starting — fetching every {INTERVAL_MIN} minute(s)")

    # Run immediately on startup so we don't wait one full interval for first data
    run_fetch_cycle()

    # Schedule subsequent runs
    schedule.every(INTERVAL_MIN).minutes.do(run_fetch_cycle)

    while True:
        schedule.run_pending()
        time.sleep(30)   # check every 30 seconds — low CPU overhead


if __name__ == "__main__":
    main()