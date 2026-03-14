"""
ingest_forecasts.py
-------------------
Daily forecast ingester. Pulls 7-day forecasts from 3 sources for all 20 cities
and upserts into the `forecasts` table.

Sources:
  1. Open-Meteo GFS   (source_id=1)
  2. Open-Meteo ECMWF  (source_id=2)
  3. NWS API           (source_id=3)

Usage:
    python ingest_forecasts.py
"""

import sys
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import date, datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_KEY_PATH = Path(r"F:/weatherbetterv2/tokens/db/db.key")

def load_db_config() -> dict:
    import os
    if os.environ.get("DB_HOST"):
        return {
            "host":     os.environ["DB_HOST"],
            "port":     int(os.environ.get("DB_PORT", 5432)),
            "dbname":   os.environ["DB_NAME"],
            "user":     os.environ["DB_USER"],
            "password": os.environ["DB_PASSWORD"],
            "sslmode":  os.environ.get("DB_SSLMODE", "require"),
        }
    lines = DB_KEY_PATH.read_text().strip().splitlines()
    return {
        "host":     lines[0],
        "port":     int(lines[1]),
        "dbname":   lines[2],
        "user":     lines[3],
        "password": lines[4],
        "sslmode":  lines[5] if len(lines) > 5 else "prefer",
    }

DB_CONFIG = load_db_config()

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
NWS_POINTS_URL = "https://api.weather.gov/points"
NWS_USER_AGENT = "WeatherBetterV2 (weather@example.com)"

FORECAST_DAYS = 7
RETRY_LIMIT   = 3
RETRY_BACKOFF = [2, 5, 10]

# Open-Meteo daily variables we need
OM_DAILY_VARS = (
    "temperature_2m_max,temperature_2m_min,"
    "precipitation_sum,precipitation_probability_max,"
    "wind_speed_10m_max,wind_gusts_10m_max"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def log_pipeline_run(status, cities_processed=0, rows_inserted=0,
                     error_message=None, notes=None, started_at=None):
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_runs
                        (run_type, started_at, finished_at, status,
                         cities_processed, rows_inserted, rows_updated,
                         error_message, notes)
                    VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, ('forecast', started_at, status,
                      cities_processed, rows_inserted, 0,
                      error_message, notes))
                run_id = cur.fetchone()[0]
        conn.close()
        return run_id
    except Exception as e:
        log.error(f"Failed to log pipeline run: {e}")
        return None


def start_pipeline_run(started_at):
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO pipeline_runs
                        (run_type, started_at, status,
                         cities_processed, rows_inserted, rows_updated)
                    VALUES (%s, %s, 'running', 0, 0, 0)
                    RETURNING id
                """, ('forecast', started_at))
                run_id = cur.fetchone()[0]
        conn.close()
        return run_id
    except Exception as e:
        log.error(f"Failed to start pipeline run: {e}")
        return None


def finish_pipeline_run(run_id, status, cities_processed=0, rows_inserted=0,
                        error_message=None, notes=None):
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_runs
                    SET finished_at = NOW(), status = %s,
                        cities_processed = %s, rows_inserted = %s,
                        error_message = %s, notes = %s
                    WHERE id = %s
                """, (status, cities_processed, rows_inserted,
                      error_message, notes, run_id))
        conn.close()
    except Exception as e:
        log.error(f"Failed to finish pipeline run: {e}")


# ---------------------------------------------------------------------------
# API fetch helpers
# ---------------------------------------------------------------------------

def _retry_get(url, headers=None, params=None, timeout=30):
    """GET with retries and backoff."""
    for attempt in range(RETRY_LIMIT):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                log.warning(f"Rate limited on {url} — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            log.warning(f"Request error ({url}) attempt {attempt+1}/{RETRY_LIMIT}: {e}")
            if attempt < RETRY_LIMIT - 1:
                time.sleep(wait)
    return None


def fetch_open_meteo(lat, lon, model, timezone):
    """Fetch 7-day daily forecast from Open-Meteo for a given model."""
    params = {
        "latitude":           lat,
        "longitude":          lon,
        "daily":              OM_DAILY_VARS,
        "temperature_unit":   "fahrenheit",
        "wind_speed_unit":    "mph",
        "precipitation_unit": "inch",
        "models":             model,
        "forecast_days":      FORECAST_DAYS,
        "timezone":           timezone,
    }
    return _retry_get(OPEN_METEO_URL, params=params)


def fetch_nws_grid(lat, lon):
    """Get NWS grid coordinates for a lat/lon."""
    url = f"{NWS_POINTS_URL}/{lat},{lon}"
    headers = {"User-Agent": NWS_USER_AGENT}
    data = _retry_get(url, headers=headers)
    if not data:
        return None
    props = data.get("properties", {})
    return {
        "gridId": props.get("gridId"),
        "gridX":  props.get("gridX"),
        "gridY":  props.get("gridY"),
    }


def fetch_nws_forecast(grid_id, grid_x, grid_y):
    """Fetch 7-day forecast from NWS gridpoint."""
    url = f"https://api.weather.gov/gridpoints/{grid_id}/{grid_x},{grid_y}/forecast"
    headers = {"User-Agent": NWS_USER_AGENT}
    return _retry_get(url, headers=headers)


# ---------------------------------------------------------------------------
# Parsers — build forecast rows from API responses
# ---------------------------------------------------------------------------

def parse_open_meteo(city_id, source_id, data, today):
    """Parse Open-Meteo response into forecast row dicts."""
    rows = []
    daily = data.get("daily")
    if not daily:
        return rows

    times = daily.get("time", [])
    highs = daily.get("temperature_2m_max", [])
    lows  = daily.get("temperature_2m_min", [])
    precip = daily.get("precipitation_sum", [])
    precip_prob = daily.get("precipitation_probability_max", [])
    wind  = daily.get("wind_speed_10m_max", [])
    gust  = daily.get("wind_gusts_10m_max", [])

    for i, date_str in enumerate(times):
        target = date.fromisoformat(date_str)
        lead = (target - today).days
        if lead < 1 or lead > FORECAST_DAYS:
            continue

        high = highs[i] if i < len(highs) else None
        low  = lows[i]  if i < len(lows)  else None

        rows.append({
            "city_id":       city_id,
            "source_id":     source_id,
            "target_date":   date_str,
            "lead_days":     lead,
            "temp_high":     round(high, 1) if high is not None else None,
            "temp_low":      round(low, 1)  if low  is not None else None,
            "temp_avg":      round((high + low) / 2.0, 1) if high is not None and low is not None else None,
            "precip":        round(precip[i], 2) if i < len(precip) and precip[i] is not None else None,
            "precip_prob":   round(precip_prob[i], 0) if i < len(precip_prob) and precip_prob[i] is not None else None,
            "wind_speed_max": round(wind[i], 1) if i < len(wind) and wind[i] is not None else None,
            "wind_gust_max": round(gust[i], 1) if i < len(gust) and gust[i] is not None else None,
        })
    return rows


def parse_nws_forecast(city_id, data, today):
    """
    Parse NWS forecast periods into forecast row dicts.
    NWS returns alternating day/night periods. We pair them to get high/low.
    """
    rows = []
    periods = data.get("properties", {}).get("periods", [])

    # Group periods by date, extracting daytime high and nighttime low
    by_date = {}
    for p in periods:
        # startTime is like "2026-03-13T06:00:00-05:00"
        dt_str = p["startTime"][:10]
        is_day = p["isDaytime"]
        temp   = p["temperature"]

        if dt_str not in by_date:
            by_date[dt_str] = {"high": None, "low": None}

        if is_day:
            by_date[dt_str]["high"] = temp
        else:
            by_date[dt_str]["low"] = temp

    for date_str, temps in sorted(by_date.items()):
        target = date.fromisoformat(date_str)
        lead = (target - today).days
        if lead < 1 or lead > FORECAST_DAYS:
            continue

        high = temps["high"]
        low  = temps["low"]

        rows.append({
            "city_id":        city_id,
            "source_id":      3,  # nws_api
            "target_date":    date_str,
            "lead_days":      lead,
            "temp_high":      float(high) if high is not None else None,
            "temp_low":       float(low)  if low  is not None else None,
            "temp_avg":       round((high + low) / 2.0, 1) if high is not None and low is not None else None,
            "precip":         None,   # NWS text forecast doesn't give numeric precip
            "precip_prob":    None,
            "wind_speed_max": None,
            "wind_gust_max":  None,
        })
    return rows


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

UPSERT_SQL = """
    INSERT INTO forecasts (
        city_id, source_id, target_date, fetched_at, lead_days,
        temp_high, temp_low, temp_avg,
        precip, precip_prob,
        wind_speed_max, wind_gust_max,
        pipeline_run_id
    ) VALUES (
        %(city_id)s, %(source_id)s, %(target_date)s, NOW(), %(lead_days)s,
        %(temp_high)s, %(temp_low)s, %(temp_avg)s,
        %(precip)s, %(precip_prob)s,
        %(wind_speed_max)s, %(wind_gust_max)s,
        %(pipeline_run_id)s
    )
    ON CONFLICT (city_id, source_id, target_date, lead_days) DO UPDATE SET
        fetched_at     = NOW(),
        temp_high      = EXCLUDED.temp_high,
        temp_low       = EXCLUDED.temp_low,
        temp_avg       = EXCLUDED.temp_avg,
        precip         = EXCLUDED.precip,
        precip_prob    = EXCLUDED.precip_prob,
        wind_speed_max = EXCLUDED.wind_speed_max,
        wind_gust_max  = EXCLUDED.wind_gust_max,
        pipeline_run_id = EXCLUDED.pipeline_run_id;
"""


def upsert_forecasts(rows, pipeline_run_id):
    """Upsert forecast rows into DB. Returns count."""
    if not rows:
        return 0
    for r in rows:
        r["pipeline_run_id"] = pipeline_run_id
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=100)
        conn.close()
        return len(rows)
    except Exception as e:
        log.error(f"DB upsert error: {e}")
        return 0


# ---------------------------------------------------------------------------
# NWS grid coordinate caching
# ---------------------------------------------------------------------------

def ensure_nws_grid(city):
    """Make sure city has nws_grid_x/y. Fetch from NWS /points/ if missing."""
    if city["nws_grid_x"] is not None and city["nws_grid_y"] is not None:
        return city["nws_site"], city["nws_grid_x"], city["nws_grid_y"]

    log.info(f"Fetching NWS grid for {city['name']} ({city['latitude']}, {city['longitude']})")
    grid = fetch_nws_grid(float(city["latitude"]), float(city["longitude"]))
    if not grid or not grid["gridX"]:
        log.warning(f"Could not get NWS grid for {city['name']}")
        return None, None, None

    # Save to DB
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE cities SET nws_grid_x = %s, nws_grid_y = %s
                    WHERE id = %s
                """, (grid["gridX"], grid["gridY"], city["id"]))
        conn.close()
        log.info(f"  Saved grid: {grid['gridId']} ({grid['gridX']}, {grid['gridY']})")
    except Exception as e:
        log.error(f"  Failed to save grid for {city['name']}: {e}")

    return grid["gridId"], grid["gridX"], grid["gridY"]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    started_at = datetime.now()
    today = date.today()

    # Start pipeline run
    run_id = start_pipeline_run(started_at)
    if not run_id:
        log.error("Could not create pipeline run")
        sys.exit(1)

    log.info(f"Forecast ingest started (pipeline_run {run_id}), target dates: "
             f"{today + timedelta(days=1)} to {today + timedelta(days=FORECAST_DAYS)}")

    # Load cities
    conn = get_db_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, name, latitude, longitude, timezone, nws_site,
                   nws_grid_x, nws_grid_y
            FROM cities WHERE active = true ORDER BY id
        """)
        cities = cur.fetchall()
    conn.close()

    if not cities:
        log.error("No active cities found")
        finish_pipeline_run(run_id, 'error', error_message='No active cities')
        sys.exit(1)

    log.info(f"Processing {len(cities)} cities × 3 sources")

    total_rows = 0
    errors = 0

    for city in cities:
        city_name = city["name"]
        lat = float(city["latitude"])
        lon = float(city["longitude"])
        tz  = city["timezone"]

        # --- Open-Meteo GFS (source_id=1) ---
        try:
            data = fetch_open_meteo(lat, lon, "gfs_seamless", tz)
            if data:
                rows = parse_open_meteo(city["id"], 1, data, today)
                n = upsert_forecasts(rows, run_id)
                total_rows += n
                log.info(f"{city_name}: GFS — {n} rows")
            else:
                log.warning(f"{city_name}: GFS — no data")
                errors += 1
        except Exception as e:
            log.error(f"{city_name}: GFS — {e}")
            errors += 1

        # --- Open-Meteo ECMWF (source_id=2) ---
        try:
            data = fetch_open_meteo(lat, lon, "ecmwf_ifs025", tz)
            if data:
                rows = parse_open_meteo(city["id"], 2, data, today)
                n = upsert_forecasts(rows, run_id)
                total_rows += n
                log.info(f"{city_name}: ECMWF — {n} rows")
            else:
                log.warning(f"{city_name}: ECMWF — no data")
                errors += 1
        except Exception as e:
            log.error(f"{city_name}: ECMWF — {e}")
            errors += 1

        # --- NWS API (source_id=3) ---
        try:
            grid_id, grid_x, grid_y = ensure_nws_grid(city)
            if grid_id and grid_x is not None:
                data = fetch_nws_forecast(grid_id, grid_x, grid_y)
                if data:
                    rows = parse_nws_forecast(city["id"], data, today)
                    n = upsert_forecasts(rows, run_id)
                    total_rows += n
                    log.info(f"{city_name}: NWS — {n} rows")
                else:
                    log.warning(f"{city_name}: NWS — no data")
                    errors += 1
            else:
                log.warning(f"{city_name}: NWS — skipped (no grid)")
                errors += 1
        except Exception as e:
            log.error(f"{city_name}: NWS — {e}")
            errors += 1

        # Small delay to be polite to APIs
        time.sleep(0.3)

    # Finish
    status = 'success' if errors == 0 else 'error'
    notes = f"{len(cities)} cities, {total_rows} rows, {errors} errors"
    finish_pipeline_run(run_id, status, cities_processed=len(cities),
                        rows_inserted=total_rows, notes=notes)

    log.info("=" * 60)
    log.info(f"Forecast ingest complete.")
    log.info(f"  Total rows upserted: {total_rows}")
    log.info(f"  Errors: {errors}")
    log.info(f"  Pipeline run: {run_id}")


if __name__ == "__main__":
    main()
