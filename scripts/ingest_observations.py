"""
ingest_observations.py
----------------------
Daily observation ingester. Fetches NWS CLI (Climate) reports for all 20 cities,
parses the plain-text reports, stores raw text in `cli_raw`, and upserts parsed
values into `observations`.

Source: https://api.weather.gov/products/types/CLI/locations/{nws_issuedby}
Target tables: cli_raw, observations

Usage:
    python ingest_observations.py
"""

import re
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
            "host":     os.environ["DB_HOST"].strip(),
            "port":     int(os.environ.get("DB_PORT", "5432").strip()),
            "dbname":   os.environ["DB_NAME"].strip(),
            "user":     os.environ["DB_USER"].strip(),
            "password": os.environ["DB_PASSWORD"].strip(),
            "sslmode":  os.environ.get("DB_SSLMODE", "require").strip(),
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

NWS_PRODUCTS_URL = "https://api.weather.gov/products/types/CLI/locations"
NWS_USER_AGENT   = "WeatherBetterV2 (weather@example.com)"

RETRY_LIMIT  = 3
RETRY_BACKOFF = [2, 5, 10]

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
                """, ('observation', started_at))
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
# NWS API fetch
# ---------------------------------------------------------------------------

def _retry_get(url, timeout=30):
    headers = {"User-Agent": NWS_USER_AGENT}
    for attempt in range(RETRY_LIMIT):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            log.warning(f"Request error ({url}) attempt {attempt+1}: {e}")
            if attempt < RETRY_LIMIT - 1:
                time.sleep(wait)
    return None


def fetch_latest_cli(nws_issuedby):
    """
    Fetch the most recent CLI product for a station.
    Returns (product_text, issuance_time) or (None, None).
    """
    url = f"{NWS_PRODUCTS_URL}/{nws_issuedby}"
    data = _retry_get(url)
    if not data:
        return None, None

    products = data.get("@graph", [])
    if not products:
        return None, None

    # Get the most recent product
    prod = products[0]
    prod_url = prod.get("@id")
    issuance = prod.get("issuanceTime", "")

    if not prod_url:
        return None, None

    prod_data = _retry_get(prod_url)
    if not prod_data:
        return None, None

    return prod_data.get("productText", ""), issuance


# ---------------------------------------------------------------------------
# CLI Report Parser
# ---------------------------------------------------------------------------

def _parse_number(text):
    """Parse a number from CLI text. Handles negatives, MM, T."""
    if not text or text.strip() in ("", "MM"):
        return None
    text = text.strip()
    if text == "T":
        return 0.0  # trace
    try:
        return float(text)
    except ValueError:
        return None


def _is_trace(text):
    """Check if a value is a trace amount."""
    return text is not None and text.strip() == "T"


def _normalize_time(text):
    """Normalize NWS time strings like '353 PM' → '3:53 PM'."""
    if not text:
        return None
    text = text.strip()
    import re
    m = re.match(r'^(\d{1,2}):?(\d{2})\s*([AP]M)$', text)
    if m:
        return f"{m.group(1)}:{m.group(2)} {m.group(3)}"
    return None


def parse_cli_date(text):
    """
    Extract the observation date from the CLI report header.
    Looks for: '...THE {CITY} CLIMATE SUMMARY FOR {MONTH} {DAY} {YEAR}...'
    """
    m = re.search(
        r'CLIMATE SUMMARY FOR\s+(\w+)\s+(\d{1,2})\s+(\d{4})',
        text, re.IGNORECASE
    )
    if not m:
        return None
    try:
        month_str, day_str, year_str = m.group(1), m.group(2), m.group(3)
        from calendar import month_name
        month_names = {name.upper(): i for i, name in enumerate(month_name) if name}
        month_num = month_names.get(month_str.upper())
        if not month_num:
            # Try abbreviation
            from calendar import month_abbr
            month_abbrs = {name.upper(): i for i, name in enumerate(month_abbr) if name}
            month_num = month_abbrs.get(month_str.upper())
        if not month_num:
            return None
        return date(int(year_str), month_num, int(day_str))
    except (ValueError, KeyError):
        return None


def parse_cli_report(text):
    """
    Parse a NWS CLI report into a dict of observation values.
    Returns dict with keys matching observations table columns, or None on failure.
    """
    if not text:
        return None

    obs_date = parse_cli_date(text)
    if not obs_date:
        log.warning("Could not parse date from CLI report")
        return None

    result = {"obs_date": obs_date, "precip_is_trace": False}

    # --- Temperature ---
    # MAXIMUM         65   4:25 PM  86    1974  72     -7       78
    # MINIMUM         51  11:27 PM  33    1969  55     -4       51
    # AVERAGE         58                        63     -5       65
    temp_max_m = re.search(
        r'MAXIMUM\s+(-?\d+)[A-Z]?\s+(\d{1,2}:?\d{2}\s*[AP]M)?',
        text
    )
    if temp_max_m:
        result["temp_high"] = float(temp_max_m.group(1))
        if temp_max_m.group(2):
            result["temp_high_time"] = _normalize_time(temp_max_m.group(2))

    temp_min_m = re.search(
        r'MINIMUM\s+(-?\d+)[A-Z]?\s+(\d{1,2}:?\d{2}\s*[AP]M)?',
        text
    )
    if temp_min_m:
        result["temp_low"] = float(temp_min_m.group(1))
        if temp_min_m.group(2):
            result["temp_low_time"] = _normalize_time(temp_min_m.group(2))

    temp_avg_m = re.search(r'AVERAGE\s+(-?\d+)', text)
    if temp_avg_m:
        result["temp_avg"] = float(temp_avg_m.group(1))

    # Normal/departure — look for values after the record section on MAXIMUM line
    # Format: MAXIMUM  65  4:25 PM  86  1974  72  -7  78
    #                                         normal depart last
    temp_max_full = re.search(
        r'MAXIMUM\s+(-?\d+)[A-Z]?\s+\d{1,2}:?\d{2}\s*[AP]M\s+(-?\d+|MM)\s+\d{4}\s+(-?\d+|MM)\s+(-?\d+|MM)',
        text
    )
    if temp_max_full:
        result["temp_high_normal"] = _parse_number(temp_max_full.group(3))
        result["temp_high_depart"] = _parse_number(temp_max_full.group(4))

    temp_low_full = re.search(
        r'MINIMUM\s+(-?\d+)[A-Z]?\s+\d{1,2}:?\d{2}\s*[AP]M\s+(-?\d+|MM)\s+\d{4}\s+(-?\d+|MM)\s+(-?\d+|MM)',
        text
    )
    if temp_low_full:
        result["temp_low_normal"] = _parse_number(temp_low_full.group(3))

    # --- Precipitation ---
    # YESTERDAY        0.09          1.17 1993   0.13  -0.04     0.00
    # or: YESTERDAY        T
    precip_section = re.search(r'PRECIPITATION.*?(?=SNOWFALL|DEGREE DAYS|WIND)', text, re.DOTALL)
    if precip_section:
        precip_text = precip_section.group()
        precip_m = re.search(r'YESTERDAY\s+([\d.]+|T|MM)', precip_text)
        if precip_m:
            val = precip_m.group(1)
            result["precip_is_trace"] = _is_trace(val)
            if val == "T":
                result["precip"] = 0.0
            else:
                result["precip"] = _parse_number(val)

    # --- Snowfall ---
    snow_section = re.search(r'SNOWFALL.*?(?=DEGREE DAYS|WIND)', text, re.DOTALL)
    if snow_section:
        snow_text = snow_section.group()
        snow_m = re.search(r'YESTERDAY\s+([\d.]+|T|MM)', snow_text)
        if snow_m:
            result["snow"] = _parse_number(snow_m.group(1))

        depth_m = re.search(r'SNOW DEPTH\s+([\d.]+|T|MM)', snow_text)
        if depth_m:
            result["snow_depth"] = _parse_number(depth_m.group(1))

    # --- Wind ---
    wind_speed_m = re.search(r'HIGHEST WIND SPEED\s+(\d+)', text)
    if wind_speed_m:
        result["wind_speed_max"] = float(wind_speed_m.group(1))

    wind_gust_m = re.search(r'HIGHEST GUST SPEED\s+(\d+)', text)
    if wind_gust_m:
        result["wind_gust_max"] = float(wind_gust_m.group(1))

    wind_avg_m = re.search(r'AVERAGE WIND SPEED\s+([\d.]+)', text)
    if wind_avg_m:
        result["wind_avg"] = float(wind_avg_m.group(1))

    # --- Sky Cover ---
    sky_m = re.search(r'AVERAGE SKY COVER\s+([\d.]+)', text)
    if sky_m:
        result["sky_cover_avg"] = float(sky_m.group(1))

    # --- Humidity ---
    hum_high_m = re.search(r'HIGHEST\s+(\d+)\s+\d{1,2}:\d{2}\s*[AP]M', text[text.find("RELATIVE HUMIDITY"):] if "RELATIVE HUMIDITY" in text else "")
    if hum_high_m:
        result["humidity_high"] = float(hum_high_m.group(1))

    hum_low_m = re.search(r'LOWEST\s+(\d+)\s+\d{1,2}:\d{2}\s*[AP]M', text[text.find("RELATIVE HUMIDITY"):] if "RELATIVE HUMIDITY" in text else "")
    if hum_low_m:
        result["humidity_low"] = float(hum_low_m.group(1))

    hum_avg_m = re.search(r'AVERAGE\s+(\d+)', text[text.find("RELATIVE HUMIDITY"):] if "RELATIVE HUMIDITY" in text else "")
    if hum_avg_m:
        result["humidity_avg"] = float(hum_avg_m.group(1))

    # Must have at least temp_high and temp_low
    if "temp_high" not in result or "temp_low" not in result:
        log.warning(f"CLI report for {obs_date} missing TMAX or TMIN")
        return None

    return result


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------

def upsert_cli_raw(city_id, obs_date, raw_text):
    """Insert or update cli_raw. Returns the cli_raw.id."""
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cli_raw (city_id, obs_date, version, fetched_at, raw_text, is_final)
                VALUES (%s, %s, 1, NOW(), %s, true)
                ON CONFLICT (city_id, obs_date, version) DO UPDATE SET
                    fetched_at = NOW(),
                    raw_text = EXCLUDED.raw_text
                RETURNING id
            """, (city_id, obs_date, raw_text))
            cli_raw_id = cur.fetchone()[0]
    conn.close()
    return cli_raw_id


def upsert_observation(city_id, parsed, cli_raw_id):
    """Upsert a parsed observation into the observations table."""
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO observations (
                    city_id, obs_date,
                    temp_high, temp_high_time, temp_low, temp_low_time, temp_avg,
                    precip, precip_is_trace, snow, snow_depth,
                    wind_speed_max, wind_gust_max, wind_avg,
                    humidity_avg, humidity_high, humidity_low,
                    sky_cover_avg,
                    temp_high_normal, temp_low_normal, temp_high_depart,
                    cli_raw_id, data_source, is_backfill
                ) VALUES (
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (city_id, obs_date) DO UPDATE SET
                    temp_high       = EXCLUDED.temp_high,
                    temp_high_time  = EXCLUDED.temp_high_time,
                    temp_low        = EXCLUDED.temp_low,
                    temp_low_time   = EXCLUDED.temp_low_time,
                    temp_avg        = EXCLUDED.temp_avg,
                    precip          = EXCLUDED.precip,
                    precip_is_trace = EXCLUDED.precip_is_trace,
                    snow            = EXCLUDED.snow,
                    snow_depth      = EXCLUDED.snow_depth,
                    wind_speed_max  = EXCLUDED.wind_speed_max,
                    wind_gust_max   = EXCLUDED.wind_gust_max,
                    wind_avg        = EXCLUDED.wind_avg,
                    humidity_avg    = EXCLUDED.humidity_avg,
                    humidity_high   = EXCLUDED.humidity_high,
                    humidity_low    = EXCLUDED.humidity_low,
                    sky_cover_avg   = EXCLUDED.sky_cover_avg,
                    temp_high_normal = EXCLUDED.temp_high_normal,
                    temp_low_normal  = EXCLUDED.temp_low_normal,
                    temp_high_depart = EXCLUDED.temp_high_depart,
                    cli_raw_id      = EXCLUDED.cli_raw_id,
                    data_source     = EXCLUDED.data_source,
                    is_backfill     = EXCLUDED.is_backfill,
                    updated_at      = NOW()
            """, (
                city_id, parsed["obs_date"],
                parsed.get("temp_high"), parsed.get("temp_high_time"),
                parsed.get("temp_low"), parsed.get("temp_low_time"),
                parsed.get("temp_avg"),
                parsed.get("precip"), parsed.get("precip_is_trace"),
                parsed.get("snow"), parsed.get("snow_depth"),
                parsed.get("wind_speed_max"), parsed.get("wind_gust_max"),
                parsed.get("wind_avg"),
                parsed.get("humidity_avg"), parsed.get("humidity_high"),
                parsed.get("humidity_low"),
                parsed.get("sky_cover_avg"),
                parsed.get("temp_high_normal"), parsed.get("temp_low_normal"),
                parsed.get("temp_high_depart"),
                cli_raw_id, "nws_cli", False
            ))
    conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    started_at = datetime.now()
    run_id = start_pipeline_run(started_at)
    if not run_id:
        log.error("Could not create pipeline run")
        sys.exit(1)

    log.info(f"Observation ingest started (pipeline_run {run_id})")

    # Load cities
    conn = get_db_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT id, name, nws_issuedby
            FROM cities WHERE active = true ORDER BY id
        """)
        cities = cur.fetchall()
    conn.close()

    if not cities:
        log.error("No active cities found")
        finish_pipeline_run(run_id, 'error', error_message='No active cities')
        sys.exit(1)

    # Group cities by nws_issuedby to handle shared stations
    # (San Antonio and Austin both use EWX but have different issuedby codes: SAT, AUS)
    log.info(f"Processing {len(cities)} cities")

    total_inserted = 0
    errors = 0

    for city in cities:
        city_name = city["name"]
        nws_code = city["nws_issuedby"]

        try:
            raw_text, issuance = fetch_latest_cli(nws_code)
            if not raw_text:
                log.warning(f"{city_name} ({nws_code}): no CLI report found")
                errors += 1
                continue

            parsed = parse_cli_report(raw_text)
            if not parsed:
                log.warning(f"{city_name} ({nws_code}): could not parse CLI report")
                errors += 1
                continue

            obs_date = parsed["obs_date"]

            # Store raw text
            cli_raw_id = upsert_cli_raw(city["id"], obs_date, raw_text)

            # Upsert observation
            upsert_observation(city["id"], parsed, cli_raw_id)
            total_inserted += 1

            log.info(f"{city_name}: {obs_date} — high={parsed.get('temp_high')} "
                     f"low={parsed.get('temp_low')} precip={parsed.get('precip')}")

        except Exception as e:
            log.error(f"{city_name} ({nws_code}): {e}")
            errors += 1

        time.sleep(0.3)

    status = 'success' if errors == 0 else 'error'
    notes = f"{len(cities)} cities, {total_inserted} observations, {errors} errors"
    finish_pipeline_run(run_id, status, cities_processed=len(cities),
                        rows_inserted=total_inserted, notes=notes)

    log.info("=" * 60)
    log.info(f"Observation ingest complete.")
    log.info(f"  Observations upserted: {total_inserted}")
    log.info(f"  Errors: {errors}")
    log.info(f"  Pipeline run: {run_id}")


if __name__ == "__main__":
    main()
