"""
backfill.py
-----------
Backfills the `observations` table with 7 years of historical daily weather data
(Jan 1, 2019 → yesterday) from NOAA Climate Data Online (GHCND) for all 20 cities.

Usage:
    python backfill.py

Requirements:
    pip install psycopg2-binary requests python-dotenv

NOAA token expected at: F:/weatherbetterv2/tokens/noaa/noaa.key
"""

import os
import sys
import time
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
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

NOAA_TOKEN_PATH = Path(r"F:/weatherbetterv2/tokens/noaa/noaa.key")
NOAA_BASE_URL   = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"

BACKFILL_START  = date(2019, 1, 1)
BACKFILL_END    = date.today() - timedelta(days=1)   # through yesterday

# NOAA limit: 5 req/s, max 1000 results per call, max 1 year per call
MAX_WORKERS     = 5
CHUNK_DAYS      = 365        # one API call per city per year
RETRY_LIMIT     = 4
RETRY_BACKOFF   = [2, 4, 8, 16]   # seconds between retries

DATATYPES = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND", "WSF2", "WSF5"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_token() -> str:
    if not NOAA_TOKEN_PATH.exists():
        log.error(f"NOAA token not found at {NOAA_TOKEN_PATH}")
        sys.exit(1)
    token = NOAA_TOKEN_PATH.read_text().strip()
    if not token:
        log.error("NOAA token file is empty.")
        sys.exit(1)
    return token


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def date_chunks(start: date, end: date, chunk_days: int):
    """Yield (chunk_start, chunk_end) pairs spanning [start, end]."""
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def fetch_noaa_chunk(token: str, station_id: str, start: date, end: date) -> list[dict]:
    headers = {"token": token}
    all_results = []
    offset = 1

    while True:
        params = {
            "datasetid":  "GHCND",
            "stationid":  f"GHCND:{station_id}",
            "startdate":  start.isoformat(),
            "enddate":    end.isoformat(),
            "datatypeid": ",".join(DATATYPES),
            "units":      "standard",
            "limit":      1000,
            "offset":     offset,
        }

        for attempt in range(RETRY_LIMIT):
            try:
                resp = requests.get(NOAA_BASE_URL, headers=headers,
                                    params=params, timeout=60)
                if resp.status_code == 429:
                    wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    log.warning(f"Rate limited on {station_id} — waiting {wait}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                total   = data.get("metadata", {}).get("resultset", {}).get("count", 0)
                all_results.extend(results)
                log.debug(f"{station_id} {start}→{end} offset={offset}: "
                          f"got {len(results)}, total={total}")

                if offset + len(results) - 1 >= total or len(results) < 1000:
                    return all_results   # got everything
                else:
                    offset += 1000      # fetch next page
                    break               # break retry loop, continue pagination
            except requests.RequestException as e:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                log.warning(f"Request error ({station_id} {start}→{end}) attempt "
                            f"{attempt+1}/{RETRY_LIMIT}: {e}. Retrying in {wait}s")
                time.sleep(wait)
                if attempt == RETRY_LIMIT - 1:
                    log.error(f"Failed to fetch {station_id} {start}→{end} "
                              f"after {RETRY_LIMIT} attempts")
                    return all_results

def parse_results(raw: list[dict]) -> dict[str, dict]:
    """
    Pivot flat NOAA results into {date_str: {datatype: value, ...}}.
    NOAA returns one row per (date, datatype).
    """
    by_date: dict[str, dict] = {}
    for row in raw:
        d = row["date"][:10]          # "2019-03-15T00:00:00" → "2019-03-15"
        dt = row["datatype"]
        val = row["value"]
        by_date.setdefault(d, {})[dt] = val
    return by_date


def build_observation(city_id: int, obs_date: str, data: dict) -> dict | None:
    """
    Convert a dict of NOAA datatype→value into an observations row.
    Returns None if we don't have at least TMAX and TMIN.
    """
    tmax = data.get("TMAX")
    tmin = data.get("TMIN")
    if tmax is None or tmin is None:
        return None

    # With units='standard', NOAA returns final values directly:
    #   TMAX/TMIN: °F, PRCP: inches, SNOW/SNWD: inches, wind: mph
    # No division needed.
    high = round(tmax, 1)
    low  = round(tmin, 1)

    def safe_round(v, digits=2):
        return round(v, digits) if v is not None else None

    # Wind: prefer AWND, fall back to max(WSF2, WSF5)
    wind_raw = data.get("AWND") or data.get("WSF5") or data.get("WSF2")

    return {
        "city_id":        city_id,
        "obs_date":       obs_date,
        "temp_high":      high,
        "temp_low":       low,
        "temp_avg":       round((high + low) / 2.0, 1),
        "precip":         safe_round(data.get("PRCP")),
        "snow":           safe_round(data.get("SNOW"), 1),
        "snow_depth":     safe_round(data.get("SNWD"), 1),
        "wind_speed_max": safe_round(wind_raw, 1),
        "data_source":    "noaa_ghcnd",
        "is_backfill":    True,
    }


# ---------------------------------------------------------------------------
# Worker — one city × one year chunk
# ---------------------------------------------------------------------------

def process_chunk(args) -> tuple[str, int, int]:
    """
    args: (token, city_id, city_name, station_id, chunk_start, chunk_end)
    Returns: (label, inserted_count, skipped_count)
    """
    token, city_id, city_name, station_id, chunk_start, chunk_end = args
    label = f"{city_name} {chunk_start.year}"

    raw = fetch_noaa_chunk(token, station_id, chunk_start, chunk_end)
    if not raw:
        log.warning(f"{label}: no data returned")
        return label, 0, 0

    by_date = parse_results(raw)
    rows = []
    skipped = 0
    for obs_date, data in sorted(by_date.items()):
        row = build_observation(city_id, obs_date, data)
        if row:
            rows.append(row)
        else:
            skipped += 1

    if not rows:
        log.warning(f"{label}: parsed 0 valid rows (skipped {skipped})")
        return label, 0, skipped

    # Upsert into observations — skip if row already exists
    insert_sql = """
        INSERT INTO observations (
            city_id, obs_date,
            temp_high, temp_low, temp_avg,
            precip, snow, snow_depth,
            wind_speed_max,
            data_source, is_backfill
        ) VALUES (
            %(city_id)s, %(obs_date)s,
            %(temp_high)s, %(temp_low)s, %(temp_avg)s,
            %(precip)s, %(snow)s, %(snow_depth)s,
            %(wind_speed_max)s,
            %(data_source)s, %(is_backfill)s
        )
        ON CONFLICT (city_id, obs_date) DO UPDATE SET
            temp_high      = EXCLUDED.temp_high,
            temp_low       = EXCLUDED.temp_low,
            temp_avg       = EXCLUDED.temp_avg,
            precip         = EXCLUDED.precip,
            snow           = EXCLUDED.snow,
            snow_depth     = EXCLUDED.snow_depth,
            wind_speed_max = EXCLUDED.wind_speed_max,
            data_source    = EXCLUDED.data_source,
            is_backfill    = EXCLUDED.is_backfill;
    """

    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=200)
        conn.close()
    except Exception as e:
        log.error(f"{label}: DB error — {e}")
        return label, 0, skipped

    log.info(f"{label}: upserted {len(rows)} rows, skipped {skipped} (no TMAX/TMIN)")
    return label, len(rows), skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def log_pipeline_run(status, cities_processed=0, rows_inserted=0, rows_updated=0,
                     error_message=None, notes=None, started_at=None):
    """Insert a row into pipeline_runs."""
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
                """, ('backfill', started_at, status,
                      cities_processed, rows_inserted, rows_updated,
                      error_message, notes))
        conn.close()
    except Exception as e:
        log.error(f"Failed to log pipeline run: {e}")


def main():
    started_at = datetime.now()
    token = load_token()
    log.info(f"NOAA token loaded from {NOAA_TOKEN_PATH}")

    # Load cities from DB
    conn = get_db_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT id, name, ghcnd_station FROM cities WHERE ghcnd_station IS NOT NULL ORDER BY id;")
        cities = cur.fetchall()
    conn.close()

    if not cities:
        log.error("No cities with ghcnd_station found. Make sure the column is populated.")
        log.error("Run:  ALTER TABLE cities ADD COLUMN IF NOT EXISTS ghcnd_station TEXT;")
        log.error("Then update each row with the GHCND station ID (e.g. 'USW00094728').")
        log_pipeline_run('error', error_message='No cities with ghcnd_station found',
                         started_at=started_at)
        sys.exit(1)

    log.info(f"Backfilling {len(cities)} cities from {BACKFILL_START} to {BACKFILL_END}")

    # Build work list: (token, city_id, city_name, station, chunk_start, chunk_end)
    work = []
    for city in cities:
        for chunk_start, chunk_end in date_chunks(BACKFILL_START, BACKFILL_END, CHUNK_DAYS):
            work.append((token, city["id"], city["name"], city["ghcnd_station"],
                         chunk_start, chunk_end))

    log.info(f"Total API calls to make: {len(work)} "
             f"({len(cities)} cities × ~{len(work)//len(cities)} chunks each)")

    total_inserted = 0
    total_skipped  = 0
    completed      = 0
    errors         = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_chunk, w): w for w in work}
        for future in as_completed(futures):
            try:
                label, inserted, skipped = future.result()
                total_inserted += inserted
                total_skipped  += skipped
            except Exception as e:
                w = futures[future]
                log.error(f"Unhandled error on {w[2]} {w[4]}: {e}")
                errors += 1
            completed += 1
            if completed % 10 == 0:
                log.info(f"Progress: {completed}/{len(work)} chunks done — "
                         f"{total_inserted} rows upserted so far")

    log.info("=" * 60)
    log.info(f"Backfill complete.")
    log.info(f"  Total upserted : {total_inserted:,}")
    log.info(f"  Total skipped  : {total_skipped:,}  (missing TMAX or TMIN)")
    log.info(f"  Chunks processed: {completed}/{len(work)}")
    log.info(f"  Errors: {errors}")

    status = 'success' if errors == 0 else 'error'
    log_pipeline_run(status, cities_processed=len(cities),
                     rows_inserted=total_inserted,
                     notes=f"{BACKFILL_START} to {BACKFILL_END}, {completed}/{len(work)} chunks, {total_skipped} skipped",
                     started_at=started_at)


if __name__ == "__main__":
    main()