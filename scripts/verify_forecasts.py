"""
verify_forecasts.py
-------------------
Forecast verification script. Runs after observations are ingested.
Finds all forecasts whose target_date now has an observation but no verification
row, computes error metrics, and inserts into `forecast_verifications`.

Usage:
    python verify_forecasts.py
"""

import sys
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
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
                """, ('verification', started_at))
                run_id = cur.fetchone()[0]
        conn.close()
        return run_id
    except Exception as e:
        log.error(f"Failed to start pipeline run: {e}")
        return None


def finish_pipeline_run(run_id, status, rows_inserted=0,
                        error_message=None, notes=None):
    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_runs
                    SET finished_at = NOW(), status = %s,
                        rows_inserted = %s,
                        error_message = %s, notes = %s
                    WHERE id = %s
                """, (status, rows_inserted, error_message, notes, run_id))
        conn.close()
    except Exception as e:
        log.error(f"Failed to finish pipeline run: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    started_at = datetime.now()
    run_id = start_pipeline_run(started_at)
    if not run_id:
        log.error("Could not create pipeline run")
        sys.exit(1)

    log.info(f"Forecast verification started (pipeline_run {run_id})")

    conn = get_db_conn()

    # Find all unverified forecasts that have a matching observation
    # A forecast is unverified if it has no row in forecast_verifications
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT
                f.id         AS forecast_id,
                f.city_id,
                f.source_id,
                f.target_date,
                f.lead_days,
                f.temp_high  AS f_high,
                f.temp_low   AS f_low,
                o.id         AS observation_id,
                o.temp_high  AS o_high,
                o.temp_low   AS o_low,
                o.temp_high_normal,
                c.name       AS city_name,
                fs.name      AS source_name
            FROM forecasts f
            JOIN observations o
                ON o.city_id = f.city_id AND o.obs_date = f.target_date
            JOIN cities c ON c.id = f.city_id
            JOIN forecast_sources fs ON fs.id = f.source_id
            LEFT JOIN forecast_verifications fv ON fv.forecast_id = f.id
            WHERE fv.id IS NULL
              AND f.temp_high IS NOT NULL
              AND o.temp_high IS NOT NULL
            ORDER BY f.target_date, f.city_id, f.source_id
        """)
        rows = cur.fetchall()

    if not rows:
        log.info("No unverified forecasts with matching observations found.")
        finish_pipeline_run(run_id, 'success', rows_inserted=0,
                           notes='No forecasts to verify')
        conn.close()
        return

    log.info(f"Found {len(rows)} forecasts to verify")

    # Build verification rows
    verifications = []
    for r in rows:
        f_high = float(r["f_high"])
        o_high = float(r["o_high"])
        high_error = round(f_high - o_high, 1)
        high_abs = abs(high_error)

        # low error (nullable — some forecasts may not have temp_low)
        low_error = None
        low_abs = None
        if r["f_low"] is not None and r["o_low"] is not None:
            low_error = round(float(r["f_low"]) - float(r["o_low"]), 1)
            low_abs = abs(low_error)

        # high_bias: same as high_error (positive = forecast too warm)
        high_bias = high_error

        # Within-X thresholds for Kalshi accuracy
        high_within_1 = high_abs <= 1.0
        high_within_2 = high_abs <= 2.0
        high_within_3 = high_abs <= 3.0
        high_within_5 = high_abs <= 5.0

        # Normal comparison (nullable — only if observation has normal)
        high_above_normal = None
        forecast_above_normal = None
        normal_direction_correct = None
        if r["temp_high_normal"] is not None:
            normal = float(r["temp_high_normal"])
            high_above_normal = o_high > normal
            forecast_above_normal = f_high > normal
            normal_direction_correct = high_above_normal == forecast_above_normal

        verifications.append({
            "forecast_id":              r["forecast_id"],
            "observation_id":           r["observation_id"],
            "pipeline_run_id":          run_id,
            "high_error":               high_error,
            "high_abs_error":           high_abs,
            "high_bias":                high_bias,
            "low_error":                low_error,
            "low_abs_error":            low_abs,
            "high_within_1f":           high_within_1,
            "high_within_2f":           high_within_2,
            "high_within_3f":           high_within_3,
            "high_within_5f":           high_within_5,
            "high_above_normal":        high_above_normal,
            "forecast_above_normal":    forecast_above_normal,
            "normal_direction_correct": normal_direction_correct,
        })

    # Insert verifications
    insert_sql = """
        INSERT INTO forecast_verifications (
            forecast_id, observation_id, pipeline_run_id,
            high_error, high_abs_error, high_bias,
            low_error, low_abs_error,
            high_within_1f, high_within_2f, high_within_3f, high_within_5f,
            high_above_normal, forecast_above_normal, normal_direction_correct,
            verified_at
        ) VALUES (
            %(forecast_id)s, %(observation_id)s, %(pipeline_run_id)s,
            %(high_error)s, %(high_abs_error)s, %(high_bias)s,
            %(low_error)s, %(low_abs_error)s,
            %(high_within_1f)s, %(high_within_2f)s, %(high_within_3f)s, %(high_within_5f)s,
            %(high_above_normal)s, %(forecast_above_normal)s, %(normal_direction_correct)s,
            NOW()
        )
        ON CONFLICT (forecast_id) DO NOTHING
    """

    try:
        with conn:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, insert_sql, verifications, page_size=200)
        inserted = len(verifications)
    except Exception as e:
        log.error(f"DB error inserting verifications: {e}")
        finish_pipeline_run(run_id, 'error', error_message=str(e))
        conn.close()
        return

    conn.close()

    # Summary stats
    if verifications:
        avg_abs = sum(v["high_abs_error"] for v in verifications) / len(verifications)
        within_3 = sum(1 for v in verifications if v["high_within_3f"]) / len(verifications) * 100
        log.info(f"  Mean absolute error (high): {avg_abs:.1f}°F")
        log.info(f"  Within 3°F: {within_3:.0f}%")

    finish_pipeline_run(run_id, 'success', rows_inserted=inserted,
                        notes=f"Verified {inserted} forecasts")

    log.info("=" * 60)
    log.info(f"Verification complete.")
    log.info(f"  Verifications inserted: {inserted}")
    log.info(f"  Pipeline run: {run_id}")


if __name__ == "__main__":
    main()
