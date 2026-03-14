"""
scheduler.py
------------
Daily pipeline scheduler. Runs all scripts in order:
  1. ingest_observations.py  (yesterday's actuals)
  2. ingest_forecasts.py     (7-day forecasts from 3 sources)
  3. verify_forecasts.py     (score past forecasts against actuals)
  4. train_and_predict.py    (retrain ML model + generate predictions)

Usage:
    python scheduler.py              # run all steps now
    python scheduler.py --step 2     # run only step 2 (forecasts)

For Windows Task Scheduler, create a task that runs daily at 8:00 AM:
    Program: python
    Arguments: F:/WeatherBetterV2/scripts/scheduler.py
    Start in: F:/WeatherBetterV2
"""

import sys
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from discord_notify import send_pipeline_summary, send_forecast_comparison

SCRIPTS_DIR = Path(r"F:/WeatherBetterV2/scripts")
PYTHON = sys.executable

STEPS = [
    ("ingest_observations.py",  "Ingest observations"),
    ("ingest_forecasts.py",     "Ingest forecasts"),
    ("verify_forecasts.py",     "Verify forecasts"),
    ("train_and_predict.py",    "Train ML & predict"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def run_step(script_name, description):
    """Run a script as a subprocess. Returns (success, detail_string)."""
    script_path = SCRIPTS_DIR / script_name
    log.info(f"--- {description} ({script_name}) ---")

    try:
        result = subprocess.run(
            [PYTHON, str(script_path)],
            cwd=str(SCRIPTS_DIR.parent),
            capture_output=True,
            text=True,
            timeout=600,  # 10 min max per step
        )

        output_lines = []
        # Print stdout/stderr
        if result.stdout:
            for line in result.stdout.strip().splitlines():
                log.info(f"  {line}")
                output_lines.append(line)
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                # Filter out pandas warnings
                if "UserWarning" in line or "consider using" in line:
                    continue
                log.warning(f"  {line}")
                output_lines.append(line)

        if result.returncode != 0:
            log.error(f"  FAILED with exit code {result.returncode}")
            return False, _extract_summary(output_lines, failed=True)

        log.info(f"  OK")
        return True, _extract_summary(output_lines)

    except subprocess.TimeoutExpired:
        log.error(f"  TIMEOUT after 600s")
        return False, "Timed out after 600s"
    except Exception as e:
        log.error(f"  ERROR: {e}")
        return False, str(e)


def _extract_summary(lines, failed=False):
    """Pull the last meaningful summary line from script output."""
    keywords = ["upserted", "inserted", "verified", "complete", "observations",
                "rows", "predictions", "stored", "errors", "MAE"]
    for line in reversed(lines):
        stripped = line.strip()
        if any(kw in stripped.lower() for kw in keywords):
            return stripped
    if lines:
        return lines[-1].strip()
    return "No output" if not failed else "Failed with no output"


def main():
    import argparse
    parser = argparse.ArgumentParser(description="WeatherBetterV2 daily pipeline scheduler")
    parser.add_argument("--step", type=int, help="Run only this step (1-4)")
    args = parser.parse_args()

    start_time = datetime.now()
    log.info(f"Pipeline started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    if args.step:
        if args.step < 1 or args.step > len(STEPS):
            log.error(f"Invalid step {args.step}. Must be 1-{len(STEPS)}")
            sys.exit(1)
        steps = [STEPS[args.step - 1]]
    else:
        steps = STEPS

    results = []
    for script_name, description in steps:
        ok, detail = run_step(script_name, description)
        results.append((description, ok, detail))
        if not ok:
            log.warning(f"Step failed — continuing with remaining steps")

    end_time = datetime.now()

    # Summary
    log.info("")
    log.info("=" * 60)
    log.info("PIPELINE SUMMARY")
    log.info("=" * 60)
    for desc, ok, detail in results:
        status = "OK" if ok else "FAILED"
        log.info(f"  {desc:30s}  {status}")
        if detail:
            log.info(f"    {detail}")

    # --- Discord notifications ---
    try:
        send_pipeline_summary(results, start_time, end_time)
        log.info("Discord notification sent")
    except Exception as e:
        log.warning(f"Discord notification failed: {e}")

    # Send forecast comparison if ML step ran successfully
    ml_ran = any(desc == "Train ML & predict" and ok for desc, ok, _ in results)
    if ml_ran:
        try:
            _send_forecast_embed()
        except Exception as e:
            log.warning(f"Discord forecast embed failed: {e}")

    failures = sum(1 for _, ok, _ in results if not ok)
    if failures:
        log.error(f"\n{failures} step(s) failed")
        sys.exit(1)
    else:
        log.info(f"\nAll steps completed successfully")


def _send_forecast_embed():
    """Query tomorrow's forecasts from DB and send to Discord."""
    import psycopg2
    import psycopg2.extras
    from datetime import date, timedelta

    db_key = Path(r"F:/weatherbetterv2/tokens/db/db.key")
    lines = db_key.read_text().strip().splitlines()
    conn = psycopg2.connect(
        host=lines[0], port=int(lines[1]), dbname=lines[2],
        user=lines[3], password=lines[4]
    )

    today = date.today()
    tomorrow = today + timedelta(days=1)

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT c.name AS city_name, f.source_id, f.temp_high
            FROM forecasts f
            JOIN cities c ON c.id = f.city_id
            WHERE f.target_date = %s AND f.lead_days = 1
            ORDER BY c.id, f.source_id
        """, (tomorrow,))
        rows = cur.fetchall()
    conn.close()

    # Pivot by city
    by_city = {}
    for r in rows:
        name = r["city_name"]
        if name not in by_city:
            by_city[name] = {"city_name": name, "gfs_high": None,
                             "ecmwf_high": None, "nws_high": None, "ml_high": None}
        sid = r["source_id"]
        high = float(r["temp_high"]) if r["temp_high"] else None
        if sid == 1:
            by_city[name]["gfs_high"] = high
        elif sid == 2:
            by_city[name]["ecmwf_high"] = high
        elif sid == 3:
            by_city[name]["nws_high"] = high
        elif sid == 4:
            by_city[name]["ml_high"] = high

    predictions = [v for v in by_city.values() if v["ml_high"] is not None]
    if predictions:
        send_forecast_comparison(predictions, tomorrow.strftime("%b %d"))
        log.info("Discord forecast comparison sent")


if __name__ == "__main__":
    main()
