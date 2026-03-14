"""
train_and_predict.py
--------------------
Daily ML pipeline: retrains an XGBoost model on all historical observations,
generates 7-day temperature predictions for each city, stores them as
source_id=4 (ml_ensemble) in the forecasts table, and prints a comparison
of all sources side by side.

Features (climatological — always available):
  - day_of_year (sin/cos encoded for cyclical seasonality)
  - city_id (label encoded)
  - lag features: temp_high for past 1, 3, 7, 14 days
  - rolling means: 7-day and 30-day temp_high
  - year trend

Features (forecast — available once data accumulates):
  - GFS predicted high (source_id=1)
  - ECMWF predicted high (source_id=2)
  - NWS predicted high (source_id=3)
  - lead_days

Usage:
    python train_and_predict.py
"""

import sys
import math
import logging
import pickle
import numpy as np
import pandas as pd
import xgboost as xgb
import psycopg2
import psycopg2.extras
from datetime import date, datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_KEY_PATH  = Path(r"F:/weatherbetterv2/tokens/db/db.key")
MODEL_DIR    = Path(r"F:/weatherbetterv2/models")
MODEL_DIR.mkdir(exist_ok=True)

ML_SOURCE_ID = 4   # ml_ensemble in forecast_sources
FORECAST_DAYS = 7

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


# ---------------------------------------------------------------------------
# Pipeline run helpers
# ---------------------------------------------------------------------------

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
                """, ('ml_predict', started_at))
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
# Data loading
# ---------------------------------------------------------------------------

def load_observations():
    """Load all observations into a DataFrame."""
    conn = get_db_conn()
    df = pd.read_sql("""
        SELECT o.city_id, o.obs_date, o.temp_high, o.temp_low, o.temp_avg,
               o.precip, o.snow, o.wind_speed_max
        FROM observations o
        WHERE o.temp_high IS NOT NULL AND o.temp_low IS NOT NULL
        ORDER BY o.city_id, o.obs_date
    """, conn)
    conn.close()
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    return df


def load_today_forecasts(today):
    """Load today's forecasts from the 3 external sources for the next 7 days."""
    conn = get_db_conn()
    df = pd.read_sql("""
        SELECT f.city_id, f.source_id, f.target_date, f.lead_days,
               f.temp_high, f.temp_low
        FROM forecasts f
        WHERE f.target_date > %s
          AND f.target_date <= %s
          AND f.source_id IN (1, 2, 3)
          AND f.temp_high IS NOT NULL
    """, conn, params=[today, today + timedelta(days=FORECAST_DAYS)])
    conn.close()
    if not df.empty:
        df["target_date"] = pd.to_datetime(df["target_date"])
    return df


def load_cities():
    """Load city list."""
    conn = get_db_conn()
    df = pd.read_sql("SELECT id, name FROM cities WHERE active = true ORDER BY id", conn)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def build_features(obs_df):
    """
    Build training features from observation history.
    Each row = one city-day with lag/rolling features.
    """
    dfs = []
    for city_id, group in obs_df.groupby("city_id"):
        g = group.sort_values("obs_date").copy()
        g = g.set_index("obs_date").asfreq("D")  # fill gaps with NaN
        g["city_id"] = city_id

        # Calendar features (cyclical encoding)
        doy = g.index.dayofyear
        g["doy_sin"] = np.sin(2 * math.pi * doy / 365.25)
        g["doy_cos"] = np.cos(2 * math.pi * doy / 365.25)
        g["year"]    = g.index.year

        # Lag features
        for lag in [1, 2, 3, 7, 14]:
            g[f"high_lag_{lag}"] = g["temp_high"].shift(lag)
            g[f"low_lag_{lag}"]  = g["temp_low"].shift(lag)

        # Rolling means
        g["high_roll_7"]  = g["temp_high"].shift(1).rolling(7, min_periods=3).mean()
        g["high_roll_30"] = g["temp_high"].shift(1).rolling(30, min_periods=7).mean()
        g["low_roll_7"]   = g["temp_low"].shift(1).rolling(7, min_periods=3).mean()
        g["low_roll_30"]  = g["temp_low"].shift(1).rolling(30, min_periods=7).mean()

        # Temperature range and trend
        g["temp_range"]     = g["temp_high"] - g["temp_low"]
        g["high_trend_7"]   = g["temp_high"].shift(1) - g["temp_high"].shift(7)

        # --- Diurnal Temperature Range (DTR) features ---
        dtr = g["temp_high"] - g["temp_low"]
        g["dtr_lag_1"]    = dtr.shift(1)
        g["dtr_lag_3"]    = dtr.shift(3)
        g["dtr_roll_7"]   = dtr.shift(1).rolling(7, min_periods=3).mean()
        g["dtr_roll_30"]  = dtr.shift(1).rolling(30, min_periods=7).mean()
        g["dtr_std_7"]    = dtr.shift(1).rolling(7, min_periods=3).std()
        # DTR anomaly: how far yesterday's DTR deviated from the 30-day average
        g["dtr_anomaly"]  = g["dtr_lag_1"] - g["dtr_roll_30"]
        # DTR trend: is the daily swing expanding or compressing?
        g["dtr_trend_7"]  = dtr.shift(1) - dtr.shift(7)

        dfs.append(g)

    result = pd.concat(dfs)
    return result


FEATURE_COLS_BASE = [
    "city_id", "doy_sin", "doy_cos", "year",
    "high_lag_1", "high_lag_2", "high_lag_3", "high_lag_7", "high_lag_14",
    "low_lag_1", "low_lag_2", "low_lag_3", "low_lag_7", "low_lag_14",
    "high_roll_7", "high_roll_30", "low_roll_7", "low_roll_30",
    "high_trend_7",
    # Diurnal Temperature Range features
    "dtr_lag_1", "dtr_lag_3", "dtr_roll_7", "dtr_roll_30",
    "dtr_std_7", "dtr_anomaly", "dtr_trend_7",
]

FORECAST_FEATURE_COLS = ["gfs_high", "ecmwf_high", "nws_high", "lead_days"]


def build_prediction_features(obs_df, cities_df, today, forecasts_df=None):
    """
    Build feature rows for the next 7 days for each city.
    Uses the latest observations as lag features.
    """
    rows = []
    today_ts = pd.Timestamp(today)

    for _, city in cities_df.iterrows():
        city_id = city["id"]
        city_obs = obs_df[obs_df["city_id"] == city_id].sort_values("obs_date")

        if len(city_obs) < 14:
            continue

        recent = city_obs.tail(30)
        latest_highs = recent["temp_high"].values
        latest_lows  = recent["temp_low"].values

        for lead in range(1, FORECAST_DAYS + 1):
            target_date = today + timedelta(days=lead)
            doy = target_date.timetuple().tm_yday

            row = {
                "city_id":     city_id,
                "city_name":   city["name"],
                "target_date": target_date,
                "lead_days":   lead,
                "doy_sin":     math.sin(2 * math.pi * doy / 365.25),
                "doy_cos":     math.cos(2 * math.pi * doy / 365.25),
                "year":        target_date.year,
            }

            # Lag features (relative to today, not target_date — these are known values)
            for lag in [1, 2, 3, 7, 14]:
                idx = -lag
                row[f"high_lag_{lag}"] = float(latest_highs[idx]) if abs(idx) <= len(latest_highs) else np.nan
                row[f"low_lag_{lag}"]  = float(latest_lows[idx])  if abs(idx) <= len(latest_lows)  else np.nan

            # Rolling means from recent observations
            if len(latest_highs) >= 7:
                row["high_roll_7"]  = float(np.mean(latest_highs[-7:]))
                row["low_roll_7"]   = float(np.mean(latest_lows[-7:]))
            else:
                row["high_roll_7"]  = float(np.mean(latest_highs))
                row["low_roll_7"]   = float(np.mean(latest_lows))

            if len(latest_highs) >= 30:
                row["high_roll_30"] = float(np.mean(latest_highs[-30:]))
                row["low_roll_30"]  = float(np.mean(latest_lows[-30:]))
            else:
                row["high_roll_30"] = float(np.mean(latest_highs))
                row["low_roll_30"]  = float(np.mean(latest_lows))

            # Trend
            if len(latest_highs) >= 7:
                row["high_trend_7"] = float(latest_highs[-1] - latest_highs[-7])
            else:
                row["high_trend_7"] = 0.0

            # --- DTR features ---
            latest_dtrs = latest_highs - latest_lows
            row["dtr_lag_1"] = float(latest_dtrs[-1]) if len(latest_dtrs) >= 1 else np.nan
            row["dtr_lag_3"] = float(latest_dtrs[-3]) if len(latest_dtrs) >= 3 else np.nan
            if len(latest_dtrs) >= 7:
                row["dtr_roll_7"]  = float(np.mean(latest_dtrs[-7:]))
                row["dtr_std_7"]   = float(np.std(latest_dtrs[-7:], ddof=1))
                row["dtr_trend_7"] = float(latest_dtrs[-1] - latest_dtrs[-7])
            else:
                row["dtr_roll_7"]  = float(np.mean(latest_dtrs))
                row["dtr_std_7"]   = float(np.std(latest_dtrs, ddof=1)) if len(latest_dtrs) > 1 else 0.0
                row["dtr_trend_7"] = 0.0
            if len(latest_dtrs) >= 30:
                row["dtr_roll_30"] = float(np.mean(latest_dtrs[-30:]))
            else:
                row["dtr_roll_30"] = float(np.mean(latest_dtrs))
            row["dtr_anomaly"] = row["dtr_lag_1"] - row["dtr_roll_30"]

            # Forecast features (if available)
            row["gfs_high"]   = np.nan
            row["ecmwf_high"] = np.nan
            row["nws_high"]   = np.nan

            if forecasts_df is not None and not forecasts_df.empty:
                city_fcasts = forecasts_df[
                    (forecasts_df["city_id"] == city_id) &
                    (forecasts_df["target_date"] == pd.Timestamp(target_date))
                ]
                for _, fc in city_fcasts.iterrows():
                    if fc["source_id"] == 1:
                        row["gfs_high"]   = float(fc["temp_high"])
                    elif fc["source_id"] == 2:
                        row["ecmwf_high"] = float(fc["temp_high"])
                    elif fc["source_id"] == 3:
                        row["nws_high"]   = float(fc["temp_high"])

            rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_model(features_df, target_col="temp_high"):
    """Train XGBoost on historical features. Returns (model, feature_columns)."""
    df = features_df.dropna(subset=[target_col])

    # Determine which feature columns to use
    # Use forecast features only if we have meaningful data
    feat_cols = list(FEATURE_COLS_BASE)

    # Check if we have forecast data in training set
    for fc in FORECAST_FEATURE_COLS:
        if fc in df.columns and df[fc].notna().sum() > 100:
            feat_cols.append(fc)

    df_clean = df.dropna(subset=feat_cols)

    if len(df_clean) < 100:
        log.error(f"Not enough training data: {len(df_clean)} rows")
        return None, None

    X = df_clean[feat_cols].values
    y = df_clean[target_col].values

    model = xgb.XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X, y, verbose=False)

    # Quick train metrics
    from sklearn.metrics import mean_absolute_error
    train_pred = model.predict(X)
    mae = mean_absolute_error(y, train_pred)
    log.info(f"  Train MAE ({target_col}): {mae:.2f}°F on {len(df_clean):,} rows")

    return model, feat_cols


# ---------------------------------------------------------------------------
# Prediction & output
# ---------------------------------------------------------------------------

def predict_and_store(model_high, feat_cols_high, model_low, feat_cols_low,
                      pred_df, forecasts_df, run_id):
    """Run predictions, store in DB, print comparison table."""

    # Predict highs
    X_high = pred_df[feat_cols_high].values
    pred_df["ml_high"] = model_high.predict(X_high)
    pred_df["ml_high"] = pred_df["ml_high"].round(1)

    # Predict lows
    if model_low is not None:
        X_low = pred_df[feat_cols_low].values
        pred_df["ml_low"] = model_low.predict(X_low)
        pred_df["ml_low"] = pred_df["ml_low"].round(1)
    else:
        pred_df["ml_low"] = np.nan

    # Store ML predictions as source_id=4 in forecasts
    conn = get_db_conn()
    upsert_sql = """
        INSERT INTO forecasts (
            city_id, source_id, target_date, fetched_at, lead_days,
            temp_high, temp_low, temp_avg, pipeline_run_id
        ) VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s)
        ON CONFLICT (city_id, source_id, target_date, lead_days) DO UPDATE SET
            fetched_at = NOW(),
            temp_high = EXCLUDED.temp_high,
            temp_low = EXCLUDED.temp_low,
            temp_avg = EXCLUDED.temp_avg,
            pipeline_run_id = EXCLUDED.pipeline_run_id
    """

    rows_stored = 0
    with conn:
        with conn.cursor() as cur:
            for _, row in pred_df.iterrows():
                high = float(row["ml_high"])
                low  = float(row["ml_low"]) if not np.isnan(row["ml_low"]) else None
                avg  = round((high + low) / 2.0, 1) if low is not None else None
                cur.execute(upsert_sql, (
                    int(row["city_id"]), ML_SOURCE_ID,
                    row["target_date"].date() if hasattr(row["target_date"], 'date') else row["target_date"],
                    int(row["lead_days"]),
                    high, low, avg, run_id
                ))
                rows_stored += 1
    conn.close()

    # --- Print comparison table ---
    print("\n" + "=" * 90)
    print(f"  FORECAST COMPARISON — {date.today()}")
    print("=" * 90)

    for target_date in sorted(pred_df["target_date"].unique()):
        day_preds = pred_df[pred_df["target_date"] == target_date]
        lead = day_preds.iloc[0]["lead_days"]
        dt = pd.Timestamp(target_date)
        print(f"\n  {dt.strftime('%A %b %d')} (lead {int(lead)}d)")
        print(f"  {'City':22s} {'GFS':>6s} {'ECMWF':>6s} {'NWS':>6s} | {'ML':>6s}")
        print(f"  {'-'*22} {'-'*6} {'-'*6} {'-'*6} + {'-'*6}")

        for _, row in day_preds.sort_values("city_name").iterrows():
            gfs   = f"{row['gfs_high']:.0f}" if not np.isnan(row.get("gfs_high", np.nan)) else "  —"
            ecmwf = f"{row['ecmwf_high']:.0f}" if not np.isnan(row.get("ecmwf_high", np.nan)) else "  —"
            nws   = f"{row['nws_high']:.0f}" if not np.isnan(row.get("nws_high", np.nan)) else "  —"
            ml    = f"{row['ml_high']:.0f}"
            print(f"  {row['city_name']:22s} {gfs:>6s} {ecmwf:>6s} {nws:>6s} | {ml:>6s}")

    print("\n" + "=" * 90)
    return rows_stored


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    started_at = datetime.now()
    today = date.today()

    run_id = start_pipeline_run(started_at)
    if not run_id:
        log.error("Could not create pipeline run")
        sys.exit(1)

    log.info(f"ML train & predict started (pipeline_run {run_id})")

    # Load data
    log.info("Loading observations...")
    obs_df = load_observations()
    log.info(f"  {len(obs_df):,} observations loaded")

    cities_df = load_cities()
    log.info(f"  {len(cities_df)} cities")

    forecasts_df = load_today_forecasts(today)
    has_forecasts = not forecasts_df.empty
    if has_forecasts:
        log.info(f"  {len(forecasts_df)} forecast rows for next {FORECAST_DAYS} days")
    else:
        log.info("  No external forecasts available yet — using climatological features only")

    # Build training features
    log.info("Building training features...")
    train_df = build_features(obs_df)
    log.info(f"  {len(train_df):,} training rows after feature engineering")

    # Train high model
    log.info("Training temp_high model...")
    model_high, feat_cols_high = train_model(train_df, "temp_high")
    if model_high is None:
        finish_pipeline_run(run_id, 'error', error_message='Not enough training data')
        sys.exit(1)

    # Train low model
    log.info("Training temp_low model...")
    model_low, feat_cols_low = train_model(train_df, "temp_low")

    # Save models
    model_path = MODEL_DIR / f"model_high_{today.isoformat()}.pkl"
    with open(model_path, "wb") as f:
        pickle.dump({"model": model_high, "features": feat_cols_high}, f)
    log.info(f"  Model saved to {model_path}")

    # Build prediction features
    log.info("Generating predictions...")
    pred_df = build_prediction_features(
        obs_df, cities_df, today,
        forecasts_df if has_forecasts else None
    )

    if pred_df.empty:
        log.error("No prediction features could be built")
        finish_pipeline_run(run_id, 'error', error_message='No prediction features')
        sys.exit(1)

    # Predict and output
    rows_stored = predict_and_store(
        model_high, feat_cols_high,
        model_low, feat_cols_low,
        pred_df, forecasts_df, run_id
    )

    # Feature importance
    log.info("\nTop 10 features (temp_high):")
    importances = model_high.feature_importances_
    for idx in np.argsort(importances)[::-1][:10]:
        log.info(f"  {feat_cols_high[idx]:20s}  {importances[idx]:.3f}")

    finish_pipeline_run(run_id, 'success', cities_processed=len(cities_df),
                        rows_inserted=rows_stored,
                        notes=f"Trained on {len(train_df):,} rows, predicted {rows_stored} rows")

    log.info(f"\nDone. {rows_stored} ML predictions stored (pipeline_run {run_id})")


if __name__ == "__main__":
    main()
