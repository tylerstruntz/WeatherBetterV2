"""
diurnal_analysis.py
-------------------
Analyzes Diurnal Temperature Range (DTR = daily high minus low) across all 20 cities
using 7 years of historical observations.

Outputs:
  1. Per-city DTR summary (mean, std, min, max) — ranked by variability
  2. Seasonal DTR by city (spring/summer/fall/winter averages)
  3. DTR anomaly days — when swing deviates most from normal (models miss most)
  4. Correlation between DTR and forecast error (once verification data exists)

Usage:
    python diurnal_analysis.py
    python diurnal_analysis.py --city "Phoenix"
    python diurnal_analysis.py --top 5           # show only top 5 high-edge cities
"""

import argparse
import logging
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date

DB_KEY_PATH = Path(r"F:/weatherbetterv2/tokens/db/db.key")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SEASONS = {
    12: "Winter", 1: "Winter", 2: "Winter",
    3: "Spring", 4: "Spring", 5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer",
    9: "Fall",  10: "Fall",  11: "Fall",
}


def get_db_conn():
    lines = DB_KEY_PATH.read_text().strip().splitlines()
    return psycopg2.connect(
        host=lines[0], port=int(lines[1]), dbname=lines[2],
        user=lines[3], password=lines[4]
    )


def load_data(city_filter=None):
    conn = get_db_conn()
    query = """
        SELECT c.name AS city, o.obs_date, o.temp_high, o.temp_low
        FROM observations o
        JOIN cities c ON c.id = o.city_id
        WHERE o.temp_high IS NOT NULL AND o.temp_low IS NOT NULL
    """
    params = []
    if city_filter:
        query += " AND LOWER(c.name) = LOWER(%s)"
        params.append(city_filter)
    query += " ORDER BY c.name, o.obs_date"
    df = pd.read_sql(query, conn, params=params if params else None)
    conn.close()
    df["obs_date"] = pd.to_datetime(df["obs_date"])
    df["dtr"] = df["temp_high"] - df["temp_low"]
    df["month"] = df["obs_date"].dt.month
    df["season"] = df["month"].map(SEASONS)
    df["year"] = df["obs_date"].dt.year
    return df


def load_verification_data():
    """Load forecast error data if available."""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM forecast_verifications")
    count = cur.fetchone()[0]
    conn.close()
    if count == 0:
        return None

    conn = get_db_conn()
    df = pd.read_sql("""
        SELECT c.name AS city, o.obs_date, o.temp_high - o.temp_low AS dtr,
               fv.high_abs_error, fs.name AS source
        FROM forecast_verifications fv
        JOIN forecasts f ON f.id = fv.forecast_id
        JOIN observations o ON o.id = fv.observation_id
        JOIN cities c ON c.id = f.city_id
        JOIN forecast_sources fs ON fs.id = f.source_id
        WHERE f.lead_days = 1
    """, conn)
    conn.close()
    return df


def print_separator(char="=", width=90):
    print(char * width)


def section(title):
    print()
    print_separator()
    print(f"  {title}")
    print_separator()


# ---------------------------------------------------------------------------
# Section 1: City DTR summary ranked by variability
# ---------------------------------------------------------------------------

def city_dtr_summary(df):
    section("CITY DTR SUMMARY — Ranked by Variability (High Std = More Edge)")
    print(f"  {'City':22s} {'Mean DTR':>9s} {'Std DTR':>8s} {'Min':>5s} {'Max':>5s} {'P25':>5s} {'P75':>5s}  Edge Tier")
    print(f"  {'-'*22} {'-'*9} {'-'*8} {'-'*5} {'-'*5} {'-'*5} {'-'*5}  ---------")

    stats = (
        df.groupby("city")["dtr"]
        .agg(mean="mean", std="std", min="min", max="max",
             p25=lambda x: x.quantile(0.25),
             p75=lambda x: x.quantile(0.75))
        .sort_values("std", ascending=False)
    )

    for city, row in stats.iterrows():
        if row["std"] >= 8:
            tier = "HIGH"
        elif row["std"] >= 5:
            tier = "MEDIUM"
        else:
            tier = "LOW"
        print(f"  {city:22s} {row['mean']:9.1f} {row['std']:8.1f} "
              f"{row['min']:5.0f} {row['max']:5.0f} "
              f"{row['p25']:5.0f} {row['p75']:5.0f}  {tier}")

    print()
    print("  Edge Tier: HIGH std = model errors larger = more Kalshi edge")
    return stats


# ---------------------------------------------------------------------------
# Section 2: Seasonal DTR by city
# ---------------------------------------------------------------------------

def seasonal_dtr(df, top_cities=None):
    section("SEASONAL DTR BY CITY (avg °F swing by season)")
    cities = top_cities if top_cities else sorted(df["city"].unique())
    season_order = ["Winter", "Spring", "Summer", "Fall"]

    pivot = (
        df.groupby(["city", "season"])["dtr"]
        .mean()
        .unstack("season")
        .reindex(columns=season_order)
    )

    print(f"  {'City':22s} {'Winter':>7s} {'Spring':>7s} {'Summer':>7s} {'Fall':>7s}  {'Range':>6s}")
    print(f"  {'-'*22} {'-'*7} {'-'*7} {'-'*7} {'-'*7}  {'-'*6}")

    for city in cities:
        if city not in pivot.index:
            continue
        row = pivot.loc[city]
        valid = row.dropna()
        rng = valid.max() - valid.min() if len(valid) >= 2 else 0
        vals = " ".join(f"{v:7.1f}" if not np.isnan(v) else "     --" for v in row)
        print(f"  {city:22s} {vals}  {rng:6.1f}")

    print()
    print("  Range = max season DTR minus min season DTR (higher = more seasonal variation)")


# ---------------------------------------------------------------------------
# Section 3: Worst DTR anomaly days per city
# ---------------------------------------------------------------------------

def dtr_anomaly_days(df, n=5):
    section(f"TOP {n} HIGHEST DTR ANOMALY DAYS PER CITY (when swing deviated most from 30-day norm)")
    print("  These are the days models historically miss the most.")
    print()

    for city, group in df.groupby("city"):
        g = group.sort_values("obs_date").set_index("obs_date")
        rolling_mean = g["dtr"].rolling(30, min_periods=10).mean().shift(1)
        g["dtr_anomaly"] = (g["dtr"] - rolling_mean).abs()
        top = g.nlargest(n, "dtr_anomaly")[["temp_high", "temp_low", "dtr", "dtr_anomaly"]]
        print(f"  {city}")
        print(f"    {'Date':>12s}  {'High':>5s}  {'Low':>5s}  {'DTR':>5s}  {'Anomaly':>8s}")
        for dt, row in top.iterrows():
            print(f"    {str(dt.date()):>12s}  {row['temp_high']:5.0f}  "
                  f"{row['temp_low']:5.0f}  {row['dtr']:5.0f}  {row['dtr_anomaly']:8.1f}°F")
        print()


# ---------------------------------------------------------------------------
# Section 4: Monthly DTR heatmap (text-based)
# ---------------------------------------------------------------------------

def monthly_dtr_heatmap(df, top_cities):
    section("MONTHLY AVERAGE DTR HEATMAP (top cities by variability)")
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    pivot = (
        df[df["city"].isin(top_cities)]
        .groupby(["city", "month"])["dtr"]
        .mean()
        .unstack("month")
    )

    header = f"  {'City':22s} " + " ".join(f"{m:>4s}" for m in months)
    print(header)
    print(f"  {'-'*22} " + " ".join("----" for _ in months))

    for city in top_cities:
        if city not in pivot.index:
            continue
        vals = []
        for m in range(1, 13):
            v = pivot.loc[city, m] if m in pivot.columns else np.nan
            vals.append(f"{v:4.0f}" if not np.isnan(v) else "   -")
        print(f"  {city:22s} " + " ".join(vals))


# ---------------------------------------------------------------------------
# Section 5: DTR vs forecast error correlation (if data exists)
# ---------------------------------------------------------------------------

def dtr_vs_error(ver_df):
    section("DTR vs FORECAST ERROR CORRELATION")
    if ver_df is None or ver_df.empty:
        print("  No verification data yet — this will populate after ~30 days of forecasts.")
        print("  Once available: shows whether high-DTR days have higher forecast errors.")
        return

    ver_df["dtr_bucket"] = pd.cut(ver_df["dtr"], bins=[0, 10, 15, 20, 25, 30, 100],
                                   labels=["0-10", "10-15", "15-20", "20-25", "25-30", "30+"])
    summary = (
        ver_df.groupby(["source", "dtr_bucket"])["high_abs_error"]
        .agg(mean="mean", count="count")
        .reset_index()
    )

    for source in summary["source"].unique():
        src_df = summary[summary["source"] == source]
        print(f"\n  {source}")
        print(f"    {'DTR Range':>10s}  {'Avg Error':>10s}  {'N':>6s}")
        for _, row in src_df.iterrows():
            print(f"    {str(row['dtr_bucket']):>10s}  {row['mean']:10.2f}°F  {int(row['count']):6d}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Diurnal Temperature Range analysis")
    parser.add_argument("--city", type=str, help="Filter to a single city")
    parser.add_argument("--top", type=int, default=None,
                        help="Show only top N cities by DTR variability")
    args = parser.parse_args()

    print()
    print_separator()
    print(f"  DIURNAL TEMPERATURE RANGE ANALYSIS  —  WeatherBetterV2")
    print(f"  Data: 7 years of NOAA GHCND observations (2019–{date.today().year})")
    print_separator()

    log.info("Loading observations...")
    df = load_data(city_filter=args.city)
    log.info(f"  {len(df):,} observation rows loaded across {df['city'].nunique()} cities")

    ver_df = load_verification_data()

    # Section 1: summary ranked by variability
    stats = city_dtr_summary(df)

    # Determine cities to spotlight
    if args.top:
        top_cities = stats.head(args.top).index.tolist()
    elif args.city:
        top_cities = [args.city] if args.city in stats.index else []
    else:
        top_cities = stats[stats["std"] >= 7].index.tolist()

    # Section 2: seasonal breakdown
    seasonal_dtr(df, top_cities=top_cities if not args.city else None)

    # Section 3: anomaly days (only for top cities or single city)
    if args.city:
        dtr_anomaly_days(df[df["city"].str.lower() == args.city.lower()])
    else:
        dtr_anomaly_days(df[df["city"].isin(top_cities)], n=3)

    # Section 4: monthly heatmap
    if not args.city:
        monthly_dtr_heatmap(df, top_cities)

    # Section 5: forecast error correlation
    dtr_vs_error(ver_df)

    print()
    print_separator()
    print("  SUMMARY FOR KALSHI BETTING")
    print_separator()
    high_edge = stats[stats["std"] >= 8].index.tolist()
    med_edge  = stats[(stats["std"] >= 5) & (stats["std"] < 8)].index.tolist()
    low_edge  = stats[stats["std"] < 5].index.tolist()
    print(f"  HIGH edge cities  ({len(high_edge)}): {', '.join(high_edge)}")
    print(f"  MEDIUM edge cities ({len(med_edge)}): {', '.join(med_edge)}")
    print(f"  LOW edge cities   ({len(low_edge)}): {', '.join(low_edge)}")
    print()
    print("  High-edge cities have the most DTR variability — forecast models")
    print("  miss the most here. Focus bets on these cities when ML prediction")
    print("  diverges significantly from the raw forecast sources.")
    print()


if __name__ == "__main__":
    main()
