"""
show_tomorrow.py
----------------
Display every Kalshi market for each city alongside ML prediction and NWS forecast high.
Used for manual review before committing paper trades.

Usage:
    python scripts/show_tomorrow.py
"""

import sys
import io
import re
import requests

# Force UTF-8 output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import psycopg2
import psycopg2.extras
from datetime import date, datetime, timedelta
from pathlib import Path
from scipy.stats import norm

# ---------------------------------------------------------------------------
# Config (shared with evaluate_markets.py)
# ---------------------------------------------------------------------------

DB_KEY_PATH = Path(r"F:/weatherbetterv2/tokens/db/db.key")
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
MODEL_MAE   = 3.7
MODEL_SIGMA = MODEL_MAE * 1.25   # ~4.6°F
MIN_EDGE    = 0.08

CITY_SERIES = {
    "New Orleans":      "KXHIGHTNOLA",
    "Washington DC":    "KXHIGHTDC",
    "Miami":            "KXHIGHMIA",
    "Oklahoma City":    "KXHIGHTOKC",
    "San Antonio":      "KXHIGHTSATX",
    "Dallas-Fort Worth":"KXHIGHTDAL",
    "Los Angeles":      "KXHIGHLAX",
    "Seattle":          "KXHIGHTSEA",
    "Houston":          "KXHIGHTHOU",
    "Philadelphia":     "KXHIGHPHIL",
    "Phoenix":          "KXHIGHTPHX",
    "Boston":           "KXHIGHTBOS",
    "Las Vegas":        "KXHIGHTLV",
    "Denver":           "KXHIGHDEN",
    "Chicago Midway":   "KXHIGHCHI",
    "Atlanta":          "KXHIGHTATL",
    "New York City":    "KXHIGHNY",
    "Minneapolis":      "KXHIGHTMIN",
    "Austin":           "KXHIGHAUS",
    "San Francisco":    "KXHIGHTSFO",
}

# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

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

def get_db_conn():
    return psycopg2.connect(**load_db_config())

def load_forecasts(target_date: date) -> dict:
    """
    Load both ML (source_id=4) and NWS (source_id=2 or 3) forecasts for target_date.
    Returns {city_id: {"ml": float|None, "nws": float|None, "city_name": str}}
    """
    conn = get_db_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Check what forecast sources exist
        cur.execute("SELECT id, name FROM forecast_sources ORDER BY id")
        sources = {r["id"]: r["name"] for r in cur.fetchall()}

        cur.execute("""
            SELECT f.city_id, f.source_id, f.temp_high, c.name AS city_name
            FROM forecasts f
            JOIN cities c ON c.id = f.city_id
            WHERE f.target_date = %s
              AND f.temp_high IS NOT NULL
        """, (target_date,))
        rows = cur.fetchall()
    conn.close()

    result = {}
    for r in rows:
        cid = r["city_id"]
        if cid not in result:
            result[cid] = {"ml": None, "nws": None, "city_name": r["city_name"]}
        src_name = sources.get(r["source_id"], "").lower()
        if r["source_id"] == 4 or "xgboost" in src_name or "ml" in src_name:
            result[cid]["ml"] = float(r["temp_high"])
        elif "nws" in src_name or "national" in src_name or "forecast" in src_name:
            result[cid]["nws"] = float(r["temp_high"])
        # fallback: if only one non-ML source, store as nws
        else:
            if result[cid]["nws"] is None:
                result[cid]["nws"] = float(r["temp_high"])

    return result

def load_city_ids() -> dict:
    conn = get_db_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT id, name FROM cities WHERE active = true")
        rows = cur.fetchall()
    conn.close()
    return {r["name"]: r["id"] for r in rows}

# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def fetch_kalshi_markets(series_ticker: str) -> list[dict]:
    try:
        r = requests.get(
            f"{KALSHI_BASE}/markets",
            headers={"accept": "application/json"},
            params={"series_ticker": series_ticker, "limit": 100},
            timeout=15,
        )
        r.raise_for_status()
        return r.json().get("markets", [])
    except requests.RequestException as e:
        print(f"  [WARN] Kalshi fetch failed for {series_ticker}: {e}")
        return []

def parse_market_strike(market: dict) -> tuple[float | None, float | None, str]:
    """Returns (strike, upper_strike, contract_type)"""
    contract_type = market.get("strike_type", "greater")
    strike = market.get("floor_strike")
    upper_strike = None

    if strike is None:
        title = market.get("title", "")
        m = re.search(r'[<>]?(\d+)(?:[-–](\d+))?[°\s]', title)
        if not m:
            return None, None, contract_type
        strike = float(m.group(1))
        if m.group(2):
            upper_strike = float(m.group(2))
    else:
        strike = float(strike)
        if contract_type == "between":
            upper_strike = strike + 2

    return strike, upper_strike, contract_type

def compute_probability(ml_pred: float, strike: float, contract_type: str,
                        upper_strike: float | None = None) -> float:
    if contract_type == "greater":
        return float(1.0 - norm.cdf(strike, loc=ml_pred, scale=MODEL_SIGMA))
    elif contract_type == "less":
        return float(norm.cdf(strike, loc=ml_pred, scale=MODEL_SIGMA))
    elif contract_type == "between" and upper_strike is not None:
        p_above_lower = 1.0 - norm.cdf(strike, loc=ml_pred, scale=MODEL_SIGMA)
        p_above_upper = 1.0 - norm.cdf(upper_strike, loc=ml_pred, scale=MODEL_SIGMA)
        return float(p_above_lower - p_above_upper)
    return 0.0

def to_cents(v) -> int | None:
    try:
        return round(float(v) * 100)
    except (TypeError, ValueError):
        return None

# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def flag(edge: float) -> str:
    if edge >= 0.40:
        return " <<< STRONG BET"
    elif edge >= 0.20:
        return " << good edge"
    elif edge >= MIN_EDGE:
        return " < edge"
    return ""

def run(target_date: date):
    today = date.today()
    print()
    print("=" * 100)
    print(f"  FULL MARKET REVIEW  —  target date: {target_date}  (lead: {(target_date-today).days}d)")
    print(f"  ML sigma: +/-{MODEL_SIGMA:.1f}F   Min edge shown: all (flagged >={MIN_EDGE*100:.0f}%)")
    print("=" * 100)

    city_ids = load_city_ids()
    forecasts = load_forecasts(target_date)

    # Print what forecast sources we have
    sample_city = next(iter(forecasts.values()), None)

    for city_name, series_ticker in CITY_SERIES.items():
        city_id = city_ids.get(city_name)
        if not city_id:
            print(f"\n  [SKIP] {city_name} — not in cities table")
            continue

        city_data = forecasts.get(city_id, {})
        ml_pred = city_data.get("ml")
        nws_pred = city_data.get("nws")

        print()
        ml_str  = f"{ml_pred:.1f}°F" if ml_pred else "N/A"
        nws_str = f"{nws_pred:.1f}°F" if nws_pred else "N/A"
        print(f"  ┌─ {city_name:22s}   ML={ml_str:>8}   NWS Forecast={nws_str:>8}   [{series_ticker}]")

        if ml_pred is None:
            print(f"  │   [NO ML PREDICTION]")
            print(f"  └{'─'*90}")
            continue

        markets = fetch_kalshi_markets(series_ticker)
        # Filter to target date
        day_markets = []
        for m in markets:
            event_ticker = m.get("event_ticker", "")
            parts = event_ticker.split("-")
            if len(parts) >= 2:
                try:
                    mdate = datetime.strptime(parts[-1], "%d%b%y").date()
                    if mdate == target_date:
                        day_markets.append(m)
                except ValueError:
                    pass

        if not day_markets:
            print(f"  │   [NO MARKETS for {target_date}]")
            print(f"  └{'─'*90}")
            continue

        # Sort by strike ascending
        def sort_key(m):
            s, _, ct = parse_market_strike(m)
            if ct == "less":
                return -9999.0
            if ct == "greater":
                return 9999.0
            return s if s is not None else 0.0

        day_markets.sort(key=sort_key)

        print(f"  │   {'Contract':38s}  {'Type':7s}  {'Strike':>8}  {'Y.Ask':>5}  {'N.Ask':>5}  {'OurP%':>5}  {'OurNo%':>5}  {'Y.Edge':>6}  {'N.Edge':>6}  {'Y.EV%':>6}  {'N.EV%':>6}")
        print(f"  │   {'-'*38}  {'-'*7}  {'-'*8}  {'-'*5}  {'-'*5}  {'-'*5}  {'-'*6}  {'-'*6}  {'-'*6}  {'-'*6}  {'-'*6}")

        for m in day_markets:
            strike, upper_strike, contract_type = parse_market_strike(m)
            if strike is None:
                continue

            yes_ask = to_cents(m.get("yes_ask_dollars"))
            no_ask  = to_cents(m.get("no_ask_dollars"))

            if yes_ask is None or no_ask is None:
                continue

            our_prob_yes = compute_probability(ml_pred, strike, contract_type, upper_strike)
            our_prob_no  = 1.0 - our_prob_yes

            y_edge = our_prob_yes - yes_ask / 100.0
            n_edge = our_prob_no  - no_ask  / 100.0

            y_ev = (our_prob_yes * 100 - yes_ask) / yes_ask if yes_ask > 0 else 0.0
            n_ev = (our_prob_no  * 100 - no_ask)  / no_ask  if no_ask  > 0 else 0.0

            title = m.get("title", "")[:38]

            # Strike label
            if contract_type == "between":
                strike_lbl = f"{strike:.0f}–{upper_strike:.0f}°F"
            elif contract_type == "less":
                strike_lbl = f"< {strike:.0f}°F"
            else:
                strike_lbl = f"> {strike:.0f}°F"

            # Directional consistency: only flag a side if ML is on the winning side
            def directionally_ok(side, ct, ml, s, us):
                if side == "YES":
                    if ct == "greater":   return ml > s
                    if ct == "less":      return ml < s
                    if ct == "between":   return s <= ml <= (us or s)
                else:  # NO
                    if ct == "greater":   return ml < s
                    if ct == "less":      return ml > s
                    if ct == "between":   return ml < s or ml > (us or s)
                return False

            y_ok = directionally_ok("YES", contract_type, ml_pred, strike, upper_strike)
            n_ok = directionally_ok("NO",  contract_type, ml_pred, strike, upper_strike)

            y_flag = flag(y_edge) if yes_ask <= 70 and y_ok else ""
            n_flag = flag(n_edge) if no_ask  <= 70 and n_ok else ""

            marker = ""
            if y_edge >= MIN_EDGE and yes_ask <= 70 and y_ok:
                marker = " [Y]"
            if n_edge >= MIN_EDGE and no_ask <= 70 and n_ok:
                marker += " [N]"

            print(f"  │   {title:38s}  {contract_type:7s}  {strike_lbl:>8}  {yes_ask:>4}¢  {no_ask:>4}¢  "
                  f"{our_prob_yes*100:>4.0f}%  {our_prob_no*100:>5.0f}%  "
                  f"{y_edge*100:>+5.0f}%  {n_edge*100:>+6.0f}%  "
                  f"{y_ev*100:>+5.0f}%  {n_ev*100:>+6.0f}%"
                  f"{marker}{y_flag}{n_flag}")

        print(f"  └{'─'*90}")

    print()
    print(f"  [Y] = YES bet has edge >={MIN_EDGE*100:.0f}% and price <= 70c")
    print(f"  [N] = NO  bet has edge >={MIN_EDGE*100:.0f}% and price <= 70c")
    print()


if __name__ == "__main__":
    tomorrow = date.today() + timedelta(days=1)
    if len(sys.argv) > 1:
        tomorrow = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    run(tomorrow)
