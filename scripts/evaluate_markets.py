"""
evaluate_markets.py
-------------------
For each city where ML and NWS agree (divergence < threshold):
  1. Compute combined forecast: 0.35*ML + 0.65*NWS
  2. Build truth table: which of the 6 contracts does combined_pred land in?
       That contract = YES.  All others = NO.
  3. For each contract: return_mult = (100 - price) / price
     Kelly size: half-Kelly using our_prob from normal distribution.
  4. Best play per city = highest return_mult.

Usage:
    python evaluate_markets.py              # ranked slate
    python evaluate_markets.py --show-all   # full truth table per city
    python evaluate_markets.py --json       # JSON for paper_trade.py
"""

import sys
import json
import re
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import date, datetime, timedelta
from pathlib import Path

# ── Config ───────────────────────────────────────────────────────────────────

DB_KEY_PATH          = Path(r"F:/weatherbetterv2/tokens/db/db.key")
KALSHI_BASE          = "https://api.elections.kalshi.com/trade-api/v2"
ML_WEIGHT            = 0.35
NWS_WEIGHT           = 0.65
DIVERGENCE_THRESHOLD = 2.0    # F -- skip city if |ML - NWS| > this
MAX_LEAD_DAYS        = 4
MIN_RETURN           = 0.3    # minimum (100-price)/price to consider a play
STARTING_BALANCE     = 100.0  # dollars
RISK_PCT             = 0.30   # fraction of balance to risk total
TOP_N                = 5      # number of plays to allocate capital to

CITY_SERIES = {
    "New Orleans":       "KXHIGHTNOLA",
    "Washington DC":     "KXHIGHTDC",
    "Miami":             "KXHIGHMIA",
    "Oklahoma City":     "KXHIGHTOKC",
    "San Antonio":       "KXHIGHTSATX",
    "Dallas-Fort Worth": "KXHIGHTDAL",
    "Los Angeles":       "KXHIGHLAX",
    "Seattle":           "KXHIGHTSEA",
    "Houston":           "KXHIGHTHOU",
    "Philadelphia":      "KXHIGHPHIL",
    "Phoenix":           "KXHIGHTPHX",
    "Boston":            "KXHIGHTBOS",
    "Las Vegas":         "KXHIGHTLV",
    "Denver":            "KXHIGHDEN",
    "Chicago Midway":    "KXHIGHCHI",
    "Atlanta":           "KXHIGHTATL",
    "New York City":     "KXHIGHNY",
    "Minneapolis":       "KXHIGHTMIN",
    "Austin":            "KXHIGHAUS",
    "San Francisco":     "KXHIGHTSFO",
}

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ── DB ───────────────────────────────────────────────────────────────────────

def load_db_config():
    import os
    if os.environ.get("DB_HOST"):
        return dict(host=os.environ["DB_HOST"].strip(),
                    port=int(os.environ.get("DB_PORT", "5432").strip()),
                    dbname=os.environ["DB_NAME"].strip(),
                    user=os.environ["DB_USER"].strip(),
                    password=os.environ["DB_PASSWORD"].strip(),
                    sslmode=os.environ.get("DB_SSLMODE", "require").strip())
    lines = DB_KEY_PATH.read_text().strip().splitlines()
    return dict(host=lines[0], port=int(lines[1]), dbname=lines[2],
                user=lines[3], password=lines[4],
                sslmode=lines[5] if len(lines) > 5 else "prefer")

def get_conn():
    return psycopg2.connect(**load_db_config())

def load_ml_preds(today):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT f.city_id, f.target_date, f.temp_high, c.name
            FROM forecasts f JOIN cities c ON c.id = f.city_id
            WHERE f.source_id = 4
              AND f.target_date > %s AND f.target_date <= %s
              AND f.temp_high IS NOT NULL
        """, (today, today + timedelta(days=MAX_LEAD_DAYS)))
        rows = cur.fetchall()
    conn.close()
    return {(r["city_id"], r["target_date"]): {"ml": float(r["temp_high"]), "name": r["name"]}
            for r in rows}

def load_nws_preds(today):
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT DISTINCT ON (f.city_id, f.target_date)
                   f.city_id, f.target_date, f.temp_high
            FROM forecasts f
            WHERE f.source_id = 3
              AND f.target_date > %s AND f.target_date <= %s
              AND f.temp_high IS NOT NULL
            ORDER BY f.city_id, f.target_date, f.lead_days ASC
        """, (today, today + timedelta(days=MAX_LEAD_DAYS)))
        rows = cur.fetchall()
    conn.close()
    return {(r["city_id"], r["target_date"]): float(r["temp_high"]) for r in rows}

def load_cities():
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT id, name FROM cities WHERE active = true")
        rows = cur.fetchall()
    conn.close()
    return {r["name"]: r["id"] for r in rows}


# ── Kalshi ───────────────────────────────────────────────────────────────────

def fetch_markets(series_ticker):
    try:
        r = requests.get(f"{KALSHI_BASE}/markets",
                         headers={"accept": "application/json"},
                         params={"series_ticker": series_ticker, "limit": 100},
                         timeout=15)
        r.raise_for_status()
        return r.json().get("markets", [])
    except requests.RequestException as e:
        log.warning(f"Kalshi {series_ticker}: {e}")
        return []

def extract_date(market):
    et = market.get("event_ticker", "")
    parts = et.split("-")
    if len(parts) >= 2:
        try:
            return datetime.strptime(parts[-1], "%d%b%y").date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(
            market.get("close_time", "").replace("Z", "+00:00")).date()
    except (ValueError, AttributeError):
        return None

def parse_strikes(market):
    """
    Returns (lower, upper, ctype).
    between [lower, lower+2)
    less    (-inf, lower)
    greater (lower, +inf)
    """
    ctype = market.get("strike_type", "greater")
    floor = market.get("floor_strike")

    if floor is None:
        m = re.search(r"(\d{2,3})", market.get("title", ""))
        floor = float(m.group(1)) if m else None

    if floor is None:
        return None, None, ctype

    lower = float(floor)
    upper = lower + 2 if ctype == "between" else None
    return lower, upper, ctype

def to_cents(v):
    try:
        return round(float(v) * 100)
    except (TypeError, ValueError):
        return None


# ── Math ─────────────────────────────────────────────────────────────────────

def correct_side(combined, ctype, lower, upper):
    """Which side wins if combined is the actual outcome?"""
    if ctype == "greater":
        return "YES" if combined > lower else "NO"
    elif ctype == "less":
        return "YES" if combined < lower else "NO"
    elif ctype == "between":
        return "YES" if lower <= combined < upper else "NO"
    return None



# ── Per-city truth table ──────────────────────────────────────────────────────

def evaluate_city(markets, combined, target_date):
    """
    Returns all qualifying contracts for one city/date, best-first by return_mult.
    Only one will have side=YES (the bucket combined lands in).
    """
    rows = []
    for m in markets:
        if m.get("status") != "active":
            continue
        if extract_date(m) != target_date:
            continue

        lower, upper, ctype = parse_strikes(m)
        if lower is None:
            continue

        yes_c = to_cents(m.get("yes_ask_dollars"))
        no_c  = to_cents(m.get("no_ask_dollars"))
        if yes_c is None or no_c is None:
            continue

        side  = correct_side(combined, ctype, lower, upper)
        price = yes_c if side == "YES" else no_c
        if price is None or price <= 0 or price >= 100:
            continue

        ret = round((100 - price) / price, 3)

        if ret < MIN_RETURN:
            continue

        rows.append({
            "ticker":       m["ticker"],
            "event_ticker": m.get("event_ticker", ""),
            "title":        m.get("title", ""),
            "ctype":        ctype,
            "lower":        lower,
            "upper":        upper,
            "side":         side,
            "price":        price,
            "yes_ask":      yes_c,
            "no_ask":       no_c,
            "return_mult":  ret,
        })

    rows.sort(key=lambda x: x["return_mult"], reverse=True)
    return rows


# ── Main ─────────────────────────────────────────────────────────────────────

def run(output_json=False, show_all=False):
    today    = date.today()
    cities   = load_cities()
    ml_preds = load_ml_preds(today)
    nws_preds = load_nws_preds(today)

    log.info(f"ML predictions: {len(ml_preds)}  NWS available: "
             f"{sum(1 for k in ml_preds if k in nws_preds)}")

    slate      = []
    skipped    = []
    all_tables = {}

    for city_name, series in CITY_SERIES.items():
        city_id = cities.get(city_name)
        if not city_id:
            continue

        markets = fetch_markets(series)
        if not markets:
            continue

        dates_to_check = {
            extract_date(m)
            for m in markets
            if extract_date(m) is not None
               and 1 <= (extract_date(m) - today).days <= MAX_LEAD_DAYS
               and (city_id, extract_date(m)) in ml_preds
        }

        for target_date in sorted(dates_to_check):
            pred_key = (city_id, target_date)
            ml  = ml_preds[pred_key]["ml"]
            nws = nws_preds.get(pred_key)

            if nws is None:
                combined   = ml
                divergence = None
            else:
                divergence = abs(ml - nws)
                if divergence > DIVERGENCE_THRESHOLD:
                    skipped.append(dict(city=city_name, date=str(target_date),
                                        ml=ml, nws=nws, div=round(divergence, 1)))
                    if show_all:
                        key = f"{city_name} {target_date}"
                        all_tables[key] = dict(
                            city=city_name, date=str(target_date),
                            ml=ml, nws=nws,
                            combined=round(ML_WEIGHT*ml + NWS_WEIGHT*nws, 2),
                            div=round(divergence, 1), skipped=True, rows=[])
                    continue
                combined = ML_WEIGHT * ml + NWS_WEIGHT * nws

            combined = round(combined, 2)
            rows     = evaluate_city(markets, combined, target_date)

            if show_all:
                key = f"{city_name} {target_date}"
                all_tables[key] = dict(city=city_name, date=str(target_date),
                                       ml=ml, nws=nws, combined=combined,
                                       div=round(divergence, 1) if divergence else None,
                                       skipped=False, rows=rows)

            if not rows:
                continue

            best = dict(rows[0])
            best.update(city=city_name, city_id=city_id, date=str(target_date),
                        lead_days=(target_date - today).days,
                        ml=ml, nws=nws, combined=combined,
                        divergence=round(divergence, 1) if divergence else None)
            slate.append(best)

    slate.sort(key=lambda x: x["return_mult"], reverse=True)

    if output_json:
        print(json.dumps(slate, indent=2, default=str))
        return slate

    if show_all:
        _print_all(all_tables, skipped, today)
    else:
        _print_slate(slate, skipped, today)

    return slate


# ── Printers ─────────────────────────────────────────────────────────────────

def _print_all(all_tables, skipped, today):
    SEP = "=" * 108
    print()
    print(SEP)
    print("  FULL TRUTH TABLE -- ALL CITIES")
    print("  * = best play  |  return = (100-price)/price  |  Kelly = half-Kelly % of bankroll")
    print(SEP)

    for key in sorted(all_tables):
        e       = all_tables[key]
        nws_str = f"{e['nws']:.1f}" if e["nws"] is not None else "N/A"
        div_str = (f"{e['div']:.1f}F apart" if e["div"] is not None else "NWS N/A")
        skip_lbl = "  !! SKIPPED (divergence)" if e.get("skipped") else ""

        print(f"\n  +-- {e['city']}  [{e['date']}]{skip_lbl}")
        print(f"  |   ML={e['ml']:.1f}F  NWS={nws_str}F  Combined={e['combined']:.1f}F  ({div_str})")
        print(f"  |")
        print(f"  |   {'Contract':46s} {'Side':>4}  {'Price':>5}  {'Return':>7}  {'':>3}")
        print(f"  |   {'-'*46} {'-'*4}  {'-'*5}  {'-'*7}  {'-'*3}")

        rows        = e["rows"]
        best_ticker = rows[0]["ticker"] if rows else None

        for r in rows:
            sel = " * " if r["ticker"] == best_ticker else "   "
            print(f"  |   {r['title'][:46]:46s} {r['side']:>4}  "
                  f"{r['price']:>4}c  {r['return_mult']:>6.2f}x  {sel}")

        if not rows:
            print(f"  |   (no plays -- all prices >= 100c or return < {MIN_RETURN}x)")
        print(f"  +{'-'*105}")

    if skipped:
        print(f"\n  SKIPPED -- |ML - NWS| > {DIVERGENCE_THRESHOLD}F:")
        for s in skipped:
            print(f"    {s['city']:22s}  {s['date']}  ML={s['ml']:.1f}  "
                  f"NWS={s['nws']:.1f}  Delta={s['div']:.1f}F")

    print(f"\n  combined = {ML_WEIGHT}*ML + {NWS_WEIGHT}*NWS  |  "
          f"min_return={MIN_RETURN}x  |  skip_delta>{DIVERGENCE_THRESHOLD}F")
    print(SEP)


def _print_slate(slate, skipped, today):
    SEP = "=" * 112
    print()
    print(SEP)
    print(f"  TRADING SLATE  --  {today}  --  {len(slate)} plays  (1 best per city, ranked by return)")
    print(SEP)
    budget     = STARTING_BALANCE * RISK_PCT
    per_play   = budget / TOP_N
    total_risk = 0.0
    total_payout = 0.0

    print(f"  {'#':>2}  {'City':22s}  {'Date':>10}  {'Contract':42s}  "
          f"{'Side':>4}  {'Price':>5}  {'ML':>5}  {'NWS':>5}  {'Comb':>5}  "
          f"{'Return':>7}  {'Contracts':>9}  {'Risk':>7}  {'Payout':>8}")
    print(f"  {'-'*2}  {'-'*22}  {'-'*10}  {'-'*42}  "
          f"{'-'*4}  {'-'*5}  {'-'*5}  {'-'*5}  {'-'*5}  "
          f"{'-'*7}  {'-'*9}  {'-'*7}  {'-'*8}")

    for i, p in enumerate(slate, 1):
        nws_str = f"{p['nws']:.1f}" if p["nws"] is not None else "  --"

        if i <= TOP_N:
            contracts = int(per_play / (p["price"] / 100))
            risk      = round(contracts * p["price"] / 100, 2)
            payout    = round(contracts * 1.00, 2)
            total_risk   += risk
            total_payout += payout
            size_str  = f"{contracts:>9,}  ${risk:>6.2f}  ${payout:>7.2f}"
            marker    = "  <--"
        else:
            size_str = f"{'--':>9}  {'--':>7}  {'--':>8}"
            marker   = ""

        print(f"  {i:>2}  {p['city']:22s}  {p['date']:>10}  "
              f"{p['title'][:42]:42s}  {p['side']:>4}  "
              f"{p['price']:>4}c  {p['ml']:>5.1f}  {nws_str:>5}  {p['combined']:>5.1f}  "
              f"{p['return_mult']:>6.2f}x  {size_str}{marker}")

    print()
    print(f"  Budget: ${budget:.2f} ({RISK_PCT*100:.0f}% of ${STARTING_BALANCE:.0f})  |  "
          f"${per_play:.2f} per play across top {TOP_N}  |  "
          f"Total risk: ${total_risk:.2f}  |  Total payout if all win: ${total_payout:.2f}")
    print()
    if skipped:
        print(f"  Skipped (|ML-NWS| > {DIVERGENCE_THRESHOLD}F): "
              + ", ".join(f"{s['city']} D={s['div']}F" for s in skipped))
        print()
    print(f"  combined={ML_WEIGHT}*ML+{NWS_WEIGHT}*NWS  |  "
          f"min_return={MIN_RETURN}x  |  skip_delta>{DIVERGENCE_THRESHOLD}F")
    print(SEP)


if __name__ == "__main__":
    run(output_json="--json"     in sys.argv,
        show_all="--show-all" in sys.argv)
