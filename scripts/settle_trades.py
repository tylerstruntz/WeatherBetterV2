"""
settle_trades.py
----------------
Checks all PENDING trades in Google Sheets against actual observations in the DB.
For each settled market, marks WIN/LOSS and updates balance.

Usage:
    python settle_trades.py          # dry run
    python settle_trades.py --commit  # write results to Google Sheets
"""

import sys
import logging
from datetime import date, datetime
from pathlib import Path

import gspread
import psycopg2
import psycopg2.extras
from google.oauth2.service_account import Credentials

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_KEY_PATH     = Path(r"F:/weatherbetterv2/tokens/db/db.key")
SHEETS_KEY_PATH = Path(r"F:/weatherbetterv2/tokens/sheets/weather-trading-491402-9130a093af1c.json")
SPREADSHEET_ID  = "1sVyy1tgHA1RFIhpFImt6Ah3ptJEQ5Tnpmk6WddZt5xA"

# Column indices in Trades sheet (1-based)
COL = {
    "trade_id":        1,
    "trade_date":      2,
    "market_id":       3,
    "city":            4,
    "target_date":     5,
    "contract_type":   6,   # ABOVE / BELOW / RANGE X-Y
    "strike":          7,
    "side":            8,
    "price_cents":     9,
    "contracts":       10,
    "amount_spent":    11,
    "potential_payout":12,
    "ml_prediction":   13,
    "ml_probability":  14,
    "implied_prob":    15,
    "edge":            16,
    "lead_days":       17,
    "settled":         18,
    "outcome":         19,
    "payout_received": 20,
    "balance_after":   21,
    "notes":           22,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DB helpers
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
        "host":     lines[0], "port": int(lines[1]), "dbname": lines[2],
        "user":     lines[3], "password": lines[4],
        "sslmode":  lines[5] if len(lines) > 5 else "prefer",
    }


def get_actual_temp(city_name: str, obs_date: date) -> float | None:
    """Look up actual observed temp_high for a city on a date."""
    conn = psycopg2.connect(**load_db_config())
    with conn.cursor() as cur:
        cur.execute("""
            SELECT o.temp_high
            FROM observations o
            JOIN cities c ON c.id = o.city_id
            WHERE c.name = %s AND o.obs_date = %s AND o.temp_high IS NOT NULL
            LIMIT 1
        """, (city_name, obs_date))
        row = cur.fetchone()
    conn.close()
    return float(row[0]) if row else None


# ---------------------------------------------------------------------------
# Settlement logic
# ---------------------------------------------------------------------------

def did_yes_win(contract_type: str, strike: float, actual_temp: float,
                upper_strike: float | None = None) -> bool:
    """
    Determine if the YES side won given the actual temperature.
    contract_type: ABOVE, BELOW, RANGE X-Y
    """
    if contract_type == "ABOVE":
        return actual_temp > strike
    elif contract_type == "BELOW":
        return actual_temp < strike
    elif contract_type.startswith("RANGE") and upper_strike is not None:
        return strike < actual_temp <= upper_strike
    return False


def parse_contract(contract_type_str: str) -> tuple[str, float, float | None]:
    """Parse the contract_type column: e.g. 'ABOVE', 'BELOW', 'RANGE 83-85'"""
    if contract_type_str.startswith("RANGE"):
        parts = contract_type_str.replace("RANGE", "").strip().split("-")
        try:
            lower = float(parts[0])
            upper = float(parts[1])
            return "RANGE", lower, upper
        except (ValueError, IndexError):
            return "RANGE", 0.0, None
    return contract_type_str.strip(), 0.0, None  # strike comes from its own column


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------

def get_sheet():
    creds = Credentials.from_service_account_file(
        str(SHEETS_KEY_PATH),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    commit = "--commit" in sys.argv
    today = date.today()

    sh = get_sheet()
    ws_trades = sh.worksheet("Trades")
    ws_balance = sh.worksheet("Balance")

    all_rows = ws_trades.get_all_values()
    if len(all_rows) <= 1:
        log.info("No trades in sheet.")
        return

    header = all_rows[0]
    data_rows = all_rows[1:]

    pending = []
    for i, row in enumerate(data_rows, start=2):  # row index is 1-based + header
        if len(row) >= 18 and row[COL["settled"] - 1] == "PENDING":
            pending.append((i, row))

    if not pending:
        log.info("No pending trades to settle.")
        return

    log.info(f"Found {len(pending)} pending trades.")

    settlements = []
    for row_idx, row in pending:
        try:
            trade_id       = row[COL["trade_id"] - 1]
            city           = row[COL["city"] - 1]
            target_date_str = row[COL["target_date"] - 1]
            contract_type_str = row[COL["contract_type"] - 1]
            strike_str     = row[COL["strike"] - 1]
            side           = row[COL["side"] - 1]
            contracts      = int(row[COL["contracts"] - 1])
            amount_spent   = float(row[COL["amount_spent"] - 1])
            balance_after_str = row[COL["balance_after"] - 1]

            target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

            # Only settle if observation is available
            if target_date >= today:
                log.debug(f"  Trade #{trade_id} ({city} {target_date}): target date not yet passed")
                continue

            actual_temp = get_actual_temp(city, target_date)
            if actual_temp is None:
                log.warning(f"  Trade #{trade_id} ({city} {target_date}): no observation yet")
                continue

            # Parse contract type and strike
            ct_type = contract_type_str.split(" ")[0]  # ABOVE, BELOW, RANGE
            try:
                strike = float(strike_str)
            except ValueError:
                strike = 0.0

            # Determine if RANGE has upper bound
            upper_strike = None
            if ct_type == "RANGE":
                parts = contract_type_str.replace("RANGE", "").strip().split("-")
                if len(parts) == 2:
                    try:
                        strike = float(parts[0])
                        upper_strike = float(parts[1])
                    except ValueError:
                        pass

            yes_won = did_yes_win(ct_type, strike, actual_temp, upper_strike)

            # Determine outcome based on which side we bought
            if side == "YES":
                won = yes_won
            else:  # NO
                won = not yes_won

            payout = round(contracts * 1.00, 2) if won else 0.0
            outcome = "WIN" if won else "LOSS"

            settlements.append({
                "row_idx":     row_idx,
                "trade_id":    trade_id,
                "city":        city,
                "target_date": target_date,
                "actual_temp": actual_temp,
                "contract":    f"{ct_type} {strike:.0f}°F",
                "side":        side,
                "outcome":     outcome,
                "payout":      payout,
                "amount_spent": amount_spent,
                "won":         won,
            })

            log.info(f"  Trade #{trade_id}: {city} {target_date} — actual={actual_temp:.0f}°F  "
                     f"{ct_type} {strike:.0f}°F {side}  →  {outcome}  payout=${payout:.2f}")

        except Exception as e:
            log.error(f"  Error processing row {row_idx}: {e}")

    if not settlements:
        log.info("No trades ready to settle (observations not yet available).")
        return

    # Summary
    wins  = sum(1 for s in settlements if s["won"])
    losses = len(settlements) - wins
    total_payout = sum(s["payout"] for s in settlements)
    total_spent  = sum(s["amount_spent"] for s in settlements)
    net = total_payout - total_spent

    print()
    print("=" * 70)
    print(f"  SETTLEMENT REPORT  —  {today}")
    print("=" * 70)
    print(f"  {'Trade':>6}  {'City':22s}  {'Date':>10}  {'Actual':>6}  {'Contract':20s}  {'Side':4}  {'Result':6}  {'P&L':>7}")
    print(f"  {'-'*6}  {'-'*22}  {'-'*10}  {'-'*6}  {'-'*20}  {'-'*4}  {'-'*6}  {'-'*7}")
    for s in settlements:
        pnl = s["payout"] - s["amount_spent"]
        print(f"  #{s['trade_id']:>5}  {s['city']:22s}  {str(s['target_date']):>10}  "
              f"{s['actual_temp']:>5.0f}°  {s['contract']:20s}  {s['side']:4}  "
              f"{'WIN' if s['won'] else 'LOSS':6}  ${pnl:>+6.2f}")

    print(f"\n  Results:  {wins}W / {losses}L")
    print(f"  Gross payout:   ${total_payout:.2f}")
    print(f"  Amount risked:  ${total_spent:.2f}")
    print(f"  Net P&L:        ${net:+.2f}")
    print()

    if not commit:
        print("  DRY RUN — pass --commit to write results to Google Sheets")
        print("=" * 70)
        return

    # Write back to sheet
    # Get current balance from last balance row
    bal_rows = ws_balance.get_all_values()
    current_balance = 100.0
    for row in reversed(bal_rows[1:]):
        if row[3]:
            try:
                current_balance = float(row[3])
                break
            except ValueError:
                pass

    for s in settlements:
        # Update Trades row: settled=YES, outcome, payout_received, balance_after
        current_balance += s["payout"] - s["amount_spent"] if s["won"] else 0
        # Actually payout is gross; we already spent the amount_spent when we opened the trade
        # So balance impact = +payout if WIN, +0 if LOSS (amount_spent already deducted)
        if s["won"]:
            current_balance += s["payout"]
        # Note: amount_spent was already deducted when trade was opened

        ws_trades.update(
            range_name=f"R{s['row_idx']}C{COL['settled']}:R{s['row_idx']}C{COL['balance_after']}",
            values=[["YES", s["outcome"], s["payout"], round(current_balance, 2)]],
        )

        # Balance log entry
        pnl_label = f"WIN +${s['payout']:.2f}" if s["won"] else "LOSS $0"
        ws_balance.append_row([
            today.isoformat(),
            f"Trade #{s['trade_id']} settled: {s['city']} {s['contract']} {s['side']} → {s['outcome']} ({pnl_label})",
            s["payout"] if s["won"] else 0,
            round(current_balance, 2),
            "",
        ], value_input_option="USER_ENTERED")

        log.info(f"  Updated trade #{s['trade_id']} → {s['outcome']}, balance=${current_balance:.2f}")

    print(f"  {len(settlements)} trades settled. New balance: ${current_balance:.2f}")
    print("=" * 70)


if __name__ == "__main__":
    main()
