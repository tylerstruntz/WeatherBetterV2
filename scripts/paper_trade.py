"""
paper_trade.py
--------------
Takes the ranked slate from evaluate_markets.py, picks the top N plays,
allocates capital evenly from 30% of the current balance, and logs to Sheets.

Usage:
    python paper_trade.py           # dry run -- print only
    python paper_trade.py --commit  # write trades to Google Sheets
"""

import sys
import json
import logging
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from evaluate_markets import run, STARTING_BALANCE, RISK_PCT, TOP_N

# ── Config ────────────────────────────────────────────────────────────────────

SHEETS_KEY_PATH = Path(r"F:/weatherbetterv2/tokens/sheets/weather-trading-491402-9130a093af1c.json")
SPREADSHEET_ID  = "1sVyy1tgHA1RFIhpFImt6Ah3ptJEQ5Tnpmk6WddZt5xA"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ── Sheets ────────────────────────────────────────────────────────────────────

def get_sheets_client():
    import os
    import gspread
    from google.oauth2.service_account import Credentials

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # In CI the service account JSON is injected via env var
    svc_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if svc_json:
        info = json.loads(svc_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(str(SHEETS_KEY_PATH), scopes=SCOPES)

    return gspread.authorize(creds)


def get_current_balance(gc) -> float:
    """Read the latest balance from the Balance sheet."""
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        ws = sh.worksheet("Balance")
        vals = ws.get_all_values()
        for row in reversed(vals[1:]):   # skip header, walk up from bottom
            try:
                return float(row[3])     # column D = Balance
            except (ValueError, IndexError):
                continue
    except Exception as e:
        log.warning(f"Could not read balance: {e}")
    return STARTING_BALANCE


def append_trades(gc, trades: list[dict], balance_after: float):
    """Append trade rows to the Trades sheet."""
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet("Trades")

    rows = []
    for t in trades:
        rows.append([
            t["trade_id"],
            t["market_id"],
            t["city"],
            t["target_date"],
            t["contract_type"],
            t["strike_label"],
            t["side"],
            t["price_cents"],
            t["contracts"],
            t["amount_spent"],
            t["potential_payout"],
            t["ml_pred"],
            t["nws_pred"] if t["nws_pred"] else "",
            t["combined_pred"],
            t["return_mult"],
            t["lead_days"],
            "PENDING",  # settled
            "",         # outcome
            "",         # payout_received
            "",         # balance_after (filled on settlement)
            "",         # notes
        ])

    ws.append_rows(rows, value_input_option="USER_ENTERED")
    log.info(f"Appended {len(rows)} trades to Sheets")


# ── Allocation ────────────────────────────────────────────────────────────────

def build_allocation(slate: list[dict], balance: float) -> list[dict]:
    """
    Take top N plays from the ranked slate.
    Split 30% of balance evenly across them.
    """
    top    = slate[:TOP_N]
    budget = balance * RISK_PCT
    per_play = budget / len(top) if top else 0

    trades = []
    for i, play in enumerate(top):
        price_dollars = play["price"] / 100
        n_contracts   = int(per_play / price_dollars)
        if n_contracts < 1:
            continue

        amount_spent     = round(n_contracts * price_dollars, 2)
        potential_payout = round(n_contracts * 1.00, 2)   # $1 per contract at resolution

        # Human-readable strike label
        ctype = play["ctype"]
        if ctype == "greater":
            strike_label = f">{play['lower']:.0f}F"
        elif ctype == "less":
            strike_label = f"<{play['lower']:.0f}F"
        else:
            strike_label = f"{play['lower']:.0f}-{play['upper']:.0f}F"

        trade_id = f"PT-{date.today().strftime('%Y%m%d')}-{i+1:02d}"

        trades.append({
            "trade_id":        trade_id,
            "market_id":       play["ticker"],
            "city":            play["city"],
            "target_date":     play["date"],
            "contract_type":   ctype,
            "strike_label":    strike_label,
            "side":            play["side"],
            "price_cents":     play["price"],
            "contracts":       n_contracts,
            "amount_spent":    amount_spent,
            "potential_payout": potential_payout,
            "ml_pred":         play["ml"],
            "nws_pred":        play.get("nws"),
            "combined_pred":   play["combined"],
            "return_mult":     play["return_mult"],
            "lead_days":       play["lead_days"],
        })

    return trades


# ── Printer ───────────────────────────────────────────────────────────────────

def print_allocation(trades: list[dict], balance: float, dry_run: bool):
    SEP = "=" * 105
    print()
    print(SEP)
    mode = "DRY RUN" if dry_run else "COMMITTING TO SHEETS"
    print(f"  PAPER TRADE ALLOCATION  --  {date.today()}  --  {mode}")
    print(SEP)
    print(f"  {'#':>2}  {'City':22s}  {'Date':>10}  {'Contract':22s}  "
          f"{'Side':>4}  {'Price':>5}  {'ML':>5}  {'NWS':>5}  {'Comb':>5}  "
          f"{'Return':>7}  {'Qty':>6}  {'Risked':>8}  {'Payout':>8}")
    print(f"  {'-'*2}  {'-'*22}  {'-'*10}  {'-'*22}  "
          f"{'-'*4}  {'-'*5}  {'-'*5}  {'-'*5}  {'-'*5}  "
          f"{'-'*7}  {'-'*6}  {'-'*8}  {'-'*8}")

    total_risk = 0.0
    total_payout = 0.0
    for i, t in enumerate(trades, 1):
        nws_str = f"{t['nws_pred']:.1f}" if t.get("nws_pred") else "  --"
        total_risk   += t["amount_spent"]
        total_payout += t["potential_payout"]
        print(f"  {i:>2}  {t['city']:22s}  {t['target_date']:>10}  "
              f"{t['strike_label']:22s}  {t['side']:>4}  "
              f"{t['price_cents']:>4}c  {t['ml_pred']:>5.1f}  {nws_str:>5}  "
              f"{t['combined_pred']:>5.1f}  {t['return_mult']:>6.2f}x  "
              f"{t['contracts']:>6,}  ${t['amount_spent']:>7.2f}  ${t['potential_payout']:>7.2f}")

    budget = balance * RISK_PCT
    print()
    print(f"  Balance: ${balance:.2f}  |  Budget (30%): ${budget:.2f}  |  "
          f"Total risked: ${total_risk:.2f}  |  If all win: ${total_payout:.2f}")
    print(SEP)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    dry_run = "--commit" not in sys.argv

    # 1. Get ranked slate
    slate = run(output_json=False, show_all=False)
    if not slate:
        log.warning("No plays found -- nothing to trade")
        return

    # 2. Determine current balance
    balance = STARTING_BALANCE
    if not dry_run:
        try:
            gc      = get_sheets_client()
            balance = get_current_balance(gc)
            log.info(f"Current balance: ${balance:.2f}")
        except Exception as e:
            log.warning(f"Sheets unavailable, using starting balance: {e}")

    # 3. Build allocation
    trades = build_allocation(slate, balance)
    if not trades:
        log.warning("No valid trades after sizing -- check prices/balance")
        return

    # 4. Print
    print_allocation(trades, balance, dry_run)

    # 5. Commit to Sheets
    if not dry_run:
        try:
            if not isinstance(gc, type(None)):
                pass
            else:
                gc = get_sheets_client()
            append_trades(gc, trades, balance)
            log.info("Trades written to Google Sheets")
        except Exception as e:
            log.error(f"Failed to write to Sheets: {e}")
            sys.exit(1)
    else:
        print("\n  Run with --commit to write to Google Sheets\n")


if __name__ == "__main__":
    main()
