"""
discord_notify.py
-----------------
Shared Discord webhook notification utility for the WeatherBetterV2 pipeline.
Sends embedded messages with pipeline run results.

Usage (standalone):
    from discord_notify import send_pipeline_summary, send_message
"""

import requests
import logging
from pathlib import Path
from datetime import datetime

WEBHOOK_PATH = Path(r"F:/weatherbetterv2/tokens/discord/webhook.key")

log = logging.getLogger(__name__)


def _load_webhook_url():
    import os
    if os.environ.get("DISCORD_WEBHOOK"):
        return os.environ["DISCORD_WEBHOOK"]
    if not WEBHOOK_PATH.exists():
        log.warning(f"Discord webhook not found at {WEBHOOK_PATH}")
        return None
    url = WEBHOOK_PATH.read_text().strip()
    return url if url else None


def send_message(content):
    """Send a simple text message to Discord."""
    url = _load_webhook_url()
    if not url:
        return False
    try:
        resp = requests.post(url, json={"content": content}, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Discord send failed: {e}")
        return False


def send_embed(embeds):
    """Send one or more embeds to Discord."""
    url = _load_webhook_url()
    if not url:
        return False
    try:
        resp = requests.post(url, json={
            "username": "WeatherBetter",
            "embeds": embeds,
        }, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Discord send failed: {e}")
        return False


def send_pipeline_summary(step_results, start_time, end_time=None):
    """
    Send a pipeline run summary embed.

    step_results: list of (step_name, success_bool, detail_str_or_None)
    """
    end_time = end_time or datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    all_ok = all(ok for _, ok, _ in step_results)
    color = 0x2ECC71 if all_ok else 0xE74C3C  # green or red

    # Build step lines
    lines = []
    for name, ok, detail in step_results:
        icon = "\u2705" if ok else "\u274C"
        line = f"{icon} **{name}**"
        if detail:
            line += f"\n\u2003\u2003{detail}"
        lines.append(line)

    embed = {
        "title": "\U0001f4ca Daily Pipeline " + ("Complete" if all_ok else "Failed"),
        "color": color,
        "description": "\n".join(lines),
        "footer": {"text": f"Ran in {minutes}m {seconds}s"},
        "timestamp": end_time.isoformat(),
    }

    return send_embed([embed])


def send_forecast_comparison(predictions, today_str):
    """
    Send a forecast comparison table for tomorrow (lead 1d).

    predictions: list of dicts with keys: city_name, gfs_high, ecmwf_high, nws_high, ml_high
    """
    if not predictions:
        return False

    lines = [f"```{'City':20s} {'GFS':>5s} {'ECMWF':>5s} {'NWS':>5s} | {'ML':>5s}"]
    lines.append(f"{'-'*20} {'-'*5} {'-'*5} {'-'*5} + {'-'*5}")

    for p in sorted(predictions, key=lambda x: x["city_name"]):
        gfs   = f"{p['gfs_high']:.0f}" if p.get("gfs_high") else "  -"
        ecmwf = f"{p['ecmwf_high']:.0f}" if p.get("ecmwf_high") else "  -"
        nws   = f"{p['nws_high']:.0f}" if p.get("nws_high") else "  -"
        ml    = f"{p['ml_high']:.0f}"
        lines.append(f"{p['city_name']:20s} {gfs:>5s} {ecmwf:>5s} {nws:>5s} | {ml:>5s}")

    lines.append("```")

    embed = {
        "title": f"\U0001f321\ufe0f Tomorrow's High Forecasts ({today_str})",
        "color": 0x3498DB,
        "description": "\n".join(lines),
    }

    return send_embed([embed])
