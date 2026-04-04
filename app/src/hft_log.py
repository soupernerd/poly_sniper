"""Shared HFT log formatting helpers."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

_ASSET_SHORT = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "xrp": "XRP",
}


def short_cid(condition_id: str, n: int = 10) -> str:
    return str(condition_id or "")[:n]


def _format_et_time(dt: datetime) -> str:
    if dt.minute == 0:
        return dt.strftime("%I%p").lstrip("0")
    return dt.strftime("%I:%M%p").lstrip("0")


def _tf_label(duration_seconds: int) -> str:
    dur = int(duration_seconds or 0)
    if dur <= 0:
        return ""
    if dur < 3600:
        return f"{dur // 60}m"
    if dur < 86400:
        return f"{dur // 3600}h"
    return "1d"


def format_market_label(market) -> str:
    """Return compact market label for logs.

    Example: "BTC 15m 2:30PM-2:45PM ET"
    """
    asset = str(getattr(market, "asset", "") or "").lower()
    asset_short = _ASSET_SHORT.get(asset, asset.upper() or "UNK")
    tf = _tf_label(int(getattr(market, "duration_seconds", 0) or 0))
    start = getattr(market, "market_start", None)
    end = getattr(market, "end_date", None)

    parts: list[str] = [asset_short]
    if tf:
        parts.append(tf)

    if start and end:
        try:
            s_et = start.astimezone(ET)
            e_et = end.astimezone(ET)
            parts.append(f"{_format_et_time(s_et)}-{_format_et_time(e_et)} ET")
            return " ".join(parts)
        except Exception:
            pass

    title = str(getattr(market, "event_title", "") or "").strip()
    if title:
        return f"{' '.join(parts)} | {title}"
    return " ".join(parts)
