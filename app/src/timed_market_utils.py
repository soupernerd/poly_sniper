"""Shared timed-market parsing and normalization helpers.

This module is intentionally scanner-agnostic so discovery/monitor/runtime
systems all use one parsing path.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

# Matches: "Bitcoin Up or Down ..."
ASSET_PATTERN = re.compile(r"(bitcoin|ethereum|solana|xrp|btc|eth|sol)\b", re.IGNORECASE)
TIMED_PATTERN = re.compile(r"up\s+or\s+down", re.IGNORECASE)

# "10:00PM-10:15PM ET"
TIME_RANGE_PATTERN = re.compile(
    r"(\d{1,2}(?::\d{2})?\s*[AP]M)\s*-\s*(\d{1,2}(?::\d{2})?\s*[AP]M)",
    re.IGNORECASE,
)

# "10PM ET"
SINGLE_TIME_PATTERN = re.compile(r"(\d{1,2}(?::\d{2})?\s*[AP]M)\s*ET", re.IGNORECASE)

TF_LABEL_TO_DURATION = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}
DURATION_TO_TF_LABEL = {v: k for k, v in TF_LABEL_TO_DURATION.items()}

# Duration-scaled discovery lookahead caps.
LOOKAHEAD_BY_DURATION = {
    300: 900,      # 5m -> 15m
    900: 2700,     # 15m -> 45m
    3600: 9000,    # 1h -> 2.5h
    14400: 21600,  # 4h -> 6h
    86400: 86400,  # 1d -> 24h
}


def normalize_asset(asset: str) -> str:
    a = str(asset or "").strip().lower()
    if a == "btc":
        return "bitcoin"
    if a == "eth":
        return "ethereum"
    if a == "sol":
        return "solana"
    return a


def parse_assets_csv(raw: str) -> set[str]:
    out: set[str] = set()
    for token in str(raw or "").split(","):
        asset = normalize_asset(token)
        if asset in {"bitcoin", "ethereum", "solana", "xrp"}:
            out.add(asset)
    return out


def parse_timeframes_csv(raw: str) -> set[int]:
    out: set[int] = set()
    for token in str(raw or "").split(","):
        tf = token.strip().lower()
        if tf in TF_LABEL_TO_DURATION:
            out.add(TF_LABEL_TO_DURATION[tf])
    return out


def parse_date_utc(date_str: str) -> datetime:
    s = str(date_str or "").strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def parse_outcome(question: str, outcomes: list, event_title: str) -> str:
    """Determine whether the market row represents Up or Down."""
    q_lower = str(question or "").lower()
    _t_lower = str(event_title or "").lower()

    if "go up" in q_lower or "higher" in q_lower or "up?" in q_lower:
        return "Up"
    if "go down" in q_lower or "lower" in q_lower or "down?" in q_lower:
        return "Down"

    if isinstance(outcomes, list):
        for raw in outcomes:
            value = str(raw).lower()
            if value in ("up", "yes") and "up" in q_lower:
                return "Up"
            if value in ("down", "no") and "down" in q_lower:
                return "Down"

    if "up" in q_lower:
        return "Up"
    if "down" in q_lower:
        return "Down"
    return ""


def get_both_tokens(market: dict) -> tuple[str, str]:
    """Extract (up_token, down_token) from a Gamma market row."""
    clob_ids = market.get("clobTokenIds", "")
    if isinstance(clob_ids, str):
        try:
            parsed = json.loads(clob_ids)
            if isinstance(parsed, list):
                if len(parsed) >= 2:
                    return str(parsed[0] or ""), str(parsed[1] or "")
                if len(parsed) == 1:
                    return str(parsed[0] or ""), ""
        except Exception:
            pass
    elif isinstance(clob_ids, list):
        if len(clob_ids) >= 2:
            return str(clob_ids[0] or ""), str(clob_ids[1] or "")
        if len(clob_ids) == 1:
            return str(clob_ids[0] or ""), ""

    tokens = market.get("tokens", [])
    if isinstance(tokens, list):
        if len(tokens) >= 2:
            t0 = tokens[0].get("token_id", "") if isinstance(tokens[0], dict) else ""
            t1 = tokens[1].get("token_id", "") if isinstance(tokens[1], dict) else ""
            return str(t0 or ""), str(t1 or "")
        if len(tokens) == 1:
            t0 = tokens[0].get("token_id", "") if isinstance(tokens[0], dict) else ""
            return str(t0 or ""), ""
    return "", ""


def parse_market_times(title: str, end_date: datetime) -> tuple[Optional[datetime], int]:
    """Parse market start + duration from title and end_date."""
    ET = ZoneInfo("America/New_York")

    def _parse_time(raw: str):
        s = raw.replace(" ", "")
        try:
            if ":" in s:
                return datetime.strptime(s, "%I:%M%p").time()
            return datetime.strptime(s, "%I%p").time()
        except ValueError:
            return None

    time_match = TIME_RANGE_PATTERN.search(title)
    if time_match:
        start_time = _parse_time(time_match.group(1).strip().upper())
        end_time = _parse_time(time_match.group(2).strip().upper())
        if not start_time or not end_time:
            return None, 0

        end_et = end_date.astimezone(ET)
        # Anchor the parsed end-time to the API end_date day, then shift by day
        # only if needed. This avoids midnight-crossing windows (e.g. 11:55PM-12:00AM)
        # being pushed one day into the future.
        end_dt_et = datetime.combine(end_et.date(), end_time, tzinfo=ET)
        if (end_dt_et - end_et) > timedelta(hours=12):
            end_dt_et -= timedelta(days=1)
        elif (end_et - end_dt_et) > timedelta(hours=12):
            end_dt_et += timedelta(days=1)

        # Start is always the most recent start_time at-or-before parsed end.
        start_dt_et = datetime.combine(end_dt_et.date(), start_time, tzinfo=ET)
        if start_dt_et >= end_dt_et:
            start_dt_et -= timedelta(days=1)

        duration = int((end_dt_et - start_dt_et).total_seconds())
        if duration <= 0:
            return None, 0
        return start_dt_et.astimezone(timezone.utc), duration

    single_match = SINGLE_TIME_PATTERN.search(title)
    if single_match:
        start_time = _parse_time(single_match.group(1).strip().upper())
        if start_time:
            end_et = end_date.astimezone(ET)
            base_date = end_et.date()
            start_dt_et = datetime.combine(base_date, start_time, tzinfo=ET)
            if start_dt_et > end_et:
                start_dt_et -= timedelta(days=1)
            start_dt = start_dt_et.astimezone(timezone.utc)
            duration = int((end_date - start_dt).total_seconds())
            if duration > 0:
                return start_dt, duration

    daily_match = re.search(r"on\s+(\w+)\s+(\d{1,2})", title, re.IGNORECASE)
    if daily_match:
        return end_date - timedelta(days=1), 86400

    return None, 0


def enhance_title(title: str, end_date: datetime, duration_secs: int = 0) -> str:
    """Expand single-time 1h+ labels to explicit ranges for display."""
    if TIME_RANGE_PATTERN.search(title):
        return title
    if duration_secs < 3600:
        return title

    single_match = SINGLE_TIME_PATTERN.search(title)
    if not single_match:
        return title

    end_et = end_date.astimezone(ZoneInfo("America/New_York"))
    if end_et.minute == 0:
        end_fmt = end_et.strftime("%I%p").lstrip("0").upper()
    else:
        end_fmt = end_et.strftime("%I:%M%p").lstrip("0").upper()
    start_part = single_match.group(1).strip()
    return title.replace(single_match.group(0), f"{start_part}-{end_fmt} ET", 1)
