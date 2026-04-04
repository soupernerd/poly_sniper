"""Deterministic lifecycle event logging helpers.

All core runtime components should emit the same schema so a CID can be
traced linearly in logs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from src.timed_market_utils import DURATION_TO_TF_LABEL


def _iso_utc(value: Optional[datetime]) -> str:
    if not value:
        return "n/a"
    try:
        return value.astimezone(timezone.utc).isoformat()
    except Exception:
        return str(value)


def log_lifecycle(
    *,
    logger: logging.Logger,
    phase: str,
    condition_id: str,
    asset: str,
    timeframe_seconds: int,
    market_start: Optional[datetime],
    market_end: Optional[datetime],
    source: str,
    detail: str = "",
) -> None:
    """Emit one deterministic lifecycle line for a CID."""
    tf = DURATION_TO_TF_LABEL.get(int(timeframe_seconds or 0), str(int(timeframe_seconds or 0)))
    extra = f" detail={detail}" if detail else ""
    logger.info(
        "[LIFECYCLE] phase=%s cid=%s asset=%s tf=%s start=%s end=%s source=%s%s",
        str(phase or "").strip().lower(),
        str(condition_id or ""),
        str(asset or "").lower(),
        tf,
        _iso_utc(market_start),
        _iso_utc(market_end),
        str(source or ""),
        extra,
    )

