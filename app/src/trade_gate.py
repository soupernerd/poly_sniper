"""Canonical execution gate checks shared across runtime paths."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class GateDecision:
    allowed: bool
    reason: str = ""
    code: str = ""


def evaluate_execution_gate(
    *,
    config,
    market,
    is_hft_runner: bool,
    is_resolved: bool,
    now_utc: Optional[datetime] = None,
) -> GateDecision:
    """Run canonical pre-execution gates.

    Gate order:
      1) Terminal market (resolved)
      2) No-bet asset
      3) HFT muzzle zone (for runner paths)
    """
    if is_resolved:
        return GateDecision(
            allowed=False,
            code="resolved_terminal",
            reason="market already resolved; execution blocked",
        )

    asset = str(getattr(market, "asset", "") or "").strip().lower()
    if asset and asset in getattr(config.execution, "no_bet_set", set()):
        return GateDecision(
            allowed=False,
            code="no_bet_asset",
            reason=f"asset {asset} is watch-only (no_bet)",
        )

    if is_hft_runner:
        now_utc = now_utc or datetime.now(timezone.utc)
        end_dt = getattr(market, "end_date", None)
        if end_dt:
            sec_to_end = (end_dt - now_utc).total_seconds()
            muzzle = max(
                int(getattr(config.trend, "hft_muzzle_seconds", 0) or 0),
                int(getattr(config.scanner, "stop_buffer_seconds", 0) or 0),
            )
            if sec_to_end < muzzle:
                return GateDecision(
                    allowed=False,
                    code="muzzle_zone",
                    reason=f"inside muzzle zone (sec_to_end={sec_to_end:.1f}, muzzle={muzzle})",
                )

    return GateDecision(allowed=True)

