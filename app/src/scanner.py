"""Market scanner -- evaluates snipe opportunities from runtime markets.

Discovery/PTB/subscription ownership is handled by MarketRuntime.
Scanner only handles trading-oriented evaluation paths.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Callable, Optional, TYPE_CHECKING

from src.api import PolymarketAPI
from src.config import Config
from src.hft_log import format_market_label, short_cid
from src.lifecycle import log_lifecycle
from src.mode2_metrics import record_latency as record_mode2_latency
from src.monitor_db import MonitorDB, MONITOR_TICK_INTERVAL, MONITOR_WINDOWS
from src.post_market import PostMarketManager
from src.ptb_scraper import PTBScraper
from src.trade_gate import evaluate_execution_gate
from src.timed_market_utils import DURATION_TO_TF_LABEL as _DURATION_TO_TF_LABEL
from src.timed_market_utils import TF_LABEL_TO_DURATION as _TF_LABEL_TO_DURATION
from src.ws_client import MarketWSClient


def _bg(coro, label: str = "background"):
    """Fire-and-forget: create_task + log any exception via callback."""
    task = asyncio.create_task(coro)
    def _on_done(t, _label=label):
        if t.cancelled():
            return
        exc = t.exception()
        if exc:
            logging.getLogger(__name__).debug("%s task error: %s", _label, exc)
    task.add_done_callback(_on_done)
    return task

if TYPE_CHECKING:
    from src.market_runtime import MarketRuntime
    from src.price_feed import BinancePriceFeed
    from src.database import Database

logger = logging.getLogger(__name__)

# Hardcoded minimum ask-side depth ($) required to snipe a market.
_DEPTH_FLOOR = 100

# Slot granularity per timeframe (minutes)
_SLOT_GRANULARITY = {300: 5, 900: 15, 3600: 60, 14400: 60, 86400: 60}

def _end_slot(market) -> int:
    """Compute the minute-of-hour slot for a market's end_date."""
    g = _SLOT_GRANULARITY.get(market.duration_seconds, 5)
    return (market.end_date.minute // g) * g

_ORACLE_SHORT = {"chainlink": "CL", "binance": "BIN"}


def _oracle_directions(
    consensus: dict | None,
    gap_info: dict | None,
) -> tuple[str | None, str | None]:
    """Extract per-oracle directions for monitor_db recording.

    Returns (chainlink_direction, binance_direction) — each "Up", "Down", or None.
    Prefers consensus dict (has both); falls back to gap_info's primary oracle.
    """
    cl_dir: str | None = None
    bin_dir: str | None = None
    if consensus:
        cl_dir = consensus.get("chainlink_side")
        bin_dir = consensus.get("binance_side")
        # Convert "unavailable" → None
        if cl_dir == "unavailable":
            cl_dir = None
        if bin_dir == "unavailable":
            bin_dir = None
    elif gap_info:
        orc = gap_info.get("current_oracle")
        ws = gap_info.get("winning_side")
        if orc == "chainlink":
            cl_dir = ws
        elif orc == "binance":
            bin_dir = ws
    return cl_dir, bin_dir


@dataclass
class TrackedMarket:
    """A single outcome market being tracked for snipe opportunity.

    Each Up/Down event has ONE market with TWO tokens:
      token_id     = clobTokenIds[0] ("Yes"/Up outcome)
      token_id_alt = clobTokenIds[1] ("No"/Down outcome)
    We subscribe to BOTH and snipe whichever becomes the winner.
    """
    condition_id: str
    token_id: str           # CLOB token for outcome 0 ("Up"/"Yes")
    token_id_alt: str       # CLOB token for outcome 1 ("Down"/"No")
    outcome: str            # "Up" or "Down" (parsed from title)
    event_title: str
    event_slug: str
    end_date: datetime      # When the time window ends
    asset: str              # "bitcoin", "ethereum", "solana"
    neg_risk: bool = False  # Whether this is a neg-risk market

    # Price-gap safety gate (populated by price_feed)
    market_start: Optional[datetime] = None   # When the market time window starts
    duration_seconds: int = 0                  # Market duration (300 = 5m, 900 = 15m, etc.)
    price_to_beat: Optional[float] = None      # Binance price at market_start
    ptb_source: str = ""                        # "live" or "backfill"

    # Live state (updated each scan)
    best_ask: float = 0.0
    midpoint: float = 0.0
    ask_depth: float = 0.0
    winning_token: str = ""  # Which token_id is the winner
    winning_side: str = ""   # "Up" or "Down" (which side we're sniping)

    def __hash__(self):
        return hash(self.condition_id)


@dataclass
class SnipeOpportunity:
    """A concrete buy opportunity identified by the scanner."""
    market: TrackedMarket
    best_ask: float
    ask_depth: float
    edge: float             # Expected profit per share (1 - best_ask)
    reason: str             # Why this was flagged
    winning_token: str = "" # Token ID to buy (the winning side)
    winning_side: str = ""  # "Up" or "Down"
    gap_pct: float | None = None        # Binance price gap % at snipe time
    gap_direction: str | None = None    # "above" or "below"
    is_post_end: bool = False           # True = post-end fast-path (skip heavy gates)
    override_amount: float | None = None # Explicit bet size (for HFT/Trend overrides)
    is_hft_runner: bool = False         # If True, trigger instant 'Flip' (Limit Sell) in executor
    timeframe_seconds: int = 0
    hft_barrier_pct: float | None = None
    hft_cap_price: float | None = None
    hft_entry_path: str | None = None   # "instant" | "trap"
    hft_trigger_ask: float | None = None


def _mkt_tag(market) -> str:
    """Compact market label for logs."""
    return format_market_label(market)


class Scanner:
    """Consumes runtime markets and executes snipe logic.

    Strategies (in priority order):
      1. WS-triggered instant sniping -- reacts to real-time price_change events
         sub-second, fires immediately when a winning token's ask < max_buy_price.
      2. Pre-end sniping -- if mid > pre_end_threshold with < pre_end_seconds,
         buys before market officially ends for high-confidence edge.
      3. Polled post-end (10s backup) -- traditional scan loop checks, last resort.
    """

    def __init__(
        self,
        config: Config,
        api: PolymarketAPI,
        ws: MarketWSClient,
        price_feed: Optional["BinancePriceFeed"] = None,
        db: Optional["Database"] = None,
        market_runtime: Optional["MarketRuntime"] = None,
    ):
        self.config = config
        self.api = api
        self._ws = ws
        self._price_feed = price_feed
        self._db = db
        self._market_runtime = market_runtime

        # Parse configured assets
        self._assets = set(
            a.strip().lower()
            for a in config.scanner.assets.split(",")
            if a.strip()
        )

        # Parse configured timeframes ("5m,15m,1h" → {300, 900, 3600})
        self._time_frames: set[int] = set()
        for tf in config.scanner.time_frames.split(","):
            tf = tf.strip().lower()
            if tf in _TF_LABEL_TO_DURATION:
                self._time_frames.add(_TF_LABEL_TO_DURATION[tf])
        if not self._time_frames:
            # Safety fallback: if nothing parsed, enable all
            self._time_frames = set(_TF_LABEL_TO_DURATION.values())
        self._asset_tf_enabled: dict[str, dict[str, bool]] = (
            config.scanner.get_asset_timeframe_enabled_matrix()
        )

        # Active markets being tracked: condition_id -> TrackedMarket
        self._tracked: dict[str, TrackedMarket] = {}

        # Cooldown tracking: condition_id -> last_buy_timestamp
        self._cooldowns: dict[str, float] = {}

        # HFT-specific cooldown tracking (separate from regular scanner cooldowns):
        #   "<cid>"       -> MODE1/MODE3 rolling cooldown
        #   "mode2:<cid>" -> MODE2 one-and-done gate
        self._hft_cooldowns: dict[str, float] = {}

        # Reverse lookup: token_id -> TrackedMarket (for instant WS-triggered sniping)
        self._token_to_market: dict[str, TrackedMarket] = {}

        # Prevent duplicate instant snipes while one is executing
        self._pending_snipes: set[str] = set()

        # Armed Traps for HFT Mode 2 WS-Instant execution
        self._hft_armed_traps: dict[str, dict] = {}
        self._hft_trap_eval_inflight: set[str] = set()

        # ── Post-market manager (fully separated module) ────────────────
        self._ptb_scraper = PTBScraper()
        self._post_market = PostMarketManager(
            config=config, api=api, ptb_scraper=self._ptb_scraper, db=db,
        )

        # Callback for instant snipe execution (set by main.py)
        self._snipe_callback: Optional[Callable] = None

        # Snapshot throttle: condition_id -> last snapshot timestamp
        self._snapshot_ts: dict[str, float] = {}
        self._SNAPSHOT_INTERVAL = 5  # seconds between snapshots per market

        # Monitor DB: separate database for wider observation windows
        self._monitor_db: MonitorDB | None = None
        self._monitor_ts: dict[str, float] = {}  # cid -> last monitor tick ts

        # Alias for backward compat (dashboard may read scanner._maker_orders)
        self._maker_orders = self._post_market.maker_orders
        self._resolved_cids = self._post_market.resolved_cids

        # REST debounce for WS-triggered checks: token_id -> last_rest_timestamp
        self._ws_rest_ts: dict[str, float] = {}

        # Runtime-adjustable WS cleanup delay (dashboard override, 0 = use config default)
        self._ws_cleanup_override: int = 0

        # Stats
        self.scan_count = 0
        self.opportunities_found = 0
        self.in_snipe_window = False

        # Verbose logging toggle (set live via dashboard)
        self.verbose: bool = False

        # Status log throttle for scan-loop heartbeat lines.
        # Default keeps existing behavior (every ~10 scans).
        self._status_log_every_scans: int = 10

        # Trend analyzer reference (set by main.py)
        self._trend_analyzer = None

    @property
    def tracked_markets(self) -> list[TrackedMarket]:
        """Return all tracked markets, sorted by end_date."""
        return sorted(self._tracked.values(), key=lambda m: m.end_date)

    @property
    def active_count(self) -> int:
        return len(self._tracked)

    def is_terminal_market(self, condition_id: str) -> bool:
        """True when resolution is known and further execution is blocked."""
        return str(condition_id or "") in self._resolved_cids

    @property
    def ws_cleanup_delay(self) -> int:
        """Current effective cleanup delay in seconds."""
        return self._ws_cleanup_override or self.config.scanner.ws_cleanup_delay

    @ws_cleanup_delay.setter
    def ws_cleanup_delay(self, value: int):
        self._ws_cleanup_override = max(10, int(value))  # min 10s safety floor

    def _get_gap_threshold(self, asset: str, duration_seconds: int = 0) -> float:
        """Get the minimum % gap required (per-timeframe -> global fallback)."""
        return self.config.scanner.get_price_gap_pct(duration_seconds)

    def _is_cell_enabled(self, asset: str, duration_seconds: int) -> bool:
        """Single effective gate for asset/timeframe runtime toggles."""
        a = str(asset or "").strip().lower()
        tf = _DURATION_TO_TF_LABEL.get(int(duration_seconds or 0), "")
        if a not in self._assets:
            return False
        if int(duration_seconds or 0) not in self._time_frames:
            return False
        if not tf:
            return False
        row = self._asset_tf_enabled.get(a)
        if isinstance(row, dict):
            return bool(row.get(tf, True))
        return True

    def _sync_runtime_markets(self):
        """Mirror canonical runtime markets into scanner-local state."""
        if not self._market_runtime:
            return
        runtime_markets = self._market_runtime.scanner_markets()
        incoming: dict[str, object] = {m.condition_id: m for m in runtime_markets}
        current_ids = set(self._tracked.keys())
        incoming_ids = set(incoming.keys())

        removed = [cid for cid in current_ids if cid not in incoming_ids]
        for cid in removed:
            m = self._tracked.get(cid)
            if not m:
                continue
            if m.market_start:
                self._post_market.cleanup_market_key(
                    m.asset,
                    m.duration_seconds or 0,
                    int(m.market_start.timestamp()),
                )
            self._token_to_market.pop(m.token_id, None)
            self._token_to_market.pop(m.token_id_alt, None)
            self._cooldowns.pop(cid, None)
            self._hft_cooldowns.pop(cid, None)
            self._hft_cooldowns.pop(f"mode2:{cid}", None)
            self._pop_hft_armed_trap(cid, m, event="expired", reason="runtime_removed")
            self._snapshot_ts.pop(cid, None)
            self._ws_rest_ts.pop(cid, None)
            self._ws.market_labels.pop(cid, None)
            del self._tracked[cid]

        for cid, rm in incoming.items():
            existing = self._tracked.get(cid)
            if existing is None:
                existing = TrackedMarket(
                    condition_id=rm.condition_id,
                    token_id=rm.token_up,
                    token_id_alt=rm.token_down,
                    outcome=rm.outcome,
                    event_title=rm.event_title[:80],
                    event_slug=rm.event_slug,
                    end_date=rm.market_end,
                    asset=rm.asset,
                    neg_risk=bool(rm.neg_risk),
                    market_start=rm.market_start,
                    duration_seconds=int(rm.timeframe_seconds or 0),
                )
                self._tracked[cid] = existing
            else:
                old_token = existing.token_id
                old_token_alt = existing.token_id_alt
                existing.token_id = rm.token_up
                existing.token_id_alt = rm.token_down
                existing.outcome = rm.outcome or existing.outcome
                existing.event_title = rm.event_title[:80]
                existing.event_slug = rm.event_slug
                existing.end_date = rm.market_end
                existing.asset = rm.asset
                existing.neg_risk = bool(rm.neg_risk)
                existing.market_start = rm.market_start
                existing.duration_seconds = int(rm.timeframe_seconds or 0)
                if old_token and old_token != existing.token_id:
                    self._token_to_market.pop(old_token, None)
                if old_token_alt and old_token_alt != existing.token_id_alt:
                    self._token_to_market.pop(old_token_alt, None)

            self._ws.market_labels[cid] = existing.event_title
            if existing.token_id:
                self._token_to_market[existing.token_id] = existing
            if existing.token_id_alt:
                self._token_to_market[existing.token_id_alt] = existing

        if removed:
            self._post_market.cleanup(removed, set(self._tracked.keys()))

        # Keep scanner PTB metadata in sync with canonical PTB map.
        if self._price_feed:
            for market in self._tracked.values():
                ptb = self._price_feed.get_price_to_beat(market.condition_id)
                if ptb:
                    market.price_to_beat = (
                        float(ptb.get("price")) if ptb.get("price") is not None else None
                    )
                    market.ptb_source = str(ptb.get("source", "") or "")
                else:
                    market.price_to_beat = None
                    market.ptb_source = ""

    def _check_oracle_gate(self, market: TrackedMarket, side: str,
                            gap_info: dict | None = None,
                            ) -> tuple[Optional[str], Optional[dict]]:
        """Combined oracle gate: gap magnitude + direction + dual-oracle consensus.

        Returns:
            (rejection_reason, consensus_result)
            - rejection_reason: None if all checks pass, or a descriptive string
            - consensus_result: dict from check_consensus() if run, else None
              (Caller uses for logging AFTER confirming all gates pass.)

        Checks (in order):
          1. Gap magnitude ≥ threshold (price moved enough from PTB)
          2. Primary oracle direction matches proposed side
          3. Both oracles agree on direction (if oracle_consensus enabled)
        """
        if not self._price_feed:
            return "BLOCKED: price feed unavailable (fail-closed)", None

        if gap_info is None:
            gap_info = self._price_feed.compute_gap(market.condition_id)
        if not gap_info:
            # If PTB exists but current price is unavailable, block the trade
            ptb = self._price_feed.get_price_to_beat(market.condition_id)
            if ptb is not None:
                return ("BLOCKED: PTB captured but no current price "
                        f"for {market.asset} (fail-closed)"), None
            return (f"BLOCKED: no oracle data for {market.asset} "
                    "(fail-closed)"), None

        gap_pct = gap_info["gap_pct"]
        direction_side = gap_info["winning_side"]  # "Up" or "Down" based on oracle
        threshold = self._get_gap_threshold(market.asset, market.duration_seconds)
        oracle = gap_info.get("current_oracle", "binance")
        orc = _ORACLE_SHORT.get(oracle, oracle.upper())

        # Check 1: Gap too small -- too close to the edge, could reverse
        if gap_pct < threshold:
            return (f"GAP TOO SMALL: {orc}:${gap_info['current_price']:.2f} vs "
                    f"{orc}_PTB:${gap_info['price_to_beat']:.2f} "
                    f"(gap={gap_pct:.3f}% < {threshold:.3f}%)"), None

        # Check 2: Primary oracle direction vs book
        if direction_side != side:
            return (f"Book={side} vs {orc}={direction_side} | "
                    f"{orc}:${gap_info['current_price']:.2f} vs "
                    f"{orc}_PTB:${gap_info['price_to_beat']:.2f} "
                    f"(gap={gap_pct:.3f}%)"), None

        # Check 3: Dual-oracle consensus (when enabled)
        consensus = None
        if self.config.execution.oracle_consensus:
            consensus = self._price_feed.check_consensus(
                market.condition_id, side
            )
            if not consensus["agree"]:
                cl = consensus["chainlink_side"]
                bn = consensus["binance_side"]
                cl_p = consensus["chainlink_price"]
                bn_p = consensus["binance_price"]
                ptb_cl = consensus["ptb_chainlink"]
                ptb_bn = consensus["ptb_binance"]

                parts = []
                if cl_p is not None and ptb_cl is not None:
                    parts.append(f"CL:${cl_p:.2f} vs CL_PTB:${ptb_cl:.2f}")
                elif cl == "unavailable":
                    parts.append("CL:unavail")
                if bn_p is not None and ptb_bn is not None:
                    parts.append(f"BIN:${bn_p:.2f} vs BIN_PTB:${ptb_bn:.2f}")
                elif bn == "unavailable":
                    parts.append("BIN:unavail")

                return (f"ORACLE SPLIT: CL={cl} BIN={bn} Book={side} | "
                        + " | ".join(parts)), consensus

            # Check 4: Secondary oracle must also clear gap threshold
            # Both oracles agree on direction, but if the secondary oracle's
            # gap is negligible, the "consensus" is an illusion — one oracle
            # is barely past its PTB and could flip any moment.
            if consensus["agree"]:
                cl_p = consensus["chainlink_price"]
                bn_p = consensus["binance_price"]
                ptb_cl = consensus["ptb_chainlink"]
                ptb_bn = consensus["ptb_binance"]

                # Determine which oracle is secondary (not the primary used in gap_info)
                if oracle == "chainlink" and bn_p is not None and ptb_bn is not None:
                    sec_gap = abs(bn_p - ptb_bn) / ptb_bn * 100 if ptb_bn > 0 else 0
                    if sec_gap < threshold:
                        return (f"WEAK CONSENSUS: BIN gap {sec_gap:.4f}% < {threshold:.3f}% | "
                                f"BIN:${bn_p:.2f} vs BIN_PTB:${ptb_bn:.2f}"), consensus
                elif oracle == "chainlink":
                    # Binance data unavailable — can't verify consensus
                    return ("BLOCKED: consensus enabled but BIN data unavailable "
                            "(fail-closed)"), consensus
                elif oracle == "binance" and cl_p is not None and ptb_cl is not None:
                    sec_gap = abs(cl_p - ptb_cl) / ptb_cl * 100 if ptb_cl > 0 else 0
                    if sec_gap < threshold:
                        return (f"WEAK CONSENSUS: CL gap {sec_gap:.4f}% < {threshold:.3f}% | "
                                f"CL:${cl_p:.2f} vs CL_PTB:${ptb_cl:.2f}"), consensus
                elif oracle == "binance":
                    # Chainlink data unavailable — can't verify consensus
                    return ("BLOCKED: consensus enabled but CL data unavailable "
                            "(fail-closed)"), consensus

        return None, consensus  # All checks passed

    def set_snipe_callback(self, callback: Callable):
        """Register async callback for instant snipe execution.

        Called with (SnipeOpportunity) when WS price update triggers a snipe.
        main.py wires this to executor.execute().
        """
        self._snipe_callback = callback

    def _log_hft_trap_event(
        self,
        event: str,
        cid: str,
        market: Optional[TrackedMarket],
        trap: Optional[dict] = None,
        **fields: Any,
    ):
        """Lifecycle telemetry for armed-trap state transitions."""
        mkt = _mkt_tag(market) if market else (trap.get("market_label") if trap else cid)
        cid_short = short_cid(cid)
        parts: list[str] = [f"event={event}", f"mkt={mkt}", f"cid={cid_short}"]
        if trap:
            side = trap.get("side")
            if side:
                parts.append(f"side={side}")
            cap = trap.get("max_price")
            if isinstance(cap, (int, float)):
                parts.append(f"cap={float(cap):.4f}")
        for k, v in fields.items():
            if v is None:
                continue
            if isinstance(v, float):
                parts.append(f"{k}={v:.4f}")
            else:
                parts.append(f"{k}={v}")
        logger.info("[HFT-TRAP] %s", " ".join(parts))
        trap_side = ""
        trap_cap = None
        if trap:
            trap_side = str(trap.get("side") or "")
            cap = trap.get("max_price")
            if isinstance(cap, (int, float)):
                trap_cap = float(cap)
        details = dict(fields)
        if trap:
            for key in ("asset", "event_title", "timeframe_seconds", "trigger_direction", "token"):
                if key in trap and key not in details:
                    details[key] = trap.get(key)
        self._record_hft_event(
            condition_id=cid,
            event_type=f"trap_{event}",
            market=market,
            side=trap_side,
            cap_price=trap_cap,
            reason=str(fields.get("reason") or "") or None,
            details=details,
            ask=self._safe_float(details.get("ask")),
            bid=self._safe_float(details.get("bid")),
            gap_pct=self._safe_float(trap.get("trigger_gap_pct")) if trap else None,
            barrier_pct=self._safe_float(trap.get("barrier_pct")) if trap else None,
            current_price=self._safe_float(trap.get("trigger_price")) if trap else None,
            price_to_beat=self._safe_float(trap.get("ptb_price")) if trap else None,
        )

    @staticmethod
    def _safe_float(v) -> float | None:
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    def _record_hft_event(
        self,
        *,
        condition_id: str,
        event_type: str,
        market: Optional[TrackedMarket],
        side: str = "",
        gap_pct: float | None = None,
        barrier_pct: float | None = None,
        current_price: float | None = None,
        price_to_beat: float | None = None,
        ask: float | None = None,
        bid: float | None = None,
        cap_price: float | None = None,
        reason: str | None = None,
        details: dict | None = None,
    ):
        """Best-effort HFT telemetry write to sniper.db (analysis-only)."""
        if not self._db:
            return
        event_title = market.event_title if market else ""
        asset = market.asset if market else str((details or {}).get("asset") or "")
        if market:
            timeframe_seconds = market.duration_seconds
        else:
            try:
                timeframe_seconds = int((details or {}).get("timeframe_seconds") or 0)
            except Exception:
                timeframe_seconds = 0
        if not event_title:
            event_title = str((details or {}).get("event_title") or "")
        try:
            _bg(
                self._db.record_hft_event(
                    condition_id=condition_id,
                    event_type=event_type,
                    event_title=event_title,
                    asset=asset,
                    timeframe_seconds=timeframe_seconds,
                    side=side or "",
                    gap_pct=gap_pct,
                    barrier_pct=barrier_pct,
                    current_price=current_price,
                    price_to_beat=price_to_beat,
                    ask=ask,
                    bid=bid,
                    cap_price=cap_price,
                    reason=reason,
                    details=details or {},
                ),
                label="hft_event_db",
            )
        except Exception:
            pass

    def _pop_hft_armed_trap(
        self,
        cid: str,
        market: Optional[TrackedMarket] = None,
        *,
        event: Optional[str] = None,
        reason: Optional[str] = None,
        **fields: Any,
    ) -> Optional[dict]:
        trap = self._hft_armed_traps.pop(cid, None)
        if trap and event:
            payload = dict(fields)
            if reason:
                payload["reason"] = reason
            self._log_hft_trap_event(event, cid, market, trap, **payload)
        return trap

    def sweep_orphan_hft_traps(self) -> int:
        """Drop armed traps whose markets are no longer tracked."""
        stale = [cid for cid in self._hft_armed_traps if cid not in self._tracked]
        for cid in stale:
            self._pop_hft_armed_trap(cid, None, event="expired", reason="orphan_sweep")
            self._hft_cooldowns.pop(f"mode2:{cid}", None)
        return len(stale)

    async def _check_hft_armed_trap(self, cid: str, token_id: str, market: TrackedMarket):
        """Armed trap checker: WS-push lifecycle from arm to spring/disarm."""
        if cid in self._hft_trap_eval_inflight:
            return True
        trap = self._hft_armed_traps.get(cid)
        if not trap:
            return False

        mkt = _mkt_tag(market)
        cid_short = short_cid(cid)
        self._hft_trap_eval_inflight.add(cid)
        try:
            trap = self._hft_armed_traps.get(cid)
            if not trap:
                return False

            # 1) Manual kill-switch
            if not self.config.trend.mode2_watch_enabled():
                self._pop_hft_armed_trap(cid, market, event="disarm_reason", reason="feature_disabled")
                self._hft_cooldowns.pop(f"mode2:{cid}", None)
                logger.info("[HFT-DISARM] mkt=%s cid=%s reason=feature_disabled", mkt, cid_short)
                return False

            # 2) Canonical execution gate (resolved/no-bet/muzzle)
            now = datetime.now(timezone.utc)
            gate = evaluate_execution_gate(
                config=self.config,
                market=market,
                is_hft_runner=True,
                is_resolved=(cid in self._resolved_cids),
                now_utc=now,
            )
            if not gate.allowed:
                self._pop_hft_armed_trap(
                    cid, market, event="disarm_reason", reason=gate.code or "gate_blocked"
                )
                self._hft_cooldowns.pop(f"mode2:{cid}", None)
                logger.info("[HFT-DISARM] mkt=%s cid=%s reason=%s", mkt, cid_short, gate.code or "gate_blocked")
                return False

            # Keep explicit muzzle log shape for historical continuity.
            sec_to_end = (market.end_date - now).total_seconds()
            muzzle = max(int(self.config.trend.hft_muzzle_seconds), self.config.scanner.stop_buffer_seconds)
            if sec_to_end < muzzle:
                self._pop_hft_armed_trap(
                    cid, market, event="disarm_reason", reason="muzzle_zone",
                    sec_to_end=sec_to_end, muzzle=muzzle,
                )
                self._hft_cooldowns.pop(f"mode2:{cid}", None)
                logger.info(
                    "[HFT-DISARM] mkt=%s cid=%s reason=muzzle_zone sec_to_end=%.1f muzzle=%d",
                    mkt, cid_short, sec_to_end, muzzle,
                )
                return False

            # 3) Side-flip guard
            gap_info = self._price_feed.compute_gap(cid) if self._price_feed else None
            if gap_info and gap_info.get("winning_side") != trap["side"]:
                self._pop_hft_armed_trap(
                    cid, market, event="disarm_reason", reason="side_flip",
                    now_side=gap_info.get("winning_side"), armed_side=trap["side"],
                )
                self._hft_cooldowns.pop(f"mode2:{cid}", None)
                logger.info(
                    "[HFT-DISARM] mkt=%s cid=%s reason=side_flip now=%s armed_side=%s",
                    mkt, cid_short, gap_info.get("winning_side"), trap["side"],
                )
                return False

            # 4) Book check on watched token
            token_id = trap["token_id"]
            ws_book = self._ws.get_book(token_id)
            if not ws_book:
                if not trap.get("no_book_logged"):
                    trap["no_book_logged"] = True
                    sub_primary = bool(market.token_id and market.token_id in self._ws._subscribed)
                    sub_alt = bool(market.token_id_alt and market.token_id_alt in self._ws._subscribed)
                    logger.info(
                        "[HFT-ARM-WAIT] mkt=%s cid=%s reason=no_ws_book sub_primary=%s sub_alt=%s ws_subs=%d",
                        mkt, cid_short, sub_primary, sub_alt, len(self._ws._subscribed),
                    )
                return True

            best_ask = float(ws_book.get("best_ask", 0) or 0.0)
            best_bid = float(ws_book.get("best_bid", 0) or 0.0)
            current_max_price = float(
                trap.get(
                    "max_price",
                    self.config.trend.get_hft_max_price_for(market.asset, market.duration_seconds),
                ) or 0.0
            )
            required_amount = float(trap.get("amount", self.config.trend.hft_bet_amount) or 0.0)
            cap_depth = float(self._ws.get_depth_up_to(token_id, current_max_price) or 0.0)
            if cap_depth <= 0:
                # Fallback for WS states that have best bid/ask but no ladder payload yet.
                cap_depth = float(ws_book.get("ask_depth", 0) or 0.0)

            if not trap.get("book_seen_logged"):
                trap["book_seen_logged"] = True
                trap["first_book_seen_ts"] = time.perf_counter()
                self._log_hft_trap_event(
                    "book_seen",
                    cid,
                    market,
                    trap,
                    ask=best_ask,
                    bid=best_bid,
                    spread=float(ws_book.get("spread_pct", 0) or 0.0),
                )

            trap["last_book_seen_ts"] = time.time()
            trap["last_best_ask"] = best_ask
            trap["last_cap_depth"] = cap_depth

            if required_amount > 0 and cap_depth < required_amount:
                now_ts = time.time()
                last_log = float(trap.get("depth_wait_logged_ts") or 0.0)
                if (now_ts - last_log) >= 2.0:
                    trap["depth_wait_logged_ts"] = now_ts
                    logger.info(
                        "[HFT-ARM-WAIT] mkt=%s cid=%s reason=cap_depth_lt_amount cap_depth=%.2f amount=%.2f cap=%.4f ask=%.4f",
                        mkt, cid_short, cap_depth, required_amount, current_max_price, best_ask,
                    )
                return True

            # 5) Spring when ask <= cap
            if 0 < best_ask <= current_max_price:
                trap = self._pop_hft_armed_trap(
                    cid, market, event="sprung", reason="ask_le_cap", ask=best_ask
                )
                if not trap:
                    return True

                spring_ts = time.perf_counter()
                trigger_ts = float(trap.get("trigger_ts") or 0.0)
                armed_ts = float(trap.get("armed_ts") or 0.0)
                logger.info(
                    "[HFT-SPRING] mkt=%s cid=%s side=%s ask=%.4f cap=%.4f",
                    mkt, cid_short, trap["side"], best_ask, current_max_price,
                )
                log_lifecycle(
                    logger=logger,
                    phase="sprung",
                    condition_id=cid,
                    asset=market.asset,
                    timeframe_seconds=market.duration_seconds,
                    market_start=market.market_start,
                    market_end=market.end_date,
                    source="scanner",
                    detail=f"ask={best_ask:.4f} cap={current_max_price:.4f}",
                )
                if trigger_ts > 0:
                    trigger_to_spring_ms = (spring_ts - trigger_ts) * 1000.0
                    record_mode2_latency(
                        stage="trigger_to_spring",
                        latency_ms=trigger_to_spring_ms,
                        condition_id=cid,
                        event_title=market.event_title,
                    )
                    logger.info(
                        "[HFT-LATENCY] mkt=%s cid=%s stage=trigger_to_spring ms=%.1f",
                        mkt, cid_short, trigger_to_spring_ms,
                    )
                if armed_ts > 0:
                    arm_to_spring_ms = (spring_ts - armed_ts) * 1000.0
                    record_mode2_latency(
                        stage="arm_to_spring",
                        latency_ms=arm_to_spring_ms,
                        condition_id=cid,
                        event_title=market.event_title,
                    )
                    logger.info(
                        "[HFT-LATENCY] mkt=%s cid=%s stage=arm_to_spring ms=%.1f",
                        mkt, cid_short, arm_to_spring_ms,
                    )

                opp = trap["opp"]
                opp.best_ask = best_ask
                # Keep execution-side thin-book checks grounded in cap-fillable depth.
                opp.ask_depth = cap_depth
                opp.edge = 1.0 - best_ask
                opp.mode2_spring_ts = spring_ts
                opp.hft_entry_path = "trap"
                opp.hft_trigger_ask = best_ask
                self._hft_cooldowns[f"mode2:{cid}"] = time.time()
                if self._snipe_callback:
                    await self._snipe_callback(opp)
                return True

            return True
        finally:
            self._hft_trap_eval_inflight.discard(cid)

    async def on_ws_price_update(self, token_id: str):
        """Called by WS client on EVERY price_change/book event.

        This is the fast path -- sub-second reaction to orderbook changes.
        Checks if this token belongs to a market in our snipe window and
        if the price is right, fires the snipe callback immediately.

        Debounced per MARKET (condition_id), not per token, so both the Up
        and Down tokens for the same market coalesce into a single evaluation.
        The evaluation checks WS cache for BOTH sides and picks the likely winner.
        """
        market = self._token_to_market.get(token_id)
        if not market:
            return  # Not one of ours

        # Skip if runtime toggles disable this asset/timeframe cell.
        if not self._is_cell_enabled(market.asset, market.duration_seconds):
            return

        gate = evaluate_execution_gate(
            config=self.config,
            market=market,
            is_hft_runner=False,
            is_resolved=(market.condition_id in self._resolved_cids),
            now_utc=datetime.now(timezone.utc),
        )
        if not gate.allowed:
            return

        # Skip if already pending snipe for this market
        cid = market.condition_id
        
        # ── HFT ARMED TRAP ────────────────────────────────────────────
        # If this market has an armed trap, we use ONLY the trap logic.
        if cid in self._hft_armed_traps:
            asyncio.create_task(self._check_hft_armed_trap(cid, token_id, market))
            return  # Consumed — either still watching or just fired
        # ──────────────────────────────────────────────────────────────
        
        if cid in self._pending_snipes:
            return

        now = datetime.now(timezone.utc)
        seconds_to_end = (market.end_date - now).total_seconds()

        # Determine which strategy applies (per-timeframe pre-end window)
        pre_end_sec = self.config.scanner.get_pre_end_seconds(market.duration_seconds)
        pre_end_thresh = self.config.scanner.get_pre_end_threshold(market.duration_seconds)
        seconds_to_start = (
            (market.market_start - now).total_seconds()
            if market.market_start else None
        )
        post_bet = self.config.scanner.post_end_bet_seconds
        pe_cfg = self.config.post_end  # PostEndConfig

        # ── POST-END FAST PATH ──────────────────────────────────────────
        # When the market has ended AND the post-end sniper is enabled,
        # take a radically shorter path: trust WS cache, skip REST
        # confirmation, skip oracle/conviction/gap gates.  Targeting
        # ~0.7-1.0s total latency vs ~1.5s for the standard pipeline.
        if seconds_to_end < 0 and pe_cfg.enabled:
            past = -seconds_to_end
            if past > pe_cfg.window_seconds:
                # Outside post-end window — fall through to legacy check
                pass
            else:
                # Fast-path post-end evaluation (WS cache only)
                await self._post_end_fast_path(market, cid, now, seconds_to_end)
                return  # Always return — either fired or skipped

        # Skip if this market's end-minute slot is blacklisted (Clock Blackout)
        # NOTE: placed AFTER post-end fast path so blackout only affects pre-end sniping
        if self.config.blackout.is_blocked(market.duration_seconds, _end_slot(market), market.end_date):
            return

        # Must be in relevant time window:
        #   Pre-end: seconds_to_end in [stop_buffer, pre_end_seconds]
        #   Post-end: seconds_to_end in [-post_bet, 0]  (0 disables post-end bets)
        stop_buf = self.config.scanner.stop_buffer_seconds
        start_buf = self.config.scanner.start_buffer_seconds
        if seconds_to_end > pre_end_sec:
            return  # Too early for snipe window
        if market.market_start and now < market.market_start:
            return  # Only BB Trend can snipe before market start
        if 0 < seconds_to_end < stop_buf:
            return  # Inside stop-buffer dead zone (anti-dump shield)
        if start_buf > 0 and market.market_start and seconds_to_end > 0:
            elapsed = (now - market.market_start).total_seconds()
            if elapsed > start_buf:
                return  # Past start buffer window — too late to bet
        if seconds_to_end < 0:
            if post_bet <= 0 or (-seconds_to_end) > post_bet:
                return  # Post-end betting disabled or window expired

        # Check cooldown
        last_buy = self._cooldowns.get(cid, 0)
        if (now.timestamp() - last_buy) < self.config.execution.cooldown_seconds:
            return

        # -- Debounce per MARKET (not per token) --
        # Disabled (was 1.0s).  _pending_snipes already prevents concurrent
        # evaluations; the debounce only added artificial latency.
        # Effective rate is still capped at ~1.5-2/s by REST round-trip time.
        _now_ts = now.timestamp()
        self._ws_rest_ts[cid] = _now_ts

        # -- Pick the best side from WS cache (check BOTH tokens) --
        best_mid = 0.0
        best_token = ""
        side = ""
        _ws_mids = {}  # WS cache pre-filter only; REST mids used for actual conviction
        for side_label, tid in [("Up", market.token_id), ("Down", market.token_id_alt)]:
            if not tid:
                continue
            ws_book = self._ws.get_book(tid)
            if not ws_book:
                continue
            mid = ws_book.get("midpoint", 0)
            _ws_mids[side_label] = mid
            if mid > best_mid:
                best_mid = mid
                best_token = tid
                side = side_label

        # Quick filter: no side looks promising (both below 0.10)
        if best_mid < 0.10 or not best_token:
            return

        # Compute gap once — reused for gate check and opportunity logging
        gap_info = self._price_feed.compute_gap(market.condition_id) if self._price_feed else None

        # Early gap-size pre-check (same as poll path) — skip silently when
        # the gap is too small rather than logging a noisy BINANCE GATE line
        # every 3 seconds throughout the snipe window.
        if gap_info:
            threshold = self._get_gap_threshold(market.asset, market.duration_seconds)
            if gap_info["gap_pct"] < threshold:
                return  # Gap too small — no need to log, happens frequently

        # NOTE: full oracle gate (direction check) deferred to post-REST
        # where we know the REST-confirmed winner side.  The gap-SIZE
        # pre-check above is direction-agnostic and safe to keep.

        # Max-gap-cap gate (signal clipping) — block suspiciously large gaps
        _max_gap = self.config.scanner.get_max_gap_pct(market.duration_seconds)
        if _max_gap > 0 and gap_info and gap_info["gap_pct"] > _max_gap:
            if self.verbose:
                logger.info(
                    "[V] [WS] %s %s \u2014 gap %.3f%% > max %.2f%% (spike filter)",
                    market.asset.upper(), market.event_title[:35],
                    gap_info["gap_pct"], _max_gap,
                )
            return

        # Lock this market BEFORE any await (REST fetch) to prevent async
        # races: two WS events for the same cid could both pass the
        # _pending_snipes check if the guard is set after a yield point.
        self._pending_snipes.add(cid)
        try:
            # -- Fetch BOTH sides from REST in parallel (co-equal with poll) --
            _ws_label = _mkt_tag(market)
            side_books: dict[str, dict] = {"Up": {}, "Down": {}}
            token_pairs = [(lbl, tid) for lbl, tid in
                           [("Up", market.token_id), ("Down", market.token_id_alt)]
                           if tid]

            async def _ws_fetch(label_tid):
                lbl, tid = label_tid
                try:
                    bk = await self.api.get_book_liquidity(tid)
                    return lbl, tid, bk
                except Exception:
                    return lbl, tid, None

            book_results = await asyncio.gather(*[_ws_fetch(tp) for tp in token_pairs])

            best_book = None
            best_side_rest = ""
            best_token_rest = ""
            for side_label, tid, bk in book_results:
                if not bk or bk.get("midpoint", 0) == 0:
                    continue
                side_books[side_label] = bk
                if best_book is None or bk["midpoint"] > best_book.get("midpoint", 0):
                    best_book = bk
                    best_side_rest = side_label
                    best_token_rest = tid

            if not best_book:
                if self.verbose:
                    logger.info("[SNIPE: DETAIL] [WS] %s — empty books from REST", _ws_label)
                return

            # Authoritative winner comes from fresh REST data
            side = best_side_rest
            best_token = best_token_rest
            best_ask = best_book.get("best_ask", 0)
            mid = best_book.get("midpoint", 0)
            ask_depth = best_book.get("ask_depth", 0)

            if best_ask <= 0:
                if self.verbose:
                    logger.info("[SNIPE: DETAIL] [WS] %s — mid=%.2f but no asks", _ws_label, mid)
                return

            # -- Evaluate ALL gates (identical to poll path) --
            is_pre_end = seconds_to_end > 0
            edge = (1.0 - best_ask) if best_ask > 0 else 0.0
            threshold = pre_end_thresh if is_pre_end else self.config.scanner.winner_threshold
            max_price = self.config.execution.get_max_buy_price(market.duration_seconds)
            _min_edge = self.config.execution.get_min_edge(market.duration_seconds)

            g_threshold = best_ask >= threshold
            g_max_price = (0 < best_ask <= max_price) if max_price > 0 else (best_ask > 0)
            g_min_edge = edge >= _min_edge
            g_depth = ask_depth >= _DEPTH_FLOOR

            # Recompute gap with fresh Binance data (avoids staleness
            # from pre-filter computed before the REST round-trip).
            gap_info = self._price_feed.compute_gap(market.condition_id) if self._price_feed else None

            # Oracle gate against REST-confirmed side with fresh gap
            oracle_rejection, consensus = self._check_oracle_gate(market, side, gap_info=gap_info)
            g_gap = oracle_rejection is None

            # Conviction from REST mids (accurate, not WS cache)
            up_mid_val = side_books.get("Up", {}).get("midpoint", 0) or 0
            dn_mid_val = side_books.get("Down", {}).get("midpoint", 0) or 0
            conviction = abs(up_mid_val - dn_mid_val)
            _min_conv = self.config.scanner.get_min_conviction(market.duration_seconds)
            # W-1 guard: if either side book is missing, conviction is
            # unreliable (0 default inflates the delta).  Block the trade.
            _both_sides = bool(side_books.get("Up")) and bool(side_books.get("Down"))
            g_conviction = (_both_sides and conviction >= _min_conv) if _min_conv > 0 else True

            # Max-gap-cap (signal clipping) — uses fresh gap_info
            # Missing data → inf so the gate blocks (fail-closed)
            _actual_gap = gap_info["gap_pct"] if gap_info else float("inf")
            g_max_gap = _actual_gap <= _max_gap if _max_gap > 0 else True

            g_all = (g_threshold and g_max_price and g_min_edge
                     and g_depth and g_gap and g_conviction and g_max_gap)

            # -- Record snapshot (throttled, same interval as poll) --
            _now_snap = _now_ts
            _last_snap = self._snapshot_ts.get(cid, 0)
            if self._db and (_now_snap - _last_snap) >= self._SNAPSHOT_INTERVAL:
                self._snapshot_ts[cid] = _now_snap
                up_b = side_books.get("Up", {})
                dn_b = side_books.get("Down", {})
                # Fire-and-forget: DB snapshot is observational, don't block hot path
                _bg(self._db.record_snapshot(
                    condition_id=cid,
                    event_title=market.event_title[:80],
                    asset=market.asset,
                    seconds_to_end=seconds_to_end,
                    duration_seconds=market.duration_seconds,
                    up_mid=up_b.get("midpoint", 0),
                    up_ask=up_b.get("best_ask", 0),
                    up_depth=up_b.get("ask_depth", 0),
                    down_mid=dn_b.get("midpoint", 0),
                    down_ask=dn_b.get("best_ask", 0),
                    down_depth=dn_b.get("ask_depth", 0),
                    best_side=side,
                    best_mid=mid,
                    best_ask=best_ask,
                    best_depth=ask_depth,
                    edge=edge,
                    binance_price=gap_info["current_price"] if gap_info else None,
                    price_to_beat=gap_info["price_to_beat"] if gap_info else None,
                    gap_pct=gap_info["gap_pct"] if gap_info else None,
                    gap_direction=gap_info["direction"] if gap_info else None,
                    ptb_source=market.ptb_source or None,
                    passed_threshold=g_threshold,
                    passed_max_price=g_max_price,
                    passed_min_edge=g_min_edge,
                    passed_depth=g_depth,
                    passed_gap=g_gap,
                    passed_conviction=g_conviction,
                    passed_max_gap=g_max_gap,
                    passed_all=g_all,
                    action="snipe" if g_all else "observe",
                ))

                # -- Monitor DB tick (WS piggyback, fire-and-forget) --
                if self._monitor_db:
                    _mon_last_ws = self._monitor_ts.get(cid, 0)
                    if (_now_snap - _mon_last_ws) >= MONITOR_TICK_INTERVAL:
                        self._monitor_ts[cid] = _now_snap
                        _up_m = side_books.get("Up", {})
                        _dn_m = side_books.get("Down", {})
                        # Extract per-oracle directions
                        _cl_dir, _bin_dir = _oracle_directions(consensus, gap_info)
                        _bg(self._monitor_db.record_tick(
                            condition_id=cid,
                            event_title=market.event_title,
                            asset=market.asset,
                            duration_seconds=market.duration_seconds,
                            seconds_to_end=seconds_to_end,
                            up_mid=_up_m.get("midpoint", 0),
                            up_ask=_up_m.get("best_ask", 0),
                            up_depth=_up_m.get("ask_depth", 0),
                            down_mid=_dn_m.get("midpoint", 0),
                            down_ask=_dn_m.get("best_ask", 0),
                            down_depth=_dn_m.get("ask_depth", 0),
                            best_side=side,
                            best_mid=mid,
                            best_ask=best_ask,
                            best_depth=ask_depth,
                            edge=edge,
                            conviction=conviction,
                            binance_price=gap_info["current_price"] if gap_info else None,
                            price_to_beat=gap_info["price_to_beat"] if gap_info else None,
                            gap_pct=gap_info["gap_pct"] if gap_info else None,
                            gap_direction=gap_info["direction"] if gap_info else None,
                            passed_threshold=g_threshold,
                            passed_max_price=g_max_price,
                            passed_min_edge=g_min_edge,
                            passed_depth=g_depth,
                            passed_gap=g_gap,
                            passed_conviction=g_conviction,
                            passed_max_gap=g_max_gap,
                            passed_all=g_all,
                            cfg_threshold=threshold,
                            cfg_max_price=max_price,
                            cfg_min_edge=_min_edge,
                            cfg_min_conviction=_min_conv,
                            cfg_gap_pct=self._get_gap_threshold(market.asset, market.duration_seconds),
                            cfg_max_gap=_max_gap,
                            source="ws",
                            event_slug=market.event_slug,
                            oracle_cl_direction=_cl_dir,
                            oracle_bin_direction=_bin_dir,
                        ))

            # -- Early-exit on gate failures --
            if not g_threshold:
                if self.verbose:
                    logger.info("[SNIPE: DETAIL] [WS] %s — ask $%.2f < threshold %.2f",
                                _ws_label, best_ask, threshold)
                return
            if not g_max_price:
                if self.verbose:
                    logger.info("[SNIPE: DETAIL] [WS] %s — ask $%.2f > max $%.2f",
                                _ws_label, best_ask, max_price)
                return
            if not g_min_edge:
                if self.verbose:
                    logger.info("[SNIPE: DETAIL] [WS] %s — edge $%.3f < min $%.2f",
                                _ws_label, edge, _min_edge)
                return
            if not g_depth:
                if self.verbose:
                    logger.info("[SNIPE: DETAIL] [WS] %s — depth $%.0f < $%d floor",
                                _ws_label, ask_depth, _DEPTH_FLOOR)
                return
            if not g_conviction:
                if self.verbose:
                    logger.info("[SNIPE: DETAIL] [WS] %s — conviction %.3f < min %.2f "
                                "(up_mid=%.3f dn_mid=%.3f)",
                                _ws_label, conviction, _min_conv, up_mid_val, dn_mid_val)
                return
            if oracle_rejection:
                logger.debug("[SNIPE: ORACLE BLOCK] [WS] %s — %s", _ws_label, oracle_rejection)
                return
            if not g_max_gap:
                if self.verbose:
                    logger.info("[SNIPE: DETAIL] [WS] %s — gap %.3f%% > max %.2f%% (spike filter)",
                                _ws_label, _actual_gap, _max_gap)
                return

            # -- All gates passed — build opportunity --
            gap_str = ""
            if gap_info:
                _orc = _ORACLE_SHORT.get(gap_info.get('current_oracle', 'binance'), 'BIN')
                gap_str = (f" | {_orc}:${gap_info['current_price']:.2f} "
                           f"gap={gap_info['gap_pct']:.3f}% {gap_info['direction']}")
            con_str = ""
            if consensus:
                cl = consensus["chainlink_side"]
                bn = consensus["binance_side"]
                if cl == "unavailable" or bn == "unavailable":
                    missing = "CL" if cl == "unavailable" else "BIN"
                    con_str = f" | {missing}:unavail"
                else:
                    con_str = f" | CL={cl} BIN={bn}"

            conv_str = f" | conv={conviction:.3f}"

            strategy = "PRE-END" if is_pre_end else "WS-INSTANT"
            logger.info("[%s] %s — %s @ $%.4f | mid=%.4f | %.1fs %s end | edge $%.4f%s%s%s",
                        strategy, _ws_label, side, best_ask, mid,
                        abs(seconds_to_end),
                        "before" if is_pre_end else "past",
                        edge, gap_str, con_str, conv_str)

            market.best_ask = best_ask
            market.midpoint = mid
            market.ask_depth = ask_depth
            market.winning_token = best_token
            market.winning_side = side

            opp = SnipeOpportunity(
                market=market,
                best_ask=best_ask,
                ask_depth=ask_depth,
                edge=edge,
                reason=f"{strategy}: {side} @ ${best_ask:.4f} ({abs(seconds_to_end):.0f}s {'before' if is_pre_end else 'past'} end)",
                winning_token=best_token,
                winning_side=side,
                gap_pct=gap_info["gap_pct"] if gap_info else None,
                gap_direction=gap_info["direction"] if gap_info else None,
            )

            if self._snipe_callback:
                self.opportunities_found += 1
                try:
                    await self._snipe_callback(opp)
                except Exception as e:
                    logger.error("Instant snipe callback error: %s", e)
        finally:
            self._pending_snipes.discard(cid)

    # ── PTB PRE-FETCH (delegated to PostMarketManager) ───────────────
    async def _pre_fetch_ptb_for_approaching(self, now: datetime):
        """Delegate to PostMarketManager."""
        await self._post_market.pre_fetch_approaching(self._tracked, now)

    # ── POST-END MAKER PATH (delegated to PostMarketManager) ───────────
    async def _post_end_fast_path(self, market, cid: str,
                                   now: datetime, seconds_to_end: float):
        """Delegate to PostMarketManager."""
        await self._post_market._post_end_fast_path(market, cid, now, seconds_to_end)

    def on_ws_resolution(self, condition_id: str, winning_outcome: str):
        """Proxy for main.py — delegates to PostMarketManager."""
        self._post_market.on_ws_resolution(condition_id, winning_outcome)
        self._pending_snipes.discard(condition_id)
        self._hft_cooldowns.pop(f"mode2:{condition_id}", None)
        self._pop_hft_armed_trap(condition_id, self._tracked.get(condition_id), event="disarm_reason", reason="resolved")
        market = self._tracked.get(condition_id)
        if market:
            log_lifecycle(
                logger=logger,
                phase="resolved",
                condition_id=condition_id,
                asset=market.asset,
                timeframe_seconds=market.duration_seconds,
                market_start=market.market_start,
                market_end=market.end_date,
                source="ws",
                detail=f"winner={winning_outcome}",
            )
            return
        log_lifecycle(
            logger=logger,
            phase="resolved",
            condition_id=condition_id,
            asset="unknown",
            timeframe_seconds=0,
            market_start=None,
            market_end=None,
            source="ws",
            detail=f"winner={winning_outcome}",
        )

    async def scan(self) -> list[SnipeOpportunity]:
        """Run one scan cycle: sync runtime markets -> check prices -> find opportunities.

        Returns list of actionable SnipeOpportunity objects.
        """
        self.scan_count += 1
        now = datetime.now(timezone.utc)
        self._sync_runtime_markets()

        # -- Phase 0: Post-end maker (FIRST — latency-critical) --
        # Delegated to PostMarketManager (fully separated module).
        await self._post_market.process_ended_markets(self._tracked, now)

        # -- Phase 1: Prune old markets --
        cleanup_delay = self._ws_cleanup_override or self.config.scanner.ws_cleanup_delay
        cutoff = now - timedelta(seconds=self.config.scanner.post_window_seconds + cleanup_delay)
        expired = [cid for cid, m in self._tracked.items()
                   if m.end_date < cutoff]
        if expired:
            for cid in expired:
                m = self._tracked.get(cid)
                if m:
                    self._token_to_market.pop(m.token_id, None)
                    self._token_to_market.pop(m.token_id_alt, None)
            for cid in expired:
                m = self._tracked.get(cid)
                # Clean market-level dedup key in PostMarketManager
                if m and m.market_start:
                    self._post_market.cleanup_market_key(
                        m.asset, m.duration_seconds or 0,
                        int(m.market_start.timestamp()),
                    )
                del self._tracked[cid]
                self._cooldowns.pop(cid, None)
                self._hft_cooldowns.pop(cid, None)
                self._hft_cooldowns.pop(f"mode2:{cid}", None)
                self._pop_hft_armed_trap(cid, m, event="expired", reason="prune_cutoff")
                self._snapshot_ts.pop(cid, None)
                self._ws_rest_ts.pop(cid, None)
                self._ws.market_labels.pop(cid, None)
                # Keep price_to_beat for prediction (dashboard reads it after prune)
            # Cleanup post-market state for expired cids
            self._post_market.cleanup(expired, set(self._tracked.keys()))
            logger.debug("Pruned %d expired markets (cutoff %ds past end), tracking %d",
                         len(expired), self.config.scanner.post_window_seconds + cleanup_delay,
                         len(self._tracked))

        # -- Phase 1a: Prune markets that are no longer enabled by toggles --
        # If assets/timeframes are changed live from dashboard, drop now-disabled
        # tracked markets immediately so WS subscriptions and counts stay aligned.
        disabled = [
            cid for cid, m in self._tracked.items()
            if not self._is_cell_enabled(m.asset, m.duration_seconds)
        ]
        if disabled:
            for cid in disabled:
                m = self._tracked.get(cid)
                if m:
                    self._token_to_market.pop(m.token_id, None)
                    self._token_to_market.pop(m.token_id_alt, None)
            for cid in disabled:
                m = self._tracked.get(cid)
                if m and m.market_start:
                    self._post_market.cleanup_market_key(
                        m.asset, m.duration_seconds or 0,
                        int(m.market_start.timestamp()),
                    )
                del self._tracked[cid]
                self._cooldowns.pop(cid, None)
                self._hft_cooldowns.pop(cid, None)
                self._hft_cooldowns.pop(f"mode2:{cid}", None)
                self._pop_hft_armed_trap(cid, m, event="expired", reason="toggle_disabled")
                self._snapshot_ts.pop(cid, None)
                self._ws_rest_ts.pop(cid, None)
                self._ws.market_labels.pop(cid, None)
            self._post_market.cleanup(disabled, set(self._tracked.keys()))
            logger.info(
                "[TOGGLE] Pruned %d disabled markets (tracking %d)",
                len(disabled), len(self._tracked),
            )

        # -- Phase 1b: Pre-fetch PTB data for markets approaching end --
        # The past-results API returns instantly-available settlement data.
        # Pre-fetch 10s before end so we have it cached when post-end fires.
        if self.config.post_end.enabled:
            await self._pre_fetch_ptb_for_approaching(now)

        # -- Phase 2c: Oracle health check (every 60 scans ≈ 1 min at 1s interval) --
        if self._price_feed and self.scan_count % 60 == 1:
            health = self._price_feed.get_oracle_health()
            cl = health["chainlink"]
            bn = health["binance"]
            degraded = []
            for asset, info in health["per_asset"].items():
                if not info["chainlink_ok"] and info["binance_ok"]:
                    degraded.append(f"{asset}(CL down)")
                elif info["chainlink_ok"] and not info["binance_ok"]:
                    degraded.append(f"{asset}(BN down)")
                elif not info["chainlink_ok"] and not info["binance_ok"]:
                    degraded.append(f"{asset}(BOTH down)")
            if degraded:
                logger.warning("[FEED: ORACLE HEALTH] Degraded: %s | CL conn=%s ticks=%d age=%s | BN conn=%s ticks=%d age=%s",
                               ", ".join(degraded),
                               cl["connected"], cl["ticks"], cl["last_tick_age"],
                               bn["connected"], bn["ticks"], bn["last_tick_age"])
            else:
                logger.debug("[FEED: ORACLE HEALTH] All oracles OK | CL ticks=%d age=%s | BN ticks=%d age=%s",
                              cl["ticks"], cl["last_tick_age"],
                              bn["ticks"], bn["last_tick_age"])

        # -- Phase 2d: Gap observation logging (every 10th scan) --
        if self._price_feed and self.scan_count % 10 == 1:
            for m in self._tracked.values():
                if not self._is_cell_enabled(m.asset, m.duration_seconds):
                    continue  # Skip disabled assets
                gap = self._price_feed.compute_gap(m.condition_id)
                if gap:
                    secs_to_end = (m.end_date - now).total_seconds()
                    logger.debug("[SNIPE: GAP OBS] %s | %s $%.4f -> $%.4f | gap=%.3f%% %s | "
                                  "%.0fs to end | ptb_src=%s | oracle=%s",
                                  m.event_title[:40], gap['asset'],
                                  gap['price_to_beat'], gap['current_price'],
                                  gap['gap_pct'], gap['direction'],
                                  secs_to_end, m.ptb_source,
                                  gap.get('current_oracle', 'binance'))

        # -- Phase 3: Check prices for markets in the snipe window --
        opportunities: list[SnipeOpportunity] = []
        # Use the wider of pre_window_seconds and the widest per-TF pre_end_seconds
        # so the candidate filter never excludes markets the gate would accept.
        pre = timedelta(seconds=max(
            self.config.scanner.pre_window_seconds,
            self.config.scanner.max_pre_end_seconds()
        ))
        post = timedelta(seconds=self.config.scanner.post_window_seconds)

        # Markets in the snipe window:
        #   1) end_date - pre_window to end_date + post_window
        # Exclude no-bet (watch-only) assets — they go through the standalone
        # monitor batch instead so monitor.db keeps recording their ticks.
        _no_bet = self.config.execution.no_bet_set

        def _in_candidate_window(m: TrackedMarket) -> bool:
            return (m.end_date - pre) <= now <= (m.end_date + post)

        candidates = [
            m for m in self._tracked.values()
            if _in_candidate_window(m)
            and m.asset not in _no_bet
        ]

        # Exclude blackout-blocked markets from the candidate count so the
        # SCANNING log accurately reflects what will actually be evaluated.
        # NOTE: only pre-end sniping is blocked — post-end markets pass through.
        _is_blocked = self.config.blackout.is_blocked
        snipeable = [
            m for m in candidates
            if not (
                (m.end_date - now).total_seconds() > 0            # pre-end only
                and _is_blocked(m.duration_seconds, _end_slot(m), m.end_date)
            )
        ]

        log_every = max(1, int(getattr(self, "_status_log_every_scans", 10)))

        # Diagnostic: show nearest market end times on the same throttle.
        if self.verbose and self.scan_count % log_every == 1:
            nearest = sorted(self._tracked.values(), key=lambda m: abs((m.end_date - now).total_seconds()))[:5]
            for nm in nearest:
                delta = (nm.end_date - now).total_seconds()
                blocked = _is_blocked(nm.duration_seconds, _end_slot(nm), nm.end_date)
                logger.info("[SNIPE: DETAIL] Nearest market: %s | ends in %.0fs%s",
                             nm.event_title[:45], delta,
                             " [BLOCKED]" if blocked else "")

        if snipeable:
            # Normal-mode: concise note about what we're checking (throttled).
            if self.scan_count % log_every == 1:
                asset_counts: dict[str, int] = {}
                for c in snipeable:
                    asset_counts[c.asset.upper()] = asset_counts.get(c.asset.upper(), 0) + 1
                summary = " ".join(f"{a}:{n}" for a, n in sorted(asset_counts.items()))
                n_blocked = len(candidates) - len(snipeable)
                blk_str = f", {n_blocked} blocked" if n_blocked else ""
                logger.info("[SNIPE: SCANNING] %d markets in window (%s%s)",
                            len(snipeable), summary, blk_str)
        elif self.scan_count % log_every == 1:
            # Periodic quiet-mode: note that we're alive but nothing to check
            # Find time to next snipe window.
            # No blackout filter here — post-end evaluation always runs, even on blocked slots.
            if self._tracked:
                next_end = min(m.end_date for m in self._tracked.values())
                secs_to_next = (next_end - now).total_seconds()
                if secs_to_next > 0:
                    logger.info("[SNIPE: IDLE] tracking %d markets, next window in %.0fs",
                                len(self._tracked), secs_to_next)
                else:
                    logger.info("[SNIPE: IDLE] tracking %d markets",
                                len(self._tracked))
            else:
                logger.info("[SNIPE: IDLE] no markets tracked")

        # Check orderbooks concurrently (only non-blocked markets)
        tasks = [self._check_market(m, now) for m in snipeable]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, SnipeOpportunity):
                opportunities.append(result)
                self.opportunities_found += 1
            elif isinstance(result, Exception):
                logger.debug("Market check error: %s", result)

        # -- Monitor: record wider observation ticks (background) --
        # Markets inside the monitor window but OUTSIDE the snipe window
        # get a lightweight REST-based observation tick recorded to monitor.db.
        # Fired as background tasks so they don't compete with snipe-critical
        # REST calls on the semaphore.
        if self._monitor_db:
            _candidate_cids = {m.condition_id for m in candidates}
            _mon_tasks = []
            for m in self._tracked.values():
                if m.condition_id in _candidate_cids:
                    continue  # Already covered by snipe-path piggyback
                if not self._is_cell_enabled(m.asset, m.duration_seconds):
                    continue
                sec = (m.end_date - now).total_seconds()
                if sec <= 0:
                    continue  # Post-end handled by snipe path
                mon_window = MONITOR_WINDOWS.get(m.duration_seconds, 90)
                if sec <= mon_window:
                    _mon_tasks.append(self._monitor_tick(m, now, sec))
            if _mon_tasks:
                async def _run_monitor_batch():
                    await asyncio.gather(*_mon_tasks, return_exceptions=True)
                _bg(_run_monitor_batch(), label="monitor_batch")

        # Flag for turbo-polling: are any markets in the snipe window?
        self.in_snipe_window = len(candidates) > 0

        return opportunities

    # ── BB TREND SIGNAL EVALUATION (modular, does not modify existing paths) ──
    #
    # Called once per scan cycle from main.py.  Completely self-contained:
    #   1. Checks config.trend.enabled — noop if False
    #   2. Iterates tracked markets that haven't started yet
    #   3. Asks TrendAnalyzer for a signal (direction + confidence)
    #   4. If signal fires → forces direction, fetches book, applies
    #      EXISTING execution settings, then fires the same _snipe_callback
    #      that lookahead/pre-end use.
    #
    # The existing system is NOT modified.  BB is a plug-in orchestrator.

    async def evaluate_bb_signals(self) -> None:
        """Evaluate Bollinger Band signals for upcoming markets.

        For each tracked market that hasn't started yet:
          - Check if BB has a directional signal for this asset+timeframe
          - If signal fires, force direction and feed into existing pipeline
          - All execution settings (max_bet, max_price, fill_fraction, etc.)
            are read from the existing config — BB only decides WHEN + DIRECTION
        """
        if not self.config.trend.enabled:
            return
        if not self._trend_analyzer:
            return
        if not self._snipe_callback:
            return

        now = datetime.now(timezone.utc)

        _TF_MAP = {300: "5m", 900: "15m", 3600: "1h", 14400: "4h", 86400: "1d"}

        for market in list(self._tracked.values()):
            # ── Respect EXISTING toggles ──
            if not self._is_cell_enabled(market.asset, market.duration_seconds):
                continue  # Asset/timeframe toggled off on dashboard
            if market.asset in self.config.execution.no_bet_set:
                continue  # Watch-only asset

            # ── Timing: only bet on markets that haven't started yet ──
            # BB needs lookahead — we're buying BEFORE the market starts
            # because BB already knows direction from technical analysis.
            if not market.market_start:
                continue
            seconds_to_start = (market.market_start - now).total_seconds()
            
            # ── BB Timing Window (Lookahead + In-Market) ──
            # Lookahead: how many seconds BEFORE start to allow bets
            la_secs = int(getattr(self.config.scanner, "lookahead_seconds", 60))
            
            # Post-start: how many seconds AFTER start to allow bets (In-Market)
            post_secs = int(getattr(self.config.trend, "post_start_window", 0))

            if seconds_to_start > la_secs:
                continue  # Too far out (before start)
            if seconds_to_start < -post_secs:
                continue  # Too far in (past the post-start window)

            cid = market.condition_id

            # ── Cooldown: don't double-bet ──
            last_buy = self._cooldowns.get(cid, 0)
            if (now.timestamp() - last_buy) < self.config.execution.cooldown_seconds:
                continue
            # Don't evaluate if already pending
            if cid in self._pending_snipes:
                continue

            # ── Get BB signal ──
            tf_label = _TF_MAP.get(market.duration_seconds, "5m")
            signal = self._trend_analyzer.get_signal(market.asset, tf_label)

            if signal.direction == "neutral":
                continue
            if signal.confidence < self.config.trend.min_confidence:
                continue

            # ── BB signal fires — force direction ──
            bb_side = signal.direction  # "Up" or "Down"
            bb_token = market.token_id if bb_side == "Up" else market.token_id_alt
            if not bb_token:
                continue

            # ── Ensure PTB is ready (main bot handles the capture) ──
            if not market.price_to_beat:
                continue

            # ── Fetch book for the BB-selected side ──
            self._pending_snipes.add(cid)
            try:
                book = await self.api.get_book_liquidity(bb_token)
                if not book or book.get("midpoint", 0) == 0:
                    continue

                best_ask = book.get("best_ask", 0)
                mid = book.get("midpoint", 0)
                ask_depth = book.get("ask_depth", 0)

                if best_ask <= 0:
                    continue

                # ── Apply EXISTING execution gates ──
                edge = (1.0 - best_ask) if best_ask > 0 else 0.0
                max_price = self.config.execution.get_max_buy_price(market.duration_seconds)
                _min_edge = self.config.execution.get_min_edge(market.duration_seconds)

                g_max_price = (0 < best_ask <= max_price) if max_price > 0 else (best_ask > 0)
                g_min_edge = edge >= _min_edge
                g_depth = ask_depth >= _DEPTH_FLOOR

                if not g_max_price:
                    if self.verbose:
                        logger.info("[BB] %s — ask $%.4f > max $%.4f (price gate)",
                                    _mkt_tag(market), best_ask, max_price)
                    continue
                if not g_min_edge:
                    if self.verbose:
                        logger.info("[BB] %s — edge $%.4f < min $%.4f (edge gate)",
                                    _mkt_tag(market), edge, _min_edge)
                    continue
                if not g_depth:
                    if self.verbose:
                        logger.info("[BB] %s — depth $%.0f < floor $%d",
                                    _mkt_tag(market), ask_depth, _DEPTH_FLOOR)
                    continue

                # ── All gates passed — build opportunity ──
                logger.info(
                    "[BB-TREND] %s — %s @ $%.4f | mid=%.4f | conf=%.0f%% | "
                    "BB pos=%.2f RSI=%.0f | edge $%.4f | %.0fs to start",
                    _mkt_tag(market), bb_side, best_ask, mid,
                    signal.confidence * 100,
                    signal.band_position, signal.rsi or 0,
                    edge, seconds_to_start,
                )

                market.best_ask = best_ask
                market.midpoint = mid
                market.ask_depth = ask_depth
                market.winning_token = bb_token
                market.winning_side = bb_side

                opp = SnipeOpportunity(
                    market=market,
                    best_ask=best_ask,
                    ask_depth=ask_depth,
                    edge=edge,
                    reason=(f"BB-TREND: {bb_side} @ ${best_ask:.4f} | "
                            f"BB={signal.band_position:.2f} RSI={signal.rsi or 0:.0f} "
                            f"conf={signal.confidence:.0%} | {signal.reason}"),
                    winning_token=bb_token,
                    winning_side=bb_side,
                )

                self.opportunities_found += 1
                try:
                    await self._snipe_callback(opp)
                except Exception as e:
                    logger.error("BB snipe callback error: %s", e)
            finally:
                self._pending_snipes.discard(cid)

    async def evaluate_hft_runner(self, asset: str, mode: str = "MODE2", price: float = 0):
        """Mode 2 HFT runner only (barrier -> immediate FOK or armed watch)."""
        if mode != "MODE2":
            return

        # Respect active assets gate.
        if asset not in self._assets:
            return

        now = datetime.now(timezone.utc)

        # 1) Find candidate markets for this asset with an active barrier break.
        candidates = []
        mode2_gap_by_cid: dict[str, dict] = {}
        mode2_threshold_by_cid: dict[str, float] = {}
        for m in self._tracked.values():
            if m.asset != asset:
                continue
            if not self._is_cell_enabled(m.asset, m.duration_seconds):
                continue
            if not m.market_start or not m.end_date:
                continue
            if m.condition_id in self._pending_snipes:
                continue
            gate = evaluate_execution_gate(
                config=self.config,
                market=m,
                is_hft_runner=True,
                is_resolved=(m.condition_id in self._resolved_cids),
                now_utc=now,
            )
            if not gate.allowed:
                continue
            # Trap already armed for this market: let WS callback own it.
            # Do not re-arm from price-feed ticks.
            if m.condition_id in self._hft_armed_traps:
                continue

            gap_info = self._price_feed.compute_gap(m.condition_id) if self._price_feed else None
            if not gap_info:
                # PTB not ready yet. The main scan loop handles live PTB capture.
                # HFT simply waits for it to appear.
                continue

            threshold2 = float(self.config.trend.get_hft_barrier_pct_for(m.asset, m.duration_seconds))
            if gap_info.get("gap_pct", 0.0) < threshold2:
                continue
            if gap_info.get("winning_side") not in ("Up", "Down"):
                continue
            # Check cooldown BEFORE evaluating market details to avoid log spam
            if self._hft_cooldowns.get(f"mode2:{m.condition_id}", 0) > 0:
                continue

            mode2_gap_by_cid[m.condition_id] = gap_info
            mode2_threshold_by_cid[m.condition_id] = threshold2

            # Active window checks
            if m.market_start <= now <= m.end_date:
                hft_muzzle = int(self.config.trend.hft_muzzle_seconds)
                stop_buf = self.config.scanner.stop_buffer_seconds
                muzzle = max(hft_muzzle, stop_buf)

                sec_to_end = (m.end_date - now).total_seconds()
                if sec_to_end < muzzle:
                    logger.debug(
                        "[HFT-MUZZLE] mkt=%s cid=%s action=skip sec_to_end=%.1f muzzle=%d hft=%d stop=%d",
                        _mkt_tag(m), short_cid(m.condition_id), sec_to_end, muzzle, hft_muzzle, stop_buf,
                    )
                    continue

                hft_barrier_delay = getattr(self.config.trend, "hft_barrier_delay", 10)
                sec_from_start = (now - m.market_start).total_seconds()
                if sec_from_start < hft_barrier_delay:
                    logger.debug(
                        "[HFT-START-DELAY] mkt=%s cid=%s action=skip sec_from_start=%.1f delay=%d",
                        _mkt_tag(m), short_cid(m.condition_id), sec_from_start, hft_barrier_delay,
                    )
                    continue

                candidates.append(m)

        if not candidates:
            return

        # 2) Pick deterministic target: shortest duration, then soonest end.
        candidates.sort(key=lambda m: (m.duration_seconds, m.end_date))
        target = candidates[0]
        cid = target.condition_id

        mode2_gap = mode2_gap_by_cid.get(cid)
        if not mode2_gap:
            return
        threshold2 = float(
            mode2_threshold_by_cid.get(
                cid,
                self.config.trend.get_hft_barrier_pct_for(target.asset, target.duration_seconds),
            )
        )
        direction = mode2_gap.get("winning_side", "")
        if direction not in ("Up", "Down"):
            return

        dist_pct = mode2_gap.get("gap_pct", 0.0)
        mkt_label = _mkt_tag(target)
        cid_short = short_cid(cid)

        mode2_watch = self.config.trend.mode2_watch_enabled()
        logger.debug("[HFT-EVAL] mkt=%s cid=%s armed=%s", mkt_label, cid_short, mode2_watch)

        # 3) One-and-done gate per market.
        hft_key = f"mode2:{cid}"
        last_hft_bet = self._hft_cooldowns.get(hft_key, 0)
        if last_hft_bet > 0:
            logger.debug(
                "[HFT-GATE] mkt=%s cid=%s action=skip reason=already_fired",
                mkt_label, cid_short,
            )
            return

        # 4) Resolve token to buy.
        token_id = target.token_id if direction == "Up" else target.token_id_alt
        if not token_id:
            logger.debug("[HFT-GATE] mkt=%s cid=%s action=skip reason=missing_token", mkt_label, cid_short)
            return
        max_p = float(self.config.trend.get_hft_max_price_for(target.asset, target.duration_seconds))
        watch_book = self._ws.get_book(token_id)
        watch_ask = float((watch_book or {}).get("best_ask", 0.0) or 0.0)
        watch_cap_depth = float(self._ws.get_depth_up_to(token_id, max_p) or 0.0)
        if watch_cap_depth <= 0:
            watch_cap_depth = float((watch_book or {}).get("ask_depth", 0.0) or 0.0)
        logger.info(
            "[HFT-BARRIER] mkt=%s cid=%s side=%s gap=%+.2f%% threshold=%.2f%% px=%.4f ptb=%.4f ask=%.4f cap=%.4f",
            mkt_label,
            cid_short,
            direction,
            dist_pct,
            threshold2,
            mode2_gap.get("current_price", 0.0),
            mode2_gap.get("price_to_beat", 0.0),
            watch_ask,
            max_p,
        )
        self._record_hft_event(
            condition_id=cid,
            event_type="barrier",
            market=target,
            side=direction,
            gap_pct=dist_pct,
            barrier_pct=threshold2,
            current_price=self._safe_float(mode2_gap.get("current_price")),
            price_to_beat=self._safe_float(mode2_gap.get("price_to_beat")),
            ask=watch_ask if watch_ask > 0 else None,
            cap_price=max_p,
            details={"cid_short": cid_short, "direction": mode2_gap.get("direction")},
        )

        # 5) Execute or arm watch.
        self._pending_snipes.add(cid)
        try:
            best_ask = max_p  # placeholder; FOK is capped by max_p
            ask_depth = watch_cap_depth
            hft_amount = self.config.trend.hft_bet_amount
            reason_str = (
                f"HFT: {direction} barrier break | gap={dist_pct:+.2f}% "
                f"(threshold={threshold2:.2f}%) | max=${max_p:.4f}"
            )

            opp = SnipeOpportunity(
                market=target,
                best_ask=best_ask,
                ask_depth=ask_depth,
                edge=(1.0 - best_ask),
                reason=reason_str,
                winning_token=token_id,
                winning_side=direction,
                gap_pct=dist_pct,
                gap_direction=mode2_gap.get("direction"),
                override_amount=hft_amount,
                is_hft_runner=True,
                timeframe_seconds=target.duration_seconds,
                hft_barrier_pct=threshold2,
                hft_cap_price=max_p,
                hft_entry_path="trap" if mode2_watch else "instant",
                hft_trigger_ask=watch_ask if watch_ask > 0 else None,
            )
            # Runtime latency markers for Mode2 path.
            opp.mode2_trigger_ts = time.perf_counter()
            opp.mode2_spring_ts = 0.0

            if self._snipe_callback:
                if mode2_watch:
                    # Arm trap: WS orderbook tick handler will spring execution.
                    self._hft_armed_traps[cid] = {
                        "side": direction,
                        "max_price": max_p,
                        "amount": hft_amount,
                        "token_id": token_id,
                        "outcome": direction,
                        "opp": opp,
                        "armed_ts": time.perf_counter(),
                        "trigger_ts": getattr(opp, "mode2_trigger_ts", 0.0),
                        "market_label": mkt_label,
                        "barrier_pct": threshold2,
                        "trigger_gap_pct": dist_pct,
                        "trigger_direction": mode2_gap.get("direction"),
                        "trigger_price": mode2_gap.get("current_price"),
                        "ptb_price": mode2_gap.get("price_to_beat"),
                        "asset": target.asset,
                        "event_title": target.event_title,
                        "timeframe_seconds": target.duration_seconds,
                    }
                    self._log_hft_trap_event("armed", cid, target, self._hft_armed_traps[cid], token=token_id)
                    log_lifecycle(
                        logger=logger,
                        phase="armed",
                        condition_id=cid,
                        asset=target.asset,
                        timeframe_seconds=target.duration_seconds,
                        market_start=target.market_start,
                        market_end=target.end_date,
                        source="scanner",
                        detail=f"side={direction} cap={max_p:.4f}",
                    )
                    # Core invariant: armed traps must be WS-visible for monitored tokens.
                    # This keeps trap logic event-driven (WS push), not scan-driven.
                    subscribe_ids = []
                    if target.token_id and target.token_id not in self._ws._subscribed:
                        subscribe_ids.append(target.token_id)
                    if target.token_id_alt and target.token_id_alt not in self._ws._subscribed:
                        subscribe_ids.append(target.token_id_alt)
                    if subscribe_ids:
                        try:
                            await self._ws.subscribe(subscribe_ids)
                        except Exception as sub_err:
                            logger.warning(
                                "[HFT-ARM] mkt=%s cid=%s ws_subscribe_error=%s",
                                mkt_label, cid_short, sub_err,
                            )
                    sub_primary = bool(target.token_id and target.token_id in self._ws._subscribed)
                    sub_alt = bool(target.token_id_alt and target.token_id_alt in self._ws._subscribed)
                    logger.info(
                        "[HFT-ARM-WS] mkt=%s cid=%s sub_primary=%s sub_alt=%s ws_subs=%d",
                        mkt_label, cid_short, sub_primary, sub_alt, len(self._ws._subscribed),
                    )

                    # Check once immediately using current WS cache only.
                    await self._check_hft_armed_trap(cid, token_id, target)
                    
                    if cid in self._hft_armed_traps:
                        logger.info(
                            "[HFT-ARM] mkt=%s cid=%s side=%s cap=%.4f monitor=ws_push",
                            mkt_label, cid_short, direction, max_p,
                        )
                    # If CID is no longer in traps — it already fired or muzzled.
                else:
                    # One-and-done gate applies after fire attempt (not at arm time).
                    self._hft_cooldowns[hft_key] = time.time()
                    opp.mode2_spring_ts = getattr(opp, "mode2_trigger_ts", 0.0)
                    await self._snipe_callback(opp)

        except Exception as e:
            logger.error("HFT mode2 error for %s: %s", asset, e)
        finally:
            self._pending_snipes.discard(cid)
    async def startup_warmup(self):
        """Prime runtime discovery/PTB before enabling trigger callbacks."""
        if self._market_runtime:
            await self._market_runtime.prime_startup()
            self._sync_runtime_markets()

    async def _check_market(self, market: TrackedMarket, now: datetime) -> Optional[SnipeOpportunity]:
        """Check a single market's orderbook for snipe opportunity.

        Supports TWO modes:
        1. Post-end sniping: market has ended, winning side clearly visible
        2. Pre-end sniping: market ending soon (<pre_end_seconds), mid > pre_end_threshold

        A market is snipeable when:
        1. One side is clearly winning (midpoint > threshold, varies by mode)
        2. The winning side's best_ask is below our max_buy_price
        3. There's enough ask-side depth to fill our order
        4. Cooldown has elapsed since last buy on this market
        """
        # Skip if the WS fast-path already has this market locked for snipe
        if market.condition_id in self._pending_snipes:
            logger.debug("Poll skip (WS pending): %s", market.event_title[:40])
            return None

        gate = evaluate_execution_gate(
            config=self.config,
            market=market,
            is_hft_runner=False,
            is_resolved=(market.condition_id in self._resolved_cids),
            now_utc=now,
        )
        if not gate.allowed:
            return None

        # Skip if runtime toggles disable this asset/timeframe cell.
        if not self._is_cell_enabled(market.asset, market.duration_seconds):
            return None

        seconds_to_end = (market.end_date - now).total_seconds()

        # Skip if this market's end-minute slot is blacklisted (Clock Blackout)
        # NOTE: only for pre-end sniping — post-end trades are never blocked
        if seconds_to_end > 0:
            _slot = _end_slot(market)
            if self.config.blackout.is_blocked(market.duration_seconds, _slot, market.end_date):
                if self.verbose:
                    _tf = _DURATION_TO_TF_LABEL.get(market.duration_seconds, '??')
                    logger.info("[SNIPE: BLACKOUT] %s (%s) \u2014 slot :%02d blocked, skipping",
                                market.event_title[:40], _tf, _slot)
                return None

        pre_end_sec = self.config.scanner.get_pre_end_seconds(market.duration_seconds)
        pre_end_thresh = self.config.scanner.get_pre_end_threshold(market.duration_seconds)
        seconds_to_start = (
            (market.market_start - now).total_seconds()
            if market.market_start else None
        )

        post_bet = self.config.scanner.post_end_bet_seconds

        stop_buf = self.config.scanner.stop_buffer_seconds
        start_buf = self.config.scanner.start_buffer_seconds

        is_pre_end = False
        if seconds_to_end > 0:
            # Market hasn't ended yet
            if market.market_start and now < market.market_start:
                return None  # Only BB Trend can snipe before market start
            if seconds_to_end < stop_buf:
                # Inside stop-buffer dead zone -- refuse bets to avoid last-second dumpers
                if self.verbose:
                    logger.info("[STOP-BUF] %s — %.1fs to end < %ds stop buffer, skipping",
                                market.event_title[:40], seconds_to_end, stop_buf)
                return None
            if start_buf > 0 and market.market_start:
                elapsed = (now - market.market_start).total_seconds()
                if elapsed > start_buf:
                    if self.verbose:
                        logger.info("[START-BUF] %s — %.0fs elapsed > %ds start buffer, skipping",
                                    market.event_title[:40], elapsed, start_buf)
                    return None
            if seconds_to_end <= pre_end_sec:
                is_pre_end = True
            else:
                if self.verbose:
                    tf_label = {300:'5m',900:'15m',3600:'1h',14400:'4h',86400:'1d'}.get(market.duration_seconds, '??')
                    logger.info("[V] %s (%s) — %.0fs to end, pre-end window=%ds (waiting)",
                                market.event_title[:40], tf_label, seconds_to_end, pre_end_sec)
                return None  # Not yet in pre-end window
        else:
            # Post-end: honour post_end_bet_seconds (0 = disable)
            if post_bet <= 0 or (-seconds_to_end) > post_bet:
                return None



        # -- Early gap-size check (in-memory, no network) --
        # If the Binance gap is below threshold, neither side can pass the
        # gate regardless of orderbook state.  Skip the expensive REST book
        # calls entirely.  The full direction check runs later after we know
        # the winning side from the orderbook.
        gap_info = self._price_feed.compute_gap(market.condition_id) if self._price_feed else None
        if gap_info:
            threshold = self._get_gap_threshold(market.asset, market.duration_seconds)
            if gap_info["gap_pct"] < threshold:
                logger.debug("[SNIPE: GAP SKIP] %s gap=%.4f%% < %.3f%% -- skipping REST",
                             market.event_title[:40], gap_info["gap_pct"], threshold)
                return None

        # -- Early cooldown check (in-memory, no network) --
        # Prevents wasting 2 REST book calls on a market we just sniped.
        last_buy = self._cooldowns.get(market.condition_id, 0)
        if (now.timestamp() - last_buy) < self.config.execution.cooldown_seconds:
            return None

        # Check BOTH tokens (Up and Down) -- snipe whichever is the winner
        # Capture both sides' book data for snapshot recording.
        best_book = None
        best_side = ""
        best_token = ""
        side_books: dict[str, dict] = {"Up": {}, "Down": {}}

        # IMPORTANT: Always use REST API for book data in the snipe window.
        # The WS cache only tracks incremental price_change events, NOT the
        # full orderbook, so its best_ask/midpoint are frequently wrong
        # (e.g. showing $1.00 when real ask is $0.65).
        # Fetch BOTH tokens in parallel to save one round-trip (~100-500ms).
        token_pairs = [(label, tid) for label, tid in
                       [("Up", market.token_id), ("Down", market.token_id_alt)]
                       if tid]

        async def _fetch_book(label_tid):
            label, tid = label_tid
            try:
                book = await self.api.get_book_liquidity(tid)
                return label, tid, book
            except Exception as e:
                logger.debug("Book check failed for %s %s: %s",
                             market.event_title[:30], label, e)
                return label, tid, None

        book_results = await asyncio.gather(*[_fetch_book(tp) for tp in token_pairs])

        for side_label, tid, book in book_results:
            if not book or book.get("midpoint", 0) == 0:
                continue

            side_books[side_label] = book
            mid = book.get("midpoint", 0)
            time_desc = f"{seconds_to_end:.0f}s before end" if is_pre_end else f"{-seconds_to_end:.0f}s past end"
            logger.debug("%s %s: mid=%.4f ask=%.4f depth=$%.2f | %s",
                         side_label, market.event_title[:30],
                         mid, book.get("best_ask", 0),
                         book.get("ask_depth", 0), time_desc)

            if best_book is None or mid > best_book.get("midpoint", 0):
                best_book = book
                best_side = side_label
                best_token = tid

        # Common throttle + label vars (used by early "no book" check AND gate messages)
        _now_ts = now.timestamp()
        _last_snap = self._snapshot_ts.get(market.condition_id, 0)
        _mkt_label = _mkt_tag(market)

        if not best_book:
            # Throttle "no book" to once per 30s per market (static condition, don't spam)
            _nb_key = f"nb:{market.condition_id}"
            _nb_last = self._snapshot_ts.get(_nb_key, 0)
            if (_now_ts - _nb_last) >= 30:
                self._snapshot_ts[_nb_key] = _now_ts
                logger.debug("[SNIPE: SKIP] %s — no asks on book", _mkt_label)
            return None

        market.best_ask = best_book["best_ask"]
        market.midpoint = best_book["midpoint"]
        market.ask_depth = best_book["ask_depth"]
        market.winning_token = best_token
        market.winning_side = best_side

        # -- Evaluate ALL gates (track pass/fail for snapshot) --
        best_ask = best_book["best_ask"]
        edge = (1.0 - best_ask) if best_ask > 0 else 0.0
        threshold = pre_end_thresh if is_pre_end else self.config.scanner.winner_threshold
        max_price = self.config.execution.get_max_buy_price(market.duration_seconds)
        _min_edge = self.config.execution.get_min_edge(market.duration_seconds)

        # Use best_ask (not midpoint) — mid includes bid side which we
        # don't interact with; an aggressive bid can spike the mid while
        # asks remain unconvinced (see .dev/2026-02-18_ask_threshold_gate.md).
        g_threshold = best_ask >= threshold
        g_max_price = (0 < best_ask <= max_price) if max_price > 0 else (best_ask > 0)
        g_min_edge = edge >= _min_edge
        g_depth = best_book["ask_depth"] >= _DEPTH_FLOOR

        # Recompute gap with fresh Binance data (avoids staleness from
        # the pre-filter computed before the REST round-trip).
        gap_info = self._price_feed.compute_gap(market.condition_id) if self._price_feed else None

        # Combined oracle gate (gap + direction + consensus)
        oracle_rejection, consensus = self._check_oracle_gate(market, best_side, gap_info=gap_info)
        g_gap = oracle_rejection is None  # None means passed

        # Conviction gate: |up_mid - down_mid| — how decisively the book leans
        up_mid_val = side_books.get("Up", {}).get("midpoint", 0) or 0
        dn_mid_val = side_books.get("Down", {}).get("midpoint", 0) or 0
        conviction = abs(up_mid_val - dn_mid_val)
        _min_conv = self.config.scanner.get_min_conviction(market.duration_seconds)
        # W-1 guard: if either side book is missing, conviction is
        # unreliable (0 default inflates the delta).  Block the trade.
        _both_sides = bool(side_books.get("Up")) and bool(side_books.get("Down"))
        g_conviction = (_both_sides and conviction >= _min_conv) if _min_conv > 0 else True

        # Max-gap-cap gate: block suspiciously large gaps (signal clipping)
        # Missing data → inf so the gate blocks (fail-closed)
        _max_gap = self.config.scanner.get_max_gap_pct(market.duration_seconds)
        _actual_gap = gap_info["gap_pct"] if gap_info else float("inf")
        g_max_gap = _actual_gap <= _max_gap if _max_gap > 0 else True

        g_all = g_threshold and g_max_price and g_min_edge and g_depth and g_gap and g_conviction and g_max_gap

        # -- Record snapshot (throttled: max 1 per market per 5s, fire-and-forget) --
        if self._db and (_now_ts - _last_snap) >= self._SNAPSHOT_INTERVAL:
            self._snapshot_ts[market.condition_id] = _now_ts
            up_b = side_books.get("Up", {})
            dn_b = side_books.get("Down", {})
            _bg(self._db.record_snapshot(
                condition_id=market.condition_id,
                event_title=market.event_title[:80],
                asset=market.asset,
                seconds_to_end=seconds_to_end,
                duration_seconds=market.duration_seconds,
                up_mid=up_b.get("midpoint", 0),
                up_ask=up_b.get("best_ask", 0),
                up_depth=up_b.get("ask_depth", 0),
                down_mid=dn_b.get("midpoint", 0),
                down_ask=dn_b.get("best_ask", 0),
                down_depth=dn_b.get("ask_depth", 0),
                best_side=best_side,
                best_mid=market.midpoint,
                best_ask=best_ask,
                best_depth=best_book["ask_depth"],
                edge=edge,
                binance_price=gap_info["current_price"] if gap_info else None,
                price_to_beat=gap_info["price_to_beat"] if gap_info else None,
                gap_pct=gap_info["gap_pct"] if gap_info else None,
                gap_direction=gap_info["direction"] if gap_info else None,
                ptb_source=market.ptb_source or None,
                passed_threshold=g_threshold,
                passed_max_price=g_max_price,
                passed_min_edge=g_min_edge,
                passed_depth=g_depth,
                passed_gap=g_gap,
                passed_conviction=g_conviction,
                passed_max_gap=g_max_gap,
                passed_all=g_all,
                action="snipe" if g_all else "observe",
            ))

        # -- Monitor DB tick (poll piggyback, fire-and-forget) --
        if self._monitor_db:
            _mon_last = self._monitor_ts.get(market.condition_id, 0)
            if (_now_ts - _mon_last) >= MONITOR_TICK_INTERVAL:
                self._monitor_ts[market.condition_id] = _now_ts
                _up_m = side_books.get("Up", {})
                _dn_m = side_books.get("Down", {})
                # Extract per-oracle directions
                _cl_dir, _bin_dir = _oracle_directions(consensus, gap_info)
                _bg(self._monitor_db.record_tick(
                    condition_id=market.condition_id,
                    event_title=market.event_title,
                    asset=market.asset,
                    duration_seconds=market.duration_seconds,
                    seconds_to_end=seconds_to_end,
                    up_mid=_up_m.get("midpoint", 0),
                    up_ask=_up_m.get("best_ask", 0),
                    up_depth=_up_m.get("ask_depth", 0),
                    down_mid=_dn_m.get("midpoint", 0),
                    down_ask=_dn_m.get("best_ask", 0),
                    down_depth=_dn_m.get("ask_depth", 0),
                    best_side=best_side,
                    best_mid=best_book["midpoint"],
                    best_ask=best_ask,
                    best_depth=best_book["ask_depth"],
                    edge=edge,
                    conviction=conviction,
                    binance_price=gap_info["current_price"] if gap_info else None,
                    price_to_beat=gap_info["price_to_beat"] if gap_info else None,
                    gap_pct=gap_info["gap_pct"] if gap_info else None,
                    gap_direction=gap_info["direction"] if gap_info else None,
                    passed_threshold=g_threshold,
                    passed_max_price=g_max_price,
                    passed_min_edge=g_min_edge,
                    passed_depth=g_depth,
                    passed_gap=g_gap,
                    passed_conviction=g_conviction,
                    passed_max_gap=g_max_gap,
                    passed_all=g_all,
                    cfg_threshold=threshold,
                    cfg_max_price=max_price,
                    cfg_min_edge=_min_edge,
                    cfg_min_conviction=_min_conv,
                    cfg_gap_pct=self._get_gap_threshold(market.asset, market.duration_seconds),
                    cfg_max_gap=_max_gap,
                    source="poll",
                    event_slug=market.event_slug,
                    oracle_cl_direction=_cl_dir,
                    oracle_bin_direction=_bin_dir,
                ))

        # -- Apply gates (early-exit on failure) --
        # Throttle normal-mode gate messages: only log when a snapshot was also recorded
        # (snapshot fires at most once per 5s per market). Verbose always logs.
        _snap_fired = self._db and (_now_ts - _last_snap) >= self._SNAPSHOT_INTERVAL

        # Is the best side clearly winning?
        if not g_threshold:
            if _snap_fired:
                logger.debug("[SNIPE: SKIP] %s — ask $%.2f < threshold %.2f (not converged)",
                            _mkt_label, best_ask, threshold)
            if self.verbose:
                mode = 'PRE-END' if is_pre_end else 'POST-END'
                binance_str = ''
                if gap_info:
                    oracle = gap_info.get('current_oracle', 'binance')
                    binance_str = (f" | {oracle}={gap_info['direction']} "
                                   f"gap={gap_info['gap_pct']:.3f}% "
                                   f"(${gap_info['current_price']:.2f} vs ptb=${gap_info['price_to_beat']:.2f})")
                logger.info('[SNIPE: DETAIL] [%s] %s best=%s mid=%.4f ask=$%.4f thresh=%.2f%s',
                             mode, market.event_title[:40], best_side, market.midpoint,
                             best_book.get('best_ask', 0), threshold, binance_str)
            return None

        if best_ask <= 0:
            if _snap_fired:
                logger.debug("[SNIPE: SKIP] %s — %s winning but no asks on book",
                            _mkt_label, best_side)
            return None

        if not g_max_price:
            if _snap_fired:
                logger.debug("[SNIPE: SKIP] %s — %s ask $%.2f > max $%.2f",
                            _mkt_label, best_side, best_ask, max_price)
            if self.verbose:
                logger.info("[SNIPE: DETAIL] %s mid=%.4f ask=$%.4f edge=$%.4f depth=$%.0f",
                            market.event_title[:45], market.midpoint, best_ask, edge,
                            best_book['ask_depth'])
            return None

        if not g_min_edge:
            if _snap_fired:
                logger.debug("[SNIPE: SKIP] %s — edge $%.3f < min $%.2f (ask $%.2f)",
                            _mkt_label, edge, _min_edge, best_ask)
            return None

        if not g_depth:
            if _snap_fired:
                logger.debug("[SNIPE: SKIP] %s — depth $%.0f < $%d floor",
                            _mkt_label, best_book["ask_depth"], _DEPTH_FLOOR)
            return None

        # Conviction gate
        if not g_conviction:
            if _snap_fired:
                logger.debug("[SNIPE: SKIP] %s \u2014 conviction %.3f < min %.2f (up_mid=%.3f dn_mid=%.3f)",
                            _mkt_label, conviction, _min_conv, up_mid_val, dn_mid_val)
            return None

        # Combined oracle gate
        if oracle_rejection:
            logger.info("[SNIPE: ORACLE BLOCK] %s \u2014 %s", _mkt_label, oracle_rejection)
            return None

        # Max-gap-cap gate (signal clipping)
        if not g_max_gap:
            if _snap_fired:
                logger.debug("[SNIPE: SKIP] %s \u2014 gap %.3f%% > max %.2f%% (spike filter)",
                            _mkt_label, _actual_gap, _max_gap)
            return None

        # -- Gap + consensus observation logging --
        gap_str = ""
        if gap_info:
            _orc = _ORACLE_SHORT.get(gap_info.get('current_oracle', 'binance'), 'BIN')
            gap_str = (f" | {_orc}:${gap_info['current_price']:.2f} "
                       f"gap={gap_info['gap_pct']:.3f}% {gap_info['direction']}")
        con_str = ""
        if consensus:
            cl = consensus["chainlink_side"]
            bn = consensus["binance_side"]
            if cl == "unavailable" or bn == "unavailable":
                missing = "CL" if cl == "unavailable" else "BIN"
                con_str = f" | {missing}:unavail"
            else:
                con_str = f" | CL={cl} BIN={bn}"

        conv_str = f" | conv={conviction:.3f}"

        strategy_label = 'PRE-END' if is_pre_end else 'POLL'
        time_label = 'before' if is_pre_end else 'past'
        secs = abs(seconds_to_end)
        reason_str = (
            f"{strategy_label}: {best_side} winner"
            f" @ ${best_ask:.4f} ({secs:.0f}s {time_label} end){gap_str}{con_str}{conv_str}"
        )
        return SnipeOpportunity(
            market=market,
            best_ask=best_ask,
            ask_depth=best_book["ask_depth"],
            edge=edge,
            reason=reason_str,
            winning_token=best_token,
            winning_side=best_side,
            gap_pct=gap_info["gap_pct"] if gap_info else None,
            gap_direction=gap_info["direction"] if gap_info else None,
        )


    def record_cooldown(self, condition_id: str):
        """Record a buy cooldown for a market."""
        self._cooldowns[condition_id] = datetime.now(timezone.utc).timestamp()

    def is_on_cooldown(self, condition_id: str) -> bool:
        """Check if a market is still within its cooldown window."""
        last = self._cooldowns.get(condition_id, 0)
        return (datetime.now(timezone.utc).timestamp() - last) < self.config.execution.cooldown_seconds

    # -- Monitor recording (wider observation windows) --

    async def _monitor_tick(self, market: TrackedMarket, now: datetime,
                            seconds_to_end: float) -> None:
        """Record an observation tick to the monitor database.

        Called for markets inside the wider monitor window but OUTSIDE the
        snipe window.  Fetches both sides' books via REST, evaluates all
        gates, and writes every field to ``data/monitor.db``.
        """
        if not self._monitor_db:
            return

        cid = market.condition_id
        _now_ts = now.timestamp()

        # Throttle: one tick per market per MONITOR_TICK_INTERVAL
        if (_now_ts - self._monitor_ts.get(cid, 0)) < MONITOR_TICK_INTERVAL:
            return
        self._monitor_ts[cid] = _now_ts

        # -- Fetch BOTH tokens via REST in parallel --
        token_pairs = [(lbl, tid) for lbl, tid in
                       [("Up", market.token_id), ("Down", market.token_id_alt)]
                       if tid]

        async def _fetch(lt):
            lbl, tid = lt
            try:
                return lbl, await self.api.get_book_liquidity(tid)
            except Exception:
                return lbl, None

        results = await asyncio.gather(*[_fetch(tp) for tp in token_pairs])

        side_books: dict[str, dict] = {"Up": {}, "Down": {}}
        best_book = None
        best_side = ""
        for side_label, book in results:
            if not book or book.get("midpoint", 0) == 0:
                continue
            side_books[side_label] = book
            if best_book is None or book["midpoint"] > best_book.get("midpoint", 0):
                best_book = book
                best_side = side_label

        if not best_book:
            return

        # -- Evaluate all gates (identical logic to _check_market) --
        best_ask = best_book["best_ask"]
        edge = (1.0 - best_ask) if best_ask > 0 else 0.0
        pre_end_thresh = self.config.scanner.get_pre_end_threshold(market.duration_seconds)
        threshold = pre_end_thresh if seconds_to_end > 0 else self.config.scanner.winner_threshold
        max_price = self.config.execution.get_max_buy_price(market.duration_seconds)
        _min_edge = self.config.execution.get_min_edge(market.duration_seconds)

        g_threshold = best_ask >= threshold
        g_max_price = (0 < best_ask <= max_price) if max_price > 0 else (best_ask > 0)
        g_min_edge = edge >= _min_edge
        g_depth = best_book["ask_depth"] >= _DEPTH_FLOOR

        gap_info = self._price_feed.compute_gap(cid) if self._price_feed else None
        oracle_rejection, _consensus = self._check_oracle_gate(market, best_side, gap_info=gap_info)
        g_gap = oracle_rejection is None

        up_mid = side_books.get("Up", {}).get("midpoint", 0) or 0
        dn_mid = side_books.get("Down", {}).get("midpoint", 0) or 0
        conviction = abs(up_mid - dn_mid)
        _min_conv = self.config.scanner.get_min_conviction(market.duration_seconds)
        _both = bool(side_books.get("Up")) and bool(side_books.get("Down"))
        g_conviction = (_both and conviction >= _min_conv) if _min_conv > 0 else True

        _max_gap = self.config.scanner.get_max_gap_pct(market.duration_seconds)
        _actual_gap = gap_info["gap_pct"] if gap_info else 0.0
        g_max_gap = _actual_gap <= _max_gap if _max_gap > 0 else True

        g_all = (g_threshold and g_max_price and g_min_edge and g_depth
                 and g_gap and g_conviction and g_max_gap)

        # -- Write tick --
        up_b = side_books.get("Up", {})
        dn_b = side_books.get("Down", {})
        # Extract per-oracle directions
        _cl_dir, _bin_dir = _oracle_directions(_consensus, gap_info)
        try:
            await self._monitor_db.record_tick(
                condition_id=cid,
                event_title=market.event_title,
                asset=market.asset,
                duration_seconds=market.duration_seconds,
                seconds_to_end=seconds_to_end,
                up_mid=up_b.get("midpoint", 0),
                up_ask=up_b.get("best_ask", 0),
                up_depth=up_b.get("ask_depth", 0),
                down_mid=dn_b.get("midpoint", 0),
                down_ask=dn_b.get("best_ask", 0),
                down_depth=dn_b.get("ask_depth", 0),
                best_side=best_side,
                best_mid=best_book["midpoint"],
                best_ask=best_ask,
                best_depth=best_book["ask_depth"],
                edge=edge,
                conviction=conviction,
                binance_price=gap_info["current_price"] if gap_info else None,
                price_to_beat=gap_info["price_to_beat"] if gap_info else None,
                gap_pct=gap_info["gap_pct"] if gap_info else None,
                gap_direction=gap_info["direction"] if gap_info else None,
                passed_threshold=g_threshold,
                passed_max_price=g_max_price,
                passed_min_edge=g_min_edge,
                passed_depth=g_depth,
                passed_gap=g_gap,
                passed_conviction=g_conviction,
                passed_max_gap=g_max_gap,
                passed_all=g_all,
                cfg_threshold=threshold,
                cfg_max_price=max_price,
                cfg_min_edge=_min_edge,
                cfg_min_conviction=_min_conv,
                cfg_gap_pct=self._get_gap_threshold(market.asset, market.duration_seconds),
                cfg_max_gap=_max_gap,
                source="monitor",
                event_slug=market.event_slug,
                oracle_cl_direction=_cl_dir,
                oracle_bin_direction=_bin_dir,
            )
        except Exception as e:
            logger.debug("[MONITOR] tick error: %s", e)

