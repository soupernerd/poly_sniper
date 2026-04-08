"""Central timed-market runtime.

Single owner for:
  - discovery
  - baseline WS subscription lifecycle
  - PTB live capture
  - stale-market pruning

Scanner/monitor consume this runtime; they do not own these paths.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

from src.lifecycle import log_lifecycle
from src.timed_market_utils import (
    ASSET_PATTERN,
    DURATION_TO_TF_LABEL,
    LOOKAHEAD_BY_DURATION,
    TF_LABEL_TO_DURATION,
    TIMED_PATTERN,
    enhance_title,
    get_both_tokens,
    normalize_asset,
    parse_assets_csv,
    parse_date_utc,
    parse_market_times,
    parse_outcome,
    parse_timeframes_csv,
)

if TYPE_CHECKING:
    from src.api import PolymarketAPI
    from src.config import Config
    from src.price_feed import BinancePriceFeed
    from src.ws_client import MarketWSClient

logger = logging.getLogger(__name__)


@dataclass
class RuntimeMarket:
    condition_id: str
    token_up: str
    token_down: str
    outcome: str
    event_title: str
    event_slug: str
    asset: str
    neg_risk: bool
    market_start: Optional[datetime]
    market_end: datetime
    timeframe_seconds: int


class MarketRuntime:
    _UP_OR_DOWN_TAG_ID = 102127
    _LOOP_SLEEP_SECONDS = 0.10
    _MIN_DISCOVERY_SECONDS = 5
    _DEFAULT_DISCOVERY_SECONDS = 10
    _WS_SUBSCRIBE_BUFFER_SECONDS = 10

    _PTB_LIVE_GRACE_SECONDS = 30.0
    _PTB_LIVE_RETRY_SECONDS = 0.50
    _PTB_BACKFILL_RETRY_SECONDS = 60.0
    _BACKFILL_CONCURRENCY = 4
    _DAILY_END_HOUR_UTC = 17

    def __init__(
        self,
        *,
        config: "Config",
        api: "PolymarketAPI",
        ws: "MarketWSClient",
        price_feed: "BinancePriceFeed",
    ):
        self.config = config
        self.api = api
        self.ws = ws
        self.price_feed = price_feed

        self._running = False
        self._task: asyncio.Task | None = None

        self._tracked: dict[str, RuntimeMarket] = {}
        self._last_discovery_ts: float = 0.0
        self._ptb_retry_after: dict[str, float] = {}

        self._backfill_sem = asyncio.Semaphore(self._BACKFILL_CONCURRENCY)
        self._backfill_tasks: dict[str, asyncio.Task] = {}
        self._backfill_inflight: set[str] = set()

        # Runtime-owned WS "claims". Physical ws._subscribed set remains in ws client.
        self._claimed_tokens: set[str] = set()
        self._ws_active_cids: set[str] = set()

        self._stats = {
            "discovered_total": 0,
            "last_discovery_at": 0.0,
            "ptb_live_captures": 0,
            "ptb_backfills": 0,
            "ptb_backfill_misses": 0,
        }

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("[MARKET-RUNTIME] started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._backfill_tasks:
            for task in list(self._backfill_tasks.values()):
                task.cancel()
            await asyncio.gather(*self._backfill_tasks.values(), return_exceptions=True)
            self._backfill_tasks.clear()
        self._backfill_inflight.clear()
        if self._claimed_tokens:
            try:
                await self.ws.unsubscribe(sorted(self._claimed_tokens))
            except Exception:
                pass
        self._claimed_tokens.clear()
        self._ws_active_cids.clear()
        logger.info("[MARKET-RUNTIME] stopped")

    async def prime_startup(self):
        """Synchronous warmup for startup readiness."""
        now = datetime.now(timezone.utc)
        await self._discover_markets(now, force=True)
        await self._sync_ws_subscriptions(now)
        await self._ensure_ptb(now, inline_backfill=True)

    def get_stats(self) -> dict:
        return {
            "running": self._running,
            "tracked_markets": len(self._tracked),
            "claimed_tokens": len(self._claimed_tokens),
            "active_ws_cids": len(self._ws_active_cids),
            "discovered_total": int(self._stats.get("discovered_total", 0)),
            "ptb_live_captures": int(self._stats.get("ptb_live_captures", 0)),
            "ptb_backfills": int(self._stats.get("ptb_backfills", 0)),
            "ptb_backfill_misses": int(self._stats.get("ptb_backfill_misses", 0)),
        }

    def subscribed_tokens(self) -> set[str]:
        return set(self._claimed_tokens)

    def scanner_markets(self) -> list[RuntimeMarket]:
        return sorted(
            (m for m in self._tracked.values() if self._scanner_cell_enabled(m.asset, m.timeframe_seconds)),
            key=lambda m: m.market_end,
        )

    def monitor_markets(self) -> list[RuntimeMarket]:
        return sorted(
            (m for m in self._tracked.values() if self._monitor_cell_enabled(m.asset, m.timeframe_seconds)),
            key=lambda m: m.market_end,
        )

    async def _run_loop(self):
        while self._running:
            started = time.monotonic()
            now = datetime.now(timezone.utc)
            try:
                await self._discover_markets(now, force=False)
                await self._sync_ws_subscriptions(now)
                await self._ensure_ptb(now, inline_backfill=False)
                await self._prune(now)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("[MARKET-RUNTIME] cycle error: %s", exc)
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.01, self._LOOP_SLEEP_SECONDS - elapsed))

    def _discover_interval_seconds(self) -> int:
        monitor_iv = int(getattr(self.config.monitor, "discovery_interval_seconds", 15) or 15)
        return max(self._MIN_DISCOVERY_SECONDS, min(self._DEFAULT_DISCOVERY_SECONDS, monitor_iv))

    def _scanner_cells(self) -> set[tuple[str, int]]:
        out: set[tuple[str, int]] = set()
        for asset in ("bitcoin", "ethereum", "solana", "xrp"):
            for tf_label, tf_secs in TF_LABEL_TO_DURATION.items():
                try:
                    if self.config.scanner.is_asset_timeframe_enabled(asset, tf_secs):
                        out.add((asset, tf_secs))
                except Exception:
                    continue
        return out

    def _monitor_cells(self) -> set[tuple[str, int]]:
        if not bool(getattr(self.config.monitor, "enabled", True)):
            return set()
        assets = parse_assets_csv(getattr(self.config.monitor, "assets", ""))
        tfs = parse_timeframes_csv(getattr(self.config.monitor, "time_frames", ""))
        return {(a, tf) for a in assets for tf in tfs}

    def _scanner_cell_enabled(self, asset: str, timeframe_seconds: int) -> bool:
        try:
            return bool(self.config.scanner.is_asset_timeframe_enabled(asset, int(timeframe_seconds or 0)))
        except Exception:
            return False

    def _monitor_cell_enabled(self, asset: str, timeframe_seconds: int) -> bool:
        return (asset, int(timeframe_seconds or 0)) in self._monitor_cells()

    def _cell_enabled_any(self, asset: str, timeframe_seconds: int) -> bool:
        key = (asset, int(timeframe_seconds or 0))
        return key in self._scanner_cells() or key in self._monitor_cells()

    def _prune_age_seconds(self) -> int:
        scanner_age = max(
            0,
            int(getattr(self.config.scanner, "post_window_seconds", 0) or 0)
            + int(getattr(self.config.scanner, "ws_cleanup_delay", 0) or 0),
        )
        monitor_age = 0
        if bool(getattr(self.config.monitor, "enabled", True)):
            monitor_age = max(0, int(getattr(self.config.monitor, "post_end_track_seconds", 120) or 120) + 300)
        return max(scanner_age, monitor_age)

    async def _discover_markets(self, now: datetime, *, force: bool):
        now_ts = time.monotonic()
        if not force and (now_ts - self._last_discovery_ts) < self._discover_interval_seconds():
            return
        self._last_discovery_ts = now_ts

        active_cells = self._scanner_cells() | self._monitor_cells()
        if not active_cells:
            # No active cells across scanner+monitor -> clear runtime state.
            if self._tracked:
                self._tracked.clear()
            await self._sync_ws_subscriptions(now)
            return

        prune_age = timedelta(seconds=self._prune_age_seconds())
        end_date_min = (now - prune_age).strftime("%Y-%m-%dT%H:%M:%SZ")
        query_limit = max(
            25,
            min(
                500,
                max(
                    int(getattr(self.config.scanner, "max_events_per_query", 100) or 100),
                    int(getattr(self.config.monitor, "max_events_per_query", 250) or 250),
                ),
            ),
        )

        try:
            events = await self.api.search_events(
                tag_id=self._UP_OR_DOWN_TAG_ID,
                active=True,
                closed=False,
                limit=query_limit,
                end_date_min=end_date_min,
                order="endDate",
                ascending=True,
            )
        except Exception as exc:
            logger.warning("[MARKET-RUNTIME] discovery failed: %s", exc)
            return

        # Targeted daily pass around 17:00 UTC.
        try:
            next_17 = now.replace(hour=self._DAILY_END_HOUR_UTC, minute=0, second=0, microsecond=0)
            if next_17 <= now:
                next_17 += timedelta(days=1)
            if (next_17 - now).total_seconds() <= LOOKAHEAD_BY_DURATION.get(86400, 86400):
                far_min = (next_17 - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
                far_max = (next_17 + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
                far_events = await self.api.search_events(
                    tag_id=self._UP_OR_DOWN_TAG_ID,
                    active=True,
                    closed=False,
                    limit=100,
                    end_date_min=far_min,
                    end_date_max=far_max,
                    order="endDate",
                    ascending=True,
                )
                if far_events:
                    seen_ids = {e.get("id") for e in events}
                    for fe in far_events:
                        if fe.get("id") not in seen_ids:
                            events.append(fe)
        except Exception:
            pass

        new_count = 0
        new_markets: list[RuntimeMarket] = []
        monitor_pre_start = max(0, int(getattr(self.config.monitor, "pre_start_capture_seconds", 15) or 15))

        for event in events:
            title = str(event.get("title", "") or "")
            if not TIMED_PATTERN.search(title):
                continue
            asset_match = ASSET_PATTERN.search(title)
            if not asset_match:
                continue
            asset = normalize_asset(asset_match.group(1))
            if asset not in {"bitcoin", "ethereum", "solana", "xrp"}:
                continue

            end_raw = str(event.get("endDate", "") or event.get("endDateIso", "") or "")
            if not end_raw:
                continue
            try:
                end_dt = parse_date_utc(end_raw)
            except Exception:
                continue
            if end_dt < now - prune_age:
                continue

            event_slug = str(event.get("slug", "") or "")
            neg_risk = bool(event.get("negRisk", False))
            for market in (event.get("markets") or []):
                cond_id = str(market.get("conditionId", "") or "")
                if not cond_id:
                    continue
                token_up, token_down = get_both_tokens(market)
                if not token_up:
                    continue

                market_start, duration_secs = parse_market_times(title, end_dt)
                if duration_secs <= 0:
                    continue
                if (asset, duration_secs) not in active_cells:
                    continue

                secs_ahead = (end_dt - now).total_seconds()
                max_ahead = max(
                    LOOKAHEAD_BY_DURATION.get(duration_secs, 7200),
                    int(duration_secs + max(900, monitor_pre_start + 120)),
                )
                if secs_ahead > max_ahead:
                    continue

                question = str(market.get("question", "") or "")
                outcomes = market.get("outcomes", "")
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except Exception:
                        outcomes = []
                outcome = parse_outcome(question, outcomes if isinstance(outcomes, list) else [], title)
                if not outcome:
                    # Keep old fallback behavior (scanner expected Up/Down label).
                    outcome = "Up"

                display_title = enhance_title(title, end_dt, duration_secs)[:120]
                runtime_market = RuntimeMarket(
                    condition_id=cond_id,
                    token_up=token_up,
                    token_down=token_down,
                    outcome=outcome,
                    event_title=display_title,
                    event_slug=event_slug,
                    asset=asset,
                    neg_risk=neg_risk,
                    market_start=market_start,
                    market_end=end_dt,
                    timeframe_seconds=duration_secs,
                )
                if cond_id not in self._tracked:
                    new_count += 1
                    new_markets.append(runtime_market)
                self._tracked[cond_id] = runtime_market
                self.ws.market_labels[cond_id] = display_title

        if new_count:
            self._stats["discovered_total"] = int(self._stats.get("discovered_total", 0)) + new_count
            self._stats["last_discovery_at"] = time.time()
            logger.info("[MARKET-RUNTIME] discovered %d market(s), tracking=%d", new_count, len(self._tracked))
            preview = []
            for m in sorted(new_markets, key=lambda x: (x.market_start or x.market_end, x.asset, x.timeframe_seconds)):
                tf = DURATION_TO_TF_LABEL.get(m.timeframe_seconds, str(m.timeframe_seconds))
                start_txt = m.market_start.isoformat() if m.market_start else "n/a"
                end_txt = m.market_end.isoformat()
                preview.append(f"{m.asset.upper()} {tf} {start_txt}->{end_txt}")
            if preview:
                shown = preview[:8]
                suffix = f" | +{len(preview) - len(shown)} more" if len(preview) > len(shown) else ""
                logger.info("[MARKET-RUNTIME] new windows: %s%s", " | ".join(shown), suffix)
            for m in new_markets:
                log_lifecycle(
                    logger=logger,
                    phase="discover",
                    condition_id=m.condition_id,
                    asset=m.asset,
                    timeframe_seconds=m.timeframe_seconds,
                    market_start=m.market_start,
                    market_end=m.market_end,
                    source="runtime",
                )

    def _monitor_window_active(self, market: RuntimeMarket, now: datetime) -> bool:
        if not self._monitor_cell_enabled(market.asset, market.timeframe_seconds):
            return False
        pre_start = max(0, int(getattr(self.config.monitor, "pre_start_capture_seconds", 15) or 15))
        post_end = max(0, int(getattr(self.config.monitor, "post_end_track_seconds", 120) or 120))
        sec_to_start = (market.market_start - now).total_seconds() if market.market_start else 0.0
        sec_to_end = (market.market_end - now).total_seconds()
        return sec_to_start <= pre_start and sec_to_end >= -post_end

    def _scanner_subscribe_window_active(self, market: RuntimeMarket, now: datetime) -> bool:
        if not self._scanner_cell_enabled(market.asset, market.timeframe_seconds):
            return False
        trend_enabled = bool(getattr(self.config.trend, "enabled", False))
        hft_active = (
            bool(getattr(self.config.trend, "hft_autobet_enabled", False))
            or bool(getattr(self.config.trend, "hft_barrier_enabled", False))
            or bool(getattr(self.config.trend, "hft_flash_enabled", False))
        )
        la_secs = max(0, int(getattr(self.config.scanner, "lookahead_seconds", 60) or 60))
        pre_end = int(self.config.scanner.get_pre_end_seconds(market.timeframe_seconds))
        la_ahead = (market.timeframe_seconds + la_secs) if (trend_enabled or hft_active) else 0
        subscribe_ahead = max(pre_end, la_ahead) + self._WS_SUBSCRIBE_BUFFER_SECONDS
        return market.market_end <= now + timedelta(seconds=subscribe_ahead)

    async def _sync_ws_subscriptions(self, now: datetime):
        desired: set[str] = set()
        desired_cids: set[str] = set()
        for market in self._tracked.values():
            if self._scanner_subscribe_window_active(market, now) or self._monitor_window_active(market, now):
                desired_cids.add(market.condition_id)
                if market.token_up:
                    desired.add(market.token_up)
                if market.token_down:
                    desired.add(market.token_down)

        new_tokens = [tid for tid in desired if tid and tid not in self.ws._subscribed]
        if new_tokens:
            await self.ws.subscribe(new_tokens)
        released_tokens = self._claimed_tokens - desired
        if released_tokens:
            await self.ws.unsubscribe(sorted(released_tokens))

        newly_subscribed_cids = desired_cids - self._ws_active_cids
        released_cids = self._ws_active_cids - desired_cids
        self._claimed_tokens = desired
        self._ws_active_cids = desired_cids

        if newly_subscribed_cids:
            for cid in sorted(newly_subscribed_cids):
                market = self._tracked.get(cid)
                if not market:
                    continue
                log_lifecycle(
                    logger=logger,
                    phase="subscribed",
                    condition_id=market.condition_id,
                    asset=market.asset,
                    timeframe_seconds=market.timeframe_seconds,
                    market_start=market.market_start,
                    market_end=market.market_end,
                    source="runtime",
                )

        if released_cids:
            for cid in sorted(released_cids):
                market = self._tracked.get(cid)
                if market:
                    asset = market.asset
                    timeframe_seconds = market.timeframe_seconds
                    market_start = market.market_start
                    market_end = market.market_end
                else:
                    # Market may have been pruned this cycle; still emit release phase.
                    asset = "unknown"
                    timeframe_seconds = 0
                    market_start = None
                    market_end = None
                log_lifecycle(
                    logger=logger,
                    phase="released",
                    condition_id=cid,
                    asset=asset,
                    timeframe_seconds=timeframe_seconds,
                    market_start=market_start,
                    market_end=market_end,
                    source="runtime",
                )
            logger.info("[MARKET-RUNTIME] released %d stale WS token claim(s)", len(released_cids))

    async def _ensure_ptb(self, now: datetime, *, inline_backfill: bool):
        if not self.price_feed:
            return
        if not self._tracked:
            return

        # Important: offset=0 is valid and must not fall back to 2.0.
        raw_offset = getattr(self.config.scanner, "ptb_capture_offset", 2.0)
        if raw_offset is None:
            raw_offset = 2.0
        offset_s = max(0.0, float(raw_offset))
        capture_delta = timedelta(seconds=offset_s)
        now_mono = time.monotonic()

        for market in self._tracked.values():
            if not market.market_start:
                continue
            if not self._cell_enabled_any(market.asset, market.timeframe_seconds):
                continue
            if now < (market.market_start - capture_delta):
                continue

            cid = market.condition_id
            if self.price_feed.get_price_to_beat(cid):
                self._ptb_retry_after.pop(cid, None)
                continue

            retry_at = self._ptb_retry_after.get(cid, 0.0)
            if retry_at > now_mono:
                continue

            sec_from_start = (now - market.market_start).total_seconds()
            label = f"{market.asset.upper()} {DURATION_TO_TF_LABEL.get(market.timeframe_seconds, market.timeframe_seconds)} {market.event_title}"[:120]

            if sec_from_start <= self._PTB_LIVE_GRACE_SECONDS:
                price = self.price_feed.capture_price_to_beat(
                    cid, market.asset, source="live", label=label
                )
                if price is not None:
                    self._ptb_retry_after.pop(cid, None)
                    self._stats["ptb_live_captures"] = int(self._stats.get("ptb_live_captures", 0)) + 1
                    log_lifecycle(
                        logger=logger,
                        phase="ptb_ready",
                        condition_id=market.condition_id,
                        asset=market.asset,
                        timeframe_seconds=market.timeframe_seconds,
                        market_start=market.market_start,
                        market_end=market.market_end,
                        source="live",
                    )
                else:
                    self._ptb_retry_after[cid] = now_mono + self._PTB_LIVE_RETRY_SECONDS
                continue

            # Backfill intentionally disabled for trading safety.
            # If we missed live PTB capture for this market (e.g. startup mid-cycle),
            # keep PTB empty and wait for the next fresh market cycle.
            self._ptb_retry_after.pop(cid, None)

    def _spawn_backfill_task(self, market: RuntimeMarket):
        cid = market.condition_id
        if cid in self._backfill_tasks and not self._backfill_tasks[cid].done():
            return

        async def _worker():
            await self._run_backfill(market)

        task = asyncio.create_task(_worker())
        self._backfill_tasks[cid] = task

        def _done(_task: asyncio.Task, _cid: str = cid):
            self._backfill_tasks.pop(_cid, None)

        task.add_done_callback(_done)

    async def _run_backfill(self, market: RuntimeMarket):
        cid = market.condition_id
        if cid in self._backfill_inflight:
            return
        if not market.market_start:
            return

        self._backfill_inflight.add(cid)
        now_mono = time.monotonic()
        try:
            async with self._backfill_sem:
                label = f"{market.asset.upper()} {DURATION_TO_TF_LABEL.get(market.timeframe_seconds, market.timeframe_seconds)} {market.event_title}"[:120]
                raw_offset = getattr(self.config.scanner, "ptb_capture_offset", 2.0)
                if raw_offset is None:
                    raw_offset = 2.0
                offset_ms = int(max(0.0, float(raw_offset)) * 1000.0)
                target_capture_ms = max(0, int(market.market_start.timestamp() * 1000) - offset_ms)
                price = await self.price_feed.backfill_price_to_beat(
                    cid,
                    market.asset,
                    target_capture_ms,
                    label=label,
                )
            if price is None:
                self._ptb_retry_after[cid] = now_mono + self._PTB_BACKFILL_RETRY_SECONDS
                self._stats["ptb_backfill_misses"] = int(self._stats.get("ptb_backfill_misses", 0)) + 1
            else:
                self._ptb_retry_after.pop(cid, None)
                self._stats["ptb_backfills"] = int(self._stats.get("ptb_backfills", 0)) + 1
                log_lifecycle(
                    logger=logger,
                    phase="ptb_ready",
                    condition_id=market.condition_id,
                    asset=market.asset,
                    timeframe_seconds=market.timeframe_seconds,
                    market_start=market.market_start,
                    market_end=market.market_end,
                    source="backfill",
                )
        except Exception as exc:
            logger.debug("[MARKET-RUNTIME] PTB backfill failed for %s: %s", cid[:16], exc)
            self._ptb_retry_after[cid] = now_mono + self._PTB_BACKFILL_RETRY_SECONDS
        finally:
            self._backfill_inflight.discard(cid)

    async def _prune(self, now: datetime):
        if not self._tracked:
            return
        prune_age = timedelta(seconds=self._prune_age_seconds())
        stale = [cid for cid, market in self._tracked.items() if market.market_end < (now - prune_age)]
        if not stale:
            return
        for cid in stale:
            self._tracked.pop(cid, None)
            self._ptb_retry_after.pop(cid, None)
            self._backfill_inflight.discard(cid)
            task = self._backfill_tasks.pop(cid, None)
            if task and not task.done():
                task.cancel()
            self.ws.market_labels.pop(cid, None)
        logger.debug("[MARKET-RUNTIME] pruned %d stale market(s)", len(stale))
        await self._sync_ws_subscriptions(now)
