"""Independent monitor worker for research-grade market sampling.

This worker is intentionally decoupled from trading execution and from
discovery/PTB ownership. It consumes canonical markets from MarketRuntime.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from src.api import PolymarketAPI
from src.config import Config
from src.market_runtime import MarketRuntime
from src.monitor_db import MonitorDB
from src.price_feed import BinancePriceFeed
from src.ws_client import MarketWSClient

logger = logging.getLogger(__name__)


@dataclass
class MonitorTrackedMarket:
    condition_id: str
    event_slug: str
    event_title: str
    asset: str
    timeframe_seconds: int
    token_up: str
    token_down: str
    market_start: datetime | None
    market_end: datetime


class MonitorWorker:
    """Collect monitor_v2 samples independently of trading scan loop."""

    _LOOP_SLEEP_SECONDS = 0.2
    _WINNER_SWEEP_SECONDS = 60
    _REST_FALLBACK_CONCURRENCY = 8
    _ASK_LADDER_LEVELS = 24

    def __init__(
        self,
        *,
        config: Config,
        api: PolymarketAPI,
        ws: MarketWSClient,
        price_feed: BinancePriceFeed,
        monitor_db: MonitorDB,
        market_runtime: MarketRuntime,
    ):
        self.config = config
        self.api = api
        self.ws = ws
        self.price_feed = price_feed
        self.monitor_db = monitor_db
        self.market_runtime = market_runtime

        self._running = False
        self._task: asyncio.Task | None = None
        self._rest_sem = asyncio.Semaphore(self._REST_FALLBACK_CONCURRENCY)

        self._tracked: dict[str, MonitorTrackedMarket] = {}
        self._last_sample_ts: dict[str, float] = {}
        self._last_winner_sweep_ts = 0.0

        self._stats = {
            "discovered_total": 0,
            "samples_written": 0,
            "last_sample_at": 0.0,
        }

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("[MONITOR V2] worker started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("[MONITOR V2] worker stopped")

    def subscribed_tokens(self) -> set[str]:
        # Runtime owns WS claim lifecycle.
        return self.market_runtime.subscribed_tokens() if self.market_runtime else set()

    def get_stats(self) -> dict:
        now = datetime.now(timezone.utc)
        pre_start = max(0, int(getattr(self.config.monitor, "pre_start_capture_seconds", 15) or 15))
        post_end = max(0, int(getattr(self.config.monitor, "post_end_track_seconds", 120) or 120))
        active = 0
        for market in self._tracked.values():
            sec_to_start = (market.market_start - now).total_seconds() if market.market_start else 0.0
            sec_to_end = (market.market_end - now).total_seconds()
            if sec_to_start <= pre_start and sec_to_end >= -post_end:
                active += 1
        return {
            "running": self._running,
            "enabled": bool(getattr(self.config.monitor, "enabled", True)),
            "tracked_markets": len(self._tracked),
            "active_markets": active,
            "ws_tokens": len(self.subscribed_tokens()),
            "discovered_total": int(self._stats.get("discovered_total", 0)),
            "samples_written": int(self._stats.get("samples_written", 0)),
            "last_sample_at": float(self._stats.get("last_sample_at", 0.0) or 0.0),
        }

    async def _run_loop(self):
        while self._running:
            started = time.monotonic()
            try:
                if bool(getattr(self.config.monitor, "enabled", True)):
                    await self._refresh_tracked_from_runtime()
                    await self._sample_due_markets()
                    await self._winner_sweep_if_due()
                else:
                    if self._tracked:
                        self._tracked.clear()
                        self._last_sample_ts.clear()
                        logger.info("[MONITOR V2] monitor disabled; cleared tracked state")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("[MONITOR V2] worker cycle error: %s", exc)

            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.01, self._LOOP_SLEEP_SECONDS - elapsed))

    async def _refresh_tracked_from_runtime(self):
        incoming = {m.condition_id: m for m in self.market_runtime.monitor_markets()}
        old_ids = set(self._tracked.keys())
        new_ids = set(incoming.keys())

        stale_ids = [cid for cid in old_ids if cid not in new_ids]
        for cid in stale_ids:
            self._tracked.pop(cid, None)
            self._last_sample_ts.pop(cid, None)
        if stale_ids:
            logger.debug("[MONITOR V2] dropped %d runtime-removed market(s)", len(stale_ids))

        new_count = 0
        for cid, rm in incoming.items():
            next_market = MonitorTrackedMarket(
                condition_id=rm.condition_id,
                event_slug=rm.event_slug,
                event_title=rm.event_title,
                asset=rm.asset,
                timeframe_seconds=rm.timeframe_seconds,
                token_up=rm.token_up,
                token_down=rm.token_down,
                market_start=rm.market_start,
                market_end=rm.market_end,
            )
            existing = self._tracked.get(cid)
            changed = (
                existing is None
                or existing.event_slug != next_market.event_slug
                or existing.event_title != next_market.event_title
                or existing.asset != next_market.asset
                or existing.timeframe_seconds != next_market.timeframe_seconds
                or existing.token_up != next_market.token_up
                or existing.token_down != next_market.token_down
                or existing.market_start != next_market.market_start
                or existing.market_end != next_market.market_end
            )
            if existing is None:
                new_count += 1
            self._tracked[cid] = next_market

            if changed:
                await self.monitor_db.upsert_v2_market(
                    condition_id=next_market.condition_id,
                    event_slug=next_market.event_slug,
                    event_title=next_market.event_title,
                    asset=next_market.asset,
                    timeframe_seconds=next_market.timeframe_seconds,
                    token_up=next_market.token_up,
                    token_down=next_market.token_down,
                    market_start=next_market.market_start.isoformat() if next_market.market_start else "",
                    market_end=next_market.market_end.isoformat(),
                )

        if new_count:
            self._stats["discovered_total"] = int(self._stats.get("discovered_total", 0)) + new_count
            logger.info("[MONITOR V2] synced %d new runtime market(s), tracking=%d", new_count, len(self._tracked))

    async def _winner_sweep_if_due(self):
        now_ts = time.monotonic()
        if (now_ts - self._last_winner_sweep_ts) < self._WINNER_SWEEP_SECONDS:
            return
        self._last_winner_sweep_ts = now_ts
        try:
            await self.monitor_db.sweep_v2_winners(self.api)
        except Exception as exc:
            logger.debug("[MONITOR V2] winner sweep error: %s", exc)

    def _barrier_for(self, timeframe_seconds: int) -> float:
        trend = getattr(self.config, "trend", None)
        if not trend:
            return 0.0
        try:
            return float(trend.get_hft_barrier_pct(int(timeframe_seconds)))
        except Exception:
            return float(getattr(trend, "hft_barrier_pct", 0.0) or 0.0)

    async def _sample_due_markets(self):
        if not self._tracked:
            return
        now = datetime.now(timezone.utc)
        pre_start = max(0, int(getattr(self.config.monitor, "pre_start_capture_seconds", 15) or 15))
        post_end = max(0, int(getattr(self.config.monitor, "post_end_track_seconds", 120) or 120))

        due: list[tuple[MonitorTrackedMarket, float, float]] = []
        for market in self._tracked.values():
            sec_to_start = (market.market_start - now).total_seconds() if market.market_start else 0.0
            sec_to_end = (market.market_end - now).total_seconds()
            if not (sec_to_start <= pre_start and sec_to_end >= -post_end):
                continue
            interval = max(1, int(self.config.monitor.sample_interval_for(market.timeframe_seconds)))
            last = self._last_sample_ts.get(market.condition_id, 0.0)
            now_ts = time.monotonic()
            if (now_ts - last) < interval:
                continue
            self._last_sample_ts[market.condition_id] = now_ts
            due.append((market, sec_to_start, sec_to_end))

        if not due:
            return
        await asyncio.gather(
            *(self._sample_market(m, sec_to_start, sec_to_end, now) for (m, sec_to_start, sec_to_end) in due),
            return_exceptions=True,
        )

    async def _read_book(self, token_id: str) -> dict:
        ws_book = self.ws.get_book(token_id)
        if ws_book:
            return {
                "best_bid": float(ws_book.get("best_bid", 0) or 0),
                "best_ask": float(ws_book.get("best_ask", 0) or 0),
                "midpoint": float(ws_book.get("midpoint", 0) or 0),
                "bid_depth": float(ws_book.get("bid_depth", 0) or 0),
                "ask_depth": float(ws_book.get("ask_depth", 0) or 0),
                "spread_pct": float(ws_book.get("spread_pct", 0) or 0),
                "asks": list(ws_book.get("asks", []) or []),
            }
        async with self._rest_sem:
            liq = await self.api.get_book_liquidity(token_id)
            return {
                "best_bid": float(liq.get("best_bid", 0) or 0),
                "best_ask": float(liq.get("best_ask", 0) or 0),
                "midpoint": float(liq.get("midpoint", 0) or 0),
                "bid_depth": float(liq.get("bid_depth", 0) or 0),
                "ask_depth": float(liq.get("ask_depth", 0) or 0),
                "spread_pct": float(liq.get("spread_pct", 0) or 0),
                "asks": list(liq.get("asks", []) or []),
            }

    @classmethod
    def _compact_asks_json(cls, asks: list) -> str:
        compact: list[list[float]] = []
        for row in asks[: cls._ASK_LADDER_LEVELS]:
            try:
                p = float((row or {}).get("price", 0) or 0)
                s = float((row or {}).get("size", 0) or 0)
            except Exception:
                continue
            if p > 0 and s > 0:
                compact.append([round(p, 6), round(s, 6)])
        return json.dumps(compact, separators=(",", ":"))

    async def _sample_market(
        self,
        market: MonitorTrackedMarket,
        sec_to_start: float,
        sec_to_end: float,
        now_utc: datetime,
    ):
        try:
            up_book, down_book = await asyncio.gather(
                self._read_book(market.token_up),
                self._read_book(market.token_down),
            )

            gap_info = self.price_feed.compute_gap(market.condition_id) if self.price_feed else None
            current_price = (
                float(gap_info.get("current_price")) if gap_info and gap_info.get("current_price") is not None
                else self.price_feed.get_price(market.asset)
            )
            price_to_beat = (
                float(gap_info.get("price_to_beat")) if gap_info and gap_info.get("price_to_beat") is not None
                else None
            )
            gap_pct = float(gap_info.get("gap_pct")) if gap_info and gap_info.get("gap_pct") is not None else None
            gap_abs = (
                float(gap_info.get("current_price")) - float(gap_info.get("price_to_beat"))
                if gap_info and gap_info.get("current_price") is not None and gap_info.get("price_to_beat") is not None
                else None
            )
            gap_direction = str(gap_info.get("direction")) if gap_info and gap_info.get("direction") else None
            winning_side = str(gap_info.get("winning_side")) if gap_info and gap_info.get("winning_side") else None

            barrier_pct = self._barrier_for(market.timeframe_seconds)
            barrier_delta_pct = (gap_pct - barrier_pct) if gap_pct is not None else None
            arm_like = bool(barrier_delta_pct is not None and barrier_delta_pct >= 0)

            await self.monitor_db.record_v2_sample(
                condition_id=market.condition_id,
                event_slug=market.event_slug,
                event_title=market.event_title,
                asset=market.asset,
                timeframe_seconds=market.timeframe_seconds,
                token_up=market.token_up,
                token_down=market.token_down,
                market_start=market.market_start.isoformat() if market.market_start else "",
                market_end=market.market_end.isoformat(),
                sec_to_start=float(sec_to_start),
                sec_to_end=float(sec_to_end),
                up_bid=float(up_book.get("best_bid", 0) or 0),
                up_ask=float(up_book.get("best_ask", 0) or 0),
                up_mid=float(up_book.get("midpoint", 0) or 0),
                up_depth=float(up_book.get("ask_depth", 0) or 0),
                up_asks_json=self._compact_asks_json(list(up_book.get("asks", []) or [])),
                down_bid=float(down_book.get("best_bid", 0) or 0),
                down_ask=float(down_book.get("best_ask", 0) or 0),
                down_mid=float(down_book.get("midpoint", 0) or 0),
                down_depth=float(down_book.get("ask_depth", 0) or 0),
                down_asks_json=self._compact_asks_json(list(down_book.get("asks", []) or [])),
                spread_up=float(up_book.get("spread_pct", 0) or 0),
                spread_down=float(down_book.get("spread_pct", 0) or 0),
                current_price=current_price,
                price_to_beat=price_to_beat,
                gap_pct=gap_pct,
                gap_abs=gap_abs,
                gap_direction=gap_direction,
                winning_side=winning_side,
                barrier_pct=barrier_pct,
                barrier_delta_pct=barrier_delta_pct,
                arm_like=arm_like,
            )
            self._stats["samples_written"] = int(self._stats.get("samples_written", 0)) + 1
            self._stats["last_sample_at"] = time.time()
        except Exception as exc:
            logger.debug("[MONITOR V2] sample error (%s): %s", market.condition_id[:16], exc)
