"""Polymarket WebSocket client -- real-time orderbook + resolution events for sniper.

Connects to the CLOB market channel for live book/price data and resolution
detection.  When a tracked market's winning side drops below max_buy_price,
fires a snipe callback immediately -- no polling delay.

Endpoints:
  wss://ws-subscriptions-clob.polymarket.com/ws/market (public, no auth)
  Events: book, price_change, last_trade_price, best_bid_ask, market_resolved, new_market
"""

import asyncio
import json
import logging
import re
import time
from typing import Callable, Optional

import aiohttp

logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 60.0
_BACKOFF_MULTIPLIER = 2.0


def _calc_spread(bid: float, ask: float) -> tuple[float, float]:
    """Compute (spread_pct, midpoint) from best bid/ask."""
    if bid > 0 and ask > 0:
        mid = (bid + ask) / 2
        spread = round((ask - bid) / mid, 4) if mid > 0 else 1.0
    else:
        mid = bid or ask or 0
        spread = 1.0
    return spread, mid


class MarketWSClient:
    """Live orderbook feed for the sniper.

    Provides:
      - Real-time book cache (best bid/ask/spread/depth per token)
      - Resolution event callbacks (instant redemption trigger)
      - New market notifications (auto-discover new Up/Down markets)
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Live book cache: token_id -> {best_bid, best_ask, bids, asks, spread_pct, midpoint, bid_depth, ask_depth, updated_at}
        self.book_cache: dict[str, dict] = {}

        # Subscribed token IDs
        self._subscribed: set[str] = set()

        # condition_id -> human-readable label (populated by scanner)
        self.market_labels: dict[str, str] = {}

        # Callbacks
        self._on_resolved: Optional[Callable] = None   # (condition_id, winning_outcome) -> coro
        self._on_new_market: Optional[Callable] = None  # (data) -> coro
        self._on_price_update: Optional[Callable] = None  # (token_id) -> coro -- fires on EVERY book/price update

        # Stats
        self._messages_received = 0
        self._last_message_at = 0.0
        self._connected_since: Optional[float] = None

    def set_resolution_callback(self, callback: Callable):
        """Register async callback for market_resolved events."""
        self._on_resolved = callback

    def set_new_market_callback(self, callback: Callable):
        """Register async callback for new_market events (discover new Up/Down markets)."""
        self._on_new_market = callback

    def set_price_callback(self, callback: Callable):
        """Register async callback for real-time price updates.

        Called on every price_change / book / best_bid_ask event with
        (token_id: str) so the scanner can react sub-second.
        """
        self._on_price_update = callback

    async def start(self):
        """Start the websocket connection in a background task."""
        if self._running:
            return
        self._running = True
        self._session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._run_forever())
        logger.info("[FEED: WS] starting -- %s", WS_URL)

    async def stop(self):
        """Gracefully close the websocket connection."""
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
        logger.info("[FEED: WS] stopped (%d messages total)", self._messages_received)

    async def subscribe(self, token_ids: list[str]):
        """Subscribe to orderbook updates for the given token IDs."""
        new_ids = [tid for tid in token_ids if tid and tid not in self._subscribed]
        if not new_ids:
            return

        self._subscribed.update(new_ids)

        if self._ws and not self._ws.closed:
            try:
                msg = json.dumps({
                    "assets_ids": new_ids,
                    "operation": "subscribe",
                    "type": "market",
                    "custom_feature_enabled": True,
                })
                await self._ws.send_str(msg)
                logger.info("[FEED: WS] subscribed to %d token(s) (total: %d)",
                            len(new_ids), len(self._subscribed))
            except Exception as e:
                logger.warning("[FEED: WS] subscribe failed: %s", e)

    async def unsubscribe(self, token_ids: list[str]):
        """Unsubscribe from token IDs."""
        ids_to_remove = [tid for tid in token_ids if tid in self._subscribed]
        if not ids_to_remove:
            return
        self._subscribed -= set(ids_to_remove)
        sent_to_ws = False
        if self._ws and not self._ws.closed:
            try:
                await self._ws.send_str(json.dumps({
                    "assets_ids": ids_to_remove,
                    "operation": "unsubscribe",
                }))
                sent_to_ws = True
            except Exception as exc:
                logger.warning("[FEED: WS] unsubscribe failed: %s", exc)
        for tid in ids_to_remove:
            self.book_cache.pop(tid, None)
        logger.info(
            "[FEED: WS] unsubscribed from %d token(s) (total: %d, sent=%s)",
            len(ids_to_remove),
            len(self._subscribed),
            sent_to_ws,
        )

    def get_book(self, token_id: str) -> Optional[dict]:
        """Get cached book data. Returns None if not subscribed or stale (>30s)."""
        entry = self.book_cache.get(token_id)
        if not entry:
            return None
        if time.time() - entry.get("updated_at", 0) > 30:
            return None
        return entry

    def get_stats(self) -> dict:
        return {
            "connected": self._ws is not None and not self._ws.closed,
            "subscribed_tokens": len(self._subscribed),
            "cached_books": len(self.book_cache),
            "messages_received": self._messages_received,
            "last_message_age": round(time.time() - self._last_message_at, 1) if self._last_message_at else None,
            "connected_since": self._connected_since,
        }

    # -- Internal --

    async def _run_forever(self):
        """Reconnecting websocket loop."""
        backoff = _INITIAL_BACKOFF
        while self._running:
            try:
                async with self._session.ws_connect(
                    WS_URL, heartbeat=20,
                    timeout=aiohttp.ClientWSTimeout(ws_close=10),
                ) as ws:
                    self._ws = ws
                    self._connected_since = time.time()
                    backoff = _INITIAL_BACKOFF
                    logger.info("[FEED: WS] connected to market channel")

                    # Re-subscribe on reconnect
                    if self._subscribed:
                        await ws.send_str(json.dumps({
                            "assets_ids": list(self._subscribed),
                            "operation": "subscribe",
                            "type": "market",
                            "custom_feature_enabled": True,
                        }))
                        logger.info("[FEED: WS] re-subscribed to %d token(s)", len(self._subscribed))

                    async for msg in ws:
                        if not self._running:
                            break
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            await self._handle_message(msg.data)
                        elif msg.type == aiohttp.WSMsgType.PING:
                            await ws.pong(msg.data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("[FEED: WS] closed/error: %s", msg.type)
                            break

            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[FEED: WS] error: %s -- reconnecting in %.0fs", str(e)[:100], backoff)

            self._ws = None
            self._connected_since = None
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * _BACKOFF_MULTIPLIER, _MAX_BACKOFF)

    async def _handle_message(self, raw: str):
        self._messages_received += 1
        self._last_message_at = time.time()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    await self._dispatch_event(item)
        elif isinstance(data, dict):
            await self._dispatch_event(data)

    async def _dispatch_event(self, data: dict):
        event_type = data.get("event_type", "")
        if event_type == "book":
            await self._handle_book(data)
        elif event_type == "price_change":
            await self._handle_price_change(data)
        elif event_type == "best_bid_ask":
            await self._handle_best_bid_ask(data)
        elif event_type == "market_resolved":
            await self._handle_resolved(data)
        elif event_type == "new_market":
            await self._handle_new_market(data)

    async def _handle_book(self, data: dict):
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        bid_depth = sum(float(b.get("price", 0)) * float(b.get("size", 0)) for b in bids)
        ask_depth = sum(float(a.get("price", 0)) * float(a.get("size", 0)) for a in asks)
        best_bid = max((float(b["price"]) for b in bids), default=0)
        best_ask = min((float(a["price"]) for a in asks), default=0)
        spread, mid = _calc_spread(best_bid, best_ask)
        self.book_cache[asset_id] = {
            "bid_depth": round(bid_depth, 2), "ask_depth": round(ask_depth, 2),
            "best_bid": best_bid, "best_ask": best_ask,
            "bids": bids, "asks": asks,
            "spread_pct": spread, "midpoint": mid,
            "updated_at": time.time(),
        }
        await self._fire_price_callback(asset_id)

    async def _handle_price_change(self, data: dict):
        for pc in data.get("price_changes", []):
            asset_id = pc.get("asset_id", "")
            if not asset_id:
                continue
            bb = float(pc["best_bid"]) if pc.get("best_bid") else None
            ba = float(pc["best_ask"]) if pc.get("best_ask") else None
            if asset_id in self.book_cache:
                entry = self.book_cache[asset_id]
                if bb is not None and bb > 0:
                    entry["best_bid"] = bb
                if ba is not None and ba > 0:
                    entry["best_ask"] = ba
                spread, mid = _calc_spread(entry["best_bid"], entry["best_ask"])
                entry["spread_pct"] = spread
                entry["midpoint"] = mid
                entry["updated_at"] = time.time()
            elif bb or ba:
                spread, mid = _calc_spread(bb or 0, ba or 0)
                self.book_cache[asset_id] = {
                    "bid_depth": 0, "ask_depth": 0,
                    "best_bid": bb or 0, "best_ask": ba or 0,
                    "spread_pct": spread, "midpoint": mid,
                    "updated_at": time.time(),
                }
            await self._fire_price_callback(asset_id)

    async def _handle_best_bid_ask(self, data: dict):
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        bb = float(data.get("best_bid", 0))
        ba = float(data.get("best_ask", 0))
        spread, mid = _calc_spread(bb, ba)
        if asset_id in self.book_cache:
            entry = self.book_cache[asset_id]
            entry.update({"best_bid": bb, "best_ask": ba,
                          "spread_pct": spread, "midpoint": mid,
                          "updated_at": time.time()})
        else:
            self.book_cache[asset_id] = {
                "bid_depth": 0, "ask_depth": 0,
                "best_bid": bb, "best_ask": ba,
                "bids": [], "asks": [],
                "spread_pct": spread, "midpoint": mid,
                "updated_at": time.time(),
            }
        await self._fire_price_callback(asset_id)

    async def _fire_price_callback(self, token_id: str):
        """Fire the price-update callback without blocking the WS reader."""
        if self._on_price_update and token_id:
            try:
                # Run in background so WS reader isn't blocked by snipe execution
                task = asyncio.create_task(self._on_price_update(token_id))
                task.add_done_callback(self._on_task_done)
            except Exception:
                pass

    @staticmethod
    def _on_task_done(task: asyncio.Task):
        """Log any unhandled exceptions from fire-and-forget WS callbacks."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error("[FEED: WS] price callback failed: %s", exc, exc_info=exc)

    async def _handle_resolved(self, data: dict):
        condition_id = data.get("market", "")
        winning = data.get("winning_outcome", "")
        label = self.market_labels.get(condition_id, "")
        if label:
            logger.info("[FEED: WS] Market resolved -- %s -> (%s) - %s", condition_id[:16], label, winning)
        else:
            logger.info("[FEED: WS] Market resolved -- %s -> %s", condition_id[:16], winning)
        if self._on_resolved and condition_id:
            try:
                await self._on_resolved(condition_id, winning)
            except Exception as e:
                logger.error("[FEED: WS] Resolution callback error: %s", e)

    # Pattern to identify crypto Up/Down markets (filter out esports, etc.)
    _CRYPTO_MARKET_RE = re.compile(r"(?:bitcoin|ethereum|solana|xrp|btc|eth|sol).*up\s+or\s+down", re.IGNORECASE)

    async def _handle_new_market(self, data: dict):
        question = data.get("question", "")[:80]
        market_title = data.get("market", "") or data.get("description", "") or question

        # Only log/process markets matching our crypto Up/Down pattern.
        # Polymarket WS broadcasts new_market for ALL markets (esports, politics,
        # etc.) -- we filter here to avoid polluting logs with irrelevant events.
        if not self._CRYPTO_MARKET_RE.search(question) and not self._CRYPTO_MARKET_RE.search(market_title):
            return  # Not a crypto Up/Down market -- ignore silently

        logger.debug("[FEED: WS] New market -- %s", question)
        if self._on_new_market:
            try:
                await self._on_new_market(data)
            except Exception:
                pass
    def get_depth_up_to(self, token_id: str, max_price: float) -> float:
        """Calculate cumulative USDC depth available at or below max_price."""
        entry = self.get_book(token_id)
        if not entry or not entry.get("asks"):
            return 0.0
        
        depth = 0.0
        for ask in entry["asks"]:
            p = float(ask.get("price") or 0)
            s = float(ask.get("size") or 0)
            if 0 < p <= max_price:
                depth += p * s
        return round(depth, 2)
