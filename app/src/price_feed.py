"""Dual-source price feed with runtime-selectable primary/fallback.

Sources:
  - Chainlink RTDS via Polymarket
  - Binance combined WebSocket stream

Architecture:
  - Chainlink RTDS: wss://ws-live-data.polymarket.com
  - Binance WS: wss://stream.binance.com:9443/stream?streams=...
  - Binance REST klines: https://api.binance.com/api/v3/klines
  - get_price() uses configured primary source with automatic fallback
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# -- Asset <-> Binance symbol mapping --
ASSET_SYMBOL_MAP: dict[str, str] = {
    "bitcoin":  "btcusdt",
    "ethereum": "ethusdt",
    "solana":   "solusdt",
    "xrp":      "xrpusdt",
}

# Reverse lookup: "btcusdt" -> "bitcoin"
_SYMBOL_ASSET_MAP: dict[str, str] = {v: k for k, v in ASSET_SYMBOL_MAP.items()}

# -- Chainlink RTDS symbol mapping --
# RTDS uses slash-separated lowercase pairs, e.g. "btc/usd"
ASSET_CHAINLINK_MAP: dict[str, str] = {
    "bitcoin":  "btc/usd",
    "ethereum": "eth/usd",
    "solana":   "sol/usd",
    "xrp":      "xrp/usd",
}
_CHAINLINK_ASSET_MAP: dict[str, str] = {v: k for k, v in ASSET_CHAINLINK_MAP.items()}

# -- WS config --
_BASE_WS = "wss://stream.binance.com:9443/stream"
_REST_BASE = "https://api.binance.com/api/v3"
_RTDS_WS = "wss://ws-live-data.polymarket.com"
_RTDS_PING_INTERVAL = 5.0  # Polymarket recommends ping every 5s

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 30.0           # General cap (Binance WS)
_MAX_BACKOFF_RTDS = 30.0      # RTDS cap (10s floor → 20s → 30s ceiling)
_BACKOFF_MULTIPLIER = 2.0
_BINANCE_STALE_SECONDS = 30.0
_CHAINLINK_STALE_SECONDS = 15.0
_VALID_PRIMARY_SOURCES = {"binance", "chainlink"}


class BinancePriceFeed:
    """Dual-source price feed with runtime-selectable primary/fallback.

    Usage:
        feed = BinancePriceFeed(
            assets=["bitcoin", "ethereum", "solana", "xrp"],
            primary_source="binance",
        )
        await feed.start()
        price = feed.get_price("bitcoin")
        snap  = feed.get_snapshot("bitcoin")     # -> {price, bid, ask, volume_24h, updated_at}
        await feed.stop()
    """

    def __init__(self, assets: Optional[list[str]] = None, primary_source: str = "binance"):
        self._assets = assets or list(ASSET_SYMBOL_MAP.keys())
        self._symbols = [ASSET_SYMBOL_MAP[a] for a in self._assets if a in ASSET_SYMBOL_MAP]
        self._primary_source = self._normalize_primary_source(primary_source)

        # Binance live price cache: asset -> {price, bid, ask, volume_24h, updated_at}
        self._cache: dict[str, dict] = {}

        # Chainlink live price cache: asset -> {price, updated_at}
        self._chainlink_cache: dict[str, dict] = {}

        # Price-to-beat snapshots: (condition_id) -> {asset, price, captured_at, source}
        self._price_to_beat: dict[str, dict] = {}

        # PTB entries older than this are pruned on save/load (25 hours — covers 1d markets)
        self.PTB_TTL_SECONDS = 25 * 3600

        # DB handle for persisting PTB across restarts (set via set_db())
        self._db_conn = None

        # Binance connection state
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._connected = False

        # Chainlink RTDS connection state
        self._rtds_session: Optional[aiohttp.ClientSession] = None
        self._rtds_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._rtds_task: Optional[asyncio.Task] = None
        self._rtds_connected = False
        self._rtds_ticks = 0
        self._rtds_last_tick_at = 0.0
        self._rtds_reconnect_count = 0

        # Stats
        self._ticks_received = 0
        self._last_tick_at = 0.0
        self._reconnect_count = 0
        self._stale_warned: set[str] = set()  # State-transition dedup for stale warnings (asset:source)
        self._fallback_warned: set[str] = set()  # State-transition dedup for fallback warnings (asset:source)

        # External price-update callbacks (e.g. TrendAnalyzer.update_price)
        self._on_price_callbacks: list = []

    @staticmethod
    def _normalize_primary_source(source: str) -> str:
        src = str(source or "").strip().lower()
        return src if src in _VALID_PRIMARY_SOURCES else "binance"

    @staticmethod
    def _source_label(source: str) -> str:
        return "Chainlink" if source == "chainlink" else "Binance"

    @staticmethod
    def _source_stale_seconds(source: str) -> float:
        return _CHAINLINK_STALE_SECONDS if source == "chainlink" else _BINANCE_STALE_SECONDS

    def _source_entry(self, asset: str, source: str) -> Optional[dict]:
        if source == "chainlink":
            return self._chainlink_cache.get(asset)
        if source == "binance":
            return self._cache.get(asset)
        return None

    def _source_age(self, asset: str, source: str) -> Optional[float]:
        entry = self._source_entry(asset, source)
        if not entry:
            return None
        try:
            return time.time() - float(entry.get("updated_at", 0.0))
        except Exception:
            return None

    def _fresh_source_entry(self, asset: str, source: str) -> Optional[dict]:
        entry = self._source_entry(asset, source)
        if not entry:
            return None
        age = self._source_age(asset, source)
        if age is None:
            return None
        if age <= self._source_stale_seconds(source):
            return entry
        return None

    @staticmethod
    def _fallback_source(source: str) -> str:
        return "chainlink" if source == "binance" else "binance"

    @staticmethod
    def _warn_key(asset: str, source: str) -> str:
        return f"{asset}:{source}"

    def get_primary_source(self) -> str:
        return self._primary_source

    def set_primary_source(self, source: str) -> str:
        """Hot-switch active primary source ("binance" or "chainlink")."""
        normalized = self._normalize_primary_source(source)
        if normalized == self._primary_source:
            return self._primary_source
        old = self._primary_source
        self._primary_source = normalized
        # Reset state-transition warnings after source-order flips.
        self._fallback_warned.clear()
        self._stale_warned.clear()
        logger.info(
            "[FEED: SOURCE] Primary switched: %s -> %s (fallback=%s)",
            self._source_label(old),
            self._source_label(self._primary_source),
            self._source_label(self._fallback_source(self._primary_source)),
        )
        return self._primary_source

    def get_price(self, asset: str) -> Optional[float]:
        """Get the current price for an asset.

        Uses configured primary source first, then falls back.
        Returns None if neither source has data.
        """
        primary = self._primary_source
        secondary = self._fallback_source(primary)
        p_key = self._warn_key(asset, primary)
        s_key = self._warn_key(asset, secondary)

        primary_entry = self._fresh_source_entry(asset, primary)
        if primary_entry:
            if p_key in self._fallback_warned:
                self._fallback_warned.discard(p_key)
            if p_key in self._stale_warned:
                self._stale_warned.discard(p_key)
                logger.info("%s price recovered for %s", self._source_label(primary), asset)
            return primary_entry["price"]

        if p_key not in self._fallback_warned:
            self._fallback_warned.add(p_key)
            age = self._source_age(asset, primary)
            age_str = f", last tick {age:.0f}s ago" if age is not None else ", no data"
            logger.warning(
                "[FEED: ORACLE FALLBACK] %s stale for %s%s -- falling back to %s",
                self._source_label(primary),
                asset,
                age_str,
                self._source_label(secondary),
            )

        secondary_entry = self._fresh_source_entry(asset, secondary)
        if secondary_entry:
            if s_key in self._stale_warned:
                self._stale_warned.discard(s_key)
                logger.info("%s price recovered for %s", self._source_label(secondary), asset)
            return secondary_entry["price"]

        if s_key not in self._stale_warned:
            self._stale_warned.add(s_key)
            sec_age = self._source_age(asset, secondary)
            if sec_age is None:
                logger.warning(
                    "%s unavailable for %s -- gap gate will pass-through until recovery",
                    self._source_label(secondary),
                    asset,
                )
            else:
                logger.warning(
                    "%s price stale for %s (%.1fs old) -- gap gate will pass-through until recovery",
                    self._source_label(secondary),
                    asset,
                    sec_age,
                )
        return None

    def get_snapshot(self, asset: str) -> Optional[dict]:
        """Get full price snapshot for an asset."""
        return self._cache.get(asset)

    def get_price_source(self, asset: str) -> str:
        """Return which feed is currently providing the price for an asset.

        Returns "binance", "chainlink", or "none".
        """
        primary = self._primary_source
        secondary = self._fallback_source(primary)
        if self._fresh_source_entry(asset, primary):
            return primary
        if self._fresh_source_entry(asset, secondary):
            return secondary
        return "none"

    def get_price_to_beat(self, condition_id: str) -> Optional[dict]:
        """Get the captured price_to_beat for a specific market."""
        return self._price_to_beat.get(condition_id)

    def set_db(self, db_conn):
        """Provide an aiosqlite connection for PTB persistence."""
        self._db_conn = db_conn

    def _prune_ptb(self) -> int:
        """Remove PTB entries older than PTB_TTL_SECONDS. Returns count removed."""
        now = datetime.now(timezone.utc)
        stale = []
        for cid, entry in self._price_to_beat.items():
            captured = entry.get("captured_at", "")
            try:
                ts = datetime.fromisoformat(captured.replace("Z", "+00:00"))
                age = (now - ts).total_seconds()
                if age > self.PTB_TTL_SECONDS:
                    stale.append(cid)
            except (ValueError, TypeError):
                stale.append(cid)  # unparseable timestamp — prune it
        for cid in stale:
            del self._price_to_beat[cid]
        if stale:
            logger.info("[FEED: PTB PRUNED] %d entries (TTL %ds)", len(stale), self.PTB_TTL_SECONDS)
        return len(stale)

    async def _save_ptb_to_db(self):
        """Persist all PTB entries to bot_state as JSON (prunes stale first)."""
        if not self._db_conn:
            return
        try:
            self._prune_ptb()
            blob = json.dumps(self._price_to_beat)
            await self._db_conn.execute(
                "INSERT OR REPLACE INTO bot_state (key, value, updated_at) "
                "VALUES ('ptb_cache', ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
                (blob,),
            )
            await self._db_conn.commit()
        except Exception as e:
            logger.debug("[FEED: PTB] DB save failed: %s", e)

    async def load_ptb_from_db(self):
        """Restore PTB cache from DB on startup (prunes stale). Call after set_db()."""
        if not self._db_conn:
            return 0
        try:
            row = await self._db_conn.execute(
                "SELECT value FROM bot_state WHERE key='ptb_cache'"
            )
            row = await row.fetchone()
            if row:
                saved = json.loads(row[0])
                self._price_to_beat.update(saved)
                pruned = self._prune_ptb()
                kept = len(saved) - pruned
                logger.info("Restored %d PTB entries from DB (%d pruned as stale)", kept, pruned)
                return kept
        except Exception as e:
            logger.warning("[FEED: PTB] DB load failed: %s", e)
        return 0

    def capture_price_to_beat(self, condition_id: str, asset: str,
                               source: str = "live",
                               label: str = "") -> Optional[float]:
        """Snapshot the current price as the price_to_beat for a market.

        Uses configured primary source (with fallback).
        NEVER overwrites a backfill PTB — backfill has the exact historical
        price at market start, which is always more accurate than "right now."

        Args:
            condition_id: Polymarket market condition ID
            asset: Asset name ("bitcoin", "ethereum", etc.)
            source: "live" (from WS feed) or "backfill" (from REST klines)
            label: Human-readable market label for logging (e.g. "BTC 5m 4:15-4:20PM")

        Returns:
            The captured price, or None if no price available.
        """
        # Never overwrite a backfill PTB with a live snapshot.
        # Backfill has the exact candle close at market start time.
        existing = self._price_to_beat.get(condition_id)
        if existing and existing.get("source") == "backfill" and source == "live":
            return existing.get("price")  # Return existing without overwriting

        price = self.get_price(asset)
        if price is None:
            logger.warning("Cannot capture price_to_beat for %s -- no price data", asset)
            return None

        oracle = self.get_price_source(asset)

        # Snapshot BOTH oracle prices at capture time for consensus checks.
        # Each oracle should be compared against its own baseline, not the other's.
        cl_entry = self._chainlink_cache.get(asset)
        cl_price = (
            cl_entry["price"]
            if cl_entry and (time.time() - cl_entry["updated_at"]) <= _CHAINLINK_STALE_SECONDS
            else None
        )
        bn_entry = self._cache.get(asset)
        bn_price = (
            bn_entry["price"]
            if bn_entry and (time.time() - bn_entry["updated_at"]) <= _BINANCE_STALE_SECONDS
            else None
        )

        self._price_to_beat[condition_id] = {
            "asset": asset,
            "price": price,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source": source,
            "oracle": oracle,
            "ptb_chainlink": cl_price,
            "ptb_binance": bn_price,
        }
        tag = label or f"{asset} {condition_id[:12]}"
        cl_str = f"CL:${cl_price:.4f}" if cl_price is not None else "CL:n/a"
        bn_str = f"BIN:${bn_price:.4f}" if bn_price is not None else "BIN:n/a"
        logger.info("[FEED: PTB LIVE] %s %s %s [%s via %s]",
                     tag, cl_str, bn_str, source, oracle)
        asyncio.ensure_future(self._save_ptb_to_db())
        return price

    async def backfill_price_to_beat(self, condition_id: str, asset: str,
                                      timestamp_ms: int,
                                      label: str = "") -> Optional[float]:
        """Backfill price_to_beat for a past timestamp.

        First checks the DB for a previously-captured PTB (exact CL + BIN
        baseline from before the restart).  Falls back to Binance REST
        klines + live CL cache if not found.

        Args:
            condition_id: Polymarket market condition ID
            asset: Asset name
            timestamp_ms: Unix timestamp in milliseconds of the intended PTB capture time
            label: Human-readable market label for logging

        Returns:
            The backfilled price, or None on failure.
        """
        # Fast path: DB already has the exact PTB from before restart
        if condition_id in self._price_to_beat:
            existing = self._price_to_beat[condition_id]
            tag = label or f"{asset} {condition_id[:12]}"
            cl_p = existing.get('ptb_chainlink')
            bn_p = existing.get('ptb_binance')
            cl_str = f"CL:${cl_p:.4f}" if cl_p is not None else "CL:n/a"
            bn_str = f"BIN:${bn_p:.4f}" if bn_p is not None else "BIN:n/a"
            logger.debug("[FEED: PTB RESTORED] %s %s %s", tag, cl_str, bn_str)
            return existing.get("price")

        symbol = ASSET_SYMBOL_MAP.get(asset)
        if not symbol:
            logger.warning("Unknown asset for backfill: %s", asset)
            return None

        try:
            # Backfill target is the same capture target used by live PTB:
            # market_start - ptb_capture_offset.
            # We map that second-level target into a 1m candle:
            #   - exact boundary (sec~0)  -> candle OPEN
            #   - otherwise               -> candle CLOSE
            target_ms = max(0, int(timestamp_ms))
            candle_open_ms = (target_ms // 60_000) * 60_000
            sec_in_candle = (target_ms - candle_open_ms) / 1000.0

            url = f"{_REST_BASE}/klines"
            params = {
                "symbol": symbol.upper(),
                "interval": "1m",
                "startTime": candle_open_ms,
                "limit": 1,
            }

            session = self._session or aiohttp.ClientSession()
            close_session = self._session is None
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        logger.warning("Kline backfill HTTP %d for %s", resp.status, asset)
                        return None
                    data = await resp.json()
            finally:
                if close_session:
                    await session.close()

            if not data or not isinstance(data, list) or len(data) == 0:
                logger.debug("Empty kline response for %s at %d", asset, candle_open_ms)
                return None

            # Kline format: [openTime, open, high, low, close, volume, ...]
            open_price = float(data[0][1])
            close_price = float(data[0][4])
            use_open = sec_in_candle <= 0.5
            backfilled_price = open_price if use_open else close_price
            selected_kline_field = "open" if use_open else "close"

            # Backfill gets Binance PTB from klines.  For Chainlink, grab
            # whatever is currently in the live cache — it's not the exact
            # price at market open, but it lets consensus compare CL vs its
            # own baseline instead of marking CL "unavailable" every time.
            cl_entry = self._chainlink_cache.get(asset)
            cl_price = (
                cl_entry["price"]
                if cl_entry and (time.time() - cl_entry["updated_at"]) <= _CHAINLINK_STALE_SECONDS
                else None
            )

            target_ts = datetime.fromtimestamp(
                target_ms / 1000, tz=timezone.utc
            ).isoformat()
            candle_ts = datetime.fromtimestamp(
                candle_open_ms / 1000, tz=timezone.utc
            ).isoformat()
            self._price_to_beat[condition_id] = {
                "asset": asset,
                "price": backfilled_price,
                "captured_at": target_ts,
                "source": "backfill",
                "ptb_binance": backfilled_price,
                "ptb_chainlink": cl_price,
            }
            tag = label or f"{asset} {condition_id[:12]}"
            cl_str = f"CL:${cl_price:.4f}" if cl_price is not None else "CL:n/a"
            logger.info(
                "[FEED: PTB BACKFILL] %s BIN:$%.4f %s (target %s, %s of candle %s)",
                tag, backfilled_price, cl_str, target_ts, selected_kline_field, candle_ts,
            )
            await self._save_ptb_to_db()
            return backfilled_price

        except Exception as e:
            logger.error("Kline backfill failed for %s: %s", asset, e)
            return None

    def compute_gap(self, condition_id: str) -> Optional[dict]:
        """Compute the current price gap for a market vs its price_to_beat.

        Returns:
            {
                "asset": str,
                "price_to_beat": float,
                "ptb_oracle": str,       # oracle used for PTB ("chainlink"/"binance"/"backfill")
                "current_price": float,
                "current_oracle": str,   # oracle used for current price
                "gap_abs": float,        # absolute $ difference
                "gap_pct": float,        # percentage gap (always positive)
                "direction": str,        # "above" or "below" price_to_beat
                "winning_side": str,     # "Up" if above, "Down" if below
            }
            or None if data is missing.
        """
        ptb = self._price_to_beat.get(condition_id)
        if not ptb:
            return None

        asset = ptb["asset"]
        ptb_oracle = ptb.get("oracle", ptb.get("source", "unknown"))
        ptb_source = str(ptb.get("source", "") or "").strip().lower()

        preferred = self.get_price_source(asset)
        if preferred == "none":
            return None
        fallback = self._fallback_source(preferred)

        current_by = {
            "chainlink": None,
            "binance": None,
        }
        for src in ("chainlink", "binance"):
            entry = self._fresh_source_entry(asset, src)
            if entry is not None:
                current_by[src] = float(entry["price"])

        ptb_by = {
            "chainlink": ptb.get("ptb_chainlink"),
            "binance": ptb.get("ptb_binance"),
        }

        # Backfill baseline is Binance kline-based; keep matching oracle math.
        # When Binance current is unavailable, fail closed instead of mixing feeds.
        if ptb_source == "backfill":
            bn_cur = current_by.get("binance")
            bn_ptb = ptb_by.get("binance")
            if bn_cur is None:
                return None
            if bn_ptb is None:
                generic = ptb.get("price")
                if generic is None:
                    return None
                bn_ptb = generic
            current_oracle = "binance"
            current = bn_cur
            price_to_beat = float(bn_ptb)
        else:
            selected = None
            for src in (preferred, fallback):
                cur = current_by.get(src)
                baseline = ptb_by.get(src)
                # Legacy compatibility: old PTB blobs may have only generic price + oracle marker.
                if baseline is None and str(ptb_oracle).lower() == src and ptb.get("price") is not None:
                    baseline = ptb.get("price")
                if cur is None or baseline is None:
                    continue
                selected = (src, float(cur), float(baseline))
                break
            if selected is None:
                # No same-source comparison available; avoid cross-oracle math.
                return None
            current_oracle, current, price_to_beat = selected

        gap_abs = current - price_to_beat
        gap_pct = abs(gap_abs / price_to_beat) * 100 if price_to_beat > 0 else 0

        return {
            "asset": asset,
            "price_to_beat": price_to_beat,
            "ptb_oracle": ptb_oracle,
            "current_price": current,
            "current_oracle": current_oracle,
            "gap_abs": gap_abs,
            "gap_pct": gap_pct,
            "direction": "above" if gap_abs > 0 else "below",
            "winning_side": "Up" if gap_abs > 0 else "Down",
        }

    def check_consensus(self, condition_id: str, proposed_side: str) -> dict:
        """Check if Chainlink and Binance agree on which side wins.

        Each oracle is compared against its OWN price_to_beat captured at
        market start.  This avoids false splits from the persistent ~$0.50-1
        offset between feeds.

        Returns:
            {
                "agree": bool,           # Both oracles agree on the proposed side
                "chainlink_side": str,   # "Up", "Down", or "unavailable"
                "binance_side": str,     # "Up", "Down", or "unavailable"
                "chainlink_price": float | None,
                "binance_price": float | None,
                "ptb_chainlink": float | None,
                "ptb_binance": float | None,
                "reason": str,           # Human-readable explanation
            }
        """
        ptb_data = self._price_to_beat.get(condition_id)
        if not ptb_data:
            return {"agree": False, "chainlink_side": "unavailable",
                    "binance_side": "unavailable", "chainlink_price": None,
                    "binance_price": None, "ptb_chainlink": None,
                    "ptb_binance": None,
                    "reason": "No price_to_beat — BLOCKED (fail-closed)"}

        asset = ptb_data["asset"]
        ptb_cl = ptb_data.get("ptb_chainlink")
        ptb_bn = ptb_data.get("ptb_binance")

        # Read Chainlink cache directly
        cl_entry = self._chainlink_cache.get(asset)
        cl_price = (
            cl_entry["price"]
            if cl_entry and (time.time() - cl_entry["updated_at"]) <= _CHAINLINK_STALE_SECONDS
            else None
        )

        # Read Binance cache directly
        bn_entry = self._cache.get(asset)
        bn_price = (
            bn_entry["price"]
            if bn_entry and (time.time() - bn_entry["updated_at"]) <= _BINANCE_STALE_SECONDS
            else None
        )

        # Determine each oracle's direction against its OWN PTB
        # Per PM rules: strictly above = Up, equal or below = Down
        if cl_price is not None and ptb_cl is not None:
            cl_side = "Up" if cl_price > ptb_cl else "Down"
        else:
            cl_side = "unavailable"

        if bn_price is not None and ptb_bn is not None:
            bn_side = "Up" if bn_price > ptb_bn else "Down"
        else:
            bn_side = "unavailable"

        # If BOTH unavailable, fail closed — no data means no confidence
        if cl_side == "unavailable" and bn_side == "unavailable":
            return {"agree": False, "chainlink_side": cl_side,
                    "binance_side": bn_side, "chainlink_price": cl_price,
                    "binance_price": bn_price,
                    "ptb_chainlink": ptb_cl, "ptb_binance": ptb_bn,
                    "reason": "Both oracles unavailable — BLOCKED (fail-closed)"}

        # If only ONE is unavailable, pass through — one oracle is better than none
        if cl_side == "unavailable" or bn_side == "unavailable":
            missing = "Chainlink" if cl_side == "unavailable" else "Binance"
            if cl_side == "unavailable" and cl_price is not None:
                missing = "Chainlink PTB"
            if bn_side == "unavailable" and bn_price is not None:
                missing = "Binance PTB"
            return {"agree": True, "chainlink_side": cl_side,
                    "binance_side": bn_side, "chainlink_price": cl_price,
                    "binance_price": bn_price,
                    "ptb_chainlink": ptb_cl, "ptb_binance": ptb_bn,
                    "reason": f"{missing} unavailable — single-oracle pass-through"}

        # Both available — do they agree with the proposed side?
        agree = (cl_side == proposed_side and bn_side == proposed_side)

        if agree:
            reason = (f"CONSENSUS: both oracles confirm {proposed_side} "
                      f"(CL=${cl_price:.4f} vs CL_PTB=${ptb_cl:.4f} | "
                      f"BN=${bn_price:.4f} vs BN_PTB=${ptb_bn:.4f})")
        else:
            reason = (f"ORACLE SPLIT: Chainlink={cl_side} (${cl_price:.4f} vs PTB=${ptb_cl:.4f}) | "
                      f"Binance={bn_side} (${bn_price:.4f} vs PTB=${ptb_bn:.4f}) | "
                      f"proposed={proposed_side}")

        return {"agree": agree, "chainlink_side": cl_side,
                "binance_side": bn_side, "chainlink_price": cl_price,
                "binance_price": bn_price,
                "ptb_chainlink": ptb_cl, "ptb_binance": ptb_bn,
                "reason": reason}

    def clear_price_to_beat(self, condition_id: str):
        """Remove a price_to_beat snapshot (market expired/pruned)."""
        self._price_to_beat.pop(condition_id, None)

    def get_stats(self) -> dict:
        """Return feed stats for dashboard / logging."""
        return {
            "primary_source": self._primary_source,
            "connected": self._connected,
            "ticks": self._ticks_received,
            "last_tick_age": round(time.time() - self._last_tick_at, 1) if self._last_tick_at else None,
            "reconnects": self._reconnect_count,
            "assets_tracked": len(self._cache),
            "prices_to_beat": len(self._price_to_beat),
            "prices": {
                asset: round(entry["price"], 4)
                for asset, entry in self._cache.items()
            },
            # Chainlink RTDS stats
            "chainlink_connected": self._rtds_connected,
            "chainlink_ticks": self._rtds_ticks,
            "chainlink_last_tick_age": (
                round(time.time() - self._rtds_last_tick_at, 1)
                if self._rtds_last_tick_at else None
            ),
            "chainlink_reconnects": self._rtds_reconnect_count,
            "chainlink_prices": {
                asset: round(entry["price"], 4)
                for asset, entry in self._chainlink_cache.items()
            },
        }

    def get_oracle_health(self) -> dict:
        """Return per-asset oracle health for dashboard display.

        Returns:
            {
                "chainlink": {"connected": bool, "ticks": int, "last_tick_age": float|None,
                               "reconnects": int, "assets": {asset: {"price": f, "age": f}}},
                "binance":   {"connected": bool, "ticks": int, "last_tick_age": float|None,
                               "reconnects": int, "assets": {asset: {"price": f, "age": f}}},
                "per_asset":  {asset: {"active_source": str, "chainlink_ok": bool, "binance_ok": bool}},
            }
        """
        now = time.time()

        cl_assets = {}
        for asset, entry in self._chainlink_cache.items():
            age = now - entry["updated_at"]
            cl_assets[asset] = {"price": round(entry["price"], 4), "age": round(age, 1), "fresh": age <= _CHAINLINK_STALE_SECONDS}

        bn_assets = {}
        for asset, entry in self._cache.items():
            age = now - entry["updated_at"]
            bn_assets[asset] = {"price": round(entry["price"], 4), "age": round(age, 1), "fresh": age <= _BINANCE_STALE_SECONDS}

        per_asset = {}
        for asset in self._assets:
            cl_ok = asset in cl_assets and cl_assets[asset]["fresh"]
            bn_ok = asset in bn_assets and bn_assets[asset]["fresh"]
            source = self.get_price_source(asset)
            per_asset[asset] = {"active_source": source, "chainlink_ok": cl_ok, "binance_ok": bn_ok}

        return {
            "primary_source": self._primary_source,
            "chainlink": {
                "connected": self._rtds_connected,
                "ticks": self._rtds_ticks,
                "last_tick_age": round(now - self._rtds_last_tick_at, 1) if self._rtds_last_tick_at else None,
                "reconnects": self._rtds_reconnect_count,
                "assets": cl_assets,
            },
            "binance": {
                "connected": self._connected,
                "ticks": self._ticks_received,
                "last_tick_age": round(now - self._last_tick_at, 1) if self._last_tick_at else None,
                "reconnects": self._reconnect_count,
                "assets": bn_assets,
            },
            "per_asset": per_asset,
        }

    # -- Lifecycle --

    def update_assets(self, assets: list[str]):
        """Hot-update the tracked assets and reconnect WS to pick up new symbols.

        Called by the dashboard when asset toggles change.
        """
        new_symbols = [ASSET_SYMBOL_MAP[a] for a in assets if a in ASSET_SYMBOL_MAP]
        if set(new_symbols) == set(self._symbols):
            return  # No change

        old = set(self._assets)
        self._assets = list(assets)
        self._symbols = new_symbols
        logger.info("Price feed assets updated: %s -> reconnecting WS", self._assets)

        # Purge cache + price_to_beat entries for removed assets
        removed = old - set(self._assets)
        if removed:
            for asset in removed:
                self._cache.pop(asset, None)
            stale_cids = [cid for cid, ptb in self._price_to_beat.items()
                          if ptb["asset"] in removed]
            for cid in stale_cids:
                del self._price_to_beat[cid]
            logger.info("Purged price data for removed assets: %s (%d PTB entries)",
                        removed, len(stale_cids))

        # Force WS reconnect so the new stream URL includes all symbols
        if self._ws and not self._ws.closed:
            asyncio.ensure_future(self._ws.close())
            # _run_loop will auto-reconnect with the new _symbols list

    async def start(self):
        """Start both Binance and Chainlink RTDS feeds."""
        if self._running:
            return

        self._running = True
        self._session = aiohttp.ClientSession()
        self._rtds_session = aiohttp.ClientSession()
        self._task = asyncio.create_task(self._run_loop())
        self._rtds_task = asyncio.create_task(self._rtds_run_loop())
        secondary = self._fallback_source(self._primary_source)
        logger.info(
            "Price feed starting -- primary=%s fallback=%s -- assets: %s",
            self._source_label(self._primary_source),
            self._source_label(secondary),
            ", ".join(self._assets),
        )

    async def stop(self):
        """Stop both feeds and clean up."""
        self._running = False

        # Close Chainlink RTDS
        if self._rtds_ws and not self._rtds_ws.closed:
            await self._rtds_ws.close()
        if self._rtds_task:
            self._rtds_task.cancel()
            try:
                await self._rtds_task
            except asyncio.CancelledError:
                pass
        if self._rtds_session and not self._rtds_session.closed:
            await self._rtds_session.close()
        self._rtds_connected = False

        # Close Binance
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()
        self._connected = False
        logger.info("Price feed stopped (Chainlink RTDS + Binance)")

    # -- Internal WS loop --

    async def _run_loop(self):
        """Main WS loop with auto-reconnect."""
        backoff = _INITIAL_BACKOFF

        while self._running:
            try:
                await self._connect_and_listen()
                # Clean disconnect -- reset backoff
                backoff = _INITIAL_BACKOFF
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                if not self._running:
                    break
                logger.warning("Binance WS error: %s -- reconnecting in %.0fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * _BACKOFF_MULTIPLIER, _MAX_BACKOFF)
                self._reconnect_count += 1

    async def _connect_and_listen(self):
        """Connect to Binance combined stream and process messages."""
        # Build combined stream URL
        streams = "/".join(f"{s}@ticker" for s in self._symbols)
        url = f"{_BASE_WS}?streams={streams}"

        logger.info("Connecting to Binance WS: %s", url)

        async with self._session.ws_connect(
            url,
            heartbeat=20,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as ws:
            self._ws = ws
            self._connected = True
            logger.info("Binance WS connected -- streaming %d symbols", len(self._symbols))

            async for msg in ws:
                if not self._running:
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        self._process_ticker(data)
                    except Exception as e:
                        logger.debug("Binance WS parse error: %s", e)

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.warning("Binance WS error frame: %s", ws.exception())
                    break

                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    logger.info("Binance WS closed by server")
                    break

        self._connected = False

    def _process_ticker(self, data: dict):
        """Process a 24hr ticker event from the combined stream.

        Combined stream format: {"stream": "btcusdt@ticker", "data": {...}}
        Ticker data fields we use:
          s  = symbol (BTCUSDT)
          c  = last price
          b  = best bid
          a  = best ask
          v  = 24h volume (base asset)
          q  = 24h volume (quote USDT)
        """
        payload = data.get("data", data)  # combined stream wraps in "data"
        symbol_raw = payload.get("s", "").lower()

        asset = _SYMBOL_ASSET_MAP.get(symbol_raw)
        if not asset:
            return

        try:
            price = float(payload.get("c", 0))
            bid = float(payload.get("b", 0))
            ask = float(payload.get("a", 0))
            volume_quote = float(payload.get("q", 0))
        except (ValueError, TypeError):
            return

        if price <= 0:
            return

        self._cache[asset] = {
            "price": price,
            "bid": bid,
            "ask": ask,
            "volume_24h": volume_quote,
            "updated_at": time.time(),
        }

        self._ticks_received += 1
        self._last_tick_at = time.time()

        # Fire external callbacks (TrendAnalyzer, etc.)
        for cb in self._on_price_callbacks:
            try:
                cb(asset, price, time.time(), source="binance")
            except Exception:
                pass

    # -- Chainlink RTDS (Real-Time Data Socket) --

    async def _rtds_run_loop(self):
        """RTDS WS loop with auto-reconnect + 429-aware backoff."""
        backoff = _INITIAL_BACKOFF

        while self._running:
            try:
                ticks_before = self._rtds_ticks
                await self._rtds_connect_and_listen()
                # If we got ticks during this connection, reset backoff
                if self._rtds_ticks > ticks_before:
                    backoff = _INITIAL_BACKOFF
                else:
                    # Connection returned without receiving any ticks
                    # (watchdog kill, server close, etc.) — apply backoff
                    # to avoid hammering the server immediately.
                    if self._running:
                        backoff = max(backoff, 5.0)
                        logger.info(
                            "Chainlink RTDS closed without ticks "
                            "-- retrying in %.0fs", backoff,
                        )
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * _BACKOFF_MULTIPLIER,
                                      _MAX_BACKOFF_RTDS)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._rtds_connected = False
                if not self._running:
                    break

                err_str = str(e)
                # 429 = rate-limited: brief pause, don't hammer
                if "429" in err_str:
                    backoff = max(backoff, 10.0)
                    logger.warning(
                        "Chainlink RTDS rate-limited (429) -- backing off %.0fs",
                        backoff,
                    )
                else:
                    logger.warning(
                        "Chainlink RTDS error: %s -- reconnecting in %.0fs",
                        e, backoff,
                    )

                await asyncio.sleep(backoff)
                backoff = min(backoff * _BACKOFF_MULTIPLIER, _MAX_BACKOFF_RTDS)
                self._rtds_reconnect_count += 1

    async def _rtds_connect_and_listen(self):
        """Connect to Polymarket RTDS and stream Chainlink oracle prices."""
        logger.info("Connecting to Chainlink RTDS: %s", _RTDS_WS)

        async with self._rtds_session.ws_connect(
            _RTDS_WS,
            heartbeat=20,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as ws:
            self._rtds_ws = ws
            self._rtds_connected = True
            logger.info("Chainlink RTDS connected")

            # Subscribe to all crypto prices (no filters = all symbols)
            subscribe_msg = json.dumps({
                "action": "subscribe",
                "subscriptions": [{
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": "",
                }],
            })
            await ws.send_str(subscribe_msg)
            logger.info("Chainlink RTDS subscribed to crypto_prices_chainlink")

            # Start ping task to keep connection alive
            ping_task = asyncio.create_task(self._rtds_ping_loop(ws))
            # Start stale-tick watchdog: force reconnect if no ticks for 30s
            watchdog_task = asyncio.create_task(self._rtds_stale_watchdog(ws))

            try:
                async for msg in ws:
                    if not self._running:
                        break

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data = json.loads(msg.data)
                            self._process_chainlink_tick(data)
                        except Exception as e:
                            logger.debug("Chainlink RTDS parse error: %s", e)

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.warning("Chainlink RTDS error frame: %s", ws.exception())
                        break

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                        logger.info("Chainlink RTDS closed by server")
                        break
            finally:
                ping_task.cancel()
                watchdog_task.cancel()
                for t in (ping_task, watchdog_task):
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass

        self._rtds_connected = False

    async def _rtds_ping_loop(self, ws):
        """Send periodic pings to keep the RTDS connection alive."""
        try:
            while not ws.closed:
                await asyncio.sleep(_RTDS_PING_INTERVAL)
                if not ws.closed:
                    await ws.send_str(json.dumps({"action": "ping"}))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("RTDS ping error: %s", e)

    async def _rtds_stale_watchdog(self, ws):
        """Force RTDS reconnect if no ticks received for a prolonged period.

        Protects against zombie connections where the TCP link is dead
        but aiohttp hasn't detected it (e.g. after network changes,
        sleep/wake, or hot-toggling dashboard settings).

        Uses a generous 45s initial grace period to avoid killing
        connections that are recovering from rate-limits (the server
        may accept the WS but delay streaming).
        """
        _INITIAL_GRACE = 45.0     # first-tick patience (was 15, too aggressive)
        _STALE_THRESHOLD = 30.0   # max silence after first tick
        try:
            await asyncio.sleep(_INITIAL_GRACE)
            while not ws.closed and self._running:
                if self._rtds_last_tick_at > 0:
                    age = time.time() - self._rtds_last_tick_at
                    if age > _STALE_THRESHOLD:
                        logger.warning(
                            "Chainlink RTDS stale (%.0fs since last tick) "
                            "-- forcing reconnect", age
                        )
                        if not ws.closed:
                            await ws.close()
                        break
                else:
                    # Never received a tick — connection is dead on arrival
                    if not self._running:
                        break
                    logger.warning(
                        "Chainlink RTDS: no ticks received in %.0fs "
                        "-- forcing reconnect", _INITIAL_GRACE
                    )
                    if not ws.closed:
                        await ws.close()
                    break
                await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("RTDS watchdog error: %s", e)

    def _process_chainlink_tick(self, data: dict):
        """Process a Chainlink price tick from RTDS.

        Message format:
            {
                "topic": "crypto_prices_chainlink",
                "type": "update",
                "timestamp": 1718000000000,
                "payload": {
                    "symbol": "btc/usd",
                    "timestamp": 1718000000000,
                    "value": 68004.71,
                    "full_accuracy_value": "68004715377000000000000"
                }
            }
        """
        topic = data.get("topic")
        if topic != "crypto_prices_chainlink":
            return

        payload = data.get("payload")
        if not payload:
            return

        symbol = payload.get("symbol", "").lower()
        asset = _CHAINLINK_ASSET_MAP.get(symbol)
        if not asset:
            return

        try:
            price = float(payload.get("value", 0))
        except (ValueError, TypeError):
            return

        if price <= 0:
            return

        self._chainlink_cache[asset] = {
            "price": price,
            "symbol": symbol,
            "updated_at": time.time(),
            "rtds_ts": payload.get("timestamp"),
        }

        self._rtds_ticks += 1
        self._rtds_last_tick_at = time.time()

        # Fire external callbacks (TrendAnalyzer, etc.)
        for cb in self._on_price_callbacks:
            try:
                cb(asset, price, time.time(), source="chainlink")
            except Exception:
                pass
