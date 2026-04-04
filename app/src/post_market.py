"""Post-market maker strategy — extracted from scanner.py for clean separation.

Handles the complete post-end lifecycle:
  1. Pre-fetch PTB data before market ends (warms cache)
  2. Launch settlement fetch early (T-1s, HTTP lands at ~T+0s)
  3. Place GTC LIMIT BUY for winning token
  4. Monitor resting orders for fills
  5. Cancel unfilled orders after timeout
  6. Record fills to database

Dependencies:
  - PTBScraper: settlement/PTB data fetching (crypto-price → API → page scrape)
  - PolymarketAPI: order placement (place_limit_buy, get_order_status, cancel_order)
  - Database: trade/position recording (optional)
  - Config.post_end: PostEndConfig (enabled, max_bet, maker_price, etc.)

The Scanner creates a PostMarketManager instance and delegates all post-end
logic to it.  No post-market state or methods remain in scanner.py.
"""

import asyncio
import json
import logging
import math
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, TYPE_CHECKING

from src.config import Config
from src.ptb_scraper import PTBScraper

if TYPE_CHECKING:
    from src.api import PolymarketAPI
    from src.database import Database

logger = logging.getLogger(__name__)


@dataclass
class _SportsMarketProxy:
    """Minimal duck-type proxy that satisfies _record_maker_fill and _mkt_tag
    for sports markets (which don't come from the scanner's TrackedMarket)."""
    condition_id: str
    token_id: str
    token_id_alt: str
    event_title: str
    event_slug: str
    asset: str
    neg_risk: bool = False
    market_start: Optional[datetime] = None
    end_date: Optional[datetime] = None
    duration_seconds: int = 0


def _parse_outcomes(market_dict: dict) -> list[str]:
    """Parse ``outcomes`` from a Gamma market dict (may be JSON string or list)."""
    raw = market_dict.get("outcomes", "")
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return []
        return parsed if isinstance(parsed, list) else []
    return raw if isinstance(raw, list) else []


def _mkt_tag(market) -> str:
    """Compact market label for logs: 'BTC 5m 2:45-2:50PM' or 'ETH 1h'."""
    _ASSET_SHORT = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "xrp": "XRP"}
    asset_short = _ASSET_SHORT.get(market.asset, market.asset.upper())
    dur = market.duration_seconds
    if dur == 0:
        tf = ""
    elif dur < 3600:
        tf = f" {dur // 60}m"
    elif dur < 86400:
        tf = f" {dur // 3600}h"
    else:
        tf = " 1d"
    time_str = ""
    try:
        if market.market_start and market.end_date:
            from zoneinfo import ZoneInfo
            ET = ZoneInfo("America/New_York")
            s = market.market_start.astimezone(ET)
            e = market.end_date.astimezone(ET)
            def _t(dt):
                if dt.minute == 0:
                    return dt.strftime("%I%p").lstrip("0")
                return dt.strftime("%I:%M%p").lstrip("0")
            time_str = f" {_t(s)}-{_t(e)}"
    except Exception:
        pass
    return f"{asset_short}{tf}{time_str}"


class PostMarketManager:
    """Manages all post-market maker logic, fully decoupled from Scanner.

    Call sites from Scanner:
      - scan() Phase 0: process_ended_markets(tracked, now)
      - scan() Phase 1c: pre_fetch_approaching(tracked, now)
      - scan() Phase 2 cleanup: cleanup(expired_cids, tracked_cids)
      - on_ws_resolution(): on_ws_resolution(cid, outcome)
    """

    def __init__(
        self,
        config: Config,
        api: "PolymarketAPI",
        ptb_scraper: PTBScraper,
        db: Optional["Database"] = None,
    ):
        self.config = config
        self.api = api
        self.ptb_scraper = ptb_scraper
        self._db = db

        # ── Isolated post-market state ──
        self._post_cooldowns: dict[str, float] = {}
        self._post_pending: set[str] = set()
        self._maker_orders: dict[str, dict] = {}
        self._maker_market_keys: set[tuple] = set()
        self._resolved_cids: set[str] = set()
        self._ptb_prefetched: set[str] = set()
        self._settlement_scheduled: set[str] = set()
        self._maker_monitor_running: bool = False
        self._pe_log_ts: dict[str, float] = {}

    # ── Properties exposed to Scanner/Dashboard ─────────────────────────

    @property
    def maker_orders(self) -> dict[str, dict]:
        """Active resting maker orders (cid → order info). Read by dashboard."""
        return self._maker_orders

    @property
    def resolved_cids(self) -> set[str]:
        """CIDs resolved on-chain. Scanner reads this for cleanup."""
        return self._resolved_cids

    # ── PUBLIC API (called from Scanner) ────────────────────────────────

    async def process_ended_markets(
        self,
        tracked: dict,   # cid -> TrackedMarket
        now: datetime,
    ):
        """Phase 0: Fire post-end maker for all ended markets in parallel.

        Called at the TOP of each scan cycle (latency-critical).
        """
        pe_cfg = self.config.post_end
        if not pe_cfg.enabled:
            return

        pe_window = pe_cfg.window_seconds
        tasks = []
        for cid, m in list(tracked.items()):
            secs_to_end = (m.end_date - now).total_seconds()
            if secs_to_end < 0 and (-secs_to_end) <= pe_window:
                tasks.append(self._post_end_fast_path(m, cid, now, secs_to_end))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def pre_fetch_approaching(
        self,
        tracked: dict,   # cid -> TrackedMarket
        now: datetime,
    ):
        """Phase 1c: Pre-fetch PTB for markets approaching their end time.

        Fires async fetch ~10s before end so the cache is warm when
        _post_end_fast_path needs it at T-1s (HTTP fetch fires early).
        """
        if not self.config.post_end.enabled:
            return

        PRE_FETCH_SECONDS = 10
        tasks = []
        for cid, m in tracked.items():
            if cid in self._ptb_prefetched:
                continue
            if not m.market_start or not m.duration_seconds:
                continue
            secs_to_end = (m.end_date - now).total_seconds()
            if secs_to_end < PRE_FETCH_SECONDS and secs_to_end > -5:
                self._ptb_prefetched.add(cid)
                start_epoch = int(m.market_start.timestamp())
                next_start_epoch = start_epoch + m.duration_seconds
                # Pre-fetch CURRENT market's PTB
                tasks.append(self.ptb_scraper.fetch_past_results(
                    m.asset, m.duration_seconds, start_epoch,
                    event_slug=m.event_slug,
                ))
                # Pre-fetch NEXT period
                tasks.append(self.ptb_scraper.fetch_past_results(
                    m.asset, m.duration_seconds, next_start_epoch,
                ))
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            ok = sum(1 for r in results if r and not isinstance(r, Exception))
            if ok:
                logger.info("[POST: WARMUP] Cached %d/%d markets approaching end",
                            ok, len(tasks))

        # Schedule background settlement fetch early (fires HTTP at T-leadtime)
        leadtime = self.config.post_end.fetch_leadtime if hasattr(self.config.post_end, 'fetch_leadtime') else 1.0
        schedule_window = leadtime + 1.0  # schedule task 1s before the fetch fires
        for cid_s, m_s in tracked.items():
            if cid_s in self._settlement_scheduled:
                continue
            if not m_s.market_start or not m_s.duration_seconds:
                continue
            ste = (m_s.end_date - now).total_seconds()
            if ste < schedule_window and ste > 0:
                self._settlement_scheduled.add(cid_s)
                asyncio.create_task(self._launch_settlement_at_boundary(m_s, ste))

        # Prune stale cache entries (>10 min old)
        self.ptb_scraper.prune_stale(600)
        # Prune prefetch tracker for expired markets
        tracked_keys = set(tracked.keys())
        self._ptb_prefetched -= self._ptb_prefetched - tracked_keys
        self._settlement_scheduled -= self._settlement_scheduled - tracked_keys

    def cleanup(self, expired_cids: list[str], tracked_cids: set[str]):
        """Clean up state for expired markets."""
        for cid in expired_cids:
            self._post_cooldowns.pop(cid, None)
            self._maker_orders.pop(cid, None)
            self._pe_log_ts.pop(cid, None)
            self._resolved_cids.discard(cid)
            self._post_pending.discard(cid)

    def cleanup_market_key(self, asset: str, duration: int, start_epoch: int):
        """Remove a specific market key from the dedup set."""
        mk = (asset.lower() if asset else "", duration or 0, start_epoch)
        self._maker_market_keys.discard(mk)

    def on_ws_resolution(self, condition_id: str, winning_outcome: str):
        """Called when WS fires a market_resolved event.

        Marks the condition_id as resolved so _post_end_fast_path won't
        retry orders on a dead orderbook.
        """
        self._resolved_cids.add(condition_id)
        tag = condition_id[:16]
        logger.info(
            "[POST: RESOLVED] %s resolved as %s — blocking further maker retries",
            tag, winning_outcome,
        )

    # ── INTERNAL: Settlement at Boundary ────────────────────────────────

    async def _launch_settlement_at_boundary(self, market, delay: float):
        """Background: sleep until T-1s, fire settlement fetch early.

        Launched ~2s before market ends.  At T-1s (1 second before boundary):
        1. Fire crypto-price API call (HTTP round-trip ~700ms-1.5s).
           By the time the response arrives, we're at/past the boundary
           and the NEXT period's openPrice is populated.
        2. Determine winner (settlement vs cached PTB).
        3. Place GTC LIMIT BUY immediately (no scan cycle wait).
        """
        tag = _mkt_tag(market)
        cid = market.condition_id

        try:
            pe_cfg = self.config.post_end
            if not pe_cfg.enabled:
                return

            # Fire before boundary — the HTTP fetch takes ~700ms+
            # so the response lands right at or just after the boundary.
            leadtime = pe_cfg.fetch_leadtime if hasattr(pe_cfg, 'fetch_leadtime') else 1.0
            await asyncio.sleep(max(0, delay - leadtime))
            if not market.market_start or not market.duration_seconds:
                return

            start_epoch = int(market.market_start.timestamp())
            t_launch = time.time()

            # Dedup: claim this market before any await
            mkt_key = (
                market.asset.lower() if market.asset else "",
                market.duration_seconds or 0,
                start_epoch,
            )
            if mkt_key in self._maker_market_keys:
                return
            if cid in self._resolved_cids:
                return
            if cid in self._maker_orders:
                return
            if cid in self._post_pending:
                return
            self._maker_market_keys.add(mkt_key)
            self._post_pending.add(cid)

            # Fetch settlement (crypto-price ~1.2s or page scrape ~2s)
            settlement = await self.ptb_scraper.fetch_settlement_fast(
                market.asset, market.duration_seconds, start_epoch,
                event_slug=market.event_slug,
            )

            if not settlement or not settlement.get("winner"):
                self._maker_market_keys.discard(mkt_key)
                self._post_pending.discard(cid)
                logger.info(
                    "[POST: SETTLE] %s — no settlement yet (%.1fms), "
                    "deferring to Phase 0",
                    tag, (time.time() - t_launch) * 1000,
                )
                return

            # ── Timing info (no gate — fire immediately) ──────────────
            # GTC limit orders sit in the book harmlessly; UMA resolution
            # takes minutes anyway, so sub-second early placement is fine.
            market_end_epoch = start_epoch + market.duration_seconds
            now_epoch = time.time()
            early_ms = max(0, (market_end_epoch - now_epoch)) * 1000

            winning_side = settlement["winner"].capitalize()
            settle_val = settlement.get("settlement_price", 0)
            ptb_val = settlement.get("ptb", 0)
            pct_val = abs(settlement.get("percent_change", 0))
            direction = "above" if settlement["winner"] == "up" else "below"
            fetch_ms = settlement.get("fetch_time_ms", 0)

            # ── FAIL-SAFE 2: Max-gap sanity check ─────────────────────
            # If the gap between PTB and settlement is unreasonably large,
            # the data is likely corrupt/stale. Abort rather than bet wrong.
            MAX_SANE_GAP_PCT = 5.0  # 5% — no real 5m market moves this much
            if pct_val > MAX_SANE_GAP_PCT:
                self._maker_market_keys.discard(mkt_key)
                self._post_pending.discard(cid)
                logger.warning(
                    "[POST: SETTLE] %s — FAIL-SAFE: gap %.4f%% exceeds %.1f%% "
                    "sanity limit, aborting (PTB=%.4f settle=%.4f)",
                    tag, pct_val, MAX_SANE_GAP_PCT, ptb_val, settle_val,
                )
                return

            # ── FAIL-SAFE 3: Zero/missing price guard ─────────────────
            # If either price is zero or missing, data is incomplete.
            if not settle_val or not ptb_val:
                self._maker_market_keys.discard(mkt_key)
                self._post_pending.discard(cid)
                logger.warning(
                    "[POST: SETTLE] %s — FAIL-SAFE: missing price data "
                    "(PTB=%s settle=%s), aborting",
                    tag, ptb_val, settle_val,
                )
                return

            winning_token = market.token_id if winning_side == "Up" else market.token_id_alt
            if not winning_token:
                self._maker_market_keys.discard(mkt_key)
                self._post_pending.discard(cid)
                logger.warning("[POST: SETTLE] %s — no token for %s side", tag, winning_side)
                return

            # Place bid immediately
            bid_price = pe_cfg.maker_price
            amount = pe_cfg.max_bet
            if bid_price > pe_cfg.max_buy_price:
                bid_price = pe_cfg.max_buy_price
            if amount < 0.01:
                self._maker_market_keys.discard(mkt_key)
                self._post_pending.discard(cid)
                return

            gtc_shares = math.ceil(amount / bid_price * 100) / 100
            now_dt = datetime.now(timezone.utc)
            past_sec = (now_dt - market.end_date).total_seconds()

            timing_str = (
                "T-%.0fms" % early_ms if early_ms > 0
                else "%.1fs past end" % past_sec
            )
            logger.info(
                "[POST: SETTLE] %s — %s %.4f%% %s (PTB=%.4f→%.4f) "
                "| GTC BUY $%.2f @ $%.4f (%.2f sh) | %s, fetch %dms",
                tag, winning_side, pct_val, direction,
                ptb_val, settle_val,
                amount, bid_price, gtc_shares,
                timing_str, fetch_ms,
            )

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None, self.api.place_limit_buy,
                winning_token, bid_price, amount,
            )

            order_id = result.get("orderID") or result.get("id", "")
            total_elapsed = (time.time() - t_launch) * 1000

            if not order_id:
                logger.warning(
                    "[POST: SETTLE] %s — order rejected: %s (%.0fms total)",
                    tag, result, total_elapsed,
                )
                self._post_cooldowns[cid] = now_dt.timestamp()
                self._maker_market_keys.discard(mkt_key)
                self._post_pending.discard(cid)
                return

            status = result.get("status", "")
            logger.info(
                "[POST: SETTLE] %s — order %s %s (%.0fms total latency)",
                tag, order_id[:16], status or "LIVE", total_elapsed,
            )

            gap_data = {
                "gap_pct": pct_val,
                "direction": direction,
                "winning_side": winning_side,
                "settlement_price": settle_val,
                "ptb": ptb_val,
            }

            if status in ("matched", "MATCHED"):
                fill_price = bid_price
                fill_shares = amount / bid_price
                self._post_cooldowns[cid] = now_dt.timestamp()
                if self._db:
                    try:
                        await self._record_maker_fill(
                            market, winning_token, winning_side,
                            amount, fill_shares, fill_price,
                            None, gap_data,
                        )
                    except Exception as e:
                        logger.debug("[POST: SETTLE] DB write error: %s", e)
            else:
                self._maker_orders[cid] = {
                    "order_id": order_id,
                    "bid_price": bid_price,
                    "amount": amount,
                    "posted_at": time.time(),
                    "side": winning_side,
                    "token_id": winning_token,
                    "market": market,
                    "gap_data": gap_data,
                    "last_matched": 0.0,
                }
                if not self._maker_monitor_running:
                    self._maker_monitor_running = True
                    asyncio.create_task(self._maker_monitor_loop())

            self._post_pending.discard(cid)

        except Exception as e:
            logger.error("[POST: SETTLE] %s — error: %s", tag, e, exc_info=True)
            if 'mkt_key' in locals():
                self._maker_market_keys.discard(mkt_key)
            self._post_pending.discard(cid)

    # ── INTERNAL: Post-End Fast Path (scan loop) ────────────────────────

    async def _post_end_fast_path(self, market, cid: str,
                                   now: datetime, seconds_to_end: float):
        """Post-end MAKER strategy — posts GTC LIMIT BUY for the winning token.

        On-chain confirmed strategy (@lesstidy):
          1. Market ends → determine winner via PM settlement
          2. Post resting GTC LIMIT BUY at maker_price (e.g. $0.99)
          3. Sellers fill against our bid → we own winning shares
          4. Redeemer handles $1.00 redemption → $0.01/share profit

        Only ONE maker order per market. A background monitor (_maker_monitor)
        polls for fills and cancels unfilled remainder after window_seconds.
        """
        pe_cfg = self.config.post_end
        _pe_tag = _mkt_tag(market)
        past_sec = -seconds_to_end

        # -- No-bet asset gate (watch-only) --
        if getattr(market, "asset", "") in self.config.execution.no_bet_set:
            return

        # Rate-limit diagnostic logging (one message per market per 30s)
        _pe_can_log = (now.timestamp() - self._pe_log_ts.get(cid, 0)) >= 30

        # Per-MARKET dedup — multiple cids can reference the same physical market.
        mkt_key = (
            market.asset.lower() if market.asset else "",
            market.duration_seconds or 0,
            int(market.market_start.timestamp()) if market.market_start else 0,
        )
        if mkt_key in self._maker_market_keys:
            return

        if cid in self._resolved_cids:
            return
        if cid in self._maker_orders:
            return

        # Per-market cooldown
        last_buy = self._post_cooldowns.get(cid, 0)
        if (now.timestamp() - last_buy) < pe_cfg.cooldown_seconds:
            return

        if cid in self._post_pending:
            return

        # Claim market + cid IMMEDIATELY (before any await)
        self._maker_market_keys.add(mkt_key)
        self._post_pending.add(cid)

        # Determine the winning side via PM settlement
        winning_side = None
        winner_source = ""
        gap_data = None

        if market.market_start and market.duration_seconds:
            start_epoch = int(market.market_start.timestamp())
            settlement = await self.ptb_scraper.get_settlement(
                market.asset, market.duration_seconds, start_epoch,
                event_slug=market.event_slug,
            )
            if settlement and settlement.get("winner"):
                winning_side = settlement["winner"].capitalize()
                winner_source = "pm_settlement"
                ptb_val = settlement.get("ptb") or 0
                settle_val = settlement.get("settlement_price") or 0
                pct_val = abs(settlement.get("percent_change", 0))
                direction = "above" if settlement["winner"] == "up" else "below"
                gap_data = {
                    "gap_pct": pct_val,
                    "direction": direction,
                    "winning_side": winning_side,
                    "settlement_price": settle_val,
                    "ptb": ptb_val,
                }
                # (settlement info folded into posting line below)

        if not winning_side:
            self._maker_market_keys.discard(mkt_key)
            self._post_pending.discard(cid)
            if _pe_can_log:
                logger.info(
                    "[POST: BID] %s — %.1fs past end — no PM settlement yet, SKIPPING "
                    "(oracle fallback disabled for safety)",
                    _pe_tag, past_sec,
                )
                self._pe_log_ts[cid] = now.timestamp()
            return

        winning_token = market.token_id if winning_side == "Up" else market.token_id_alt

        if not winning_token:
            self._maker_market_keys.discard(mkt_key)
            self._post_pending.discard(cid)
            if _pe_can_log:
                logger.info(
                    "[POST: BID] %s — %.1fs past end — no token for %s side",
                    _pe_tag, past_sec, winning_side,
                )
                self._pe_log_ts[cid] = now.timestamp()
            return

        # Post GTC LIMIT BUY
        bid_price = pe_cfg.maker_price
        amount = pe_cfg.max_bet
        if bid_price > pe_cfg.max_buy_price:
            bid_price = pe_cfg.max_buy_price

        if amount < 0.01:
            self._maker_market_keys.discard(mkt_key)
            self._post_pending.discard(cid)
            return

        try:
            gtc_shares = math.ceil(amount / bid_price * 100) / 100

            logger.info(
                "[POST: BID] %s — %s %.4f%% %s (PTB=%.4f→%.4f) via %s "
                "| GTC BUY $%.2f @ $%.4f (%.2f sh) | %.1fs past end",
                _pe_tag, winning_side,
                gap_data.get("gap_pct", 0), gap_data.get("direction", "?"),
                gap_data.get("ptb", 0), gap_data.get("settlement_price", 0),
                winner_source,
                amount, bid_price, gtc_shares, past_sec,
            )

            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None, self.api.place_limit_buy,
                winning_token, bid_price, amount,
            )

            order_id = result.get("orderID") or result.get("id", "")
            status = result.get("status", "")

            if not order_id:
                logger.warning(
                    "[POST: BID] %s — GTC order rejected: %s",
                    _pe_tag, result,
                )
                self._post_cooldowns[cid] = now.timestamp()
                return

            if status in ("matched", "MATCHED"):
                fill_price = bid_price
                fill_shares = amount / bid_price
                fill_amount = amount
                buy_tx_hash = None

                if order_id:
                    try:
                        od = await loop.run_in_executor(
                            None, self.api.get_order_status, order_id,
                        )
                        if isinstance(od, str):
                            try:
                                od = json.loads(od)
                            except Exception:
                                od = {}
                        if not isinstance(od, dict):
                            od = {}
                        real_matched = float(
                            od.get("size_matched") or od.get("sizeMatched") or 0
                        )
                        if real_matched > 0:
                            fill_shares = real_matched
                        trades = od.get("associate_trades") or []
                        if trades:
                            total_cost = sum(
                                float(t.get("price", 0)) * float(t.get("size", 0))
                                for t in trades
                            )
                            total_sz = sum(float(t.get("size", 0)) for t in trades)
                            if total_sz > 0:
                                fill_price = total_cost / total_sz
                                fill_shares = total_sz
                        fill_amount = fill_shares * fill_price

                        tx_hashes = (od.get("transactionsHashes")
                                     or od.get("transactionHashes"))
                        if tx_hashes and isinstance(tx_hashes, list):
                            buy_tx_hash = ",".join(h for h in tx_hashes if h)

                        logger.info(
                            "[POST: BID] %s — INSTANT MATCH! %.2f shares @ $%.4f ($%.4f)",
                            _pe_tag, fill_shares, fill_price, fill_amount,
                        )
                    except Exception as e:
                        logger.warning(
                            "[POST: BID] %s — order detail fetch failed, "
                            "using bid_price fallback: %s", _pe_tag, e,
                        )

                if not buy_tx_hash:
                    tx_hashes = (result.get("transactionsHashes")
                                 or result.get("transactionHashes"))
                    if tx_hashes and isinstance(tx_hashes, list):
                        buy_tx_hash = ",".join(h for h in tx_hashes if h)

                await self._record_maker_fill(
                    market, winning_token, winning_side,
                    fill_amount, fill_shares, fill_price,
                    buy_tx_hash, gap_data,
                )
                self._post_cooldowns[cid] = now.timestamp()
                return

            # Order is resting ("live")
            self._maker_orders[cid] = {
                "order_id": order_id,
                "token_id": winning_token,
                "side": winning_side,
                "amount": amount,
                "bid_price": bid_price,
                "posted_at": now.timestamp(),
                "market": market,
                "gap_data": gap_data,
                "last_matched": 0.0,
                "window_seconds": pe_cfg.window_seconds,
                "poll_interval": pe_cfg.poll_interval,
            }

            logger.info(
                "[POST: BID] %s — order %s LIVE (resting bid @ $%.4f, $%.2f)",
                _pe_tag, order_id[:16], bid_price, amount,
            )

            if not self._maker_monitor_running:
                self._maker_monitor_running = True
                asyncio.create_task(self._maker_monitor_loop())

        except Exception as e:
            logger.error("[POST: BID] %s — order placement failed: %s", _pe_tag, e)
            self._post_cooldowns[cid] = now.timestamp()
            err_msg = str(e).lower()
            if "does not exist" in err_msg or "not found" in err_msg:
                logger.info(
                    "[POST: BID] %s — orderbook gone, marking resolved",
                    _pe_tag,
                )
                self._resolved_cids.add(cid)
            else:
                self._maker_market_keys.discard(mkt_key)
        finally:
            self._post_pending.discard(cid)

    # ── INTERNAL: Maker Fill Monitor ────────────────────────────────────

    async def _maker_monitor_loop(self):
        """Background loop: polls resting maker orders for fills and cancels on timeout."""
        pe_cfg = self.config.post_end
        # Use min poll interval across all resting orders (re-evaluated each cycle)
        base_poll = pe_cfg.poll_interval

        logger.info("[POST: MONITOR] Started — polling every %.1fs", base_poll)
        try:
            while self._maker_orders:
                # Adaptive poll interval: use shortest per-order setting
                intervals = [o.get("poll_interval", base_poll) for o in self._maker_orders.values()]
                poll_interval = min(intervals) if intervals else base_poll
                await asyncio.sleep(poll_interval)
                now_ts = time.time()
                to_remove: list[str] = []

                for cid, order in list(self._maker_orders.items()):
                    order_id = order["order_id"]
                    market = order["market"]
                    tag = _mkt_tag(market)
                    elapsed = now_ts - order["posted_at"]

                    try:
                        loop = asyncio.get_running_loop()
                        status_data = await loop.run_in_executor(
                            None, self.api.get_order_status, order_id
                        )
                    except Exception as e:
                        logger.warning("[POST: MONITOR] %s — status check failed: %s", tag, e)
                        continue

                    if isinstance(status_data, str):
                        try:
                            status_data = json.loads(status_data)
                        except Exception:
                            status_data = {}
                    if not isinstance(status_data, dict):
                        status_data = {}

                    size_matched = float(
                        status_data.get("size_matched")
                        or status_data.get("sizeMatched")
                        or 0
                    )
                    order_status = status_data.get("status", "").lower()
                    prev_matched = order["last_matched"]

                    # Detect new fills
                    if size_matched > prev_matched:
                        new_shares = size_matched - prev_matched
                        fill_price = float(status_data.get("price") or order["bid_price"])
                        trades = status_data.get("associate_trades") or []
                        if isinstance(trades, str):
                            try:
                                trades = json.loads(trades)
                            except Exception:
                                trades = []
                        if isinstance(trades, list) and trades:
                            total_cost = sum(
                                float(t.get("price", 0)) * float(t.get("size", 0))
                                for t in trades if isinstance(t, dict)
                            )
                            total_sz = sum(float(t.get("size", 0)) for t in trades if isinstance(t, dict))
                            if total_sz > 0:
                                fill_price = total_cost / total_sz
                        new_amount = new_shares * fill_price

                        logger.info(
                            "[POST: FILL] %s — %s +%.2f shares @ $%.4f ($%.2f) "
                            "| total %.2f shares | %.1fs elapsed",
                            tag, order["side"], new_shares, fill_price,
                            new_amount, size_matched, elapsed,
                        )

                        buy_tx_hash = None
                        tx_hashes = (status_data.get("transactionsHashes")
                                     or status_data.get("transactionHashes"))
                        if tx_hashes and isinstance(tx_hashes, list):
                            buy_tx_hash = ",".join(h for h in tx_hashes if h)

                        await self._record_maker_fill(
                            market, order["token_id"], order["side"],
                            new_amount, new_shares, fill_price,
                            buy_tx_hash, order["gap_data"],
                        )
                        order["last_matched"] = size_matched
                        self._post_cooldowns[cid] = now_ts

                    # Fully filled or cancelled
                    if order_status in ("matched", "cancelled"):
                        logger.info(
                            "[POST: MONITOR] %s — order %s (%.2f shares filled)",
                            tag, order_status.upper(), size_matched,
                        )
                        to_remove.append(cid)
                        continue

                    # Timeout: cancel unfilled remainder
                    order_window = order.get("window_seconds", pe_cfg.window_seconds)
                    if elapsed >= order_window:
                        logger.info(
                            "[POST: MONITOR] %s — timeout %.0fs — cancelling "
                            "(%.2f shares filled of $%.2f order)",
                            tag, elapsed, size_matched, order["amount"],
                        )
                        try:
                            await loop.run_in_executor(
                                None, self.api.cancel_order, order_id
                            )
                        except Exception as ce:
                            logger.warning("[POST: MONITOR] cancel failed: %s", ce)

                        # Final fill check after cancel
                        try:
                            final = await loop.run_in_executor(
                                None, self.api.get_order_status, order_id
                            )
                            if isinstance(final, str):
                                try:
                                    final = json.loads(final)
                                except Exception:
                                    final = {}
                            if not isinstance(final, dict):
                                final = {}
                            final_matched = float(
                                final.get("size_matched")
                                or final.get("sizeMatched")
                                or 0
                            )
                            if final_matched > order["last_matched"]:
                                extra = final_matched - order["last_matched"]
                                fp = float(final.get("price") or order["bid_price"])
                                ftrades = final.get("associate_trades") or []
                                if isinstance(ftrades, str):
                                    try:
                                        ftrades = json.loads(ftrades)
                                    except Exception:
                                        ftrades = []
                                if isinstance(ftrades, list) and ftrades:
                                    tc = sum(
                                        float(t.get("price", 0)) * float(t.get("size", 0))
                                        for t in ftrades if isinstance(t, dict)
                                    )
                                    ts = sum(float(t.get("size", 0)) for t in ftrades if isinstance(t, dict))
                                    if ts > 0:
                                        fp = tc / ts
                                logger.info(
                                    "[POST: FILL] %s — +%.2f shares filled before cancel @ $%.4f",
                                    tag, extra, fp,
                                )
                                ftx = None
                                fhashes = (final.get("transactionsHashes")
                                           or final.get("transactionHashes"))
                                if fhashes and isinstance(fhashes, list):
                                    ftx = ",".join(h for h in fhashes if h)
                                await self._record_maker_fill(
                                    market, order["token_id"], order["side"],
                                    extra * fp, extra, fp,
                                    ftx, order["gap_data"],
                                )
                        except Exception:
                            pass

                        to_remove.append(cid)

                for cid in to_remove:
                    self._maker_orders.pop(cid, None)

        except Exception as e:
            logger.error("[POST: MONITOR] Loop crashed: %s", e, exc_info=True)
        finally:
            self._maker_monitor_running = False
            logger.info("[POST: MONITOR] Stopped (no active orders)")

    # ── SPORTS: Event Discovery ────────────────────────────────────────

    @staticmethod
    def _build_slug_candidates(league: str, home: str, away: str,
                               game_date: str) -> list[str]:
        """Build candidate Polymarket slugs from WS data.

        Polymarket slug format: ``{league}-{team1}-{team2}-{YYYY-MM-DD}``
        Team codes are lowercase 2-6 char abbreviations.  We try the
        raw WS team name lowered plus truncated variants (5, 4, 3 chars)
        in both team orderings.
        """
        if not league or (not home and not away):
            return []

        h = re.sub(r"[^a-z0-9]", "", home.lower().strip())
        a = re.sub(r"[^a-z0-9]", "", away.lower().strip())
        league = league.lower().strip()

        h_variants = list(dict.fromkeys(filter(None, [h, h[:5], h[:4], h[:3]])))
        a_variants = list(dict.fromkeys(filter(None, [a, a[:5], a[:4], a[:3]])))

        candidates: list[str] = []
        for hv in h_variants:
            for av in a_variants:
                candidates.append(f"{league}-{hv}-{av}-{game_date}")
                candidates.append(f"{league}-{av}-{hv}-{game_date}")

        seen: set[str] = set()
        return [s for s in candidates if not (s in seen or seen.add(s))]  # type: ignore[func-returns-value]

    @staticmethod
    def _score_team_match(home: str, away: str, text: str) -> int:
        """Score how well *home* and *away* team names match *text*.

        Returns 0-4:
          4 = both exact words found
          3 = both found as substrings (≥4 chars) or word-boundary matches
          2 = one exact + one substring
          1 = one found only
          0 = no match

        Short names (≤4 chars like ATH, CLE) require word-boundary matching
        to avoid false positives (e.g. "ath" matching inside "mcgrath").

        Also tries matching with spaces stripped (WS may send "LAZERCATS"
        while Polymarket uses "Lazer Cats").
        """
        text_lower = text.lower()
        text_words = set(text_lower.split())
        text_nospace = text_lower.replace(" ", "")
        h = home.lower().strip()
        a = away.lower().strip()

        if not h and not a:
            return 0

        def _is_sub(name: str) -> bool:
            """Check if *name* appears in text as a meaningful match.

            Short names (≤4 chars) must match on a word boundary (\\b)
            to avoid 'ath' matching inside 'mcgrath'.  Longer names
            are safe to match as plain substrings.
            """
            if not name or len(name) < 3:
                return False
            if len(name) <= 4:
                # Word-boundary match for short names
                return bool(re.search(r'\b' + re.escape(name) + r'\b', text_lower))
            # Longer names: plain substring is safe
            return name in text_lower or name.replace(" ", "") in text_nospace

        h_exact = h in text_words
        a_exact = a in text_words
        h_sub = _is_sub(h)
        a_sub = _is_sub(a)

        if h_exact and a_exact:
            return 4
        if h_sub and a_sub:
            return 3
        if (h_exact and a_sub) or (a_exact and h_sub):
            return 2
        if h_exact or a_exact or h_sub or a_sub:
            return 1
        return 0

    @staticmethod
    def _pick_winner_market(
        markets: list[dict],
        winner: str = "",
    ) -> tuple[dict | None, int | None]:
        """Pick the main winner market and winning outcome index.

        Handles two Polymarket patterns:

        **Pattern A — direct-team outcomes** (tennis, basketball, etc.)::

            question: "Marcondes vs Buse"
            outcomes: ["Marcondes", "Buse"]

        **Pattern B — Yes/No per-team** (soccer, hockey, etc.)::

            question: "Will Orlando City SC win on 2026-02-21?"
            outcomes: ["Yes", "No"]

        For Pattern A, returns (market, winning_outcome_idx).
        For Pattern B, returns the "Will [winner] win?" market with idx=0 (Yes).
        If ``winner`` is empty, returns the first suitable market with idx=None.
        """
        _skip_re = re.compile(
            r"\b(over|under|o/u|spread|handicap|set\s*\d|map\s*\d|game\s*\d|"
            r"winner:\s|toss|batter|sixes|more\s+markets|draw)\b",
            re.IGNORECASE,
        )

        winner_lower = winner.lower().strip()

        # --- Pass 1: look for Pattern A (direct team outcomes) ---
        best_a: dict | None = None
        best_a_score = -1
        best_a_idx: int | None = None

        for m in markets:
            question = m.get("question", "")
            outcomes = _parse_outcomes(m)
            if len(outcomes) != 2:
                continue
            if _skip_re.search(question):
                continue
            oc_lower = [o.lower() for o in outcomes]
            if "yes" in oc_lower or "no" in oc_lower:
                continue  # skip Yes/No — handled in Pass 2

            score = 10 - min(len(question), 10)
            if "vs" in question.lower():
                score += 5
            if m.get("outcomePrices"):
                score += 1

            if score > best_a_score:
                best_a_score = score
                best_a = m
                # Try to determine winning index
                best_a_idx = None
                if winner_lower:
                    for i, oc in enumerate(oc_lower):
                        if winner_lower in oc or oc in winner_lower:
                            best_a_idx = i
                            break

        if best_a is not None:
            return best_a, best_a_idx

        # --- Pass 2: look for Pattern B (Yes/No per-team) ---
        if winner_lower:
            winner_nospace = winner_lower.replace(" ", "")
            for m in markets:
                question = m.get("question", "")
                outcomes = _parse_outcomes(m)
                if len(outcomes) != 2:
                    continue
                oc_lower = [o.lower() for o in outcomes]
                if "yes" not in oc_lower:
                    continue
                if _skip_re.search(question):
                    continue

                q_lower = question.lower()
                q_nospace = q_lower.replace(" ", "")
                # Check if this market is about the winning team
                if winner_lower in q_lower or winner_nospace in q_nospace:
                    yes_idx = oc_lower.index("yes")
                    return m, yes_idx

        # No match
        return None, None

    async def _find_sports_event(
        self, event_info: dict,
    ) -> tuple[dict | None, dict | None, int | None]:
        """Find the Polymarket event + winner market for a game.

        Multi-strategy:
          1. **Slug lookup** — construct candidate slugs from WS data and
             do direct ``/events?slug=X`` lookups (fastest).
          2. **Paginated tag search** — fetch closed + active sports events
             (tag_id=1) with date-range, matching by team-name substring.
             Paginates to get past tennis-dominated first pages.

        Returns ``(event_dict, market_dict, winning_idx)`` or triple-None.
        ``winning_idx`` may be ``None`` if the winner can't be determined
        from ``event_info`` alone (caller falls back to score-based logic).
        """
        home = event_info.get("home_team", "").strip()
        away = event_info.get("away_team", "").strip()
        league = event_info.get("sport", "").strip()
        winner = event_info.get("winner", "").strip()

        if not home and not away:
            return None, None, None

        # ── Strategy 1: Slug-based direct lookup ──────────────────────
        det_ts = event_info.get("detected_at") or time.time()
        game_date = datetime.fromtimestamp(det_ts, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )
        slugs = self._build_slug_candidates(league, home, away, game_date)
        # Also try yesterday (UTC edge-case around midnight)
        prev_date = (
            datetime.fromtimestamp(det_ts, tz=timezone.utc) - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        slugs += self._build_slug_candidates(league, home, away, prev_date)

        for slug in slugs[:24]:       # cap to ≤24 attempts
            try:
                evts = await self.api.search_events(slug=slug, limit=1)
                if evts:
                    ev = evts[0]
                    mkt, w_idx = self._pick_winner_market(
                        ev.get("markets", []), winner,
                    )
                    if mkt:
                        logger.debug(
                            "[SPORTS: SLUG-HIT] %s → %s",
                            slug, ev.get("title", "?")[:50],
                        )
                        return ev, mkt, w_idx
            except Exception:
                pass

        # ── Strategy 2: Paginated event search ────────────────────────
        now_utc = datetime.now(timezone.utc)
        date_min = (now_utc - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        date_max = (now_utc + timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")

        best_ev: dict | None = None
        best_mkt: dict | None = None
        best_w_idx: int | None = None
        best_score = 0

        for search_cfg in [
            # Closed is the primary bucket (PM closes events fast after game end)
            {"closed": True, "active": False, "pages": 5},
            # Active as fallback (event may briefly remain open)
            {"closed": False, "active": True, "pages": 2},
        ]:
            for page in range(search_cfg["pages"]):
                try:
                    evts = await self.api.search_events(
                        tag_id=1,                   # Sports (tag label is broken)
                        closed=search_cfg["closed"],
                        active=search_cfg["active"],
                        end_date_min=date_min,
                        end_date_max=date_max,
                        limit=100,
                        offset=page * 100,
                        order="endDate",
                        ascending=False,
                    )
                except Exception:
                    break

                if not evts:
                    break

                for ev in evts:
                    ev_title = ev.get("title", "")
                    ev_markets = ev.get("markets", [])
                    # Match team names against title, questions, AND slug
                    ev_slug = ev.get("slug", "")
                    all_text = (
                        ev_title + " " + ev_slug.replace("-", " ") + " "
                        + " ".join(m.get("question", "") for m in ev_markets)
                    )
                    score = self._score_team_match(home, away, all_text)
                    if score > best_score:
                        mkt, w_idx = self._pick_winner_market(
                            ev_markets, winner,
                        )
                        if mkt:
                            best_score = score
                            best_ev = ev
                            best_mkt = mkt
                            best_w_idx = w_idx
                            if score >= 4:
                                break
                if best_score >= 4:
                    break
            if best_score >= 4:
                break

        if best_ev and best_mkt and best_score >= 2:
            logger.debug(
                "[SPORTS: SEARCH-HIT] score=%d | %s",
                best_score, best_ev.get("title", "?")[:50],
            )
            return best_ev, best_mkt, best_w_idx

        return None, None, None

    # ── SPORTS: Game-End Handler ───────────────────────────────────────

    async def on_sports_game_end(self, event_info: dict):
        """Handle a game-ending signal from the Sports WebSocket.

        Flow:
          1. Check if sports category is enabled
          2. Look up the relevant Polymarket market via Gamma API
          3. Determine the winning outcome from the game result
          4. Place a GTC LIMIT BUY on the winning token

        Called directly from SportsWSClient callback (not from scan loop).
        """
        cat_cfg = self.config.categories
        if not cat_cfg.sports_enabled:
            return

        sport = event_info.get("sport", "") or "unknown"
        if not cat_cfg.is_league_allowed(sport):
            logger.debug("[SPORTS: SKIP] League %r not in allowed list", sport)
            return

        title = event_info.get("title", "")
        event_id = event_info.get("event_id", "")
        market_id = event_info.get("market_id", "")

        # Try to find the Polymarket market for this game
        condition_id = None
        winning_token = None
        winning_side = None
        gamma_market = None
        found_w_idx: int | None = None
        event_title = title or f"Sports event {event_id[:16]}"
        event_slug = ""

        try:
            # Approach 1: If market_id is a Polymarket condition_id
            if market_id and len(market_id) > 20:
                gm = await self.api.get_gamma_market(market_id)
                if gm:
                    condition_id = gm.get("conditionId", market_id)
                    gamma_market = gm

            # Approach 2: Multi-strategy Gamma search
            # The Gamma tag= label filter is broken. Use tag_id=1 (Sports).
            # Games are marked closed=True almost immediately on Polymarket,
            # so we must search BOTH closed and active events.
            if not condition_id:
                found_ev, found_mkt, found_w_idx = await self._find_sports_event(
                    event_info,
                )
                if found_ev and found_mkt:
                    condition_id = found_mkt.get("conditionId", "")
                    event_title = found_ev.get("title", event_title)
                    event_slug = found_ev.get("slug", "")
                    gamma_market = found_mkt

            if not condition_id:
                logger.info(
                    "[SPORTS: MISS] No Polymarket market found for: %s",
                    title[:60] or event_id[:16],
                )
                return

            # Dedup
            if condition_id in self._post_cooldowns:
                return
            if condition_id in self._maker_orders:
                return
            if condition_id in self._post_pending:
                return
            self._post_pending.add(condition_id)

            # If we got the market from the event search, it may have
            # embedded data (conditionId, outcomes, clobTokenIds).
            # Only fall back to get_gamma_market if we don't have tokens.
            if not gamma_market or not gamma_market.get("clobTokenIds"):
                gm = await self.api.get_gamma_market(condition_id)
                if gm:
                    gamma_market = gm

            if not gamma_market:
                logger.warning(
                    "[SPORTS: SKIP] Can't fetch market details for %s",
                    condition_id[:16],
                )
                self._post_pending.discard(condition_id)
                return

            # Determine winning outcome from game result
            # Sports markets are typically "Team A wins" / "Team B wins"
            # or "Yes" / "No" on a specific outcome.
            outcomes_raw = gamma_market.get("outcomes", "")
            if isinstance(outcomes_raw, str):
                try:
                    outcomes = json.loads(outcomes_raw)
                except Exception:
                    outcomes = []
            else:
                outcomes = outcomes_raw or []

            token_ids_raw = gamma_market.get("clobTokenIds", "")
            if isinstance(token_ids_raw, str):
                try:
                    token_ids = json.loads(token_ids_raw)
                except Exception:
                    token_ids = []
            else:
                token_ids = token_ids_raw or []

            if len(outcomes) < 2 or len(token_ids) < 2:
                logger.warning(
                    "[SPORTS: SKIP] Incomplete market data for %s: "
                    "outcomes=%s tokens=%d",
                    condition_id[:16], outcomes, len(token_ids),
                )
                self._post_pending.discard(condition_id)
                return

            # ── PRICE-BASED winner detection ──────────────────────
            # Instead of fragile name-matching or score-parsing across
            # systems that use different naming conventions, we let the
            # MARKET tell us who won.  After a game ends, the winning
            # outcome token trades near $1.00 and the loser near $0.00.
            #
            # Strategy:
            #   1. Try Gamma outcomePrices first (fast, no extra call)
            #   2. If unavailable/ambiguous, fetch live CLOB midpoints
            #   3. Require clear separation (>$0.60 vs <$0.40) to act
            #   4. Skip if prices are ambiguous (protects against mid-game)
            #
            # This eliminates ALL dependence on WS team names, score
            # format, or abbreviation-to-fullname mapping.

            winning_idx = None
            price_method = ""

            # --- Source 1: Gamma outcomePrices (embedded in market data) ---
            raw_prices = gamma_market.get("outcomePrices", "")
            if isinstance(raw_prices, str):
                try:
                    gp = json.loads(raw_prices)
                except Exception:
                    gp = []
            else:
                gp = raw_prices or []

            if len(gp) >= 2:
                try:
                    p0 = float(gp[0])
                    p1 = float(gp[1])
                    if p0 >= 0.60 and p1 <= 0.40:
                        winning_idx = 0
                        price_method = f"gamma-prices({p0:.4f} vs {p1:.4f})"
                    elif p1 >= 0.60 and p0 <= 0.40:
                        winning_idx = 1
                        price_method = f"gamma-prices({p0:.4f} vs {p1:.4f})"
                except (ValueError, TypeError):
                    pass

            # --- Source 2: Live CLOB midpoints (real-time fallback) ---
            if winning_idx is None and len(token_ids) >= 2:
                try:
                    book0 = await self.api.get_book_liquidity(token_ids[0])
                    book1 = await self.api.get_book_liquidity(token_ids[1])
                    mid0 = book0.get("midpoint", 0) or 0
                    mid1 = book1.get("midpoint", 0) or 0

                    if mid0 >= 0.60 and mid1 <= 0.40:
                        winning_idx = 0
                        price_method = f"clob-mid({mid0:.4f} vs {mid1:.4f})"
                    elif mid1 >= 0.60 and mid0 <= 0.40:
                        winning_idx = 1
                        price_method = f"clob-mid({mid0:.4f} vs {mid1:.4f})"
                    elif mid0 > 0 or mid1 > 0:
                        logger.info(
                            "[SPORTS: SKIP] Prices ambiguous for %s — "
                            "mid0=$%.4f mid1=$%.4f (need >$0.60 vs <$0.40). "
                            "Game may still be live or market illiquid.",
                            event_title[:40], mid0, mid1,
                        )
                        self._post_pending.discard(condition_id)
                        return
                except Exception as e:
                    logger.debug(
                        "[SPORTS] CLOB price fetch failed for %s: %s",
                        event_title[:30], e,
                    )

            if winning_idx is None:
                logger.info(
                    "[SPORTS: SKIP] Can't determine winner for %s — "
                    "no price signal from Gamma or CLOB. outcomes=%s",
                    event_title[:40], outcomes,
                )
                self._post_pending.discard(condition_id)
                return

            logger.info(
                "[SPORTS: PRICE] %s → %s wins (idx=%d) via %s",
                event_title[:40], outcomes[winning_idx],
                winning_idx, price_method,
            )

            winning_token = token_ids[winning_idx]
            winning_side = outcomes[winning_idx]

            # Place GTC LIMIT BUY
            bid_price = cat_cfg.sports_maker_price
            amount = cat_cfg.sports_max_bet
            # Safety cap: never bid above $0.995 (same hardcoded ceiling as crypto)
            if bid_price > 0.995:
                bid_price = 0.995
            if amount < 1.0:
                self._post_pending.discard(condition_id)
                return

            gtc_shares = math.ceil(amount / bid_price * 100) / 100

            logger.info(
                "[SPORTS: BID] %s — %s | GTC BUY $%.2f @ $%.4f (%.2f sh) "
                "| winner: %s | %s",
                event_title[:50], sport or "?",
                amount, bid_price, gtc_shares,
                winning_side, event_info.get("status", ""),
            )

            t_start = time.time()
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None, self.api.place_limit_buy,
                winning_token, bid_price, amount,
            )

            order_id = result.get("orderID") or result.get("id", "")
            total_ms = (time.time() - t_start) * 1000

            if not order_id:
                logger.warning(
                    "[SPORTS: BID] %s — order rejected: %s (%.0fms)",
                    event_title[:40], result, total_ms,
                )
                self._post_cooldowns[condition_id] = time.time()
                self._post_pending.discard(condition_id)
                return

            status = result.get("status", "")
            logger.info(
                "[SPORTS: BID] %s — order %s %s (%.0fms latency)",
                event_title[:40], order_id[:16], status or "LIVE", total_ms,
            )

            gap_data = {
                "gap_pct": 0,
                "direction": "sports",
                "winning_side": winning_side,
                "settlement_price": 0,
                "ptb": 0,
            }

            if status in ("matched", "MATCHED"):
                fill_price = bid_price
                fill_shares = amount / bid_price

                # Build a minimal market-like object for DB recording
                sports_market = _SportsMarketProxy(
                    condition_id=condition_id,
                    token_id=winning_token,
                    token_id_alt=token_ids[1 - winning_idx] if len(token_ids) > 1 else "",
                    event_title=event_title,
                    event_slug=event_slug,
                    asset=sport or "sports",
                    neg_risk=False,
                )

                await self._record_maker_fill(
                    sports_market, winning_token, winning_side,
                    amount, fill_shares, fill_price,
                    None, gap_data,
                )
                self._post_cooldowns[condition_id] = time.time()
            else:
                # Build proxy for the resting order
                sports_market = _SportsMarketProxy(
                    condition_id=condition_id,
                    token_id=winning_token,
                    token_id_alt=token_ids[1 - winning_idx] if len(token_ids) > 1 else "",
                    event_title=event_title,
                    event_slug=event_slug,
                    asset=sport or "sports",
                    neg_risk=False,
                )

                self._maker_orders[condition_id] = {
                    "order_id": order_id,
                    "token_id": winning_token,
                    "side": winning_side,
                    "amount": amount,
                    "bid_price": bid_price,
                    "posted_at": time.time(),
                    "market": sports_market,
                    "gap_data": gap_data,
                    "last_matched": 0.0,
                    "window_seconds": cat_cfg.sports_window_seconds,
                    "poll_interval": cat_cfg.sports_poll_interval,
                }

                if not self._maker_monitor_running:
                    self._maker_monitor_running = True
                    asyncio.create_task(self._maker_monitor_loop())

            self._post_pending.discard(condition_id)

        except Exception as e:
            logger.error(
                "[SPORTS: BID] Error handling game end for %s: %s",
                title[:40] or event_id[:16], e, exc_info=True,
            )
            if condition_id:
                self._post_pending.discard(condition_id)

    # ── INTERNAL: DB Recording ──────────────────────────────────────────

    async def _record_maker_fill(
        self, market, token_id: str, side: str,
        amount: float, shares: float, price: float,
        buy_tx_hash: str | None, gap_data: dict,
    ):
        """Record a maker fill in the database (trade + open position)."""
        if not self._db:
            return
        edge = 1.0 - price
        gap_pct = gap_data.get("gap_pct") if gap_data else None
        gap_dir = gap_data.get("direction") if gap_data else None
        try:
            await self._db.record_fill(
                condition_id=market.condition_id,
                token_id=token_id,
                event_title=market.event_title,
                outcome=side,
                asset=market.asset,
                action="MAKER_SNIPE",
                amount=amount,
                shares=shares,
                price=price,
                edge=edge,
                event_slug=market.event_slug,
                buy_tx_hash=buy_tx_hash,
                gap_pct=gap_pct,
                gap_direction=gap_dir,
                outcome_index=0 if side == "Up" else 1,
                neg_risk=market.neg_risk,
            )
            logger.info(
                "[POST: DB] Recorded %s %s | %.2f shares @ $%.4f ($%.2f) | edge $%.4f",
                side, market.event_title[:40], shares, price, amount, edge,
            )
        except Exception as db_err:
            logger.error("[POST: DB] Write failed: %s", db_err)
