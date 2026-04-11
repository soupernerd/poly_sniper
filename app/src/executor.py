"""Sniper executor -- buys winning shares at below $1 for redemption profit.

Execution flow:
  1. Receive SnipeOpportunity from scanner (already passed oracle gate)
  2. Validate: balance, position count
  3. Execute FOK market buy
  4. Record in database
"""

import asyncio
import json
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial

from src.api import PolymarketAPI
from src.config import Config
from src.database import Database
from src.hft_log import format_market_label, short_cid
from src.mode2_metrics import record_latency as record_mode2_latency
from src.scanner import SnipeOpportunity
from src.trade_gate import evaluate_execution_gate

from typing import TYPE_CHECKING, Callable, Optional
if TYPE_CHECKING:
    from src.price_feed import BinancePriceFeed

logger = logging.getLogger(__name__)


class Executor:
    """Executes snipe trades and tracks results."""

    def __init__(self, config: Config, api: PolymarketAPI, db: Database,
                 price_feed: "BinancePriceFeed | None" = None,
                 is_market_terminal: Optional[Callable[[str], bool]] = None):
        self.config = config
        self.api = api
        self.db = db
        self._price_feed = price_feed
        self._is_market_terminal = is_market_terminal
        self._cached_balance: float = 0.0
        self._balance_cache_ts: float = 0.0
        self._BALANCE_CACHE_TTL: float = 10.0  # seconds
        self._zero_balance_streak: int = 0

        # Dedicated thread pool for CLOB signing / HTTP calls so they
        # never compete with other run_in_executor work.
        self._clob_pool = ThreadPoolExecutor(
            max_workers=20, thread_name_prefix="clob"
        )

        # Active snipe guard: prevents two concurrent executions for the
        # same condition_id (race between WS instant path and poll scan path).
        self._active_snipes: set[str] = set()

        # Balance lock: serializes the balance-check ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ order window so two
        # parallel snipes can't both read the same cached balance and overspend.
        self._balance_lock = asyncio.Lock()

        # Stats
        self.trades_executed = 0
        self.total_spent = 0.0
        
        # HFT Burst guard
        self._hft_burst_count = 0
        self._hft_relist_retrying: set[str] = set()

    @staticmethod
    def _safe_float(v) -> float:
        try:
            return float(v or 0.0)
        except Exception:
            return 0.0

    @staticmethod
    def _is_service_not_ready_error(exc: Exception) -> bool:
        """True when upstream CLOB is temporarily not ready (HTTP 425)."""
        status_code = getattr(exc, "status_code", None)
        if status_code == 425:
            return True
        api_msg = str(getattr(exc, "error_message", "") or "").lower()
        err_msg = str(exc).lower()
        return "service not ready" in api_msg or "service not ready" in err_msg

    async def _submit_hft_buy_with_retry(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        token_id: str,
        amount: float,
        max_price: float,
        mkt: str,
        cid_short: str,
    ) -> dict:
        """Submit HFT market buy; retry once on transient 425 service-not-ready."""
        try:
            return await loop.run_in_executor(
                self._clob_pool, self.api.place_market_buy, token_id, amount, max_price
            )
        except Exception as e:
            if not self._is_service_not_ready_error(e):
                raise
            logger.warning(
                "[HFT-RETRY] mkt=%s cid=%s reason=service_not_ready retry_in_ms=200",
                mkt,
                cid_short,
            )
            await asyncio.sleep(0.2)
            return await loop.run_in_executor(
                self._clob_pool, self.api.place_market_buy, token_id, amount, max_price
            )

    @classmethod
    def _fill_from_trade_rows(cls, rows) -> tuple[float, float, float]:
        """Return (shares, avg_price, total_cost) from trade rows."""
        if not isinstance(rows, list):
            return 0.0, 0.0, 0.0
        total_shares = 0.0
        total_cost = 0.0
        for tr in rows:
            if not isinstance(tr, dict):
                continue
            px = cls._safe_float(tr.get("price"))
            sz = cls._safe_float(tr.get("size"))
            if px <= 0 or sz <= 0:
                continue
            total_shares += sz
            total_cost += (px * sz)
        if total_shares <= 0 or total_cost <= 0:
            return 0.0, 0.0, 0.0
        return total_shares, (total_cost / total_shares), total_cost

    @classmethod
    def _extract_order_fill(
        cls, order_payload: dict | str | None,
    ) -> tuple[float, float, float, bool, str]:
        """Parse CLOB order payload into fill tuple.

        Returns:
            (shares, avg_price, total_cost, exact_trade_breakdown, source_tag)
        """
        if isinstance(order_payload, str):
            try:
                order_payload = json.loads(order_payload)
            except Exception:
                order_payload = {}
        if not isinstance(order_payload, dict):
            return 0.0, 0.0, 0.0, False, "none"

        # Best source: per-trade fills.
        assoc = order_payload.get("associate_trades") or order_payload.get("associateTrades") or []
        if isinstance(assoc, str):
            try:
                assoc = json.loads(assoc)
            except Exception:
                assoc = []
        if isinstance(assoc, list):
            trade_rows = [t for t in assoc if isinstance(t, dict)]
            s, a, c = cls._fill_from_trade_rows(trade_rows)
            if s > 0 and a > 0 and c > 0:
                return s, a, c, True, "order.associate_trades"

        # Fallback: order-level matched summary (can be limit-price-biased).
        shares = cls._safe_float(
            order_payload.get("size_matched") or order_payload.get("sizeMatched")
        )
        avg = cls._safe_float(
            order_payload.get("avg_price")
            or order_payload.get("avgPrice")
            or order_payload.get("price")
        )
        if shares > 0 and avg > 0:
            return shares, avg, (shares * avg), False, "order.summary"
        return 0.0, 0.0, 0.0, False, "none"

    async def _fill_from_wallet_position(
        self,
        *,
        condition_id: str,
        token_id: str,
        is_hft: bool,
    ) -> tuple[float, float, float, str]:
        """Fallback truth source: wallet position row for this condition/token."""
        wallet = (self.config.wallet_address or "").strip()
        if not wallet:
            return 0.0, 0.0, 0.0, "none"

        attempts = 8 if is_hft else 4
        sleep_s = 0.15 if is_hft else 0.30

        for i in range(attempts):
            try:
                rows = await self.api.get_positions(wallet)
            except Exception as e:
                logger.debug("[FILL CONFIRM] wallet position fetch %d failed: %s", i + 1, e)
                rows = []

            for row in rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("conditionId", "")) != str(condition_id):
                    continue

                row_token = str(row.get("tokenId") or row.get("asset") or "")
                if row_token and str(token_id) and row_token != str(token_id):
                    continue

                shares = self._safe_float(row.get("size"))
                avg = self._safe_float(row.get("avgPrice"))
                cost = self._safe_float(row.get("initialValue"))
                if cost <= 0 and shares > 0 and avg > 0:
                    cost = round(shares * avg, 6)
                if avg <= 0 and shares > 0 and cost > 0:
                    avg = cost / shares
                if shares > 0 and avg > 0:
                    return shares, avg, cost, "wallet.position"

            if i < (attempts - 1):
                await asyncio.sleep(sleep_s)

        return 0.0, 0.0, 0.0, "none"

    # -- Internal Helpers --

    async def _confirm_fill(
        self,
        condition_id: str,
        *,
        token_id: str,
        event_title: str,
        outcome: str,
        asset: str,
        event_slug: str,
        buy_tx_hash: str | None,
        gap_pct: float | None,
        gap_direction: str | None,
        timeframe_seconds: int = 0,
        hft_barrier_pct: float | None = None,
        hft_cap_price: float | None = None,
        hft_path: str | None = None,
        hft_trigger_ask: float | None = None,
        outcome_index: int,
        neg_risk: bool,
        is_hft: bool = False,
        fill_shares: float = 0.0,
        fill_price: float = 0.0,
        order_id: str | None = None,
        mode2_trigger_ts: float = 0.0,
        mode2_spring_ts: float = 0.0,
        mode2_submit_ts: float = 0.0,
        mode2_ack_ts: float = 0.0,
    ):
        """Write trade + position only after CLOB truth fill data is available."""
        persisted_fill = False
        try:
            # Persist inflight marker
            await self.db.set_state(f"inflight:{condition_id}", condition_id)

            confirmed_shares = float(fill_shares or 0.0)
            confirmed_avg = float(fill_price or 0.0)
            confirmed_cost = round(confirmed_shares * confirmed_avg, 6) if (
                confirmed_shares > 0 and confirmed_avg > 0
            ) else 0.0
            source_tag = "initial_response"

            # Always attempt an order-status truth pull when order_id exists.
            # We prefer explicit trade breakdown over summary fields so DB/UI
            # are not anchored to max/limit price guesses.
            if order_id:
                loop = asyncio.get_running_loop()
                attempts = 40 if is_hft else 10
                retry_sleep = 0.1 if is_hft else 0.2
                have_exact_breakdown = False
                fallback_shares = 0.0
                fallback_avg = 0.0
                fallback_cost = 0.0
                fallback_source = ""
                for i in range(attempts):
                    try:
                        final = await loop.run_in_executor(
                            self._clob_pool, self.api.get_order_status, order_id
                        )

                        # 1) Parse order payload directly.
                        sm, fp, fc, exact, src = self._extract_order_fill(final)
                        if sm > 0 and fp > 0:
                            if exact:
                                confirmed_shares = sm
                                confirmed_avg = fp
                                confirmed_cost = fc
                                source_tag = src
                                have_exact_breakdown = True
                                break
                            # Hold summary fallback in case nothing better appears.
                            fallback_shares = sm
                            fallback_avg = fp
                            fallback_cost = fc
                            fallback_source = src

                        # 2) If order payload lacks expanded trades, query trade
                        # history filtered to this order id and token.
                        if i == 0 or i == (attempts - 1):
                            order_trades = await loop.run_in_executor(
                                self._clob_pool,
                                partial(self.api.get_order_trades, order_id, token_id=token_id),
                            )
                            ts, ta, tc = self._fill_from_trade_rows(order_trades)
                            if ts > 0 and ta > 0 and tc > 0:
                                confirmed_shares = ts
                                confirmed_avg = ta
                                confirmed_cost = tc
                                source_tag = "trades.endpoint"
                                have_exact_breakdown = True
                                break
                    except Exception as e:
                        logger.debug("[FILL CONFIRM] order status retry %d failed: %s", i + 1, e)
                    if i < (attempts - 1):
                        await asyncio.sleep(retry_sleep)


                # If we have an exact truth from associate_trades, we use it (already set above).
                # If we don't, but we have a fallback summary from polling that is DIFFERENT
                # from the initial result, we assume the initial result was truncated.
                if not have_exact_breakdown and fallback_shares > 0 and fallback_avg > 0:
                    if confirmed_shares <= 0 or abs(confirmed_shares - fallback_shares) < 0.0001:
                       # Basically keep initial or use fallback if they are likely the same
                       confirmed_shares = fallback_shares
                       confirmed_avg = fallback_avg
                       confirmed_cost = fallback_cost
                       source_tag = fallback_source

            # Last fallback: use wallet position truth if CLOB payloads are delayed.
            if confirmed_shares <= 0 or confirmed_avg <= 0:
                ws, wa, wc, wsrc = await self._fill_from_wallet_position(
                    condition_id=condition_id, token_id=token_id, is_hft=is_hft
                )
                if ws > 0 and wa > 0:
                    confirmed_shares = ws
                    confirmed_avg = wa
                    confirmed_cost = wc if wc > 0 else round(ws * wa, 6)
                    source_tag = wsrc

            if confirmed_shares <= 0 or confirmed_avg <= 0:
                logger.warning(
                    "[FILL CONFIRM] Missing fill truth for %s; keeping inflight marker for recovery",
                    condition_id[:8],
                )
                return

            if confirmed_cost <= 0:
                confirmed_cost = round(confirmed_shares * confirmed_avg, 6)

            await self.db.record_confirmed_fill(
                condition_id=condition_id,
                token_id=token_id,
                event_title=event_title,
                outcome=outcome,
                asset=asset,
                action="HFT" if is_hft else "SNIPE",
                shares=confirmed_shares,
                avg_price=confirmed_avg,
                total_cost=confirmed_cost,
                event_slug=event_slug,
                buy_tx_hash=buy_tx_hash,
                gap_pct=gap_pct,
                gap_direction=gap_direction,
                timeframe_seconds=timeframe_seconds,
                hft_barrier_pct=hft_barrier_pct,
                hft_cap_price=hft_cap_price,
                hft_path=hft_path,
                hft_trigger_ask=hft_trigger_ask,
                outcome_index=outcome_index,
                neg_risk=neg_risk,
            )
            persisted_fill = True
            if is_hft and mode2_ack_ts > 0:
                now_ts = time.perf_counter()
                cid_short = short_cid(condition_id, 16)
                ack_to_db_ms = (now_ts - mode2_ack_ts) * 1000.0
                record_mode2_latency(
                    stage="ack_to_db",
                    latency_ms=ack_to_db_ms,
                    condition_id=condition_id,
                    event_title=event_title,
                )
                logger.info(
                    "[HFT-LATENCY] cid=%s stage=ack_to_db ms=%.1f",
                    cid_short,
                    ack_to_db_ms,
                )
                if mode2_trigger_ts > 0:
                    trigger_to_db_ms = (now_ts - mode2_trigger_ts) * 1000.0
                    record_mode2_latency(
                        stage="trigger_to_db",
                        latency_ms=trigger_to_db_ms,
                        condition_id=condition_id,
                        event_title=event_title,
                    )
                    logger.info(
                        "[HFT-LATENCY] cid=%s stage=trigger_to_db ms=%.1f",
                        cid_short,
                        trigger_to_db_ms,
                    )

            prefix = "[HFT-FILL]" if is_hft else "[SNIPE FILL]"
            logger.info(
                "%s %s - %.4f sh @ $%.4f ($%.4f) confirmed via clob (%s)",
                prefix, condition_id[:16], confirmed_shares, confirmed_avg, confirmed_cost, source_tag,
            )

            # HFT has its own profit target, main bot has Post-Fill Offload.
            hft_profit_pct = self._safe_float(
                getattr(self.config.trend, "hft_flip_profit_pct", 0.0)
            )
            mode2_only_runtime = bool(
                getattr(self.config, "hft_mode2_only_runtime", False)
            )
            if is_hft and hft_profit_pct > 0:
                sell_price = round(confirmed_avg * (1.0 + hft_profit_pct / 100.0), 3)
                sell_price = min(0.99, max(0.01, sell_price))

                safe_shares = math.floor(confirmed_shares * 1000000) / 1000000
                if safe_shares <= 0:
                    logger.warning("[HFT FLIP] No sellable shares after floor for %s", condition_id[:16])
                    return
                if safe_shares < 5:
                    logger.info(
                        "[HFT FLIP] Skip relist %s: %.6f shares < 5-share relist threshold",
                        condition_id[:16], safe_shares,
                    )
                    return
                if (safe_shares * sell_price) < 1.0:
                    logger.info(
                        "[HFT FLIP] Skip relist %s: notional $%.4f < $1.00 minimum",
                        condition_id[:16], safe_shares * sell_price,
                    )
                    return

                # HFT relists are scheduled with wallet-readiness gates.
                # This avoids immediate post-fill relist spam while wallet shares
                # are still zero/not-ready in position endpoints.
                logger.info(
                    "[HFT FLIP] Queued relist plan: %.6f shares @ $%.3f (+%.1f%%) for %s",
                    safe_shares, sell_price, hft_profit_pct, condition_id[:16],
                )
                asyncio.create_task(
                    self._run_hft_relist_plan(
                        condition_id=condition_id,
                        token_id=token_id,
                        event_title=event_title,
                        sell_price=sell_price,
                        target_shares=safe_shares,
                        profit_pct=hft_profit_pct,
                    )
                )
            elif is_hft and mode2_only_runtime:
                logger.info(
                    "[HFT] Legacy offloaders disabled for hft-only runtime (%s)",
                    condition_id[:16],
                )
            else:
                pct_offload = int(getattr(self.config.offload, "pct_offload", 0) or 0)
                if pct_offload > 0:
                    await self._try_pct_offload_sell(
                        condition_id=condition_id,
                        token_id=token_id,
                        event_title=event_title,
                        chain_shares=confirmed_shares,
                        chain_avg=confirmed_avg,
                    )
                else:
                    await self._try_offload_sell(
                        condition_id=condition_id,
                        token_id=token_id,
                        event_title=event_title,
                        chain_shares=confirmed_shares,
                        chain_avg=confirmed_avg,
                    )

        except Exception as e:
            logger.error("[CONFIRM FILL ERROR] %s: %s", condition_id[:16], e)
        finally:
            self._active_snipes.discard(condition_id)
            if persisted_fill:
                try:
                    await self.db.delete_state(f"inflight:{condition_id}")
                except Exception:
                    pass
    async def hft_execute_fast(self, opp: "SnipeOpportunity", market: "TrackedMarket") -> dict:
        """Absolute zero-gate HFT fast-path for small bets."""
        snipe_token = opp.winning_token or market.token_id
        amount = float(opp.override_amount or 5.0)
        max_price = float(self.config.trend.get_hft_max_price_for(market.asset, market.duration_seconds))

        # Dynamic max-price cap (respect globally enabled setting even in HFT mode)
        sg_cfg = self.config.execution
        if sg_cfg.dynamic_max_price_enabled and opp.best_ask > 0:
            dyn_ceiling = opp.best_ask * (1.0 + sg_cfg.dynamic_max_price_pct)
            if dyn_ceiling < max_price:
                logger.info(
                    "[DYNAMIC MAX HFT] %s - cap ceiling $%.4f -> $%.4f (eval ask $%.4f, +%.1f%%)",
                    market.event_title[:40], max_price, dyn_ceiling,
                    opp.best_ask, sg_cfg.dynamic_max_price_pct * 100,
                )
                max_price = min(max_price, dyn_ceiling)

        trigger_ts = float(getattr(opp, "mode2_trigger_ts", 0.0) or 0.0)
        spring_ts = float(getattr(opp, "mode2_spring_ts", 0.0) or 0.0)
        cid_short = short_cid(market.condition_id)
        mkt = format_market_label(market)
        entry_path = (
            "trap" if (spring_ts > 0 and trigger_ts > 0 and (spring_ts - trigger_ts) > 0.0001)
            else "instant"
        )
        reason = (getattr(opp, "reason", "") or "").strip()

        logger.info(
            "[HFT-FIRE] mkt=%s cid=%s side=%s path=%s ask=%.4f cap=%.4f amt=%.2f%s",
            mkt,
            cid_short,
            opp.winning_side or "Side",
            entry_path,
            opp.best_ask,
            max_price,
            amount,
            (f" | {reason}" if reason else ""),
        )

        try:
            loop = asyncio.get_running_loop()
            submit_ts = time.perf_counter()
            if spring_ts > 0:
                spring_to_submit_ms = (submit_ts - spring_ts) * 1000.0
                record_mode2_latency(
                    stage="spring_to_submit",
                    latency_ms=spring_to_submit_ms,
                    condition_id=market.condition_id,
                    event_title=market.event_title,
                )
                logger.info(
                    "[HFT-LATENCY] mkt=%s cid=%s stage=spring_to_submit ms=%.1f",
                    mkt, cid_short, spring_to_submit_ms,
                )
            elif trigger_ts > 0:
                trigger_to_submit_ms = (submit_ts - trigger_ts) * 1000.0
                record_mode2_latency(
                    stage="trigger_to_submit",
                    latency_ms=trigger_to_submit_ms,
                    condition_id=market.condition_id,
                    event_title=market.event_title,
                )
                logger.info(
                    "[HFT-LATENCY] mkt=%s cid=%s stage=trigger_to_submit ms=%.1f",
                    mkt, cid_short, trigger_to_submit_ms,
                )

            # Fire buy immediately (no routing/GTC fallback roundtrip).
            # Retry once only for transient "service not ready" (425).
            result = await self._submit_hft_buy_with_retry(
                loop=loop,
                token_id=snipe_token,
                amount=amount,
                max_price=max_price,
                mkt=mkt,
                cid_short=cid_short,
            )
            ack_ts = time.perf_counter()
            submit_to_ack_ms = (ack_ts - submit_ts) * 1000.0
            record_mode2_latency(
                stage="submit_to_ack",
                latency_ms=submit_to_ack_ms,
                condition_id=market.condition_id,
                event_title=market.event_title,
            )
            logger.info(
                "[HFT-ACK] mkt=%s cid=%s submit_ack_ms=%.1f",
                mkt, cid_short, submit_to_ack_ms,
            )

            status = result.get("status", "")
            if status not in ("matched", "MATCHED"):
                return {"ok": False, "action": "REJECTED", "reason": f"CLOB rejected: {status}"}
            order_id = result.get("orderID") or result.get("id", "")

            # Extract actual fill data: no guessing and no synthetic fallbacks.
            fill_price = float(result.get("price") or 0)
            fill_shares = float(
                result.get("size_matched")
                or result.get("sizeMatched")
                or 0
            )

            # CLOB often returns "matched" without fill detail.
            # Query the order for definitive fill data.
            if fill_price <= 0 or fill_shares <= 0:
                if order_id:
                    try:
                        final = await loop.run_in_executor(
                            self._clob_pool, self.api.get_order_status, order_id
                        )
                        sm = float(
                            final.get("size_matched")
                            or final.get("sizeMatched")
                            or 0
                        )
                        fp = float(final.get("price") or 0)
                        if sm > 0 and fp > 0:
                            fill_shares = sm
                            fill_price = fp
                            logger.info(
                                "[HFT-FILL] mkt=%s cid=%s shares=%.6f avg=%.4f source=order_status",
                                mkt, cid_short, fill_shares, fill_price,
                            )
                    except Exception as oq_err:
                        logger.debug("[HFT] Order query failed: %s", oq_err)

            # Log exact fill from first-attempt query
            if fill_shares > 0 and fill_price > 0:
                logger.info(
                    "[HFT-FILL] mkt=%s cid=%s shares=%.4f avg=%.4f source=submit_response",
                    mkt, cid_short, fill_shares, fill_price,
                )

            # Confirm to DB + flip sell in background (unified path)
            tx_hashes = result.get("transactionsHashes") or result.get("transactionHashes")
            buy_tx_hash = ",".join(h for h in tx_hashes) if tx_hashes else None

            self.trades_executed += 1
            self.total_spent += amount
            self._invalidate_balance_cache()

            asyncio.create_task(self._confirm_fill(
                market.condition_id,
                token_id=snipe_token,
                event_title=market.event_title,
                outcome=opp.winning_side or "Up",
                asset=market.asset,
                event_slug=market.event_slug,
                buy_tx_hash=buy_tx_hash,
                gap_pct=float(opp.gap_pct or 0.0),
                gap_direction=str(opp.gap_direction or ""),
                timeframe_seconds=int(getattr(opp, "timeframe_seconds", 0) or market.duration_seconds or 0),
                hft_barrier_pct=getattr(opp, "hft_barrier_pct", None),
                hft_cap_price=getattr(opp, "hft_cap_price", None),
                hft_path=getattr(opp, "hft_entry_path", None),
                hft_trigger_ask=getattr(opp, "hft_trigger_ask", None),
                outcome_index=0 if opp.winning_side == "Up" else 1,
                neg_risk=market.neg_risk,
                is_hft=True,
                fill_shares=fill_shares,
                fill_price=fill_price,
                order_id=order_id,
                mode2_trigger_ts=trigger_ts,
                mode2_spring_ts=spring_ts,
                mode2_submit_ts=submit_ts,
                mode2_ack_ts=ack_ts,
            ))
            result_action = "SNIPED_PENDING" if (fill_shares <= 0 or fill_price <= 0) else "SNIPED"
            return {
                "ok": True,
                "action": result_action,
                "amount": amount,
                "shares": fill_shares,
                "avg_price": fill_price,
                "edge": (1.0 - fill_price) if fill_price > 0 else 0.0,
            }

        except Exception as e:
            # Distinguish between API rejections (order failed to fill) and true system errors
            err_msg = str(e).lower()
            err_type = type(e).__name__
            status_code = getattr(e, "status_code", None)
            api_msg = getattr(e, "error_message", None)
            if "fully filled" in err_msg or "liquidity" in err_msg:
                logger.warning(
                    "[HFT-FAIL] mkt=%s cid=%s type=fok_no_fill err=%s status=%s api=%s",
                    mkt, cid_short, e, status_code, api_msg,
                )
            else:
                logger.error(
                    "[HFT-ERROR] mkt=%s cid=%s type=%s err=%s status=%s api=%s",
                    mkt, cid_short, err_type, e, status_code, api_msg,
                )
            return {"ok": False, "action": "ERROR", "reason": str(e)}

    def _get_cached_balance(self) -> float:
        """Return balance from cache if fresh, otherwise fetch and cache.

        Guards against transient 0.00 reads from the balance endpoint by
        requiring repeated zero confirmations before replacing a known-good
        positive cached balance.
        """
        now = time.monotonic()
        if (now - self._balance_cache_ts) < self._BALANCE_CACHE_TTL:
            return self._cached_balance
        balance = float(self.api.get_balance() or 0.0)

        if balance > 0:
            self._zero_balance_streak = 0
            self._cached_balance = balance
            self._balance_cache_ts = now
            return balance

        # Re-check once immediately to reduce one-off transport glitches.
        second = float(self.api.get_balance() or 0.0)
        if second > 0:
            self._zero_balance_streak = 0
            self._cached_balance = second
            self._balance_cache_ts = now
            return second

        self._zero_balance_streak += 1
        if self._cached_balance > 0 and self._zero_balance_streak < 3:
            # Keep last known-good value for a couple of failed reads.
            # This avoids false economic-pause trips on transient API zeroes.
            logger.warning(
                "[BALANCE GUARD] Zero balance read (%d/3); retaining cached $%.2f",
                self._zero_balance_streak, self._cached_balance,
            )
            self._balance_cache_ts = now
            return self._cached_balance

        # Zero confirmed repeatedly (or no prior positive cache): accept it.
        if self._cached_balance > 0:
            logger.warning(
                "[BALANCE GUARD] Zero balance confirmed after %d reads; accepting $0.00 (last cached $%.2f)",
                self._zero_balance_streak, self._cached_balance,
            )
        self._cached_balance = 0.0
        self._balance_cache_ts = now
        return 0.0

    def _invalidate_balance_cache(self):
        """Force next balance check to hit the network."""
        self._balance_cache_ts = 0.0

    def prime_balance_cache(self, balance: float):
        """Seed the balance cache from startup pre-flight check."""
        self._cached_balance = float(balance or 0.0)
        self._zero_balance_streak = 0
        self._balance_cache_ts = time.monotonic()

    # Removed redundant _confirm_fill - logic consolidated into _confirm_fill above.

    @staticmethod
    def _is_order_rejected(*, order_id: str, status: str, err: str) -> bool:
        if not order_id:
            return True
        if err:
            return True
        return status in {"rejected", "failed", "killed", "cancelled", "canceled"}

    @staticmethod
    def _is_balance_allowance_error(text: str) -> bool:
        t = str(text or "").lower()
        return (
            ("not enough balance" in t)
            or ("insufficient balance" in t)
            or ("balance / allowance" in t)
            or ("allowance" in t)
        )

    async def _wallet_sellable_shares(
        self,
        *,
        condition_id: str,
        token_id: str,
        attempts: int = 3,
        sleep_s: float = 0.20,
    ) -> float:
        """Return wallet-truth shares for this condition/token."""
        wallet = (self.config.wallet_address or "").strip()
        if not wallet:
            return 0.0

        for i in range(max(1, int(attempts))):
            try:
                rows = await self.api.get_positions(wallet)
            except Exception as e:
                logger.debug("[SELL RETRY] wallet position fetch %d failed: %s", i + 1, e)
                rows = []

            for row in rows:
                if not isinstance(row, dict):
                    continue
                if str(row.get("conditionId", "")) != str(condition_id):
                    continue
                row_token = str(row.get("tokenId") or row.get("asset") or "")
                if row_token and str(token_id) and row_token != str(token_id):
                    continue
                size = self._safe_float(row.get("size"))
                if size > 0:
                    return math.floor(size * 1000000) / 1000000

            if i < (attempts - 1):
                await asyncio.sleep(sleep_s)

        return 0.0

    async def _post_limit_sell_with_wallet_retry(
        self,
        *,
        log_tag: str,
        event_title: str,
        condition_id: str,
        token_id: str,
        sell_price: float,
        requested_shares: float,
        min_shares: float = 0.0,
    ) -> dict:
        """Post GTC sell, retrying once on balance/allowance rejection."""
        requested_shares = math.floor(float(requested_shares or 0.0) * 1000000) / 1000000
        if requested_shares <= 0:
            return {
                "ok": False,
                "order_id": "",
                "status": "",
                "err": "no_shares",
                "shares": 0.0,
                "from_retry": False,
            }

        async def _post_once(shares: float, *, from_retry: bool) -> dict:
            try:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    self._clob_pool,
                    self.api.place_limit_sell,
                    token_id,
                    sell_price,
                    shares,
                )
            except Exception as e:
                return {
                    "ok": False,
                    "order_id": "",
                    "status": "",
                    "err": str(e),
                    "shares": shares,
                    "from_retry": from_retry,
                }

            order_id = str(result.get("orderID", "") or result.get("order_id", "") or "")
            status = str(result.get("status", "")).lower()
            err = str(result.get("errorMsg") or result.get("error") or "")
            if self._is_order_rejected(order_id=order_id, status=status, err=err):
                return {
                    "ok": False,
                    "order_id": order_id,
                    "status": status,
                    "err": err,
                    "shares": shares,
                    "from_retry": from_retry,
                }
            return {
                "ok": True,
                "order_id": order_id,
                "status": status,
                "err": "",
                "shares": shares,
                "from_retry": from_retry,
            }

        first = await _post_once(requested_shares, from_retry=False)
        if first["ok"]:
            return first

        why = first.get("err") or first.get("status") or "unknown"
        if not self._is_balance_allowance_error(str(why)):
            return first

        logger.warning(
            "[%s] Sell blocked by balance/allowance for %s. Retrying once with wallet clamp (%s)",
            log_tag, event_title[:40], why,
        )

        wallet_shares = await self._wallet_sellable_shares(
            condition_id=condition_id,
            token_id=token_id,
            attempts=4,
            sleep_s=0.25,
        )
        retry_shares = math.floor(min(requested_shares, wallet_shares) * 1000000) / 1000000
        if retry_shares <= 0:
            return {
                "ok": False,
                "order_id": "",
                "status": first.get("status", ""),
                "err": "wallet_shares_zero_after_retry",
                "shares": 0.0,
                "from_retry": True,
            }
        if min_shares > 0 and retry_shares < min_shares:
            return {
                "ok": False,
                "order_id": "",
                "status": first.get("status", ""),
                "err": f"wallet_shares_below_min:{retry_shares:.6f}<{min_shares:.6f}",
                "shares": retry_shares,
                "from_retry": True,
            }
        if (retry_shares * sell_price) < 1.0:
            return {
                "ok": False,
                "order_id": "",
                "status": first.get("status", ""),
                "err": f"wallet_notional_below_min:${retry_shares * sell_price:.4f}",
                "shares": retry_shares,
                "from_retry": True,
            }

        if retry_shares < requested_shares:
            logger.info(
                "[%s] Retry clamped shares %.6f -> %.6f for %s",
                log_tag, requested_shares, retry_shares, event_title[:40],
            )

        return await _post_once(retry_shares, from_retry=True)

    async def _run_hft_relist_plan(
        self,
        *,
        condition_id: str,
        token_id: str,
        event_title: str,
        sell_price: float,
        target_shares: float,
        profit_pct: float,
    ) -> None:
        """Timed HFT relist plan with wallet-readiness gating.

        Plan (from fill-confirm time):
          - Attempt 1 at +10s
          - Attempt 2 at +20s
          - Attempt 3 at +37s
          - Optional cleanup attempt at +90s
        """
        if condition_id in self._hft_relist_retrying:
            return
        self._hft_relist_retrying.add(condition_id)
        try:
            schedule_seconds = (10.0, 20.0, 37.0)
            cleanup_second = 90.0
            plan_start = time.monotonic()
            cid_short = condition_id[:16]
            logger.info(
                "[HFT FLIP] Relist plan start for %s: +10s/+20s/+37s (wallet-ready gate)",
                cid_short,
            )

            for idx, target_second in enumerate(schedule_seconds, start=1):
                elapsed = time.monotonic() - plan_start
                sleep_s = max(0.0, target_second - elapsed)
                if sleep_s > 0:
                    await asyncio.sleep(sleep_s)

                wallet_shares = await self._wallet_sellable_shares(
                    condition_id=condition_id,
                    token_id=token_id,
                    attempts=3,
                    sleep_s=0.25,
                )
                if wallet_shares <= 0:
                    logger.info(
                        "[HFT FLIP] Attempt %d deferred for %s: wallet shares 0 (waiting)",
                        idx, cid_short,
                    )
                    continue

                requested = min(target_shares, wallet_shares)
                requested = math.floor(float(requested or 0.0) * 1000000) / 1000000
                if requested < 5.0:
                    logger.info(
                        "[HFT FLIP] Attempt %d deferred for %s: wallet shares %.6f < 5-share minimum",
                        idx, cid_short, requested,
                    )
                    continue
                if (requested * sell_price) < 1.0:
                    logger.info(
                        "[HFT FLIP] Attempt %d deferred for %s: notional $%.4f < $1.00",
                        idx, cid_short, requested * sell_price,
                    )
                    continue

                logger.info(
                    "[HFT FLIP] RELIST ATTEMPT %d: %.6f @ $%.3f (+%.1f%%) | wallet=%.6f",
                    idx, requested, sell_price, profit_pct, wallet_shares,
                )
                post = await self._post_limit_sell_with_wallet_retry(
                    log_tag=f"HFT FLIP A{idx}",
                    event_title=event_title,
                    condition_id=condition_id,
                    token_id=token_id,
                    sell_price=sell_price,
                    requested_shares=requested,
                    min_shares=5.0,
                )
                if post.get("ok"):
                    order_id = str(post.get("order_id") or "")
                    await self.db.set_offload_order(condition_id, token_id, order_id, sell_price)
                    logger.info(
                        "[HFT FLIP] Posted relist attempt %d: %.6f @ $%.3f | %s",
                        idx,
                        float(post.get("shares") or 0.0),
                        sell_price,
                        order_id[:16],
                    )
                    return

                err = str(post.get("err") or "")
                logger.warning(
                    "[HFT FLIP] Relist attempt %d failed for %s (status=%s err=%s)",
                    idx, cid_short, post.get("status") or "-", err or "-",
                )

            # Optional low-priority cleanup pass.
            elapsed = time.monotonic() - plan_start
            cleanup_sleep_s = max(0.0, cleanup_second - elapsed)
            if cleanup_sleep_s > 0:
                await asyncio.sleep(cleanup_sleep_s)

            wallet_shares = await self._wallet_sellable_shares(
                condition_id=condition_id,
                token_id=token_id,
                attempts=2,
                sleep_s=0.30,
            )
            cleanup_req = min(target_shares, wallet_shares)
            cleanup_req = math.floor(float(cleanup_req or 0.0) * 1000000) / 1000000

            if cleanup_req >= 5.0 and (cleanup_req * sell_price) >= 1.0:
                logger.info(
                    "[HFT FLIP] CLEANUP ATTEMPT: %.6f @ $%.3f (+%.1f%%) | wallet=%.6f",
                    cleanup_req, sell_price, profit_pct, wallet_shares,
                )
                post = await self._post_limit_sell_with_wallet_retry(
                    log_tag="HFT FLIP CLEANUP",
                    event_title=event_title,
                    condition_id=condition_id,
                    token_id=token_id,
                    sell_price=sell_price,
                    requested_shares=cleanup_req,
                    min_shares=5.0,
                )
                if post.get("ok"):
                    order_id = str(post.get("order_id") or "")
                    await self.db.set_offload_order(condition_id, token_id, order_id, sell_price)
                    logger.info(
                        "[HFT FLIP] Cleanup relist posted: %.6f @ $%.3f | %s",
                        float(post.get("shares") or 0.0),
                        sell_price,
                        order_id[:16],
                    )
                    return
                logger.warning(
                    "[HFT FLIP] Cleanup relist failed for %s (status=%s err=%s)",
                    cid_short,
                    post.get("status") or "-",
                    str(post.get("err") or "-"),
                )

            logger.warning(
                "[HFT FLIP] Relist plan exhausted for %s (no post)",
                cid_short,
            )
        finally:
            self._hft_relist_retrying.discard(condition_id)
    async def _try_offload_sell(
        self,
        *,
        condition_id: str,
        token_id: str,
        event_title: str,
        chain_shares: float,
        chain_avg: float,
    ):
        """Post a GTC limit sell immediately after fill confirmation.

        Uses confirmed fill values directly (not DB). The sell price is
        determined by the offload tier config based on fill price.
        If the sell fails or offload is disabled, we silently continue -
        the position will redeem normally.
        """
        sell_price = self.config.offload.resolve_sell_price(chain_avg)
        if sell_price is None:
            return

        if sell_price <= chain_avg + 0.02:
            logger.debug(
                "[OFFLOAD] Margin too thin (sell $%.2f, bought $%.2f) - skipping %s",
                sell_price, chain_avg, event_title[:40],
            )
            return

        shares_to_sell = math.floor(chain_shares * 1000000) / 1000000
        if shares_to_sell <= 0:
            return
        if (shares_to_sell * sell_price) < 1.0:
            logger.debug(
                "[OFFLOAD] Notional too small ($%.4f) -- skipping %s",
                shares_to_sell * sell_price, event_title[:40],
            )
            return

        post = await self._post_limit_sell_with_wallet_retry(
            log_tag="OFFLOAD",
            event_title=event_title,
            condition_id=condition_id,
            token_id=token_id,
            sell_price=sell_price,
            requested_shares=shares_to_sell,
        )
        if not post.get("ok"):
            logger.warning(
                "[OFFLOAD] Sell rejected for %s (status=%s err=%s)",
                event_title[:40], post.get("status") or "-", post.get("err") or "-",
            )
            return

        order_id = str(post.get("order_id") or "")
        posted_shares = float(post.get("shares") or 0.0)
        logger.info(
            "[OFFLOAD] Posted sell: %.6f shares @ $%.2f for %s | order %s%s",
            posted_shares, sell_price, event_title[:40], order_id[:16],
            " (retry)" if post.get("from_retry") else "",
        )
        await self.db.set_offload_order(condition_id, token_id, order_id, sell_price)

    async def _try_pct_offload_sell(
        self,
        *,
        condition_id: str,
        token_id: str,
        event_title: str,
        chain_shares: float,
        chain_avg: float,
    ):
        """Post a GTC limit sell from configured percent markup over fill price."""
        sell_price = self.config.offload.resolve_pct_sell_price(chain_avg)
        if sell_price is None:
            return

        if sell_price <= chain_avg + 0.02:
            logger.debug(
                "[OFFLOAD %%] Margin too thin (sell $%.2f, bought $%.2f) - skipping %s",
                sell_price, chain_avg, event_title[:40],
            )
            return

        shares_to_sell = math.floor(chain_shares * 1000000) / 1000000
        if shares_to_sell <= 0:
            return
        if (shares_to_sell * sell_price) < 1.0:
            logger.debug(
                "[OFFLOAD %%] Notional too small ($%.4f) -- skipping %s",
                shares_to_sell * sell_price, event_title[:40],
            )
            return

        pct = int(getattr(self.config.offload, "pct_offload", 0) or 0)
        post = await self._post_limit_sell_with_wallet_retry(
            log_tag="OFFLOAD %",
            event_title=event_title,
            condition_id=condition_id,
            token_id=token_id,
            sell_price=sell_price,
            requested_shares=shares_to_sell,
        )
        if not post.get("ok"):
            logger.warning(
                "[OFFLOAD %%] Sell rejected for %s (status=%s err=%s)",
                event_title[:40], post.get("status") or "-", post.get("err") or "-",
            )
            return

        order_id = str(post.get("order_id") or "")
        posted_shares = float(post.get("shares") or 0.0)
        logger.info(
            "[OFFLOAD %%] Posted sell: %.6f shares @ $%.2f (%d%% above $%.4f) for %s | order %s%s",
            posted_shares, sell_price, pct, chain_avg, event_title[:40], order_id[:16],
            " (retry)" if post.get("from_retry") else "",
        )
        await self.db.set_offload_order(condition_id, token_id, order_id, sell_price)

    async def execute(self, opp: SnipeOpportunity) -> dict:
        """Execute a snipe trade.

        Returns dict with result info:
            {ok: bool, action: str, amount: float, shares: float,
             avg_price: float, edge: float, tx: str}
        """
        market = opp.market
        cid = market.condition_id
        is_hft = getattr(opp, "is_hft_runner", False)

        is_resolved = False
        if self._is_market_terminal:
            try:
                is_resolved = bool(self._is_market_terminal(cid))
            except Exception:
                is_resolved = False
        gate = evaluate_execution_gate(
            config=self.config,
            market=market,
            is_hft_runner=is_hft,
            is_resolved=is_resolved,
        )
        if not gate.allowed:
            return {
                "ok": False,
                "action": "SKIP",
                "reason": gate.reason or gate.code or "execution gate blocked",
            }

        # -- Gate: Prevent concurrent executions for the same market --
        # Races: WS instant-snipe fires during scan(), or two WS events
        # arrive for the same cid within the same async tick.
        if cid in self._active_snipes:
            return {"ok": False, "action": "SKIP",
                    "reason": f"Already executing snipe for this market"}
                    
        # -- HFT Burst Rate Limit Guard --
        if is_hft:
            limit = self.config.trend.hft_max_concurrent
            if self._hft_burst_count >= limit:
                return {"ok": False, "action": "SKIP",
                        "reason": f"HFT Rate Limit Burst Triggered ({self._hft_burst_count}/{limit})"}
            self._hft_burst_count += 1
            
        self._active_snipes.add(cid)
        try:
            # Keep HFT on dedicated low-latency route; main bot stays on unified path.
            if is_hft:
                result = await self.hft_execute_fast(opp, market)
            else:
                result = await self._execute_inner(opp, market)
        except Exception:
            if is_hft: self._hft_burst_count -= 1
            self._active_snipes.discard(cid)
            raise
        
        if is_hft: self._hft_burst_count -= 1
        
        # If snipe didn't succeed, clear the guard now (no _confirm_fill).
        # On success, _confirm_fill's finally block will clear it after
        # the confirmed DB write completes.
        if not result.get("ok"):
            self._active_snipes.discard(cid)
        return result

    async def _execute_inner(self, opp: SnipeOpportunity, market) -> dict:
        """Inner execution logic (guarded by _active_snipes in execute())."""
        # Canonical gate function, reused by all entry paths.
        is_resolved = False
        if self._is_market_terminal:
            try:
                is_resolved = bool(self._is_market_terminal(market.condition_id))
            except Exception:
                is_resolved = False
        gate = evaluate_execution_gate(
            config=self.config,
            market=market,
            is_hft_runner=bool(getattr(opp, "is_hft_runner", False)),
            is_resolved=is_resolved,
        )
        if not gate.allowed:
            return {"ok": False, "action": "SKIP", "reason": gate.reason or gate.code or "execution gate blocked"}

        # -- Post-end sniper uses its own config section --
        is_post_end = getattr(opp, "is_post_end", False)
        pe_cfg = self.config.post_end

        # Dynamic sizing: fraction of ask depth, clamped to [1, max_bet]
        # Use timeframe-aware max_bet if configured, else global default
        if getattr(opp, "override_amount", None) is not None:
            amount = float(opp.override_amount)
        elif is_post_end:
            # NOTE: Post-end maker path now bypasses executor entirely
            # (scanner posts GTC directly). This branch is a safety fallback.
            effective_max_bet = pe_cfg.max_bet
            fill_frac = 1.0  # Maker posts full amount
            raw = opp.ask_depth * fill_frac
            amount = max(0.01, min(raw, effective_max_bet))
        else:
            effective_max_bet = self.config.execution.get_max_bet(
                market.duration_seconds
            )
            fill_frac = self.config.execution.fill_fraction
            raw = opp.ask_depth * fill_frac
            amount = max(0.01, min(raw, effective_max_bet))
        # Use the dynamically determined winning token, not the default token_id
        snipe_token = opp.winning_token or market.winning_token or market.token_id
        snipe_side = opp.winning_side or market.winning_side or market.outcome

        # -- Gate: Position limit + Balance check + Swing guard (parallel) --
        # Fire all pre-execution checks at once.  Swing guard is an async
        # REST call that doesn't depend on the gate results, so running it
        # in parallel saves ~100-300ms when it's enabled.
        # Combined DB query returns (total_open, open_for_this_market) in one
        # round-trip instead of two separate queries.
        pos_count_task = self.db.count_open_positions_combined(market.condition_id)
        balance_task = asyncio.get_running_loop().run_in_executor(
            self._clob_pool, self._get_cached_balance
        )

        sg_cfg = self.config.execution
        swing_guard_active = sg_cfg.swing_guard_enabled and not is_post_end
        swing_guard_task = (
            self.api.get_book_liquidity(snipe_token)
            if swing_guard_active else None
        )

        if swing_guard_task:
            pos_result, balance, fresh_book = await asyncio.gather(
                pos_count_task, balance_task, swing_guard_task,
                return_exceptions=True,
            )
            # If the swing guard REST call failed, don't block the trade
            if isinstance(fresh_book, Exception):
                logger.debug("[SWING GUARD] fetch error (proceeding): %s", fresh_book)
                fresh_book = None
            # If DB or balance failed, abort ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â these are hard gates
            if isinstance(pos_result, Exception):
                raise pos_result
            if isinstance(balance, Exception):
                raise balance
            open_count, existing = pos_result
        else:
            (open_count, existing), balance = await asyncio.gather(
                pos_count_task, balance_task
            )
            fresh_book = None

        # Gate: Already have an open position for this exact market
        if existing > 0:
            return {"ok": False, "action": "SKIP",
                    "reason": f"Already have open position for this market"}

        if open_count >= self.config.execution.max_concurrent:
            return {"ok": False, "action": "SKIP",
                    "reason": f"At position limit ({open_count}/{self.config.execution.max_concurrent})"}

        self._cached_balance = balance
        if balance < amount:
            if balance >= 0.01:
                amount = balance  # Use remaining balance
                logger.info("Reduced snipe to $%.2f (remaining balance)", amount)
            else:
                return {"ok": False, "action": "SKIP",
                        "reason": f"Insufficient balance: ${balance:.2f}"}

        # -- Gate: Minimum amount ($1 CLOB minimum) --
        if amount < 0.01:
            return {"ok": False, "action": "SKIP",
                    "reason": f"Amount ${amount:.2f} below CLOB minimum $0.01"}

        # -- EXECUTION --
        try:
            logger.info(
                "%sSNIPING %s -- %s @ $%.4f | $%.2f | edge $%.4f | %s",
                "[POST-END] " if is_post_end else "",
                snipe_side, market.event_title[:50], opp.best_ask,
                amount, opp.edge, opp.reason,
            )

            # Oracle consensus already checked by scanner's oracle gate.

            # Choose order routing: FOK (fastest) or skip-to-GTC (thin book)
            is_hft_runner = getattr(opp, "is_hft_runner", False)
            if is_hft_runner:
                max_price = float(self.config.trend.get_hft_max_price_for(market.asset, market.duration_seconds))
            elif is_post_end:
                max_price = pe_cfg.max_buy_price
            else:
                max_price = self.config.execution.get_max_buy_price(market.duration_seconds)

            # Dynamic max-price cap (separate from swing guard):
            # cap execution ceiling relative to eval best_ask with zero extra I/O.
            if (not is_post_end
                    and sg_cfg.dynamic_max_price_enabled
                    and opp.best_ask > 0):
                dyn_ceiling = opp.best_ask * (1.0 + sg_cfg.dynamic_max_price_pct)
                if dyn_ceiling < max_price:
                    logger.info(
                        "[DYNAMIC MAX] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â cap ceiling $%.4f -> $%.4f "
                        "(eval ask $%.4f, +%.1f%%)",
                        market.event_title[:40],
                        max_price,
                        dyn_ceiling,
                        opp.best_ask,
                        sg_cfg.dynamic_max_price_pct * 100,
                    )
                max_price = min(max_price, dyn_ceiling)

            # ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ Swing guard: evaluate the pre-fetched book data ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬
            # The REST call already ran in parallel with gate checks above.
            # Now just evaluate the result ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â pure memory, ~0ms.
            if swing_guard_active and fresh_book and not isinstance(fresh_book, Exception):
                fresh_ask = fresh_book.get("best_ask", 0) if isinstance(fresh_book, dict) else 0
                if fresh_ask > 0:
                    swing = fresh_ask - opp.best_ask
                    swing_pct = abs(swing) / opp.best_ask if opp.best_ask > 0 else 0
                    if swing_pct > max_pct and not is_hft_runner:
                        direction = "DROP" if swing < 0 else "SPIKE"
                        logger.warning(
                            "[SWING GUARD] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â %s %.1f%% since eval "
                            "(was $%.4f, now $%.4f, limit %.0f%%) ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ABORTING",
                            market.event_title[:40], direction,
                            swing_pct * 100, opp.best_ask, fresh_ask, max_pct * 100,
                        )
                        return {"ok": False, "action": "SWING_GUARD",
                                "reason": f"Price {direction.lower()} {swing_pct*100:.1f}% > {max_pct*100:.0f}% limit"}
                    logger.info(
                        "[SWING GUARD] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â OK (swing=%.2f%%, fresh_ask=$%.4f)",
                        market.event_title[:30], swing_pct * 100, fresh_ask,
                    )

            gtc_fallback = False
            loop = asyncio.get_running_loop()

            # Smart routing: if ask depth < bet amount, FOK will certainly
            # be killed.  Skip straight to GTC to save ~100ms round-trip.
            # Post-end always goes direct GTC for speed (skip FOK round-trip).
            # Important: GTC needs a positive limit price.
            thin_book = opp.ask_depth < amount * 1.15
            gtc_allowed = max_price > 0
            if not self.config.execution.gtc_fallback_enabled or is_hft_runner:
                gtc_allowed = False  # Toggle OFF or HFT-Runner (FOK only)
            skip_fok = (is_post_end and not is_hft_runner) or (thin_book and gtc_allowed)
            if thin_book and not is_post_end and not gtc_allowed:
                reason = "global GTC fallback toggle is OFF" if not self.config.execution.gtc_fallback_enabled else \
                         "is HFT mode" if is_hft_runner else \
                         "max_buy_price is OFF (<=0)"
                logger.info(
                    "[THIN BOOK] Only $%.0f available on book but we want $%.0f "
                    "for %s. Trying FOK only because GTC fallback is disabled (%s).",
                    opp.ask_depth, amount, market.event_title[:40], reason,
                )
            elif skip_fok and not is_post_end:
                logger.info(
                    "[THIN BOOK] Only $%.0f available on book but we want $%.0f "
                    "ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â skipping instant-fill, posting limit order instead for %s",
                    opp.ask_depth, amount, market.event_title[:40],
                )

            if not skip_fok:
                try:
                    result = await loop.run_in_executor(
                        self._clob_pool, self.api.place_market_buy,
                        snipe_token, amount, max_price
                    )
                except Exception as fok_err:
                    err_msg = str(fok_err).lower()
                    if ("fully filled" in err_msg
                            or "killed" in err_msg
                            or "invalid amounts" in err_msg):
                        if is_hft_runner:
                            logger.info(
                                "[INSTANT-FILL FAILED] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â FOK failed and GTC fallback "
                                "is disabled (HFT Mode). Skipping.",
                                market.event_title[:40],
                            )
                            return {"ok": False, "action": "SKIP",
                                    "reason": "FOK failed and GTC fallback is disabled (HFT Mode)"}
                        
                        if not self.config.execution.gtc_fallback_enabled:
                            logger.info(
                                "[INSTANT-FILL FAILED] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â FOK failed and global "
                                "GTC fallback toggle is OFF. Skipping.",
                                market.event_title[:40],
                            )
                            return {"ok": False, "action": "SKIP",
                                    "reason": "FOK failed and global GTC fallback is OFF"}

                        if max_price <= 0:
                            logger.info(
                                "[INSTANT-FILL FAILED] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â FOK failed and max_buy_price "
                                "is OFF (<=0), so GTC fallback is disabled. Skipping.",
                                market.event_title[:40],
                            )
                            return {"ok": False, "action": "SKIP",
                                    "reason": "FOK failed and GTC fallback disabled when max_buy_price is OFF"}
                        
                        logger.info(
                            "[INSTANT-FILL FAILED] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â not enough shares at $%.4f to fill "
                            "$%.2f in one shot. Falling back to limit order...",
                            market.event_title[:40], opp.best_ask, amount,
                        )
                        skip_fok = True  # Fall through to GTC below
                    else:
                        logger.warning("[ORDER ERROR] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â unexpected: %s",
                                       market.event_title[:40], fok_err)
                        raise  # re-raise non-FOK errors

            if skip_fok:
                # GTC limit uses max_price as the ceiling.
                limit_price = max_price
                if limit_price <= 0:
                    logger.warning(
                        "[GTC SKIP] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â invalid limit price %.4f (max_buy_price OFF). "
                        "Skipping instead of placing a zero-price limit order.",
                        market.event_title[:40], limit_price,
                    )
                    return {"ok": False, "action": "SKIP",
                            "reason": "GTC fallback requires positive max_buy_price"}

                # CLOB requires minimum 5 shares for GTC orders
                import math
                gtc_shares = math.ceil(amount / limit_price * 100) / 100
                if gtc_shares < 5:
                    logger.warning(
                        "[TOO SMALL] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â $%.2f only buys %.1f shares at $%.4f, "
                        "but Polymarket requires at least 5 shares for limit orders. Skipping.",
                        market.event_title[:40], amount, gtc_shares, limit_price,
                    )
                    return {"ok": False, "action": "GTC_MIN_SIZE",
                            "reason": f"GTC size {gtc_shares:.2f} < 5 share minimum"}

                logger.info(
                    "[LIMIT ORDER] Posting buy at $%.4f (our ceiling) for %s "
                    "ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ask was $%.4f, waiting up to a few seconds for a seller...",
                    limit_price, market.event_title[:40], opp.best_ask,
                )
                result = await loop.run_in_executor(
                    self._clob_pool, self.api.place_limit_buy,
                    snipe_token, limit_price, amount
                )
                gtc_fallback = True

            # Parse result
            status = result.get("status", "")
            order_id = result.get("orderID") or result.get("id", "")
            if status not in ("matched", "MATCHED"):
                # GTC may return "live" -- cancel resting order
                if order_id and gtc_fallback:
                    await loop.run_in_executor(self._clob_pool, self.api.cancel_order, order_id)
                    # ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ Race-condition guard: check if the order was filled
                    # between placement and cancel.  The CLOB processes
                    # matches atomically, so if a seller hit our bid before
                    # the cancel arrived, size_matched will be > 0 even
                    # though the initial status was "live".
                    final = await loop.run_in_executor(
                        self._clob_pool, self.api.get_order_status, order_id
                    )
                    size_matched = float(
                        final.get("size_matched")
                        or final.get("sizeMatched")
                        or 0
                    )
                    if size_matched > 0:
                        # GTC order was filled before cancel ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â treat as success
                        fill_price = float(final.get("price") or opp.best_ask)
                        est_shares = size_matched
                        # Complement match check ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â log if CLOB reports more
                        # shares than expected; clamp to notional-consistent estimate
                        if fill_price > 0:
                            expected_shares = amount / fill_price
                            if est_shares > expected_shares * 1.10:
                                logger.info(
                                    "[COMPLEMENT MATCH GTC] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â size_matched=%.2f "
                                    "but $%.2f/$%.4f=%.2f; using estimate",
                                    market.event_title[:40], est_shares,
                                    amount, fill_price, expected_shares,
                                )
                                est_shares = expected_shares
                        fill_price = amount / est_shares if est_shares > 0 else fill_price
                        amount_actual = amount  # Always the dollars we sent
                        logger.info(
                            "[BOUGHT via GTC race-fill] %s %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â a seller matched %.2f shares @ $%.4f "
                            "($%.2f spent) just before we cancelled! edge $%.4f/share",
                            snipe_side, market.event_title[:45],
                            est_shares, fill_price, amount_actual,
                            1.0 - fill_price,
                        )
                        self.trades_executed += 1
                        self.total_spent += amount_actual
                        self._invalidate_balance_cache()

                        buy_tx_hash = None
                        tx_hashes = (final.get("transactionsHashes")
                                     or final.get("transactionHashes"))
                        if tx_hashes and isinstance(tx_hashes, list):
                            buy_tx_hash = ",".join(h for h in tx_hashes if h)

                        # -- HFT RUNNER: Flip via unified confirm path --
                        is_hft = getattr(opp, "is_hft_runner", False)

                        asyncio.create_task(self._confirm_fill(
                            market.condition_id,
                            token_id=snipe_token,
                            event_title=market.event_title,
                            outcome=snipe_side,
                            asset=market.asset,
                            event_slug=market.event_slug,
                            buy_tx_hash=buy_tx_hash,
                            gap_pct=opp.gap_pct,
                            gap_direction=opp.gap_direction,
                            timeframe_seconds=int(getattr(opp, "timeframe_seconds", 0) or market.duration_seconds or 0),
                            hft_barrier_pct=getattr(opp, "hft_barrier_pct", None),
                            hft_cap_price=getattr(opp, "hft_cap_price", None),
                            hft_path=getattr(opp, "hft_entry_path", None),
                            hft_trigger_ask=getattr(opp, "hft_trigger_ask", None),
                            outcome_index=0 if snipe_side == "Up" else 1,
                            neg_risk=market.neg_risk,
                            is_hft=is_hft,
                            fill_shares=est_shares,
                            fill_price=fill_price,
                            order_id=order_id,
                        ))

                        return {"ok": True, "action": "SNIPED",
                                "amount": amount_actual, "shares": est_shares,
                                "avg_price": fill_price, "edge": 1.0 - fill_price,
                                "tx": buy_tx_hash or ""}
                    logger.warning(
                        "[NO FILL] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â limit order got no sellers within timeout. "
                        "Book may have moved away or liquidity dried up. Order cancelled.",
                        market.event_title[:40],
                    )
                else:
                    logger.warning(
                        "[NO FILL] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â order returned status '%s' (not matched). "
                        "No shares were bought.",
                        market.event_title[:40], status,
                    )
                return {"ok": False, "action": "REJECTED",
                        "reason": f"Order status: {status}"}

            # ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ Extract actual fill data from CLOB response ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬
            # The CLOB may return price/size_matched for matched FOK orders.
            # Use those when available, and fall back to order query.
            # If still unavailable, defer accounting to confirmation truth.
            fill_price = float(result.get("price") or 0)
            fill_shares = float(
                result.get("size_matched")
                or result.get("sizeMatched")
                or 0
            )

            # GTC orders often return "matched" without fill detail in the
            # initial response.  Query the order for definitive fill data
            # (same call the race-condition guard already uses).
            # FOK orders can also lack fill fields ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â apply the same logic.
            if fill_price <= 0 or fill_shares <= 0:
                if order_id:
                    try:
                        final = await loop.run_in_executor(
                            self._clob_pool, self.api.get_order_status, order_id
                        )
                        sm = float(
                            final.get("size_matched")
                            or final.get("sizeMatched")
                            or 0
                        )
                        fp = float(final.get("price") or 0)
                        if sm > 0 and fp > 0:
                            fill_shares = sm
                            fill_price = fp
                            logger.info(
                                "[FILL CONFIRMED] Got actual fill data: %.2f shares @ $%.4f",
                                fill_shares, fill_price,
                            )
                    except Exception as oq_err:
                        logger.debug("Order query failed: %s", oq_err)

            route_tag = "GTC" if gtc_fallback else "FOK"

            if fill_price > 0 and fill_shares > 0:
                # ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ Estimate shares & price for immediate display ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚ÂÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬
                # CLOB `price` = order limit price, `size_matched` = shares
                # (may be inflated by complement matching). We clamp to a
                # dollar-consistent estimate for immediate logs/UI.
                #
                # amount (dollars spent) is ALWAYS authoritative ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â never
                # recompute it from shares ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â price.
                clob_limit_price = fill_price

                fill_amount = fill_shares * fill_price
                fill_edge = 1.0 - fill_price
                slip = fill_price - opp.best_ask
                slip_str = ""
                if abs(slip) >= 0.001:
                    slip_str = " | slippage $%.4f (book moved from $%.4f to $%.4f)" % (
                        slip, opp.best_ask, fill_price)
                logger.info(
                    "[BOUGHT via %s] %s %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â %.2f shares @ $%.4f ($%.2f spent) | "
                    "edge $%.4f/share (profit if wins: $%.2f)%s",
                    route_tag, snipe_side, market.event_title[:50],
                    fill_shares, fill_price, fill_amount,
                    fill_edge, fill_shares - fill_amount, slip_str,
                )
            else:
                # No estimate fallback: keep UI/DB tied to confirmation truth only.
                fill_shares = 0.0
                fill_price = 0.0
                fill_amount = amount
                fill_edge = 0.0
                logger.warning(
                    "[BOUGHT via %s] %s %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â matched but CLOB fill data missing; "
                    "waiting for confirmation before accounting",
                    route_tag, snipe_side, market.event_title[:50],
                )
                logger.debug("%s result keys (no fill data): %s", route_tag, list(result.keys()))

            self.trades_executed += 1
            self.total_spent += fill_amount
            self._invalidate_balance_cache()  # Balance changed ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â force fresh read next time

            # Extract buy tx hash from CLOB response
            buy_tx_hash = None
            tx_hashes = result.get("transactionsHashes") or result.get("transactionHashes")
            if tx_hashes and isinstance(tx_hashes, list):
                buy_tx_hash = ",".join(h for h in tx_hashes if h)

            is_hft = getattr(opp, "is_hft_runner", False)
            
            # Confirmation: write trade + position only from CLOB truth.
            # Nothing hits DB/UI final accounting until CLOB fill truth exists.
            # No chain/indexer fallback in this runtime path.
            asyncio.create_task(self._confirm_fill(
                market.condition_id,
                token_id=snipe_token,
                event_title=market.event_title,
                outcome=snipe_side,
                asset=market.asset,
                event_slug=market.event_slug,
                buy_tx_hash=buy_tx_hash,
                gap_pct=opp.gap_pct,
                gap_direction=opp.gap_direction,
                timeframe_seconds=int(getattr(opp, "timeframe_seconds", 0) or market.duration_seconds or 0),
                hft_barrier_pct=getattr(opp, "hft_barrier_pct", None),
                hft_cap_price=getattr(opp, "hft_cap_price", None),
                hft_path=getattr(opp, "hft_entry_path", None),
                hft_trigger_ask=getattr(opp, "hft_trigger_ask", None),
                outcome_index=0 if snipe_side == "Up" else 1,
                neg_risk=market.neg_risk,
                is_hft=is_hft,
                fill_shares=float(fill_shares),
                fill_price=float(fill_price),
                order_id=order_id,
            ))

            result_action = "SNIPED_PENDING" if (fill_shares <= 0 or fill_price <= 0) else "SNIPED"
            return {"ok": True, "action": result_action,
                    "amount": fill_amount, "shares": fill_shares,
                    "avg_price": fill_price, "edge": fill_edge,
                    "tx": buy_tx_hash or ""}

        except Exception as e:
            logger.error("[SNIPE CRASHED] %s ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â unexpected error: %s",
                         market.event_title[:40], e)
            return {"ok": False, "action": "ERROR", "reason": str(e)}



