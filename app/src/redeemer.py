"""Sniper redeemer -- auto-redeems resolved positions for $1 each share."""

import asyncio
import logging
import math
import time
from datetime import datetime, timezone

from src.api import PolymarketAPI
from src.config import Config
from src.database import Database

logger = logging.getLogger(__name__)


class Redeemer:
    """Checks open sniper positions for resolution and redeems them."""

    MISSING_CYCLES_BEFORE_CLOSE = 4  # 4 cycles x 60s = 4 min
    MISSING_WARN_REPEAT_CYCLES = 10  # After first warning, remind every N cycles
    MISSING_HARD_ALERT_CYCLES = 30   # Escalate to error after prolonged unresolved missing state
    REDEEM_COOLDOWN_SECONDS = 120    # Don't re-attempt a redeem within 120s

    def __init__(self, config: Config, api: PolymarketAPI, db: Database):
        self.config = config
        self.api = api
        self.db = db
        self._missing_counts: dict[tuple[str, str], int] = {}
        self._missing_warned_at: dict[tuple[str, str], int] = {}
        # Track in-flight/recently-attempted redemptions: cid -> monotonic timestamp
        self._redeem_attempted: dict[str, float] = {}
        # Permanently ignore orphans that we confirmed have 0 balance on-chain
        self._dead_orphans: set[str] = set()

        # Stats
        self.redeemed_count = 0
        self.total_pnl = 0.0

    @staticmethod
    def _cp_token_id(cp: dict) -> str:
        """Best-effort token identifier from chain position row."""
        return str(cp.get("tokenId", "") or cp.get("asset", "") or "")

    @staticmethod
    def _cp_size(cp: dict) -> float:
        try:
            return float(cp.get("size", 0) or 0)
        except Exception:
            return 0.0

    def _find_chain_pos(
        self,
        condition_id: str,
        token_id: str,
        chain_map: dict,
        chain_positions: list[dict],
    ) -> tuple[dict | None, str]:
        """Resolve chain position for a DB row.

        Resolution order:
          1) Exact key match (condition_id + token_id).
          2) Unique active position for same condition_id (safe fallback).
        """
        cp = chain_map.get((condition_id, token_id))
        if cp:
            return cp, "exact"

        active = [
            row for row in chain_positions
            if str(row.get("conditionId", "") or "") == condition_id
            and self._cp_size(row) >= 0.01
        ]
        if len(active) == 1:
            return active[0], "cid_unique"
        return None, "none"

    async def check_and_redeem(
        self,
        *,
        run_finalizer: bool = True,
        finalizer_max_rows: int = 80,
    ) -> dict:
        """Check all open positions for resolved markets and redeem.

        Thin wrapper: calls sync_positions() then redeem_resolved().
        Returns: {checked, redeemed, pnl, lost}
        """
        if not self.config.redemption.auto_redeem:
            return {"checked": 0, "redeemed": 0, "pnl": 0.0, "lost": 0}

        wallet = self.config.wallet_address
        if not wallet:
            return {"checked": 0, "redeemed": 0, "pnl": 0.0, "lost": 0}

        positions = await self.db.get_open_positions()

        # Fetch FULL chain positions (shared by sync + redeem).
        # Root fix: single-page reads (limit=100) can miss active positions and
        # cause false "missing from chain" warnings and wrong orphan handling.
        try:
            chain_positions = await self.api.get_all_positions(wallet, page_size=500)
        except Exception as e:
            logger.error("Failed to fetch positions for redemption: %s", e)
            return {"checked": 0, "redeemed": 0, "pnl": 0.0, "lost": 0}

        chain_map = {}
        for cp in chain_positions:
            cid = str(cp.get("conditionId", "") or "")
            tid = str(cp.get("tokenId", "") or cp.get("asset", "") or "")
            chain_map[(cid, tid)] = cp

        await self.sync_positions(positions, chain_map, chain_positions)
        # Re-fetch positions so redeem_resolved sees corrected shares/avg_price
        # from fill correction (stale in-memory values cause wrong pnl_override).
        positions = await self.db.get_open_positions()

        # Phase 1.5: Auto-sell stale positions that haven't resolved
        stale_cfg = self.config.redemption
        if stale_cfg.stale_sell_after_seconds > 0:
            await self._sell_stale_positions(positions, chain_map)
            # Re-fetch again — some positions may have been closed by sell
            positions = await self.db.get_open_positions()

        out = await self.redeem_resolved(positions, chain_positions, chain_map)
        if run_finalizer:
            try:
                fin = await self.db.finalize_resolved_pnl(max_rows=finalizer_max_rows)
                out["finalizer"] = fin
                if fin.get("finalized", 0) or fin.get("cashflow_missing", 0):
                    logger.info(
                        "[FINALIZER] checked=%d finalized=%d cashflow_missing=%d skipped=%d limit=%d",
                        int(fin.get("checked", 0) or 0),
                        int(fin.get("finalized", 0) or 0),
                        int(fin.get("cashflow_missing", 0) or 0),
                        int(fin.get("skipped", 0) or 0),
                        int(fin.get("limit", 0) or 0),
                    )
            except Exception as exc:
                logger.error("PnL finalizer failed: %s", exc)
        return out

    # ─────────────────────────────────────────────────────────────────
    #  Phase 1: Sync DB with chain reality (fill correction + missing)
    # ─────────────────────────────────────────────────────────────────

    async def sync_positions(self, positions: list, chain_map: dict, chain_positions: list[dict]) -> None:
        """Sync DB with on-chain reality for every open position.

        ALL accounting truth comes from the chain Data API:
          • size      → shares held
          • avgPrice  → actual average cost per share
          • initialValue → total USDC spent (size × avgPrice)

        Uses confirm_fill() to write chain truth — same codepath as
        the executor's initial confirmation.  No separate SQL.
        """
        seen_keys = set()

        for pos in positions:
            cid = pos["condition_id"]
            token_id = pos["token_id"]
            key = (cid, token_id)
            seen_keys.add(key)
            chain_pos, match_mode = self._find_chain_pos(
                cid, token_id, chain_map, chain_positions
            )

            if chain_pos:
                if match_mode == "cid_unique":
                    logger.info(
                        "[SYNC] Recovered chain match by CID (token drift): %s | db_token=%s chain_token=%s",
                        pos["event_title"][:50],
                        str(token_id)[:16],
                        self._cp_token_id(chain_pos)[:16],
                    )
                # Reset missing counters and emit a one-time recovery log if needed.
                prev_missing = self._missing_counts.pop(key, None)
                self._missing_warned_at.pop(key, None)
                if prev_missing and prev_missing >= self.MISSING_CYCLES_BEFORE_CLOSE:
                    logger.info(
                        "[SYNC] Position reappeared on chain after %d missing cycles: %s",
                        prev_missing, pos["event_title"][:50],
                    )

                # ── Chain truth: shares, avg price, total cost ──
                chain_shares = float(chain_pos.get("size", 0))
                chain_avg = float(chain_pos.get("avgPrice", 0))
                chain_cost = float(chain_pos.get("initialValue", 0))

                # Sanity: initialValue should ≈ size × avgPrice.
                # If missing, derive from the other two.
                if chain_cost <= 0 and chain_shares > 0 and chain_avg > 0:
                    chain_cost = round(chain_shares * chain_avg, 6)
                if chain_avg <= 0 and chain_shares > 0 and chain_cost > 0:
                    chain_avg = chain_cost / chain_shares

                if chain_shares <= 0:
                    # If this position had an offload sell, all shares were sold.
                    # Close it as "offloaded" — profit is locked in from the sale.
                    offload_id = pos.get("offload_order_id")
                    if offload_id:
                        # Revenue from chain: we sold all shares at offload_sell_price
                        sell_price = pos.get("offload_sell_price") or 0
                        db_shares = pos["shares"]
                        revenue = db_shares * sell_price
                        pnl = revenue - pos["fill_cost"]
                        logger.info(
                            "[OFFLOAD] Fully sold: %s | %.2f shares @ $%.2f | "
                            "revenue $%.2f | cost $%.2f | pnl $%+.2f",
                            pos["event_title"][:40], db_shares, sell_price,
                            revenue, pos["fill_cost"], pnl,
                        )
                        await self.db.update_offload_status(
                            cid, "filled", revenue=revenue,
                        )
                        await self.db.close_and_resolve(
                            cid, won=True, pnl=pnl, status="offloaded",
                        )
                        self.redeemed_count += 1
                        self.total_pnl += pnl
                    else:
                        # No offload record, but shares are gone on-chain.
                        # Could be a loss or manual sell. 
                        # Check resolution to distinguish.
                        resolved_as = await self._check_resolution(cid, pos["outcome"])
                        if resolved_as == "lost":
                             pnl = -pos["fill_cost"]
                             logger.info("[LOSS] detected via zero-balance (Sync Phase): %s | loss $%.2f", pos["event_title"][:40], abs(pnl))
                             await self.db.close_and_resolve(cid, won=False, pnl=pnl, status="lost", payout_usdc=0.0)
                        elif resolved_as == "won":
                             # If we won but have 0 shares, it's externally redeemed.
                             pnl = pos["shares"] - pos["fill_cost"]
                             logger.info("[OK] External redeem detected via zero-balance (Sync Phase): %s", pos["event_title"][:40])
                             await self.db.close_and_resolve(cid, won=True, pnl=pnl, status="redeemed")
                        else:
                             # Still zero but unknown resolution? Mark closed as 'orphaned' to stop phantom entries
                             logger.warning("[SYNC] Zero shares but unknown resolution: %s | Closing as orphaned.", pos["event_title"][:40])
                             await self.db.close_and_resolve(cid, won=False, pnl=0, status="orphaned")
                    continue

                db_shares = pos["shares"]
                db_cost = pos["total_cost"]

                # Only update if something meaningful changed
                shares_diff = abs(db_shares - chain_shares)
                cost_diff = abs(db_cost - chain_cost)
                shares_changed = shares_diff > 0.001 and shares_diff > db_shares * 0.005
                cost_changed = cost_diff > 0.005

                if not shares_changed and not cost_changed:
                    continue

                # Detect partial offload fills (shares decreased + offload active)
                offload_id = pos.get("offload_order_id")
                if offload_id and chain_shares < db_shares:
                    sold_shares = db_shares - chain_shares
                    sell_price = pos.get("offload_sell_price") or 0
                    revenue = sold_shares * sell_price
                    logger.info(
                        "[OFFLOAD] Partial fill: %s | sold %.2f/%.2f shares @ $%.2f | "
                        "revenue $%.2f",
                        pos["event_title"][:40], sold_shares, db_shares,
                        sell_price, revenue,
                    )
                    prev_revenue = pos.get("offload_revenue") or 0
                    await self.db.update_offload_status(
                        cid, "partial", revenue=prev_revenue + revenue,
                    )

                logger.info(
                    "Fill correction (chain): %s | "
                    "DB %.4f sh @ $%.4f ($%.4f) -> "
                    "chain %.4f sh @ $%.4f ($%.4f)",
                    pos["event_title"][:40],
                    db_shares, pos["avg_price"], db_cost,
                    chain_shares, chain_avg, chain_cost,
                )

                # Write chain truth via confirm_fill (same path as executor)
                await self.db.confirm_fill(
                    condition_id=cid,
                    token_id=token_id,
                    shares=chain_shares,
                    avg_price=chain_avg,
                    total_cost=chain_cost,
                )

                # Recalculate pnl for already-resolved trades using chain cost
                n_trades_cur = await self.db._conn.execute(
                    "SELECT COUNT(*) FROM trades "
                    "WHERE condition_id = ? AND dry_run = 0 AND result IN ('won', 'lost')",
                    (cid,),
                )
                n_trades = (await n_trades_cur.fetchone())[0] or 1
                per_trade_cost = pos["fill_cost"] / n_trades
                async with self.db._write_lock:
                    await self.db._conn.execute(
                        """UPDATE trades SET pnl = CASE
                               WHEN result = 'won'  THEN shares - amount
                               WHEN result = 'lost' THEN ?
                               ELSE pnl
                           END
                           WHERE condition_id = ? AND dry_run = 0
                             AND result IN ('won', 'lost')""",
                        (-per_trade_cost, cid,),
                    )
                    await self.db._conn.commit()
            else:
                # Position missing from chain — bump counter (redeem_resolved handles closure)
                miss_count = self._missing_counts.get(key, 0) + 1
                self._missing_counts[key] = miss_count

        # Clean missing counters for positions no longer tracked
        for key in list(self._missing_counts.keys()):
            if key not in seen_keys:
                del self._missing_counts[key]
                self._missing_warned_at.pop(key, None)

    def _should_log_missing_unresolved(
        self, key: tuple[str, str], miss_count: int
    ) -> bool:
        """Return True only when unresolved-missing warning should be emitted."""
        if miss_count < self.MISSING_CYCLES_BEFORE_CLOSE:
            return False
        last_warn = self._missing_warned_at.get(key, 0)
        if last_warn <= 0 or (miss_count - last_warn) >= self.MISSING_WARN_REPEAT_CYCLES:
            self._missing_warned_at[key] = miss_count
            return True
        return False

    # ─────────────────────────────────────────────────────────────────
    #  Phase 1.5: Auto-sell stale positions (won but unresolved on-chain)
    # ─────────────────────────────────────────────────────────────────

    async def _sell_stale_positions(self, positions: list, chain_map: dict) -> None:
        """Sell positions that won but haven't resolved within the stale window.

        Safety gates (ALL must pass before a sell is attempted):
        1. Position age > stale_sell_after_seconds (default: 1 hour)
        2. Market is still accepting orders on the CLOB
        3. Our side appears to have won (opposite token price ≤ $0.05)
        4. Bid liquidity at ≥ stale_sell_min_price exceeds our shares
        5. GTC SELL at min_price — fills at that price or better only

        If any gate fails, we skip and let normal redemption handle it.
        """
        stale_seconds = self.config.redemption.stale_sell_after_seconds
        min_price = self.config.redemption.stale_sell_min_price
        now_utc = datetime.now(timezone.utc)

        for pos in positions:
            cid = pos["condition_id"]
            token_id = pos["token_id"]
            shares = pos["shares"]
            total_cost = pos["fill_cost"]
            title = pos["event_title"][:60]

            # Gate 1: Position age check
            try:
                opened_at = datetime.fromisoformat(
                    pos["opened_at"].replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                continue  # Can't parse timestamp — skip
            age_seconds = (now_utc - opened_at).total_seconds()
            if age_seconds < stale_seconds:
                continue  # Not stale yet

            # Gate 2: Check market status on CLOB (still accepting orders?)
            try:
                market = await self.api.get_market(cid)
            except Exception as e:
                logger.debug("Stale sell: CLOB market check failed for %s: %s", cid[:16], e)
                continue
            if not market:
                continue
            if not market.get("accepting_orders", False):
                logger.debug("Stale sell: market not accepting orders — skipping %s", title)
                continue

            # Gate 3: Our side appears to have won
            # Check if the opposite token is trading near $0 (≤ $0.05)
            tokens = market.get("tokens", [])
            our_outcome = pos["outcome"].lower()
            opposite_price = None
            for tok in tokens:
                tok_outcome = (tok.get("outcome") or "").lower()
                if tok_outcome and tok_outcome != our_outcome:
                    try:
                        opposite_price = float(tok.get("price", 1.0))
                    except (ValueError, TypeError):
                        opposite_price = 1.0
                    break

            if opposite_price is None or opposite_price > 0.05:
                logger.debug(
                    "Stale sell: opposite token price $%.4f too high — "
                    "not confident we won, skipping %s",
                    opposite_price or 0, title,
                )
                continue

            # Gate 4: Sufficient bid liquidity at our min price
            bid_info = await self.api.get_book_bids(token_id, min_price=min_price)
            bid_volume = bid_info["total_size"]
            best_bid = bid_info["best_bid"]

            if bid_volume < shares:
                logger.warning(
                    "Stale sell: insufficient bids at ≥$%.2f "
                    "(%.2f available vs %.2f needed) — skipping %s",
                    min_price, bid_volume, shares, title,
                )
                continue

            if best_bid < min_price:
                logger.debug(
                    "Stale sell: best bid $%.4f below min $%.2f — skipping %s",
                    best_bid, min_price, title,
                )
                continue

            # All gates passed — execute the sell
            sell_size = math.floor(shares * 100) / 100  # Round down to be safe
            if sell_size <= 0:
                continue

            ratio = bid_volume / shares
            logger.info(
                "STALE SELL: %s | %.2f shares @ min $%.2f | "
                "best bid $%.2f | liquidity %.0fx | age %.0f min",
                title, sell_size, min_price, best_bid,
                ratio, age_seconds / 60,
            )

            try:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None,  # Default executor
                    self.api.place_limit_sell,
                    token_id, min_price, sell_size,
                )

                status = result.get("status", "")
                order_id = result.get("orderID", "") or result.get("order_id", "")
                success = result.get("success", False)
                taking = float(result.get("takingAmount", 0) or 0)

                if status.lower() == "matched" or success:
                    # Sell filled — calculate PnL
                    # takingAmount is the USDC we received
                    payout = taking if taking > 0 else sell_size * best_bid
                    pnl = payout - total_cost
                    logger.info(
                        "[OK] STALE SELL FILLED: %s | %.2f shares | "
                        "received $%.2f | cost $%.2f | pnl $%.2f",
                        title, sell_size, payout, total_cost, pnl,
                    )
                    await self.db.close_and_resolve(
                        cid, won=True, pnl=pnl, status="sold",
                    )
                    self.redeemed_count += 1
                    self.total_pnl += pnl
                elif order_id:
                    # GTC order placed but not immediately filled — check after delay
                    logger.info(
                        "STALE SELL: GTC order placed (not instant fill) for %s | "
                        "order %s — will check next cycle",
                        title, order_id[:20],
                    )
                    # Don't close position yet — let next cycle's redeem handle it
                    # or it fills in the background and chain balance goes to 0
                else:
                    logger.warning(
                        "STALE SELL: unexpected result for %s: %s",
                        title, result,
                    )

            except Exception as e:
                logger.error("STALE SELL failed for %s: %s", title, e)
                # Don't close — leave position open for retry or normal redemption

    # ─────────────────────────────────────────────────────────────────
    #  Phase 2: Redeem resolved positions (DB + orphan + cooldown)
    # ─────────────────────────────────────────────────────────────────

    async def redeem_resolved(self, positions: list,
                              chain_positions: list,
                              chain_map: dict) -> dict:
        """Redeem all resolved positions — DB-tracked, missing, and orphan.

        Returns: {checked, redeemed, pnl, lost}
        """
        redeemed = 0
        lost = 0
        total_pnl = 0.0
        redeemed_cids = set()  # cids redeemed this cycle (for orphan dedup)

        # ── DB-tracked positions ──
        for pos in positions:
            cid = pos["condition_id"]
            chain_pos, match_mode = self._find_chain_pos(
                cid, pos["token_id"], chain_map, chain_positions
            )

            if chain_pos:
                if match_mode == "cid_unique":
                    logger.info(
                        "[REDEEM] Recovered chain match by CID (token drift): %s | db_token=%s chain_token=%s",
                        pos["event_title"][:50],
                        str(pos["token_id"])[:16],
                        self._cp_token_id(chain_pos)[:16],
                    )
                # Check if redeemable
                redeemable = chain_pos.get("redeemable", False)
                if not redeemable:
                    continue

                # Cooldown: don't re-attempt if we recently tried this cid
                last_attempt = self._redeem_attempted.get(cid)
                if last_attempt and (time.monotonic() - last_attempt) < self.REDEEM_COOLDOWN_SECONDS:
                    continue  # Silently skip -- avoids error spam

                # Pre-check resolution to save gas on LOST bets
                resolved_as = await self._check_resolution(cid, pos["outcome"])
                if resolved_as == "lost":
                    logger.info("[LOSS] Market %s resolved against us. Skipping $0 on-chain redeem to save gas.", pos["event_title"][:40])
                    pnl = -pos["fill_cost"]
                    await self.db.close_and_resolve(cid, won=False, pnl=pnl, status="lost", payout_usdc=0.0)
                    lost += 1
                    self.redeemed_count += 1
                    self.total_pnl += pnl
                    redeemed_cids.add(cid)
                    continue
                elif resolved_as == "unknown":
                    # Polymarket says redeemable=True but API hasn't updated winning_outcome yet.
                    # Wait for next cycle to be safe.
                    continue
                    
                # If resolved_as == "won", proceed to actual Redeem! (Run in background thread)
                try:
                    self._redeem_attempted[cid] = time.monotonic()
                    neg_risk = bool(pos.get("neg_risk", 0))
                    
                    loop = asyncio.get_running_loop()
                    if neg_risk:
                        result = await loop.run_in_executor(
                            None,
                            self.api.redeem_negrisk_onchain,
                            cid, pos["token_id"], pos["outcome_index"]
                        )
                    else:
                        result = await loop.run_in_executor(
                            None,
                            self.api.redeem_positions_onchain,
                            cid
                        )

                    if result == "already_redeemed":
                        self._redeem_attempted.pop(cid, None)
                        # Token balance was 0 -- redeemed by another app/wallet.
                        # Fall back to CLOB API to determine outcome.
                        # NOTE: no chain payout available here, so P&L uses estimates
                        resolved_as = await self._check_resolution(cid, pos["outcome"])
                        if resolved_as == "won":
                            pnl = pos["shares"] - pos["fill_cost"]
                            logger.info(
                                "Already redeemed (won): %s | $%.2f (estimated)",
                                pos["event_title"][:50], pnl,
                            )
                            await self.db.close_and_resolve(cid, won=True, pnl=pnl, status="redeemed")
                            redeemed += 1
                            total_pnl += pnl
                            redeemed_cids.add(cid)
                        elif resolved_as == "lost":
                            pnl = -pos["fill_cost"]
                            logger.warning(
                                "[LOSS] already_redeemed (lost) %s -- %s | loss $%.2f",
                                pos["outcome"], pos["event_title"][:50], abs(pnl),
                            )
                            await self.db.close_and_resolve(cid, won=False, pnl=pnl, status="lost")
                            lost += 1
                            self.redeemed_count += 1
                            self.total_pnl += pnl
                            redeemed_cids.add(cid)
                        else:
                            # Can't determine -- leave position open, try next cycle
                            logger.warning(
                                "already_redeemed but resolution unknown -- "
                                "leaving open for retry: %s",
                                pos["event_title"][:50],
                            )
                        continue

                    # ── On-chain payout is the source of truth ──
                    # result is a dict: {"tx_hash": str, "payout_usdc": float}
                    tx_hash = result["tx_hash"]
                    payout = result["payout_usdc"]

                    self._redeem_attempted.pop(cid, None)  # Success -- clear cooldown

                    if payout > 0:
                        # WON: payout is the USDC the chain sent us
                        offload_rev = pos.get("offload_revenue") or 0
                        pnl = payout + offload_rev - pos["fill_cost"]
                        logger.info(
                            "[OK] REDEEMED %s -- %s | %.2f shares | "
                            "payout $%.6f%s | cost $%.2f | profit $%.2f | tx: %s",
                            pos["outcome"], pos["event_title"][:50],
                            pos["shares"], payout,
                            f" + offload ${offload_rev:.2f}" if offload_rev > 0 else "",
                            pos["fill_cost"], pnl, tx_hash[:16],
                        )
                        await self.db.close_and_resolve(
                            cid, won=True, pnl=pnl, status="redeemed",
                            redeem_tx_hash=tx_hash, payout_usdc=payout,
                        )
                        redeemed += 1
                        total_pnl += pnl
                        self.redeemed_count += 1
                        self.total_pnl += pnl
                        redeemed_cids.add(cid)
                    else:
                        # LOST: chain paid us $0
                        offload_revenue = pos.get("offload_revenue") or 0.0
                        pnl = offload_revenue - pos["fill_cost"]
                        logger.warning(
                            "[LOSS] REDEEMED (lost) %s -- %s | %.2f shares @ $%.4f | "
                            "loss $%.2f | offload_rev $%.2f | payout $0 | tx: %s",
                            pos["outcome"], pos["event_title"][:50],
                            pos["shares"], pos["avg_price"], abs(pnl),
                            offload_revenue, tx_hash[:16],
                        )
                        await self.db.close_and_resolve(
                            cid, won=False, pnl=pnl, status="lost",
                            redeem_tx_hash=tx_hash, payout_usdc=0.0,
                        )
                        lost += 1
                        self.redeemed_count += 1
                        self.total_pnl += pnl
                        redeemed_cids.add(cid)

                except Exception as e:
                    logger.error("Redemption failed for %s: %s",
                                 pos["event_title"][:40], e)
            else:
                # Position missing from chain -- could be redeemed externally
                key_tup = (cid, pos["token_id"])
                miss_count = self._missing_counts.get(key_tup, 0)
                if miss_count >= self.MISSING_CYCLES_BEFORE_CLOSE:
                    # Check if market resolved in our favor (external redemption)
                    resolved_as = await self._check_resolution(cid, pos["outcome"])
                    if resolved_as == "won":
                        self._missing_counts.pop(key_tup, None)
                        self._missing_warned_at.pop(key_tup, None)
                        # NOTE: no chain payout for external redeems, P&L is estimated
                        pnl = pos["shares"] - pos["fill_cost"]
                        logger.info(
                            "[OK] EXTERNAL REDEEM %s -- %s | %.2f shares | profit $%.2f "
                            "(redeemed by another app/wallet, estimated)",
                            pos["outcome"], pos["event_title"][:50],
                            pos["shares"], pnl,
                        )
                        await self.db.close_and_resolve(cid, won=True, pnl=pnl, status="redeemed")
                        redeemed += 1
                        total_pnl += pnl
                        self.redeemed_count += 1
                        self.total_pnl += pnl
                        redeemed_cids.add(cid)
                    elif resolved_as == "lost":
                        self._missing_counts.pop(key_tup, None)
                        self._missing_warned_at.pop(key_tup, None)
                        # Definitive loss path: avoid future orphan redeem churn for this cid.
                        self._dead_orphans.add(cid)
                        logger.info(
                            "Position missing -- market resolved against us: %s -- closing as loss",
                            pos["event_title"][:50],
                        )
                        pnl_lost = -pos["fill_cost"]
                        await self.db.close_and_resolve(cid, won=False, pnl=pnl_lost, status="lost")
                        lost += 1
                        self.redeemed_count += 1
                        self.total_pnl += pnl_lost
                        redeemed_cids.add(cid)
                    else:
                        # Not resolved yet: keep waiting, with throttled alerts.
                        if self._should_log_missing_unresolved(key_tup, miss_count):
                            if miss_count >= self.MISSING_HARD_ALERT_CYCLES:
                                logger.error(
                                    "Position still missing from chain (%d cycles, unresolved): %s",
                                    miss_count, pos["event_title"][:50],
                                )
                            else:
                                logger.warning(
                                    "Position missing from chain (%d cycles) but market unresolved: %s",
                                    miss_count, pos["event_title"][:50],
                                )

        # ── Chain-first: redeem orphaned positions the DB doesn't know about ──
        # Covers DB wipes, external deposits, or any chain/DB mismatch.
        # Only redeems positions that are already resolved & redeemable.
        db_cids = {pos["condition_id"] for pos in positions}
        for cp in chain_positions:
            cp_cid = cp.get("conditionId", "")
            if cp_cid in db_cids or cp_cid in redeemed_cids or cp_cid in self._dead_orphans:
                continue
            if not cp.get("redeemable"):
                continue
            # Filter out microscopic "dust" shares left over from indexer rounding errors
            cp_size = float(cp.get("size", 0))
            if cp_size < 0.01:
                continue
            # Cooldown: don't re-attempt if we recently tried this cid
            last_attempt = self._redeem_attempted.get(cp_cid)
            if last_attempt and (time.monotonic() - last_attempt) < self.REDEEM_COOLDOWN_SECONDS:
                continue
            title = cp.get("title", "Unknown")[:50]
            is_negrisk = bool(cp.get("negativeRisk", False))
            try:
                self._redeem_attempted[cp_cid] = time.monotonic()
                loop = asyncio.get_running_loop()
                
                if is_negrisk:
                    token_id = cp.get("asset", "")
                    outcome_idx = int(cp.get("outcomeIndex", 0))
                    result = await loop.run_in_executor(
                        None,
                        self.api.redeem_negrisk_onchain,
                        cp_cid, token_id, outcome_idx,
                    )
                    if result == "already_redeemed":
                        self._dead_orphans.add(cp_cid)
                        continue
                else:
                    result = await loop.run_in_executor(
                        None,
                        self.api.redeem_positions_onchain,
                        cp_cid
                    )
                    
                # Do NOT pop self._redeem_attempted here! Leave the cooldown active so 
                # we don't spam the network while waiting for Polyamrket's API to update!
                payout = result["payout_usdc"]
                tag = "NegRisk " if is_negrisk else ""
                logger.info(
                    "[ORPHAN] %sREDEEMED %.2f shares of %s | "
                    "payout $%.6f (not in DB) | tx: %s",
                    tag, cp_size, title, payout, result["tx_hash"][:16],
                )
                
                # If we successfully redeemed but got 0 payout, the chain says we have no 
                # shares. Blacklist this orphan so we never block the event loop checking it again.
                if payout <= 0.0001:
                    self._dead_orphans.add(cp_cid)
                    
                redeemed += 1
                total_pnl += payout  # no cost basis -- just count gross payout
            except Exception as e:
                logger.error("Orphan redeem failed for %s: %s", title, e)

        # Purge expired cooldown entries (positions already closed or cooldown elapsed)
        now_mono = time.monotonic()
        expired = [k for k, v in self._redeem_attempted.items()
                   if (now_mono - v) > self.REDEEM_COOLDOWN_SECONDS * 2]
        for k in expired:
            del self._redeem_attempted[k]

        if redeemed > 0 or lost > 0:
            logger.info(
                "Redemption check: %d checked, %d redeemed ($%.2f pnl), %d lost",
                len(positions), redeemed, total_pnl, lost,
            )

        return {
            "checked": len(positions),
            "redeemed": redeemed,
            "pnl": total_pnl,
            "lost": lost,
        }

    async def _check_resolution(self, condition_id: str, our_outcome: str) -> str:
        """Check if a market has resolved and whether our side won.

        Returns: "won" | "lost" | "unknown"
        """
        try:
            market = await self.api.get_market(condition_id)
            if not market:
                return "unknown"

            # Primary check: tokens array with winner flag (always present on CLOB API)
            tokens = market.get("tokens", [])
            winning_outcome = None
            for tok in tokens:
                winner = tok.get("winner", False)
                if winner:
                    winning_outcome = tok.get("outcome", "")
                    break

            if winning_outcome:
                if winning_outcome.lower() == our_outcome.lower():
                    return "won"
                else:
                    return "lost"

            # Fallback: check closed + price fields (price=1 means winner)
            if market.get("closed", False):
                for tok in tokens:
                    price = tok.get("price", 0)
                    if price == 1:
                        winning_outcome = tok.get("outcome", "")
                        if winning_outcome.lower() == our_outcome.lower():
                            return "won"
                        else:
                            return "lost"

            # Fallback: top-level winning_outcome field
            winning = market.get("winning_outcome") or market.get("winner") or ""
            if winning:
                return "won" if winning.lower() == our_outcome.lower() else "lost"

            return "unknown"
        except Exception as e:
            logger.warning("Resolution check failed for %s: %s", condition_id[:16], e)
            return "unknown"
