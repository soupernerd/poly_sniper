"""Sniper database -- SQLite persistence for positions and trades."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from src.config import Config, PROJECT_ROOT

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    event_title TEXT NOT NULL,
    outcome TEXT NOT NULL,
    asset TEXT NOT NULL,
    action TEXT NOT NULL,
    amount REAL NOT NULL,
    shares REAL NOT NULL,
    price REAL NOT NULL,
    edge REAL NOT NULL DEFAULT 0,
    dry_run INTEGER NOT NULL DEFAULT 1,
    pnl REAL DEFAULT NULL,
    result TEXT NOT NULL DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    event_title TEXT NOT NULL,
    outcome TEXT NOT NULL,
    asset TEXT NOT NULL,
    outcome_index INTEGER NOT NULL DEFAULT 0,
    shares REAL NOT NULL,
    avg_price REAL NOT NULL,
    total_cost REAL NOT NULL,
    fill_cost REAL NOT NULL DEFAULT 0,
    neg_risk INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open',
    opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    closed_at TEXT DEFAULT NULL,
    pnl REAL DEFAULT NULL,
    UNIQUE(condition_id, token_id)
);

CREATE TABLE IF NOT EXISTS bot_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    level TEXT NOT NULL,
    message TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hft_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    condition_id TEXT NOT NULL,
    event_title TEXT NOT NULL DEFAULT '',
    asset TEXT NOT NULL DEFAULT '',
    timeframe_seconds INTEGER NOT NULL DEFAULT 0,
    side TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    gap_pct REAL DEFAULT NULL,
    barrier_pct REAL DEFAULT NULL,
    current_price REAL DEFAULT NULL,
    price_to_beat REAL DEFAULT NULL,
    ask REAL DEFAULT NULL,
    bid REAL DEFAULT NULL,
    cap_price REAL DEFAULT NULL,
    reason TEXT DEFAULT NULL,
    details_json TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_condition ON trades(condition_id);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_hft_events_cid_ts ON hft_events(condition_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_hft_events_type_ts ON hft_events(event_type, timestamp);
"""

_SNAPSHOTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    condition_id TEXT NOT NULL,
    event_title TEXT NOT NULL,
    asset TEXT NOT NULL,

    -- Time context
    seconds_to_end REAL NOT NULL,
    duration_seconds INTEGER NOT NULL DEFAULT 0,

    -- Book data: Up side
    up_mid REAL NOT NULL DEFAULT 0,
    up_ask REAL NOT NULL DEFAULT 0,
    up_depth REAL NOT NULL DEFAULT 0,

    -- Book data: Down side
    down_mid REAL NOT NULL DEFAULT 0,
    down_ask REAL NOT NULL DEFAULT 0,
    down_depth REAL NOT NULL DEFAULT 0,

    -- Best side (scanner pick)
    best_side TEXT NOT NULL,
    best_mid REAL NOT NULL DEFAULT 0,
    best_ask REAL NOT NULL DEFAULT 0,
    best_depth REAL NOT NULL DEFAULT 0,
    edge REAL NOT NULL DEFAULT 0,

    -- Binance gap data
    binance_price REAL,
    price_to_beat REAL,
    gap_pct REAL,
    gap_direction TEXT,
    ptb_source TEXT,

    -- Gate results (1 = passed, 0 = failed)
    passed_threshold INTEGER NOT NULL DEFAULT 0,
    passed_max_price INTEGER NOT NULL DEFAULT 0,
    passed_min_edge INTEGER NOT NULL DEFAULT 0,
    passed_depth INTEGER NOT NULL DEFAULT 0,
    passed_gap INTEGER NOT NULL DEFAULT 0,
    passed_conviction INTEGER NOT NULL DEFAULT 0,
    passed_max_gap INTEGER NOT NULL DEFAULT 1,
    passed_all INTEGER NOT NULL DEFAULT 0,

    -- What happened
    action TEXT NOT NULL DEFAULT 'observe',

    -- Resolution (backfilled when market resolves)
    actual_winner TEXT DEFAULT NULL,
    would_have_won INTEGER DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_snapshots_condition ON snapshots(condition_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_ts ON snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshots_asset ON snapshots(asset);
"""

_MIGRATIONS = [
    # Add result column to trades if it doesn't exist (v2)
    "ALTER TABLE trades ADD COLUMN result TEXT NOT NULL DEFAULT 'pending'",
    "CREATE INDEX IF NOT EXISTS idx_trades_result ON trades(result)",
    # Add event_slug for direct Polymarket links (v3)
    "ALTER TABLE trades ADD COLUMN event_slug TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE positions ADD COLUMN event_slug TEXT NOT NULL DEFAULT ''",
    # Audit-trail columns (v4): tx hashes, chain payout, gap at snipe time
    "ALTER TABLE trades ADD COLUMN buy_tx_hash TEXT DEFAULT NULL",
    "ALTER TABLE trades ADD COLUMN gap_pct REAL DEFAULT NULL",
    "ALTER TABLE trades ADD COLUMN gap_direction TEXT DEFAULT NULL",
    "ALTER TABLE positions ADD COLUMN redeem_tx_hash TEXT DEFAULT NULL",
    "ALTER TABLE positions ADD COLUMN payout_usdc REAL DEFAULT NULL",
    # Instant resolution result (v5): show won/lost on dashboard before redeemer cashes out
    "ALTER TABLE positions ADD COLUMN result TEXT DEFAULT NULL",
    # Conviction gate column (v6)
    "ALTER TABLE snapshots ADD COLUMN passed_conviction INTEGER NOT NULL DEFAULT 0",
    # Max-gap-cap column (v7)
    "ALTER TABLE snapshots ADD COLUMN passed_max_gap INTEGER NOT NULL DEFAULT 1",
    # fill_cost: original USDC from CLOB fill, never overwritten by chain sync (v8)
    "ALTER TABLE positions ADD COLUMN fill_cost REAL NOT NULL DEFAULT 0",
    # Backfill: set fill_cost = total_cost for existing rows where fill_cost is 0
    "UPDATE positions SET fill_cost = total_cost WHERE fill_cost = 0",
    # Post-fill offload tracking (v9)
    "ALTER TABLE positions ADD COLUMN offload_order_id TEXT DEFAULT NULL",
    "ALTER TABLE positions ADD COLUMN offload_sell_price REAL DEFAULT NULL",
    "ALTER TABLE positions ADD COLUMN offload_status TEXT DEFAULT NULL",
    "ALTER TABLE positions ADD COLUMN offload_revenue REAL DEFAULT NULL",
    # HFT analysis columns on trades (v10)
    "ALTER TABLE trades ADD COLUMN timeframe_seconds INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE trades ADD COLUMN hft_barrier_pct REAL DEFAULT NULL",
    "ALTER TABLE trades ADD COLUMN hft_cap_price REAL DEFAULT NULL",
    "ALTER TABLE trades ADD COLUMN hft_path TEXT DEFAULT NULL",
    "ALTER TABLE trades ADD COLUMN hft_trigger_ask REAL DEFAULT NULL",
    # PnL finalizer bookkeeping (v11)
    "ALTER TABLE positions ADD COLUMN pnl_finalized INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE positions ADD COLUMN pnl_finalized_at TEXT DEFAULT NULL",
    "ALTER TABLE positions ADD COLUMN pnl_finalizer_state TEXT DEFAULT NULL",
    "CREATE INDEX IF NOT EXISTS idx_positions_finalizer ON positions(status, pnl_finalized)",
]


class Database:
    """Async SQLite database for the sniper."""

    def __init__(self, config: Config):
        self.config = config
        db_path = PROJECT_ROOT / config.database.path
        self._db_path = str(db_path)
        self._conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

    async def start(self):
        """Open database and initialize schema."""
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA busy_timeout=5000")
        await self._conn.executescript(_SCHEMA)
        await self._conn.executescript(_SNAPSHOTS_SCHEMA)
        await self._conn.commit()
        # Run migrations (ignore errors if already applied)
        for sql in _MIGRATIONS:
            try:
                await self._conn.execute(sql)
                await self._conn.commit()
            except Exception:
                pass  # Column already exists
        logger.info("Database opened: %s", self._db_path)

    async def stop(self):
        """Close database connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    # -- Atomic compound writes --

    async def record_confirmed_fill(
        self,
        *,
        condition_id: str,
        token_id: str,
        event_title: str,
        outcome: str,
        asset: str,
        action: str,
        shares: float,
        avg_price: float,
        total_cost: float,
        event_slug: str = "",
        buy_tx_hash: str | None = None,
        gap_pct: float | None = None,
        gap_direction: str | None = None,
        timeframe_seconds: int = 0,
        hft_barrier_pct: float | None = None,
        hft_cap_price: float | None = None,
        hft_path: str | None = None,
        hft_trigger_ask: float | None = None,
        outcome_index: int = 0,
        neg_risk: bool = False,
    ):
        """Atomic: insert trade + position with chain-truth values.

        Called ONLY after the chain Data API confirms the fill.
        All cost data comes from chain:
          shares     = size
          avg_price  = avgPrice
          total_cost = initialValue  (or size × avgPrice)

        Position is created with status='open' — no pending state.
        """
        async with self._write_lock:
            try:
                # 1. Insert trade row with chain-truth values
                await self._conn.execute(
                    """INSERT INTO trades
                       (condition_id, token_id, event_title, outcome, asset,
                        action, amount, shares, price, edge, dry_run, pnl,
                        event_slug, buy_tx_hash, gap_pct, gap_direction,
                        timeframe_seconds, hft_barrier_pct, hft_cap_price, hft_path, hft_trigger_ask)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (condition_id, token_id, event_title, outcome, asset,
                     action, total_cost, shares, avg_price, 1.0 - avg_price,
                     event_slug, buy_tx_hash, gap_pct, gap_direction,
                     int(timeframe_seconds or 0), hft_barrier_pct, hft_cap_price, hft_path, hft_trigger_ask),
                )
                # 2. Insert position (ON CONFLICT updates for re-confirm/drift)
                await self._conn.execute(
                    """INSERT INTO positions
                       (condition_id, token_id, event_title, outcome, asset,
                        outcome_index, shares, avg_price, total_cost,
                        fill_cost, neg_risk, event_slug, status)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')
                       ON CONFLICT(condition_id, token_id) DO UPDATE SET
                           shares     = excluded.shares,
                           avg_price  = excluded.avg_price,
                           total_cost = excluded.total_cost,
                           status     = 'open'
                       WHERE positions.status = 'open'""",
                    (condition_id, token_id, event_title, outcome, asset,
                     outcome_index, shares, avg_price, total_cost,
                     total_cost, int(neg_risk), event_slug),
                )
                await self._conn.commit()
            except Exception:
                try:
                    await self._conn.rollback()
                except Exception:
                    pass
                raise

    async def confirm_fill(
        self,
        *,
        condition_id: str,
        token_id: str,
        shares: float,
        avg_price: float,
        total_cost: float,
    ):
        """Update an existing position with chain-truth values.

        Used by the redeemer's sync_positions to correct drift.
        For new fills, use record_confirmed_fill() instead.
        """
        async with self._write_lock:
            try:
                await self._conn.execute(
                    """UPDATE positions
                       SET shares = ?, avg_price = ?, total_cost = ?,
                           status = 'open'
                       WHERE condition_id = ? AND token_id = ?
                         AND status = 'open'""",
                    (shares, avg_price, total_cost, condition_id, token_id),
                )
                # Distribute chain values across trade rows
                cur = await self._conn.execute(
                    "SELECT id, amount FROM trades "
                    "WHERE condition_id = ? AND token_id = ? AND action = 'SNIPE'",
                    (condition_id, token_id),
                )
                trades = await cur.fetchall()
                n = len(trades)
                if n > 0:
                    for t in trades:
                        t_shares = shares / n
                        t_cost = total_cost / n
                        t_price = t_cost / t_shares if t_shares > 0 else avg_price
                        await self._conn.execute(
                            "UPDATE trades SET shares = ?, price = ?, "
                            "edge = ?, amount = ? WHERE id = ?",
                            (t_shares, t_price, 1.0 - t_price, t_cost, t["id"]),
                        )
                await self._conn.commit()
            except Exception:
                try:
                    await self._conn.rollback()
                except Exception:
                    pass
                raise

    # Legacy wrapper — kept for any callers that haven't migrated yet.
    async def record_fill(
        self,
        *,
        condition_id: str,
        token_id: str,
        event_title: str,
        outcome: str,
        asset: str,
        action: str,
        amount: float,
        shares: float,
        price: float,
        edge: float = 0.0,
        event_slug: str = "",
        buy_tx_hash: str | None = None,
        gap_pct: float | None = None,
        gap_direction: str | None = None,
        timeframe_seconds: int = 0,
        hft_barrier_pct: float | None = None,
        hft_cap_price: float | None = None,
        hft_path: str | None = None,
        hft_trigger_ask: float | None = None,
        outcome_index: int = 0,
        neg_risk: bool = False,
    ):
        """DEPRECATED: record_pending_fill + confirm_fill is the new path.

        Kept as a compatibility shim — writes directly with estimates.
        """
        async with self._write_lock:
            try:
                await self._conn.execute(
                    """INSERT INTO trades
                       (condition_id, token_id, event_title, outcome, asset,
                        action, amount, shares, price, edge, dry_run, pnl,
                        event_slug, buy_tx_hash, gap_pct, gap_direction,
                        timeframe_seconds, hft_barrier_pct, hft_cap_price, hft_path, hft_trigger_ask)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (condition_id, token_id, event_title, outcome, asset,
                     action, amount, shares, price, edge, event_slug,
                     buy_tx_hash, gap_pct, gap_direction,
                     int(timeframe_seconds or 0), hft_barrier_pct, hft_cap_price, hft_path, hft_trigger_ask),
                )
                fill_amount = round(shares * price, 6)
                await self._conn.execute(
                    """INSERT INTO positions
                       (condition_id, token_id, event_title, outcome, asset,
                        outcome_index, shares, avg_price, total_cost,
                        fill_cost, neg_risk, event_slug)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT(condition_id, token_id) DO UPDATE SET
                           shares     = positions.shares + excluded.shares,
                           total_cost = positions.total_cost + excluded.total_cost,
                           fill_cost  = positions.fill_cost + excluded.fill_cost,
                           avg_price  = (positions.total_cost + excluded.total_cost)
                                        / (positions.shares + excluded.shares)
                       WHERE positions.status = 'open'""",
                    (condition_id, token_id, event_title, outcome, asset,
                     outcome_index, shares, price, fill_amount,
                     fill_amount, int(neg_risk), event_slug),
                )
                await self._conn.commit()
            except Exception:
                try:
                    await self._conn.rollback()
                except Exception:
                    pass
                raise

    async def record_hft_event(
        self,
        *,
        condition_id: str,
        event_type: str,
        event_title: str = "",
        asset: str = "",
        timeframe_seconds: int = 0,
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
        """Persist compact HFT lifecycle telemetry in sniper.db.

        This is analysis-only data and does not participate in trading decisions.
        """
        async with self._write_lock:
            await self._conn.execute(
                """INSERT INTO hft_events
                   (condition_id, event_title, asset, timeframe_seconds, side, event_type,
                    gap_pct, barrier_pct, current_price, price_to_beat, ask, bid, cap_price,
                    reason, details_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    condition_id,
                    event_title,
                    asset,
                    int(timeframe_seconds or 0),
                    side,
                    event_type,
                    gap_pct,
                    barrier_pct,
                    current_price,
                    price_to_beat,
                    ask,
                    bid,
                    cap_price,
                    reason,
                    json.dumps(details or {}, separators=(",", ":"), default=str),
                ),
            )
            await self._conn.commit()

    async def close_and_resolve(
        self,
        condition_id: str,
        *,
        won: bool,
        pnl: float,
        status: str = "redeemed",
        redeem_tx_hash: str | None = None,
        payout_usdc: float | None = None,
    ):
        """Atomic: close position + resolve trade results in one transaction.

        Replaces separate close_position() + resolve_live_trade() calls in
        the redeemer so position status and trade result are always consistent.
        """
        async with self._write_lock:
            try:
                now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                # 1. Close position
                await self._conn.execute(
                    """UPDATE positions SET status = ?, closed_at = ?, pnl = ?,
                           pnl_finalized = 0,
                           pnl_finalized_at = NULL,
                           pnl_finalizer_state = NULL,
                           redeem_tx_hash = COALESCE(?, redeem_tx_hash),
                           payout_usdc = COALESCE(?, payout_usdc)
                       WHERE condition_id = ? AND status = 'open'""",
                    (status, now, pnl, redeem_tx_hash, payout_usdc, condition_id),
                )
                changed_cur = await self._conn.execute("SELECT changes() AS n")
                changed_row = await changed_cur.fetchone()
                closed_rows = int((changed_row["n"] if changed_row else 0) or 0)

                # Hard guard: do not touch trade rows when no open position was
                # actually closed in this call.
                if closed_rows <= 0:
                    await self._conn.commit()
                    return False
                # 2. Resolve trade results (proportional distribution)
                # We calculate PnL for each trade as: (trade.shares * payout_per_share) - trade.amount
                # payout_per_share = (total_pnl + total_cost) / total_shares
                
                # Fetch position details to get total shares/cost
                cur = await self._conn.execute(
                    "SELECT shares, fill_cost FROM positions WHERE condition_id = ?",
                    (condition_id,)
                )
                pos = await cur.fetchone()
                
                if pos and pos["fill_cost"] > 0:
                    # payout_total = the actual USDC returned (redemption + offload)
                    payout_total = pnl + pos["fill_cost"]
                    
                    # Distribute total payout proportionally based on each trade's initial cost (amount)
                    result_str = "won" if won else "lost"
                    trades_cur = await self._conn.execute(
                        "SELECT id, amount FROM trades WHERE condition_id = ? AND dry_run = 0",
                        (condition_id,)
                    )
                    trades_rows = await trades_cur.fetchall()
                    
                    original_total_cost = sum(t["amount"] for t in trades_rows)
                    if original_total_cost > 0:
                        for t in trades_rows:
                            weight = t["amount"] / original_total_cost
                            t_payout = payout_total * weight
                            t_pnl = t_payout - t["amount"]
                            await self._conn.execute(
                                "UPDATE trades SET result = ?, pnl = ? WHERE id = ?",
                                (result_str, t_pnl, t["id"]),
                            )
                    else:
                        # Fallback if trades sum to 0
                        await self._conn.execute(
                            """UPDATE trades SET result = ?, pnl = 0
                               WHERE condition_id = ? AND dry_run = 0""",
                            (result_str, condition_id),
                        )
                else:
                    # Fallback (should not happen for resolved positions with shares)
                    result_str = "won" if won else "lost"
                    await self._conn.execute(
                        """UPDATE trades SET result = ?, pnl = ?
                           WHERE condition_id = ? AND dry_run = 0""",
                        (result_str, pnl, condition_id),
                    )
                await self._conn.commit()
                return True
            except Exception:
                try:
                    await self._conn.rollback()
                except Exception:
                    pass
                raise

    # -- Offload tracking (DB is for dashboard display, not operational decisions) --

    async def set_offload_order(
        self,
        condition_id: str,
        token_id: str,
        order_id: str,
        sell_price: float,
    ):
        """Record an offload sell order posted for a position."""
        async with self._write_lock:
            await self._conn.execute(
                """UPDATE positions
                   SET offload_order_id = ?, offload_sell_price = ?,
                       offload_status = 'posted'
                   WHERE condition_id = ? AND token_id = ? AND status = 'open'""",
                (order_id, sell_price, condition_id, token_id),
            )
            await self._conn.commit()

    async def update_offload_status(
        self,
        condition_id: str,
        status: str,
        revenue: float | None = None,
    ):
        """Update offload status and optionally set revenue from chain data.

        status values: 'posted' | 'partial' | 'filled' | None
        """
        async with self._write_lock:
            if revenue is not None:
                await self._conn.execute(
                    """UPDATE positions
                       SET offload_status = ?, offload_revenue = ?,
                           pnl_finalized = 0,
                           pnl_finalized_at = NULL,
                           pnl_finalizer_state = NULL
                       WHERE condition_id = ?""",
                    (status, revenue, condition_id),
                )
            else:
                await self._conn.execute(
                    """UPDATE positions
                       SET offload_status = ?,
                           pnl_finalized = 0,
                           pnl_finalized_at = NULL,
                           pnl_finalizer_state = NULL
                       WHERE condition_id = ?""",
                    (status, condition_id),
                )
            await self._conn.commit()

    async def finalize_resolved_pnl(
        self,
        *,
        thin_balance_ratio: float = 0.2,
        max_rows: int = 80,
    ) -> dict:
        """Finalize closed-position PnL once per row (idempotent).

        Purpose:
        - Reconcile exit cashflow for closed positions.
        - Catch "won but missing offload cashflow" rows conservatively.

        Notes:
        - This does not use wallet balance and is unaffected by deposits/withdrawals.
        - If offload cashflow arrives later, update_offload_status() resets finalizer
          flags and this method can safely re-finalize on the next cycle.
        """
        stats = {
            "checked": 0,
            "finalized": 0,
            "cashflow_missing": 0,
            "skipped": 0,
            "limit": 0,
        }
        async with self._write_lock:
            lim = max(1, min(2000, int(max_rows or 80)))
            stats["limit"] = lim
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            cur = await self._conn.execute(
                """SELECT
                       id, condition_id, status, result, pnl,
                       fill_cost, total_cost, payout_usdc, offload_revenue
                   FROM positions
                   WHERE status != 'open'
                     AND COALESCE(pnl_finalized, 0) = 0
                   ORDER BY id ASC
                   LIMIT ?""",
                (lim,),
            )
            rows = await cur.fetchall()
            stats["checked"] = len(rows)

            for pos in rows:
                pos_id = int(pos["id"])
                condition_id = str(pos["condition_id"] or "")
                result = str(pos["result"] or "").lower()
                status = str(pos["status"] or "").lower()
                fill_cost = float(pos["fill_cost"] or 0.0)
                total_cost = float(pos["total_cost"] or 0.0)
                payout = float(pos["payout_usdc"] or 0.0)
                offload_raw = pos["offload_revenue"]
                offload = float(offload_raw or 0.0)

                if result not in {"won", "lost"}:
                    await self._conn.execute(
                        """UPDATE positions
                           SET pnl_finalized = 1,
                               pnl_finalized_at = ?,
                               pnl_finalizer_state = ?
                           WHERE id = ?""",
                        (now, f"skipped:{status or 'unknown'}", pos_id),
                    )
                    stats["skipped"] += 1
                    continue

                state = "ok"
                if result == "won":
                    missing_cashflow = (
                        offload_raw is None
                        and fill_cost > 0
                        and total_cost >= 0
                        and total_cost <= (fill_cost * float(thin_balance_ratio))
                        and payout > 0
                    )
                    if missing_cashflow:
                        # Conservative fallback: treat missing sold portion at cost
                        # so we do not emit a false large loss.
                        final_pnl = payout - total_cost
                        state = "cashflow_missing_provisional"
                        stats["cashflow_missing"] += 1
                    else:
                        final_pnl = (payout + offload) - fill_cost
                else:
                    final_pnl = offload - fill_cost

                await self._conn.execute(
                    """UPDATE positions
                       SET pnl = ?,
                           pnl_finalized = 1,
                           pnl_finalized_at = ?,
                           pnl_finalizer_state = ?
                       WHERE id = ?""",
                    (final_pnl, now, state, pos_id),
                )

                trades_cur = await self._conn.execute(
                    "SELECT id, amount FROM trades WHERE condition_id = ? AND dry_run = 0",
                    (condition_id,),
                )
                trades_rows = await trades_cur.fetchall()
                original_total_cost = sum(float(t["amount"] or 0.0) for t in trades_rows)
                if trades_rows and original_total_cost > 0:
                    payout_total = final_pnl + original_total_cost
                    for t in trades_rows:
                        amt = float(t["amount"] or 0.0)
                        weight = amt / original_total_cost
                        t_payout = payout_total * weight
                        t_pnl = t_payout - amt
                        await self._conn.execute(
                            "UPDATE trades SET result = ?, pnl = ? WHERE id = ?",
                            (result, t_pnl, int(t["id"])),
                        )
                elif trades_rows:
                    await self._conn.execute(
                        """UPDATE trades
                           SET result = ?, pnl = 0
                           WHERE condition_id = ? AND dry_run = 0""",
                        (result, condition_id),
                    )

                stats["finalized"] += 1

            await self._conn.commit()
        return stats

    # -- Trades --

    async def get_trades(self, limit: int = 50) -> list[dict]:
        """Get recent trades."""
        cursor = await self._conn.execute(
            "SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def mark_resolution(self, condition_id: str, winning_outcome: str):
        """Instantly mark position + live trades with the resolution result.

        Called when market_resolved fires on WS — BEFORE the redeemer runs.
        Sets position.result and trade.result so dashboards show ✓/✗ immediately.
        """
        cursor = await self._conn.execute(
            "SELECT outcome, shares, total_cost, fill_cost FROM positions "
            "WHERE condition_id = ? AND status = 'open'",
            (condition_id,),
        )
        pos = await cursor.fetchone()
        if not pos:
            return

        won = pos["outcome"].lower() == winning_outcome.lower()
        result = "won" if won else "lost"

        # Mark position
        await self._conn.execute(
            "UPDATE positions SET result = ? WHERE condition_id = ? AND status = 'open'",
            (result, condition_id),
        )

        # Mark live trades (same logic as resolve_live_trade but by outcome)
        # Loss PnL = -total_cost (chain truth, split across trades)
        if won:
            await self._conn.execute(
                """UPDATE trades SET result = 'won', pnl = shares - amount
                   WHERE condition_id = ? AND dry_run = 0 AND result = 'pending'""",
                (condition_id,),
            )
        else:
            total_cost = pos["fill_cost"]  # frozen chain cost from initial fill
            num_trades_cur = await self._conn.execute(
                "SELECT COUNT(*) FROM trades WHERE condition_id = ? AND dry_run = 0 AND result = 'pending'",
                (condition_id,),
            )
            num_trades = (await num_trades_cur.fetchone())[0]
            if num_trades > 0:
                per_trade = total_cost / num_trades
                await self._conn.execute(
                    """UPDATE trades SET result = 'lost', pnl = ?
                       WHERE condition_id = ? AND dry_run = 0 AND result = 'pending'""",
                    (-per_trade, condition_id),
                )
            else:
                await self._conn.execute(
                    """UPDATE trades SET result = 'lost', pnl = -amount
                       WHERE condition_id = ? AND dry_run = 0""",
                    (condition_id,),
                )

        await self._conn.commit()
        logger.info("Resolution marked: %s -> %s (winner=%s)",
                    condition_id[:16], result, winning_outcome)

    async def get_recent_results(self, limit: int = 10) -> list[str]:
        """Return the most recent resolved trade results (newest first).

        Each entry is 'won' or 'lost'.  Pending/unresolved trades are excluded.
        Used by the 3-loss auto-pause safety check.
        """
        cursor = await self._conn.execute(
            "SELECT result FROM trades WHERE result IN ('won', 'lost') "
            "ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [row[0] for row in rows]

    # -- Positions --

    async def get_open_positions(self) -> list[dict]:
        """Get all open positions."""
        cursor = await self._conn.execute(
            "SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def count_open_positions(self) -> int:
        """Count open positions."""
        cursor = await self._conn.execute(
            "SELECT COUNT(*) as cnt FROM positions WHERE status = 'open'"
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    async def count_open_positions_for(self, condition_id: str) -> int:
        """Check if we hold an open position for a specific condition_id."""
        cursor = await self._conn.execute(
            "SELECT COUNT(*) as cnt FROM positions "
            "WHERE condition_id = ? AND status = 'open'",
            (condition_id,),
        )
        row = await cursor.fetchone()
        return row["cnt"] if row else 0

    async def count_open_positions_combined(self, condition_id: str) -> tuple[int, int]:
        """Single-query gate: return (total_open, open_for_market).

        Replaces two separate queries with one round-trip to the DB.
        """
        cursor = await self._conn.execute(
            "SELECT COUNT(*) as total,"
            " SUM(CASE WHEN condition_id = ? THEN 1 ELSE 0 END) as this_market"
            " FROM positions WHERE status = 'open'",
            (condition_id,),
        )
        row = await cursor.fetchone()
        return (row["total"] or 0, row["this_market"] or 0) if row else (0, 0)

    # -- Stats --

    async def get_stats(self) -> dict:
        """Get summary statistics."""
        cursor = await self._conn.execute("""
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN dry_run = 0 THEN 1 ELSE 0 END) as live_trades,
                SUM(CASE WHEN dry_run = 0 THEN amount ELSE 0 END) as total_spent,
                SUM(CASE WHEN dry_run = 0 THEN pnl ELSE 0 END) as total_pnl
            FROM trades
        """)
        row = await cursor.fetchone()

        cursor2 = await self._conn.execute("""
            SELECT
                COUNT(*) as open_positions,
                SUM(total_cost) as position_value
            FROM positions WHERE status = 'open'
        """)
        row2 = await cursor2.fetchone()

        cursor3 = await self._conn.execute("""
            SELECT
                COUNT(*) as redeemed,
                SUM(pnl) as redeemed_pnl
            FROM positions WHERE status IN ('redeemed', 'lost', 'sold')
        """)
        row3 = await cursor3.fetchone()

        return {
            "total_trades": row["total_trades"] or 0,
            "live_trades": row["live_trades"] or 0,
            "total_spent": row["total_spent"] or 0.0,
            "total_pnl": row["total_pnl"] or 0.0,
            "open_positions": row2["open_positions"] or 0,
            "position_value": row2["position_value"] or 0.0,
            "redeemed": row3["redeemed"] or 0,
            "redeemed_pnl": row3["redeemed_pnl"] or 0.0,
        }

    async def get_historical_stats(self) -> dict:
        """Load counters from DB to seed runtime stats on restart."""
        cursor = await self._conn.execute("""
            SELECT
                SUM(CASE WHEN dry_run = 0 THEN 1 ELSE 0 END) as live_trades,
                SUM(CASE WHEN dry_run = 1 THEN 1 ELSE 0 END) as dry_trades,
                SUM(CASE WHEN dry_run = 0 THEN amount ELSE 0 END) as total_spent
            FROM trades
        """)
        trade_row = await cursor.fetchone()

        cursor2 = await self._conn.execute("""
            SELECT COUNT(*) as count, COALESCE(SUM(pnl), 0.0) as pnl
            FROM positions WHERE status IN ('redeemed', 'lost', 'sold')
        """)
        pos_row = await cursor2.fetchone()

        return {
            "live_trades": trade_row["live_trades"] or 0,
            "dry_trades": trade_row["dry_trades"] or 0,
            "total_spent": trade_row["total_spent"] or 0.0,
            "redeemed_count": pos_row["count"] or 0,
            "redeemed_pnl": pos_row["pnl"] or 0.0,
        }

    # -- Bot State --

    async def get_state(self, key: str, default: str = "") -> str:
        """Get a bot state value."""
        cursor = await self._conn.execute(
            "SELECT value FROM bot_state WHERE key = ?", (key,)
        )
        row = await cursor.fetchone()
        return row["value"] if row else default

    async def set_state(self, key: str, value: str):
        """Set a bot state value."""
        await self._conn.execute(
            """INSERT INTO bot_state (key, value, updated_at) VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
               ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
            (key, value),
        )
        await self._conn.commit()

    async def delete_state(self, key: str):
        """Delete a bot state key."""
        await self._conn.execute("DELETE FROM bot_state WHERE key = ?", (key,))
        await self._conn.commit()

    async def get_inflight_cids(self) -> list[str]:
        """Return condition_ids for any in-flight snipes from a prior session.

        These are 'inflight:<cid>' markers written before the CLOB order
        and cleared after chain confirmation.  If any remain at startup,
        the bot crashed mid-confirmation.
        """
        cursor = await self._conn.execute(
            "SELECT value FROM bot_state WHERE key LIKE 'inflight:%'"
        )
        rows = await cursor.fetchall()
        return [row["value"] for row in rows]

    async def clear_all_inflight(self):
        """Delete all inflight markers (called after startup reconciliation)."""
        await self._conn.execute("DELETE FROM bot_state WHERE key LIKE 'inflight:%'")
        await self._conn.commit()

    # -- Snapshots (settings-tuning data) --

    async def record_snapshot(
        self,
        condition_id: str,
        event_title: str,
        asset: str,
        seconds_to_end: float,
        duration_seconds: int,
        up_mid: float,
        up_ask: float,
        up_depth: float,
        down_mid: float,
        down_ask: float,
        down_depth: float,
        best_side: str,
        best_mid: float,
        best_ask: float,
        best_depth: float,
        edge: float,
        binance_price: float | None,
        price_to_beat: float | None,
        gap_pct: float | None,
        gap_direction: str | None,
        ptb_source: str | None,
        passed_threshold: bool,
        passed_max_price: bool,
        passed_min_edge: bool,
        passed_depth: bool,
        passed_gap: bool,
        passed_conviction: bool = True,
        passed_max_gap: bool = True,
        passed_all: bool = False,
        action: str = "observe",
    ):
        """Record a snipe-window snapshot for settings-tuning analysis."""
        await self._conn.execute(
            """INSERT INTO snapshots
               (condition_id, event_title, asset,
                seconds_to_end, duration_seconds,
                up_mid, up_ask, up_depth,
                down_mid, down_ask, down_depth,
                best_side, best_mid, best_ask, best_depth, edge,
                binance_price, price_to_beat, gap_pct, gap_direction, ptb_source,
                passed_threshold, passed_max_price, passed_min_edge,
                passed_depth, passed_gap, passed_conviction,
                passed_max_gap, passed_all, action)
               VALUES (?,?,?, ?,?, ?,?,?, ?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?,?,?,?,?)""",
            (condition_id, event_title, asset,
             seconds_to_end, duration_seconds,
             up_mid, up_ask, up_depth,
             down_mid, down_ask, down_depth,
             best_side, best_mid, best_ask, best_depth, edge,
             binance_price, price_to_beat, gap_pct, gap_direction, ptb_source,
             int(passed_threshold), int(passed_max_price), int(passed_min_edge),
             int(passed_depth), int(passed_gap), int(passed_conviction),
             int(passed_max_gap), int(passed_all), action),
        )
        await self._conn.commit()

    async def resolve_snapshots(self, condition_id: str, winning_outcome: str):
        """Backfill resolution data on all snapshots for a resolved market."""
        await self._conn.execute(
            """UPDATE snapshots SET actual_winner = ?,
                   would_have_won = CASE WHEN LOWER(best_side) = LOWER(?) THEN 1 ELSE 0 END
               WHERE condition_id = ? AND actual_winner IS NULL""",
            (winning_outcome, winning_outcome, condition_id),
        )
        await self._conn.commit()

    async def get_snapshots(self, limit: int = 200, asset: str | None = None) -> list[dict]:
        """Get recent snapshots for analysis."""
        if asset:
            cursor = await self._conn.execute(
                "SELECT * FROM snapshots WHERE asset = ? ORDER BY id DESC LIMIT ?",
                (asset, limit),
            )
        else:
            cursor = await self._conn.execute(
                "SELECT * FROM snapshots ORDER BY id DESC LIMIT ?", (limit,)
            )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_snapshot_stats(self) -> dict:
        """Get aggregate snapshot statistics for tuning analysis."""
        cursor = await self._conn.execute("""
            SELECT
                COUNT(*) as total_snapshots,
                COUNT(CASE WHEN actual_winner IS NOT NULL THEN 1 END) as resolved,
                COUNT(CASE WHEN would_have_won = 1 THEN 1 END) as correct_side,
                COUNT(CASE WHEN passed_all = 1 THEN 1 END) as passed_all_gates,
                COUNT(CASE WHEN passed_all = 1 AND would_have_won = 1 THEN 1 END) as passed_and_won,
                AVG(best_mid) as avg_best_mid,
                AVG(best_ask) as avg_best_ask,
                AVG(best_depth) as avg_best_depth,
                AVG(edge) as avg_edge,
                AVG(gap_pct) as avg_gap_pct,
                AVG(seconds_to_end) as avg_seconds_to_end
            FROM snapshots
        """)
        row = await cursor.fetchone()
        return dict(row) if row else {}

    # -- Direct query (for dashboard reads) --

    async def query_all(self, sql: str, params: tuple = ()) -> list[dict]:
        """Execute a read query and return all rows as dicts."""
        cursor = await self._conn.execute(sql, params)
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
