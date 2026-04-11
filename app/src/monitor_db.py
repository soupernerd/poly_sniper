"""Dedicated monitor database — records market observations with wider windows than
the snipe pipeline for post-hoc analytics.

Stores every tick of price/gap/gate data for markets approaching their end time,
even those outside the snipe window.  Separate from sniper.db to avoid bloating
the core operational database.

Schema matches sniper.db snapshots but adds conviction + config thresholds at
record time so each row is fully self-contained for analysis.
"""

import aiosqlite
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.api import PolyApi

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    condition_id TEXT NOT NULL,
    event_title TEXT NOT NULL,
    asset TEXT NOT NULL,

    -- Time context
    duration_seconds INTEGER NOT NULL DEFAULT 0,
    seconds_to_end REAL NOT NULL,

    -- Book data: Up side
    up_mid REAL NOT NULL DEFAULT 0,
    up_ask REAL NOT NULL DEFAULT 0,
    up_depth REAL NOT NULL DEFAULT 0,

    -- Book data: Down side
    down_mid REAL NOT NULL DEFAULT 0,
    down_ask REAL NOT NULL DEFAULT 0,
    down_depth REAL NOT NULL DEFAULT 0,

    -- Best side (scanner pick)
    best_side TEXT NOT NULL DEFAULT '',
    best_mid REAL NOT NULL DEFAULT 0,
    best_ask REAL NOT NULL DEFAULT 0,
    best_depth REAL NOT NULL DEFAULT 0,
    edge REAL NOT NULL DEFAULT 0,
    conviction REAL NOT NULL DEFAULT 0,

    -- Binance gap data
    binance_price REAL,
    price_to_beat REAL,
    gap_pct REAL,
    gap_direction TEXT,

    -- Gate results (1 = passed, 0 = failed)
    passed_threshold INTEGER NOT NULL DEFAULT 0,
    passed_max_price INTEGER NOT NULL DEFAULT 0,
    passed_min_edge INTEGER NOT NULL DEFAULT 0,
    passed_depth INTEGER NOT NULL DEFAULT 0,
    passed_gap INTEGER NOT NULL DEFAULT 0,
    passed_conviction INTEGER NOT NULL DEFAULT 0,
    passed_max_gap INTEGER NOT NULL DEFAULT 1,
    passed_all INTEGER NOT NULL DEFAULT 0,

    -- Config thresholds at record time (for replay analysis)
    cfg_threshold REAL NOT NULL DEFAULT 0,
    cfg_max_price REAL NOT NULL DEFAULT 0,
    cfg_min_edge REAL NOT NULL DEFAULT 0,
    cfg_min_conviction REAL NOT NULL DEFAULT 0,
    cfg_gap_pct REAL NOT NULL DEFAULT 0,
    cfg_max_gap REAL NOT NULL DEFAULT 0,

    -- Source: 'ws' or 'poll'
    source TEXT NOT NULL DEFAULT 'poll',

    -- Resolution (backfilled when market resolves)
    actual_winner TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_ticks_cid ON ticks(condition_id);
CREATE INDEX IF NOT EXISTS idx_ticks_ts ON ticks(timestamp);
CREATE INDEX IF NOT EXISTS idx_ticks_asset_dur ON ticks(asset, duration_seconds);
"""

_SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS monitor_v2_markets (
    condition_id TEXT PRIMARY KEY,
    event_slug TEXT NOT NULL DEFAULT '',
    event_title TEXT NOT NULL DEFAULT '',
    asset TEXT NOT NULL DEFAULT '',
    timeframe_seconds INTEGER NOT NULL DEFAULT 0,
    token_up TEXT NOT NULL DEFAULT '',
    token_down TEXT NOT NULL DEFAULT '',
    market_start TEXT NOT NULL DEFAULT '',
    market_end TEXT NOT NULL DEFAULT '',
    winner TEXT DEFAULT NULL,
    resolved_at TEXT DEFAULT NULL,
    last_seen TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS monitor_v2_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    condition_id TEXT NOT NULL,
    event_slug TEXT NOT NULL DEFAULT '',
    event_title TEXT NOT NULL DEFAULT '',
    asset TEXT NOT NULL DEFAULT '',
    timeframe_seconds INTEGER NOT NULL DEFAULT 0,
    market_start TEXT NOT NULL DEFAULT '',
    market_end TEXT NOT NULL DEFAULT '',
    sec_to_start REAL NOT NULL DEFAULT 0,
    sec_to_end REAL NOT NULL DEFAULT 0,
    up_bid REAL NOT NULL DEFAULT 0,
    up_ask REAL NOT NULL DEFAULT 0,
    up_mid REAL NOT NULL DEFAULT 0,
    up_depth REAL NOT NULL DEFAULT 0,
    up_asks_json TEXT NOT NULL DEFAULT '',
    down_bid REAL NOT NULL DEFAULT 0,
    down_ask REAL NOT NULL DEFAULT 0,
    down_mid REAL NOT NULL DEFAULT 0,
    down_depth REAL NOT NULL DEFAULT 0,
    down_asks_json TEXT NOT NULL DEFAULT '',
    spread_up REAL NOT NULL DEFAULT 0,
    spread_down REAL NOT NULL DEFAULT 0,
    current_price REAL DEFAULT NULL,
    price_to_beat REAL DEFAULT NULL,
    gap_pct REAL DEFAULT NULL,
    gap_abs REAL DEFAULT NULL,
    gap_direction TEXT DEFAULT NULL,
    winning_side TEXT DEFAULT NULL,
    barrier_pct REAL DEFAULT NULL,
    barrier_delta_pct REAL DEFAULT NULL,
    arm_like INTEGER NOT NULL DEFAULT 0,
    winner TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_monitor_v2_samples_cid_ts
ON monitor_v2_samples(condition_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_monitor_v2_samples_asset_tf_ts
ON monitor_v2_samples(asset, timeframe_seconds, timestamp);

CREATE INDEX IF NOT EXISTS idx_monitor_v2_markets_winner
ON monitor_v2_markets(winner);
"""

# Default monitor observation windows (seconds before market end)
# These are intentionally wider than the snipe pre_end_seconds.
MONITOR_WINDOWS = {
    300: 90,        # 5m  -> last 90s
    900: 300,       # 15m -> last 5 min
    3600: 600,      # 1h  -> last 10 min
    14400: 1200,    # 4h  -> last 20 min
    86400: 3600,    # 1d  -> last 60 min
}

MONITOR_TICK_INTERVAL = 5  # seconds between ticks per market


class MonitorDB:
    """Async write-only interface to data/monitor.db."""

    def __init__(self, db_path: str | Path = "data/monitor.db"):
        self._path = str(db_path)
        self._db: aiosqlite.Connection | None = None
        # Keep monitor_v2 fidelity while reducing per-row duplication:
        # static market metadata (title/slug/start/end) lives in monitor_v2_markets.
        self._compact_v2_metadata_writes = str(
            os.environ.get("MONITOR_V2_COMPACT_METADATA_WRITES", "1")
        ).strip().lower() not in {"0", "false", "no", "off"}

    async def init(self):
        self._db = await aiosqlite.connect(self._path)
        await self._db.executescript(_SCHEMA)
        await self._db.executescript(_SCHEMA_V2)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        # Schema migrations — add columns that didn't exist in v1
        _migrations = [
            ("event_slug", "TEXT DEFAULT NULL"),
            ("oracle_cl_direction", "TEXT DEFAULT NULL"),
            ("oracle_bin_direction", "TEXT DEFAULT NULL"),
            ("sniped", "INTEGER NOT NULL DEFAULT 0"),
        ]
        for col, typedef in _migrations:
            try:
                await self._db.execute(f"ALTER TABLE ticks ADD COLUMN {col} {typedef}")
            except Exception:
                pass  # Column already exists
        _v2_migrations = [
            ("up_asks_json", "TEXT NOT NULL DEFAULT ''"),
            ("down_asks_json", "TEXT NOT NULL DEFAULT ''"),
        ]
        for col, typedef in _v2_migrations:
            try:
                await self._db.execute(f"ALTER TABLE monitor_v2_samples ADD COLUMN {col} {typedef}")
            except Exception:
                pass  # Column already exists
        await self._db.commit()
        logger.info("[MONITOR DB] Initialized: %s", self._path)
        logger.info(
            "[MONITOR DB] monitor_v2 compact metadata writes: %s",
            "ON" if self._compact_v2_metadata_writes else "OFF",
        )

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def record_tick(
        self, *, condition_id: str, event_title: str, asset: str,
        duration_seconds: int, seconds_to_end: float,
        up_mid: float, up_ask: float, up_depth: float,
        down_mid: float, down_ask: float, down_depth: float,
        best_side: str, best_mid: float, best_ask: float,
        best_depth: float, edge: float, conviction: float,
        binance_price: float | None, price_to_beat: float | None,
        gap_pct: float | None, gap_direction: str | None,
        passed_threshold: bool, passed_max_price: bool,
        passed_min_edge: bool, passed_depth: bool,
        passed_gap: bool, passed_conviction: bool,
        passed_max_gap: bool, passed_all: bool,
        cfg_threshold: float, cfg_max_price: float,
        cfg_min_edge: float, cfg_min_conviction: float,
        cfg_gap_pct: float, cfg_max_gap: float,
        source: str = "poll",
        event_slug: str | None = None,
        oracle_cl_direction: str | None = None,
        oracle_bin_direction: str | None = None,
        sniped: bool = False,
    ):
        if not self._db:
            return
        await self._db.execute(
            """INSERT INTO ticks (
                condition_id, event_title, asset,
                duration_seconds, seconds_to_end,
                up_mid, up_ask, up_depth,
                down_mid, down_ask, down_depth,
                best_side, best_mid, best_ask, best_depth, edge, conviction,
                binance_price, price_to_beat, gap_pct, gap_direction,
                passed_threshold, passed_max_price, passed_min_edge,
                passed_depth, passed_gap, passed_conviction, passed_max_gap, passed_all,
                cfg_threshold, cfg_max_price, cfg_min_edge,
                cfg_min_conviction, cfg_gap_pct, cfg_max_gap,
                source,
                event_slug, oracle_cl_direction, oracle_bin_direction, sniped
            ) VALUES (
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?,
                ?, ?, ?, ?
            )""",
            (
                condition_id, event_title[:80], asset,
                duration_seconds, seconds_to_end,
                up_mid, up_ask, up_depth,
                down_mid, down_ask, down_depth,
                best_side, best_mid, best_ask, best_depth, edge, conviction,
                binance_price, price_to_beat, gap_pct, gap_direction,
                int(passed_threshold), int(passed_max_price), int(passed_min_edge),
                int(passed_depth), int(passed_gap), int(passed_conviction),
                int(passed_max_gap), int(passed_all),
                cfg_threshold, cfg_max_price, cfg_min_edge,
                cfg_min_conviction, cfg_gap_pct, cfg_max_gap,
                source,
                event_slug, oracle_cl_direction, oracle_bin_direction, int(sniped),
            ),
        )
        await self._db.commit()

    async def backfill_winner(self, condition_id: str, winner: str):
        """Set actual_winner on all ticks for a resolved market."""
        if not self._db:
            return
        await self._db.execute(
            "UPDATE ticks SET actual_winner = ? WHERE condition_id = ? AND actual_winner IS NULL",
            (winner, condition_id),
        )
        await self._db.commit()

    async def prune(self, days: int = 14):
        """Delete monitor rows older than N days."""
        if not self._db:
            return
        # Legacy ticks table retention.
        await self._db.execute(
            "DELETE FROM ticks WHERE REPLACE(timestamp, 'T', ' ') < datetime('now', ?)",
            (f"-{days} days",),
        )
        # monitor_v2 retention (core replay dataset).
        await self._db.execute(
            "DELETE FROM monitor_v2_samples WHERE REPLACE(timestamp, 'T', ' ') < datetime('now', ?)",
            (f"-{days} days",),
        )
        # Remove market metadata rows no longer referenced by retained samples.
        await self._db.execute(
            "DELETE FROM monitor_v2_markets "
            "WHERE condition_id NOT IN (SELECT DISTINCT condition_id FROM monitor_v2_samples)"
        )
        await self._db.commit()
        logger.info("[MONITOR DB] Pruned monitor rows older than %d days", days)

    async def mark_sniped(self, condition_id: str):
        """Mark the most recent tick for a market as sniped."""
        if not self._db:
            return
        await self._db.execute(
            "UPDATE ticks SET sniped = 1 WHERE condition_id = ? "
            "AND id = (SELECT MAX(id) FROM ticks WHERE condition_id = ?)",
            (condition_id, condition_id),
        )
        await self._db.commit()

    async def sweep_winners(self, api: "PolyApi"):
        """Periodic backfill: query CLOB API for unresolved markets and stamp actual_winner.

        Called on a timer (e.g., every 60s) to catch markets the WS resolution
        callback missed — ones where we weren't subscribed at resolve time.
        """
        if not self._db:
            return
        # Find distinct condition_ids that ended but have no winner yet.
        # Only look at markets that ended > 60s ago (give time to resolve).
        # NOTE: ticks.timestamp uses ISO-8601 'T' separator (strftime %Y-%m-%dT…)
        #       but datetime() returns space-separated format.  Use REPLACE to
        #       normalise before comparing.
        rows = await self._db.execute_fetchall(
            """SELECT DISTINCT condition_id FROM ticks
               WHERE actual_winner IS NULL
               AND seconds_to_end < 30
               AND REPLACE(timestamp, 'T', ' ') < datetime('now', '-60 seconds')
               ORDER BY timestamp DESC
               LIMIT 50""",
        )
        if not rows:
            return

        filled = 0
        for (cid,) in rows:
            try:
                market = await api.get_market(cid)
                if not market:
                    continue
                tokens = market.get("tokens", [])
                winner = None
                for tok in tokens:
                    if tok.get("winner", False):
                        winner = tok.get("outcome", "")
                        break
                if not winner and market.get("closed", False):
                    for tok in tokens:
                        if tok.get("price", 0) == 1:
                            winner = tok.get("outcome", "")
                            break
                if winner:
                    await self.backfill_winner(cid, winner)
                    filled += 1
            except Exception as e:
                logger.debug("[MONITOR DB] sweep error for %s: %s", cid[:16], e)
        if filled:
            logger.info("[MONITOR DB] Reconciled winners for %d markets", filled)

    # -- Monitor V2 (independent research pipeline) --

    async def upsert_v2_market(
        self,
        *,
        condition_id: str,
        event_slug: str,
        event_title: str,
        asset: str,
        timeframe_seconds: int,
        token_up: str,
        token_down: str,
        market_start: str,
        market_end: str,
    ):
        if not self._db:
            return
        await self._db.execute(
            """INSERT INTO monitor_v2_markets
               (condition_id, event_slug, event_title, asset, timeframe_seconds,
                token_up, token_down, market_start, market_end, last_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
               ON CONFLICT(condition_id) DO UPDATE SET
                 event_slug=excluded.event_slug,
                 event_title=excluded.event_title,
                 asset=excluded.asset,
                 timeframe_seconds=excluded.timeframe_seconds,
                 token_up=excluded.token_up,
                 token_down=excluded.token_down,
                 market_start=excluded.market_start,
                 market_end=excluded.market_end,
                 last_seen=strftime('%Y-%m-%dT%H:%M:%fZ', 'now')""",
            (
                condition_id,
                event_slug or "",
                event_title or "",
                asset or "",
                int(timeframe_seconds or 0),
                token_up or "",
                token_down or "",
                market_start or "",
                market_end or "",
            ),
        )
        await self._db.commit()

    async def ensure_v2_market_row(
        self,
        *,
        condition_id: str,
        event_slug: str,
        event_title: str,
        asset: str,
        timeframe_seconds: int,
        token_up: str,
        token_down: str,
        market_start: str,
        market_end: str,
    ):
        """Ensure a monitor_v2_markets row exists for this condition_id.

        Core invariant: every sampled CID must have a corresponding market row.
        Uses INSERT OR IGNORE so normal lifecycle upsert ownership stays intact.
        """
        if not self._db:
            return
        await self._db.execute(
            """INSERT OR IGNORE INTO monitor_v2_markets
               (condition_id, event_slug, event_title, asset, timeframe_seconds,
                token_up, token_down, market_start, market_end, last_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))""",
            (
                condition_id,
                event_slug or "",
                event_title or "",
                asset or "",
                int(timeframe_seconds or 0),
                token_up or "",
                token_down or "",
                market_start or "",
                market_end or "",
            ),
        )

    async def record_v2_sample(
        self,
        *,
        condition_id: str,
        event_slug: str,
        event_title: str,
        asset: str,
        timeframe_seconds: int,
        token_up: str | None,
        token_down: str | None,
        market_start: str,
        market_end: str,
        sec_to_start: float,
        sec_to_end: float,
        up_bid: float,
        up_ask: float,
        up_mid: float,
        up_depth: float,
        up_asks_json: str | None,
        down_bid: float,
        down_ask: float,
        down_mid: float,
        down_depth: float,
        down_asks_json: str | None,
        spread_up: float,
        spread_down: float,
        current_price: float | None,
        price_to_beat: float | None,
        gap_pct: float | None,
        gap_abs: float | None,
        gap_direction: str | None,
        winning_side: str | None,
        barrier_pct: float | None,
        barrier_delta_pct: float | None,
        arm_like: bool,
    ):
        if not self._db:
            return
        # Invariant guard: samples must never outlive market metadata linkage.
        await self.ensure_v2_market_row(
            condition_id=condition_id,
            event_slug=event_slug,
            event_title=event_title,
            asset=asset,
            timeframe_seconds=timeframe_seconds,
            token_up=token_up or "",
            token_down=token_down or "",
            market_start=market_start or "",
            market_end=market_end or "",
        )

        event_slug_store = event_slug or ""
        event_title_store = event_title or ""
        market_start_store = market_start or ""
        market_end_store = market_end or ""

        if self._compact_v2_metadata_writes:
            # Duplicated static metadata is already persisted in monitor_v2_markets.
            # Keep sample rows lean to slow DB growth without changing replay fidelity.
            event_slug_store = ""
            event_title_store = ""
            market_start_store = ""
            market_end_store = ""

        await self._db.execute(
            """INSERT INTO monitor_v2_samples
               (condition_id, event_slug, event_title, asset, timeframe_seconds,
                market_start, market_end, sec_to_start, sec_to_end,
                up_bid, up_ask, up_mid, up_depth, up_asks_json,
                down_bid, down_ask, down_mid, down_depth, down_asks_json,
                spread_up, spread_down,
                current_price, price_to_beat, gap_pct, gap_abs, gap_direction,
                winning_side, barrier_pct, barrier_delta_pct, arm_like)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                condition_id,
                event_slug_store,
                event_title_store,
                asset or "",
                int(timeframe_seconds or 0),
                market_start_store,
                market_end_store,
                float(sec_to_start),
                float(sec_to_end),
                float(up_bid),
                float(up_ask),
                float(up_mid),
                float(up_depth),
                up_asks_json or "[]",
                float(down_bid),
                float(down_ask),
                float(down_mid),
                float(down_depth),
                down_asks_json or "[]",
                float(spread_up),
                float(spread_down),
                current_price,
                price_to_beat,
                gap_pct,
                gap_abs,
                gap_direction,
                winning_side,
                barrier_pct,
                barrier_delta_pct,
                int(arm_like),
            ),
        )
        await self._db.commit()

    async def reconcile_v2_markets_from_samples(self, *, limit: int = 2000) -> int:
        """Repair orphan sample CIDs by ensuring corresponding market rows exist."""
        if not self._db:
            return 0
        capped = max(1, min(10000, int(limit or 2000)))
        await self._db.execute(
            """
            INSERT OR IGNORE INTO monitor_v2_markets
            (condition_id, event_slug, event_title, asset, timeframe_seconds,
             token_up, token_down, market_start, market_end, winner, resolved_at, last_seen)
            SELECT
                s.condition_id,
                COALESCE(MAX(NULLIF(s.event_slug, '')), ''),
                COALESCE(MAX(NULLIF(s.event_title, '')), ''),
                COALESCE(MAX(NULLIF(s.asset, '')), ''),
                COALESCE(MAX(CASE WHEN s.timeframe_seconds > 0 THEN s.timeframe_seconds END), 0),
                '',
                '',
                COALESCE(MAX(NULLIF(s.market_start, '')), ''),
                COALESCE(MAX(NULLIF(s.market_end, '')), ''),
                NULL,
                NULL,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            FROM monitor_v2_samples s
            LEFT JOIN monitor_v2_markets m ON m.condition_id = s.condition_id
            WHERE m.condition_id IS NULL
            GROUP BY s.condition_id
            LIMIT ?
            """,
            (capped,),
        )
        cur = await self._db.execute("SELECT changes()")
        row = await cur.fetchone()
        inserted = int(row[0] or 0) if row else 0
        if inserted:
            await self._db.commit()
            logger.info("[MONITOR V2] Reconciled %d missing market row(s) from samples", inserted)
        return inserted

    async def sweep_v2_winners(self, api: "PolyApi"):
        """Backfill winners for monitor_v2 markets + samples."""
        if not self._db:
            return
        try:
            await self.reconcile_v2_markets_from_samples(limit=2000)
        except Exception as e:
            logger.debug("[MONITOR V2] reconcile before winner sweep failed: %s", e)
        rows = await self._db.execute_fetchall(
            """SELECT condition_id FROM monitor_v2_markets
               WHERE winner IS NULL
               ORDER BY last_seen DESC
               LIMIT 100"""
        )
        if not rows:
            return
        filled = 0
        for (cid,) in rows:
            try:
                market = await api.get_market(cid)
                if not market:
                    continue
                tokens = market.get("tokens", [])
                winner = None
                for tok in tokens:
                    if tok.get("winner", False):
                        winner = tok.get("outcome", "")
                        break
                if not winner and market.get("closed", False):
                    for tok in tokens:
                        try:
                            if float(tok.get("price", 0) or 0) == 1.0:
                                winner = tok.get("outcome", "")
                                break
                        except Exception:
                            continue
                if winner:
                    await self._db.execute(
                        "UPDATE monitor_v2_markets SET winner=?, resolved_at=strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE condition_id=?",
                        (winner, cid),
                    )
                    await self._db.execute(
                        "UPDATE monitor_v2_samples SET winner=? WHERE condition_id=? AND winner IS NULL",
                        (winner, cid),
                    )
                    filled += 1
            except Exception as e:
                logger.debug("[MONITOR V2] winner sweep error for %s: %s", cid[:16], e)
        if filled:
            await self._db.commit()
            logger.info("[MONITOR V2] Reconciled winners for %d markets", filled)

    async def wipe_v2(self) -> int:
        """Delete monitor_v2 data. Returns rows removed from samples."""
        if not self._db:
            return 0
        cur = await self._db.execute("SELECT COUNT(*) FROM monitor_v2_samples")
        row = await cur.fetchone()
        sample_count = int(row[0] or 0) if row else 0
        await self._db.execute("DELETE FROM monitor_v2_samples")
        await self._db.execute("DELETE FROM monitor_v2_markets")
        await self._db.commit()
        return sample_count

    @staticmethod
    def get_monitor_window(duration_seconds: int) -> int:
        """Return the observation window for a given market duration."""
        return MONITOR_WINDOWS.get(duration_seconds, 90)
