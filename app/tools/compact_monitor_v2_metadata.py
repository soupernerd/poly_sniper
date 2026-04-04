#!/usr/bin/env python3
"""Compact duplicated monitor_v2 sample metadata in-place.

Why:
- monitor_v2_samples stores static market metadata per row (slug/title/start/end)
- same fields already live in monitor_v2_markets (condition_id keyed)
- clearing duplicated per-sample copies keeps fidelity while reducing growth

Safety:
- dry-run by default
- only compacts rows that have a corresponding market row with non-empty
  event_title and market_end
- does not touch price/depth/gap/winner/replay-critical sample fields
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path


def _bytes_human(n: int) -> str:
    v = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024.0 or unit == "TB":
            return f"{v:.2f}{unit}"
        v /= 1024.0
    return f"{n}B"


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compact duplicated monitor_v2 metadata.")
    p.add_argument("--db", default="data/monitor.db", help="Path to monitor.db")
    p.add_argument("--batch", type=int, default=50000, help="Rows per batch")
    p.add_argument("--apply", action="store_true", help="Apply updates (default: dry-run)")
    p.add_argument("--vacuum", action="store_true", help="Run VACUUM after compaction (only with --apply)")
    return p.parse_args()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return bool(row)


def main() -> None:
    args = _parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    before_size = db_path.stat().st_size
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")

    if not _table_exists(conn, "monitor_v2_samples") or not _table_exists(conn, "monitor_v2_markets"):
        raise SystemExit("Required tables missing: monitor_v2_samples and/or monitor_v2_markets")

    total = int(conn.execute("SELECT COUNT(*) FROM monitor_v2_samples").fetchone()[0] or 0)
    candidates = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM monitor_v2_samples
            WHERE event_slug <> '' OR event_title <> '' OR market_start <> '' OR market_end <> ''
            """
        ).fetchone()[0]
        or 0
    )
    eligible = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM monitor_v2_samples s
            JOIN monitor_v2_markets m ON m.condition_id = s.condition_id
            WHERE (s.event_slug <> '' OR s.event_title <> '' OR s.market_start <> '' OR s.market_end <> '')
              AND m.event_title <> ''
              AND m.market_end <> ''
            """
        ).fetchone()[0]
        or 0
    )

    print(f"db={db_path}")
    print(f"size_before={_bytes_human(before_size)} ({before_size} bytes)")
    print(f"rows_total={total}")
    print(f"rows_with_duplicate_metadata={candidates}")
    print(f"rows_eligible_for_safe_compaction={eligible}")

    if not args.apply:
        print("dry-run complete (no changes applied).")
        conn.close()
        return

    if args.batch <= 0:
        raise SystemExit("--batch must be > 0")

    updated = 0
    started = time.perf_counter()
    while True:
        before_changes = conn.total_changes
        conn.execute(
            """
            WITH to_compact AS (
                SELECT s.id
                FROM monitor_v2_samples s
                JOIN monitor_v2_markets m ON m.condition_id = s.condition_id
                WHERE (s.event_slug <> '' OR s.event_title <> '' OR s.market_start <> '' OR s.market_end <> '')
                  AND m.event_title <> ''
                  AND m.market_end <> ''
                LIMIT ?
            )
            UPDATE monitor_v2_samples
            SET event_slug = '',
                event_title = '',
                market_start = '',
                market_end = ''
            WHERE id IN (SELECT id FROM to_compact)
            """,
            (int(args.batch),),
        )
        conn.commit()
        delta = conn.total_changes - before_changes
        if delta <= 0:
            break
        updated += int(delta)
        print(f"updated={updated}")

    if args.vacuum:
        print("running VACUUM...")
        conn.execute("VACUUM")
        conn.commit()
        print("VACUUM done.")

    conn.close()
    after_size = db_path.stat().st_size
    elapsed = time.perf_counter() - started
    print(f"updated_total={updated}")
    print(f"size_after={_bytes_human(after_size)} ({after_size} bytes)")
    print(f"elapsed_s={elapsed:.2f}")


if __name__ == "__main__":
    main()
