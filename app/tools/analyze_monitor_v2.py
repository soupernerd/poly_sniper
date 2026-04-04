#!/usr/bin/env python
"""Incremental monitor_v2 analyzer with cross-wipe state merge.

Usage:
  python tools/analyze_monitor_v2.py
  python tools/analyze_monitor_v2.py --db data/monitor.db --hours 48

Outputs:
  - latest report JSON (default: data/monitor_report_latest.json)
  - cumulative state JSON (default: data/monitor_report_state.json)
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


@dataclass
class Args:
    db: Path
    state: Path
    output: Path
    hours: int
    reset_state: bool


def _parse_args() -> Args:
    p = argparse.ArgumentParser(description="Analyze monitor_v2 samples incrementally.")
    p.add_argument("--db", default="data/monitor.db", help="Path to monitor.db")
    p.add_argument("--state", default="data/monitor_report_state.json", help="Path to cumulative state JSON")
    p.add_argument("--output", default="data/monitor_report_latest.json", help="Path to latest report JSON")
    p.add_argument("--hours", type=int, default=24, help="Latest-window hours for snapshot block")
    p.add_argument("--reset-state", action="store_true", help="Reset cumulative state before processing")
    a = p.parse_args()
    return Args(
        db=Path(a.db),
        state=Path(a.state),
        output=Path(a.output),
        hours=max(1, min(720, int(a.hours))),
        reset_state=bool(a.reset_state),
    )


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_state() -> dict:
    return {
        "version": 1,
        "updated_at": _iso_now(),
        "last_timestamp": "",
        "rows_processed_total": 0,
        "aggregate": {},
    }


def _load_state(path: Path, reset: bool) -> dict:
    if reset or not path.exists():
        return _new_state()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return _new_state()
        data.setdefault("version", 1)
        data.setdefault("updated_at", _iso_now())
        data.setdefault("last_timestamp", "")
        data.setdefault("rows_processed_total", 0)
        data.setdefault("aggregate", {})
        return data
    except Exception:
        return _new_state()


def _session_from_iso(ts: str) -> str:
    try:
        # sqlite rows are like 2026-03-09T20:11:04.123Z
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return "am" if dt.astimezone(ET).hour < 12 else "pm"
    except Exception:
        return "unknown"


def _agg_key(asset: str, tf: int, session: str) -> str:
    return f"{asset}|{tf}|{session}"


def _ensure_bucket(agg: dict, key: str, *, asset: str, tf: int, session: str) -> dict:
    if key not in agg:
        agg[key] = {
            "asset": asset,
            "timeframe_seconds": tf,
            "session": session,
            "samples": 0,
            "arm_hits": 0,
            "barrier_hits": 0,
            "resolved_rows": 0,
            "right_side_rows": 0,
            "sum_gap_pct": 0.0,
            "min_gap_pct": None,
            "max_gap_pct": None,
            "sum_barrier_pct": 0.0,
        }
    return agg[key]


def _update_bucket(bucket: dict, row: sqlite3.Row):
    bucket["samples"] += 1
    arm_like = int(row["arm_like"] or 0)
    bucket["arm_hits"] += arm_like

    gap_pct = row["gap_pct"]
    barrier_pct = row["barrier_pct"]
    if gap_pct is not None:
        g = float(gap_pct)
        bucket["sum_gap_pct"] += g
        bucket["min_gap_pct"] = g if bucket["min_gap_pct"] is None else min(bucket["min_gap_pct"], g)
        bucket["max_gap_pct"] = g if bucket["max_gap_pct"] is None else max(bucket["max_gap_pct"], g)
        if barrier_pct is not None and g >= float(barrier_pct):
            bucket["barrier_hits"] += 1
    if barrier_pct is not None:
        bucket["sum_barrier_pct"] += float(barrier_pct)

    winner = row["winner"]
    side = row["winning_side"]
    if winner:
        bucket["resolved_rows"] += 1
        if side and str(side) == str(winner):
            bucket["right_side_rows"] += 1


def _build_latest_snapshot(conn: sqlite3.Connection, hours: int) -> dict:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = conn.execute(
        """
        SELECT
            asset,
            timeframe_seconds,
            COUNT(*) AS samples,
            SUM(CASE WHEN arm_like = 1 THEN 1 ELSE 0 END) AS arm_hits,
            AVG(gap_pct) AS avg_gap_pct,
            MIN(gap_pct) AS min_gap_pct,
            MAX(gap_pct) AS max_gap_pct,
            AVG(barrier_pct) AS avg_barrier_pct,
            SUM(CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END) AS resolved_rows,
            SUM(CASE WHEN winner IS NOT NULL AND winning_side = winner THEN 1 ELSE 0 END) AS right_side_rows
        FROM monitor_v2_samples
        WHERE timestamp >= ?
        GROUP BY asset, timeframe_seconds
        ORDER BY timeframe_seconds, asset
        """,
        (cutoff,),
    ).fetchall()
    out = []
    for r in rows:
        samples = int(r["samples"] or 0)
        resolved = int(r["resolved_rows"] or 0)
        arm_hits = int(r["arm_hits"] or 0)
        right = int(r["right_side_rows"] or 0)
        out.append(
            {
                "asset": str(r["asset"] or ""),
                "timeframe_seconds": int(r["timeframe_seconds"] or 0),
                "samples": samples,
                "arm_hits": arm_hits,
                "arm_rate_pct": (arm_hits / samples * 100.0) if samples else 0.0,
                "avg_gap_pct": float(r["avg_gap_pct"] or 0.0),
                "min_gap_pct": float(r["min_gap_pct"] or 0.0),
                "max_gap_pct": float(r["max_gap_pct"] or 0.0),
                "avg_barrier_pct": float(r["avg_barrier_pct"] or 0.0),
                "resolved_rows": resolved,
                "right_side_rows": right,
                "right_side_pct": (right / resolved * 100.0) if resolved else 0.0,
            }
        )
    return {
        "hours": hours,
        "cutoff_utc": cutoff,
        "rows": out,
    }


def main():
    args = _parse_args()
    if not args.db.exists():
        raise SystemExit(f"monitor db not found: {args.db}")

    args.state.parent.mkdir(parents=True, exist_ok=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)

    state = _load_state(args.state, args.reset_state)
    last_ts = str(state.get("last_timestamp", "") or "")

    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='monitor_v2_samples' LIMIT 1"
    ).fetchone()
    if not has_table:
        state["updated_at"] = _iso_now()
        args.state.write_text(json.dumps(state, indent=2), encoding="utf-8")
        empty_latest = {
            "generated_at": _iso_now(),
            "db_path": str(args.db),
            "state_path": str(args.state),
            "delta_rows_added": 0,
            "state_last_timestamp": state.get("last_timestamp", ""),
            "aggregate_keys": len(state.get("aggregate", {})),
            "hours": args.hours,
            "cutoff_utc": (datetime.now(timezone.utc) - timedelta(hours=args.hours)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "rows": [],
        }
        args.output.write_text(json.dumps(empty_latest, indent=2), encoding="utf-8")
        print(f"monitor_v2 table not present yet in {args.db}; wrote empty report.")
        return

    rows = conn.execute(
        """
        SELECT timestamp, asset, timeframe_seconds, gap_pct, barrier_pct, arm_like, winner, winning_side
        FROM monitor_v2_samples
        WHERE (? = '' OR timestamp > ?)
        ORDER BY timestamp ASC
        """,
        (last_ts, last_ts),
    ).fetchall()

    agg = state.setdefault("aggregate", {})
    max_ts = last_ts
    for row in rows:
        ts = str(row["timestamp"] or "")
        if ts and (not max_ts or ts > max_ts):
            max_ts = ts
        session = _session_from_iso(ts)
        asset = str(row["asset"] or "")
        tf = int(row["timeframe_seconds"] or 0)
        key = _agg_key(asset, tf, session)
        bucket = _ensure_bucket(agg, key, asset=asset, tf=tf, session=session)
        _update_bucket(bucket, row)

    state["last_timestamp"] = max_ts
    state["rows_processed_total"] = int(state.get("rows_processed_total", 0)) + len(rows)
    state["updated_at"] = _iso_now()
    args.state.write_text(json.dumps(state, indent=2), encoding="utf-8")

    latest = _build_latest_snapshot(conn, args.hours)
    latest["generated_at"] = _iso_now()
    latest["db_path"] = str(args.db)
    latest["state_path"] = str(args.state)
    latest["delta_rows_added"] = len(rows)
    latest["state_last_timestamp"] = state["last_timestamp"]
    latest["aggregate_keys"] = len(state.get("aggregate", {}))
    args.output.write_text(json.dumps(latest, indent=2), encoding="utf-8")

    print(
        f"monitor_v2 analyzed: +{len(rows)} rows | "
        f"state keys={len(state.get('aggregate', {}))} | "
        f"latest={args.output}"
    )


if __name__ == "__main__":
    main()
