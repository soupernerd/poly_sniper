#!/usr/bin/env python
"""Merge one or more monitor report state files into a single aggregate JSON.

Usage:
  python tools/merge_monitor_reports.py --inputs data/state_a.json data/state_b.json --output data/monitor_merged.json
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"invalid json object in {path}")
    return data


def _bucket() -> dict:
    return {
        "asset": "",
        "timeframe_seconds": 0,
        "session": "",
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


def main():
    p = argparse.ArgumentParser(description="Merge monitor report state files.")
    p.add_argument("--inputs", nargs="+", required=True, help="Input state JSON files")
    p.add_argument("--output", required=True, help="Output merged JSON")
    args = p.parse_args()

    merged = {
        "version": 1,
        "generated_at": _iso_now(),
        "inputs": args.inputs,
        "rows_processed_total": 0,
        "aggregate": {},
    }

    for src in args.inputs:
        path = Path(src)
        data = _load(path)
        merged["rows_processed_total"] += int(data.get("rows_processed_total", 0) or 0)
        src_agg = data.get("aggregate", {}) or {}
        for key, row in src_agg.items():
            dst = merged["aggregate"].setdefault(key, _bucket())
            dst["asset"] = row.get("asset", dst["asset"])
            dst["timeframe_seconds"] = int(row.get("timeframe_seconds", dst["timeframe_seconds"]) or 0)
            dst["session"] = row.get("session", dst["session"])
            for fld in (
                "samples",
                "arm_hits",
                "barrier_hits",
                "resolved_rows",
                "right_side_rows",
            ):
                dst[fld] += int(row.get(fld, 0) or 0)
            for fld in ("sum_gap_pct", "sum_barrier_pct"):
                dst[fld] += float(row.get(fld, 0.0) or 0.0)

            mn = row.get("min_gap_pct")
            mx = row.get("max_gap_pct")
            if mn is not None:
                mn = float(mn)
                dst["min_gap_pct"] = mn if dst["min_gap_pct"] is None else min(dst["min_gap_pct"], mn)
            if mx is not None:
                mx = float(mx)
                dst["max_gap_pct"] = mx if dst["max_gap_pct"] is None else max(dst["max_gap_pct"], mx)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    print(f"merged {len(args.inputs)} file(s) -> {out}")


if __name__ == "__main__":
    main()
