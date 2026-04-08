#!/usr/bin/env python
"""Build data-backed rule formulas from realized trades + monitor history.

Produces two independent formula tracks:
  1) Trade-realized formula (what your executed bets actually returned).
  2) Monitor-wide formula (all collected arm-like opportunities).

Usage:
  python tools/analyze_profit_formulas.py
  python tools/analyze_profit_formulas.py --output data/profit_formula_report.json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Callable, Iterable
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


@dataclass
class TradeRow:
    timestamp_utc: datetime
    timeframe_seconds: int
    amount: float
    pnl: float


@dataclass
class MonitorArmRow:
    timestamp_utc: datetime
    asset: str
    timeframe_seconds: int
    winning_side: str
    winner: str
    ask: float

    @property
    def hour_et(self) -> int:
        return self.timestamp_utc.astimezone(ET).hour

    @property
    def correct(self) -> bool:
        return self.winning_side == self.winner

    @property
    def ev_per_1(self) -> float:
        # Expected PnL for a $1 entry at captured ask.
        return (1.0 / self.ask - 1.0) if self.correct else -1.0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Analyze profitable rule formulas from local DBs.")
    p.add_argument("--sniper-db", default="data/sniper.db", help="Path to sniper.db")
    p.add_argument("--monitor-db", default="data/monitor.db", help="Path to monitor.db")
    p.add_argument("--output", default="data/profit_formula_report.json", help="Output JSON report")
    p.add_argument(
        "--timeframes",
        default="300,900",
        help="Comma-separated timeframe seconds for monitor analysis (default: 300,900)",
    )
    p.add_argument(
        "--min-hour-trades",
        type=int,
        default=8,
        help="Minimum 5m trades per split-half hour to be considered robust",
    )
    p.add_argument(
        "--min-monitor-hour-samples",
        type=int,
        default=200,
        help="Minimum monitor first-arm samples per ET hour",
    )
    p.add_argument(
        "--min-positive-assets",
        type=int,
        default=3,
        help="For monitor formula, minimum assets (of 4) with positive EV at that hour",
    )
    return p.parse_args()


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _summary_from_rows(rows: Iterable[TradeRow], *, size_cap: float | None = None) -> dict:
    n = 0
    pnl = 0.0
    deployed = 0.0
    wins = 0
    for r in rows:
        scale = 1.0
        if size_cap is not None and r.amount > 0:
            scale = min(1.0, float(size_cap) / r.amount)
        adj_pnl = r.pnl * scale
        adj_amt = r.amount * scale
        n += 1
        pnl += adj_pnl
        deployed += adj_amt
        if adj_pnl > 0:
            wins += 1
    return {
        "n": n,
        "pnl": pnl,
        "deployed": deployed,
        "win_rate_pct": (wins / n * 100.0) if n else 0.0,
        "roi_pct": (pnl / deployed * 100.0) if deployed else 0.0,
    }


def _load_trades(path: Path) -> list[TradeRow]:
    if not path.exists():
        raise FileNotFoundError(f"sniper db not found: {path}")
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT timestamp, timeframe_seconds, amount, pnl
        FROM trades
        WHERE pnl IS NOT NULL
        ORDER BY timestamp ASC
        """
    ).fetchall()
    out: list[TradeRow] = []
    for r in rows:
        out.append(
            TradeRow(
                timestamp_utc=datetime.fromisoformat(str(r["timestamp"]).replace("Z", "+00:00")),
                timeframe_seconds=int(r["timeframe_seconds"] or 0),
                amount=float(r["amount"] or 0.0),
                pnl=float(r["pnl"] or 0.0),
            )
        )
    return out


def _hour_stats_5m(rows: list[TradeRow]) -> dict[int, dict]:
    by_hour: dict[int, list[TradeRow]] = {h: [] for h in range(24)}
    for r in rows:
        if r.timeframe_seconds != 300:
            continue
        by_hour[r.timestamp_utc.astimezone(ET).hour].append(r)
    stats: dict[int, dict] = {}
    for h in range(24):
        stats[h] = _summary_from_rows(by_hour[h])
    return stats


def _filter_rows(rows: list[TradeRow], pred: Callable[[TradeRow], bool]) -> list[TradeRow]:
    return [r for r in rows if pred(r)]


def _derive_trade_formula(rows: list[TradeRow], *, min_hour_trades: int) -> dict:
    if not rows:
        return {"error": "no realized trades found"}

    mid = len(rows) // 2
    train = rows[:mid]
    test = rows[mid:]
    hs_train = _hour_stats_5m(train)
    hs_test = _hour_stats_5m(test)

    robust_hours: list[int] = []
    for h in range(24):
        tr = hs_train[h]
        te = hs_test[h]
        if tr["n"] < min_hour_trades or te["n"] < min_hour_trades:
            continue
        if tr["roi_pct"] > 0 and te["roi_pct"] > 0:
            robust_hours.append(h)

    if not robust_hours:
        # Fallback: top 6 full-sample 5m ROI hours with decent sample size.
        hs_all = _hour_stats_5m(rows)
        ranked = [
            (h, s["roi_pct"])
            for h, s in hs_all.items()
            if s["n"] >= max(4, min_hour_trades // 2)
        ]
        ranked.sort(key=lambda x: x[1], reverse=True)
        robust_hours = [h for h, _ in ranked[:6]]

    def formula_pred(r: TradeRow) -> bool:
        if r.timeframe_seconds == 900:
            return True
        if r.timeframe_seconds == 300:
            return r.timestamp_utc.astimezone(ET).hour in robust_hours
        return False

    baseline = _summary_from_rows(rows)
    only_15m = _summary_from_rows(_filter_rows(rows, lambda r: r.timeframe_seconds == 900))
    formula_rows = _filter_rows(rows, formula_pred)
    formula = _summary_from_rows(formula_rows)

    cap_trials: dict[str, dict] = {}
    for cap in (1.0, 2.0, 3.0, 5.0):
        cap_trials[f"cap_{cap:g}"] = _summary_from_rows(formula_rows, size_cap=cap)
    best_cap_key = max(cap_trials.keys(), key=lambda k: cap_trials[k]["roi_pct"])

    return {
        "description": "Always keep 15m; gate 5m by robust ET hours from realized PnL.",
        "robust_5m_hours_et": robust_hours,
        "split_method": "first_half_vs_second_half",
        "min_hour_trades_each_half": min_hour_trades,
        "baseline_all": baseline,
        "only_15m": only_15m,
        "formula_no_cap": formula,
        "formula_size_caps": cap_trials,
        "best_size_cap_by_roi": best_cap_key,
    }


def _load_monitor_first_arms(path: Path, *, timeframes: list[int]) -> list[MonitorArmRow]:
    if not path.exists():
        raise FileNotFoundError(f"monitor db not found: {path}")
    if not timeframes:
        raise ValueError("timeframes cannot be empty")

    placeholders = ",".join("?" for _ in timeframes)
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    q = f"""
    WITH arms AS (
      SELECT
        condition_id, timestamp, asset, timeframe_seconds,
        winning_side, winner, up_ask, down_ask,
        ROW_NUMBER() OVER (PARTITION BY condition_id ORDER BY timestamp ASC) AS rn
      FROM monitor_v2_samples
      WHERE arm_like = 1
        AND winner IS NOT NULL
        AND winning_side IN ('Up', 'Down')
        AND timeframe_seconds IN ({placeholders})
    )
    SELECT *
    FROM arms
    WHERE rn = 1
    """
    rows = con.execute(q, tuple(timeframes)).fetchall()
    out: list[MonitorArmRow] = []
    for r in rows:
        side = str(r["winning_side"] or "")
        ask = float(r["up_ask"] if side == "Up" else r["down_ask"] or 0.0)
        if ask <= 0:
            continue
        out.append(
            MonitorArmRow(
                timestamp_utc=datetime.fromisoformat(str(r["timestamp"]).replace("Z", "+00:00")),
                asset=str(r["asset"] or ""),
                timeframe_seconds=int(r["timeframe_seconds"] or 0),
                winning_side=side,
                winner=str(r["winner"] or ""),
                ask=ask,
            )
        )
    return out


def _derive_monitor_formula(
    rows: list[MonitorArmRow],
    *,
    min_hour_samples: int,
    min_positive_assets: int,
) -> dict:
    if not rows:
        return {"error": "no monitor first-arm rows found"}

    assets = sorted({r.asset for r in rows if r.asset})
    by_hour: dict[int, list[MonitorArmRow]] = {h: [] for h in range(24)}
    by_hour_asset: dict[tuple[int, str], list[MonitorArmRow]] = {}
    by_asset_tf: dict[tuple[str, int], list[MonitorArmRow]] = {}

    for r in rows:
        h = r.hour_et
        by_hour[h].append(r)
        by_hour_asset.setdefault((h, r.asset), []).append(r)
        by_asset_tf.setdefault((r.asset, r.timeframe_seconds), []).append(r)

    hour_table: list[dict] = []
    recommended_hours: list[int] = []
    for h in range(24):
        bucket = by_hour[h]
        if not bucket:
            continue
        n = len(bucket)
        acc = sum(1 for r in bucket if r.correct) / n * 100.0
        ev = mean(r.ev_per_1 for r in bucket)
        pos_assets = 0
        for a in assets:
            sub = by_hour_asset.get((h, a), [])
            if len(sub) < 20:
                continue
            sub_ev = mean(r.ev_per_1 for r in sub)
            if sub_ev > 0:
                pos_assets += 1
        row = {
            "hour_et": h,
            "n": n,
            "acc_pct": acc,
            "ev_per_1": ev,
            "positive_assets": pos_assets,
        }
        hour_table.append(row)
        if n >= min_hour_samples and ev > 0 and pos_assets >= min_positive_assets:
            recommended_hours.append(h)

    hour_table.sort(key=lambda x: x["ev_per_1"], reverse=True)

    asset_tf_rows: list[dict] = []
    for (asset, tf), bucket in by_asset_tf.items():
        n = len(bucket)
        if n < 80:
            continue
        asset_tf_rows.append(
            {
                "asset": asset,
                "timeframe_seconds": tf,
                "n": n,
                "acc_pct": sum(1 for r in bucket if r.correct) / n * 100.0,
                "ev_per_1": mean(r.ev_per_1 for r in bucket),
                "avg_ask": mean(r.ask for r in bucket),
            }
        )
    asset_tf_rows.sort(key=lambda x: x["ev_per_1"], reverse=True)

    return {
        "description": "From all collected monitor first-arm opportunities, pick ET hours with positive EV and broad asset support.",
        "assets_seen": assets,
        "rows_analyzed": len(rows),
        "recommended_hours_et": sorted(recommended_hours),
        "hour_gate_requirements": {
            "min_hour_samples": min_hour_samples,
            "min_positive_assets": min_positive_assets,
        },
        "hour_ranked": hour_table,
        "asset_timeframe_ranked": asset_tf_rows,
    }


def main() -> None:
    args = _parse_args()
    root = Path(__file__).resolve().parents[1]
    sniper_db = (root / args.sniper_db).resolve()
    monitor_db = (root / args.monitor_db).resolve()
    output = (root / args.output).resolve()
    tfs = [int(x.strip()) for x in str(args.timeframes).split(",") if x.strip()]

    trades = _load_trades(sniper_db)
    trade_formula = _derive_trade_formula(trades, min_hour_trades=max(2, int(args.min_hour_trades)))

    monitor_arms = _load_monitor_first_arms(monitor_db, timeframes=tfs)
    monitor_formula = _derive_monitor_formula(
        monitor_arms,
        min_hour_samples=max(20, int(args.min_monitor_hour_samples)),
        min_positive_assets=max(1, int(args.min_positive_assets)),
    )

    overlap = sorted(
        set(trade_formula.get("robust_5m_hours_et", []))
        & set(monitor_formula.get("recommended_hours_et", []))
    )

    report = {
        "generated_at_utc": _iso_now(),
        "sniper_db": str(sniper_db),
        "monitor_db": str(monitor_db),
        "trade_formula": trade_formula,
        "monitor_formula": monitor_formula,
        "conservative_overlap_hours_et": overlap,
    }

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote report: {output}")
    print(f"Trade robust 5m hours ET: {trade_formula.get('robust_5m_hours_et', [])}")
    print(f"Monitor recommended hours ET: {monitor_formula.get('recommended_hours_et', [])}")
    print(f"Conservative overlap hours ET: {overlap}")


if __name__ == "__main__":
    main()
