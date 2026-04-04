#!/usr/bin/env python
"""Bucketed state-machine replay for HFT Mode2 time-of-day analysis.

Purpose:
  - Split markets into ET clock buckets (default 4h chunks).
  - Run the same replay logic used by monitor backtest per bucket.
  - Compare current settings vs bucket-best settings (barrier + max price).

Example:
  python tools/analyze_shadow_time_buckets.py --hours 48
  python tools/analyze_shadow_time_buckets.py --hours 48 --bucket-hours 4 --output data/shadow_bucket_report.json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.config import load_config  # noqa: E402


ET = ZoneInfo("America/New_York")
TF_LABEL_TO_SECONDS = {"5m": 300, "15m": 900, "1h": 3600}
TF_SECONDS_TO_LABEL = {v: k for k, v in TF_LABEL_TO_SECONDS.items()}
ASSETS = ("bitcoin", "ethereum", "solana", "xrp")

# Keep aligned with shadow optimizer defaults.
BARRIER_GRID = (0.08, 0.1, 0.12, 0.15, 0.18, 0.2, 0.22, 0.25, 0.3, 0.35)
PRICE_GRID = (0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.92, 0.94, 0.96)


@dataclass
class Args:
    db: Path
    hours: int
    bucket_hours: int
    assets: list[str]
    timeframes: list[str]
    max_bet: float
    muzzle_seconds: int
    start_delay_seconds: int
    armed_sniper: bool
    require_depth: bool
    output: Path | None


def _barrier_for(barriers: dict, asset: str, timeframe_seconds: int) -> float:
    tf_label = TF_SECONDS_TO_LABEL.get(int(timeframe_seconds), "")
    row = barriers.get(asset, {})
    if tf_label and isinstance(row, dict):
        try:
            return float(row.get(tf_label, 0.0) or 0.0)
        except Exception:
            return 0.0
    return 0.0


def _simulate_market_rows(rows: list[sqlite3.Row], params: dict) -> dict | None:
    """State-machine replay logic, aligned with monitor backtest endpoint."""
    if not rows:
        return None
    first = rows[0]
    asset = str(first["asset"] or "")
    tf_seconds = int(first["timeframe_seconds"] or 0)
    barrier = _barrier_for(params["barriers"], asset, tf_seconds)
    max_price = float(params["max_price"])
    max_bet = float(params["max_bet"])
    muzzle = int(params["muzzle_seconds"])
    start_delay = int(params["start_delay_seconds"])
    armed_mode = bool(params["armed_sniper"])
    require_depth = bool(params["require_depth"])

    armed = False
    armed_side = ""
    armed_ts = ""
    winner = None
    trade: dict | None = None
    arm_count = 0

    for r in rows:
        if r["winner"] and winner is None:
            winner = str(r["winner"])
        sec_to_start = float(r["sec_to_start"] or 0.0)
        sec_to_end = float(r["sec_to_end"] or 0.0)
        if sec_to_end < muzzle:
            if armed:
                armed = False
            break
        if sec_to_start > -start_delay:
            continue

        side = str(r["winning_side"] or "")

        def _depth_to_cap(asks_raw: str | None, fallback_depth: float) -> float:
            if asks_raw:
                try:
                    asks = json.loads(str(asks_raw))
                    if isinstance(asks, list):
                        depth = 0.0
                        for lvl in asks:
                            if isinstance(lvl, dict):
                                p = float(lvl.get("price", 0) or 0.0)
                                s = float(lvl.get("size", 0) or 0.0)
                            elif isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                                p = float(lvl[0] or 0.0)
                                s = float(lvl[1] or 0.0)
                            else:
                                continue
                            if p <= 0 or s <= 0:
                                continue
                            if p <= max_price:
                                depth += p * s
                        return depth
                except Exception:
                    pass
            return fallback_depth

        def _ask_depth_for(pick_side: str) -> tuple[float, float]:
            if pick_side == "Up":
                ask = float(r["up_ask"] or 0.0)
                fallback_depth = float(r["up_depth"] or 0.0)
                asks_raw = r["up_asks_json"] if "up_asks_json" in r.keys() else None
                return ask, _depth_to_cap(asks_raw, fallback_depth)
            ask = float(r["down_ask"] or 0.0)
            fallback_depth = float(r["down_depth"] or 0.0)
            asks_raw = r["down_asks_json"] if "down_asks_json" in r.keys() else None
            return ask, _depth_to_cap(asks_raw, fallback_depth)

        if armed:
            if side in {"Up", "Down"} and side != armed_side:
                armed = False
                armed_side = ""
                armed_ts = ""
                continue

            trap_ask, trap_depth = _ask_depth_for(armed_side)
            if trap_ask > 0 and trap_ask <= max_price and (trap_depth >= max_bet if require_depth else True):
                shares = max_bet / trap_ask
                try:
                    t_arm = datetime.fromisoformat(armed_ts.replace("Z", "+00:00"))
                    t_ent = datetime.fromisoformat(str(r["timestamp"]).replace("Z", "+00:00"))
                    arm_to_entry = max(0.0, (t_ent - t_arm).total_seconds())
                except Exception:
                    arm_to_entry = None
                trade = {
                    "condition_id": str(r["condition_id"]),
                    "event_title": str(r["event_title"] or ""),
                    "asset": asset,
                    "timeframe_seconds": tf_seconds,
                    "timeframe": TF_SECONDS_TO_LABEL.get(tf_seconds, str(tf_seconds)),
                    "path": "trap",
                    "side": armed_side,
                    "entry_price": trap_ask,
                    "shares": shares,
                    "cost": max_bet,
                    "entry_ts": str(r["timestamp"] or ""),
                    "arm_ts": armed_ts,
                    "arm_to_entry_s": arm_to_entry,
                    "barrier_pct": barrier,
                    "gap_pct_entry": float(r["gap_pct"]) if r["gap_pct"] is not None else None,
                }
                break
            continue

        if side not in {"Up", "Down"}:
            continue

        gap_pct = r["gap_pct"]
        if gap_pct is None:
            continue
        gap_pct = float(gap_pct)
        if gap_pct < barrier:
            continue

        ask, depth = _ask_depth_for(side)
        if ask <= 0:
            continue

        def _can_fill() -> bool:
            if not require_depth:
                return True
            return depth >= max_bet

        if not armed_mode:
            if ask <= max_price and _can_fill():
                shares = max_bet / ask
                trade = {
                    "condition_id": str(r["condition_id"]),
                    "event_title": str(r["event_title"] or ""),
                    "asset": asset,
                    "timeframe_seconds": tf_seconds,
                    "timeframe": TF_SECONDS_TO_LABEL.get(tf_seconds, str(tf_seconds)),
                    "path": "instant",
                    "side": side,
                    "entry_price": ask,
                    "shares": shares,
                    "cost": max_bet,
                    "entry_ts": str(r["timestamp"] or ""),
                    "arm_ts": None,
                    "arm_to_entry_s": 0.0,
                    "barrier_pct": barrier,
                    "gap_pct_entry": gap_pct,
                }
                break
            continue

        if ask <= max_price and _can_fill():
            arm_count += 1
            shares = max_bet / ask
            trade = {
                "condition_id": str(r["condition_id"]),
                "event_title": str(r["event_title"] or ""),
                "asset": asset,
                "timeframe_seconds": tf_seconds,
                "timeframe": TF_SECONDS_TO_LABEL.get(tf_seconds, str(tf_seconds)),
                "path": "trap",
                "side": side,
                "entry_price": ask,
                "shares": shares,
                "cost": max_bet,
                "entry_ts": str(r["timestamp"] or ""),
                "arm_ts": str(r["timestamp"] or ""),
                "arm_to_entry_s": 0.0,
                "barrier_pct": barrier,
                "gap_pct_entry": gap_pct,
            }
            break
        armed = True
        armed_side = side
        armed_ts = str(r["timestamp"] or "")
        arm_count += 1

    if trade is None:
        return None

    trade["winner"] = winner
    if winner is None:
        trade["result"] = "open"
        trade["pnl"] = 0.0
    elif trade["side"] == winner:
        trade["result"] = "won"
        trade["pnl"] = float(trade["shares"]) * (1.0 - float(trade["entry_price"]))
    else:
        trade["result"] = "lost"
        trade["pnl"] = -float(trade["cost"])
    trade["arm_count"] = arm_count
    return trade


def _parse_args() -> Args:
    p = argparse.ArgumentParser(description="Analyze time-of-day bucket settings using monitor state-machine replay.")
    p.add_argument("--db", default="data/monitor.db", help="Path to monitor.db")
    p.add_argument("--hours", type=int, default=48, help="History window in hours")
    p.add_argument("--bucket-hours", type=int, default=4, help="Clock bucket size (hours)")
    p.add_argument("--assets", default="bitcoin,ethereum", help="CSV assets")
    p.add_argument("--timeframes", default="5m,15m,1h", help="CSV timeframes")
    p.add_argument("--max-bet", type=float, default=-1, help="Replay max bet override (<=0 uses config)")
    p.add_argument("--muzzle", type=int, default=-1, help="Muzzle seconds override (<0 uses config)")
    p.add_argument("--start-delay", type=int, default=-1, help="Start-delay override (<0 uses config)")
    p.add_argument("--armed-sniper", type=int, choices=(0, 1), default=-1, help="Armed sniper override (0/1, -1 uses config)")
    p.add_argument("--require-depth", type=int, choices=(0, 1), default=1, help="Require depth >= bet (1/0)")
    p.add_argument("--output", default="", help="Optional JSON output path")
    ns = p.parse_args()

    cfg = load_config()
    assets = [a.strip().lower() for a in str(ns.assets).split(",") if a.strip().lower() in ASSETS]
    if not assets:
        assets = ["bitcoin", "ethereum"]
    tfs = [t.strip().lower() for t in str(ns.timeframes).split(",") if t.strip().lower() in TF_LABEL_TO_SECONDS]
    if not tfs:
        tfs = ["5m", "15m", "1h"]

    max_bet = float(ns.max_bet) if ns.max_bet and ns.max_bet > 0 else float(getattr(cfg.trend, "hft_bet_amount", 5.0))
    muzzle = int(ns.muzzle) if ns.muzzle is not None and int(ns.muzzle) >= 0 else int(getattr(cfg.trend, "hft_muzzle_seconds", 0))
    delay = int(ns.start_delay) if ns.start_delay is not None and int(ns.start_delay) >= 0 else int(getattr(cfg.trend, "hft_barrier_delay", 0))
    armed = bool(int(ns.armed_sniper)) if int(ns.armed_sniper) in (0, 1) else bool(getattr(cfg.trend, "hft_armed_sniper_enabled", False))

    return Args(
        db=Path(ns.db),
        hours=max(1, min(720, int(ns.hours))),
        bucket_hours=max(1, min(12, int(ns.bucket_hours))),
        assets=assets,
        timeframes=tfs,
        max_bet=max(0.01, max_bet),
        muzzle_seconds=max(0, muzzle),
        start_delay_seconds=max(0, delay),
        armed_sniper=armed,
        require_depth=bool(int(ns.require_depth)),
        output=Path(ns.output) if ns.output else None,
    )


def _parse_iso(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _bucket_label_from_market_end(market_end: str, bucket_hours: int) -> str:
    dt = _parse_iso(market_end)
    if not dt:
        return "unknown"
    et = dt.astimezone(ET)
    start = (et.hour // bucket_hours) * bucket_hours
    end = (start + bucket_hours) % 24
    return f"{start:02d}:00-{end:02d}:00"


def _rank(summary: dict) -> tuple[float, float, float, int]:
    return (
        float(summary.get("pnl", 0.0) or 0.0),
        float(summary.get("roi_pct", 0.0) or 0.0),
        float(summary.get("win_rate_pct", 0.0) or 0.0),
        int(summary.get("trades", 0) or 0),
    )


def _new_summary() -> dict:
    return {
        "trades": 0,
        "won": 0,
        "lost": 0,
        "open": 0,
        "spent": 0.0,
        "pnl": 0.0,
        "win_rate_pct": 0.0,
        "roi_pct": 0.0,
    }


def _summarize_markets(markets: list[list[sqlite3.Row]], params: dict) -> dict:
    s = _new_summary()
    for rows in markets:
        t = _simulate_market_rows(rows, params)
        if not t:
            continue
        s["trades"] += 1
        s["spent"] += float(t.get("cost", 0.0) or 0.0)
        s["pnl"] += float(t.get("pnl", 0.0) or 0.0)
        result = str(t.get("result", ""))
        if result == "won":
            s["won"] += 1
        elif result == "lost":
            s["lost"] += 1
        else:
            s["open"] += 1
    decided = s["won"] + s["lost"]
    if decided > 0:
        s["win_rate_pct"] = (s["won"] / decided) * 100.0
    if s["spent"] > 0:
        s["roi_pct"] = (s["pnl"] / s["spent"]) * 100.0
    return s


def _base_barriers(cfg, assets: list[str], tfs: list[str]) -> dict:
    out: dict[str, dict[str, float]] = {}
    for a in assets:
        out[a] = {}
        for tf in tfs:
            secs = TF_LABEL_TO_SECONDS[tf]
            out[a][tf] = float(cfg.trend.get_hft_barrier_pct(secs))
    return out


def _clone_barriers_with_override(base: dict, tf_label: str, barrier: float) -> dict:
    cloned = {a: dict(v) for a, v in base.items()}
    for a in cloned:
        if tf_label in cloned[a]:
            cloned[a][tf_label] = float(barrier)
    return cloned


def _fetch_market_buckets(args: Args) -> tuple[dict[tuple[str, str], list[list[sqlite3.Row]]], int]:
    if not args.db.exists():
        raise SystemExit(f"monitor db not found: {args.db}")
    conn = sqlite3.connect(str(args.db))
    conn.row_factory = sqlite3.Row
    has = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='monitor_v2_samples' LIMIT 1"
    ).fetchone()
    if not has:
        return {}, 0

    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=args.hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    tf_seconds = [TF_LABEL_TO_SECONDS[tf] for tf in args.timeframes]
    q_assets = ",".join(["?"] * len(args.assets))
    q_tfs = ",".join(["?"] * len(tf_seconds))
    sql = f"""
        SELECT
            s.condition_id,
            s.timestamp,
            s.asset,
            s.timeframe_seconds,
            COALESCE(NULLIF(s.event_title, ''), m.event_title) AS event_title,
            COALESCE(NULLIF(s.market_end, ''), m.market_end) AS market_end,
            s.sec_to_start, s.sec_to_end,
            s.up_ask, s.down_ask, s.up_depth, s.down_depth,
            s.up_asks_json, s.down_asks_json,
            s.gap_pct, s.winning_side,
            COALESCE(s.winner, m.winner) AS winner
        FROM monitor_v2_samples s
        LEFT JOIN monitor_v2_markets m ON m.condition_id = s.condition_id
        WHERE s.timestamp >= ?
          AND s.asset IN ({q_assets})
          AND s.timeframe_seconds IN ({q_tfs})
        ORDER BY s.condition_id ASC, s.timestamp ASC
    """
    rows = conn.execute(sql, [cutoff_iso, *args.assets, *tf_seconds]).fetchall()
    conn.close()

    markets_by_key: dict[tuple[str, str], list[list[sqlite3.Row]]] = {}
    current_cid = ""
    market_rows: list[sqlite3.Row] = []
    for r in rows:
        cid = str(r["condition_id"] or "")
        if not cid:
            continue
        if not current_cid:
            current_cid = cid
        if cid != current_cid:
            if market_rows:
                _push_market(markets_by_key, market_rows, args.bucket_hours)
            market_rows = [r]
            current_cid = cid
        else:
            market_rows.append(r)
    if market_rows:
        _push_market(markets_by_key, market_rows, args.bucket_hours)
    return markets_by_key, len(rows)


def _push_market(dst: dict[tuple[str, str], list[list[sqlite3.Row]]], market_rows: list[sqlite3.Row], bucket_hours: int) -> None:
    if not market_rows:
        return
    tf_secs = int(market_rows[0]["timeframe_seconds"] or 0)
    tf_label = TF_SECONDS_TO_LABEL.get(tf_secs, "")
    if not tf_label:
        return
    market_end = str(market_rows[0]["market_end"] or "")
    bucket = _bucket_label_from_market_end(market_end, bucket_hours)
    dst.setdefault((tf_label, bucket), []).append(market_rows)


def main() -> None:
    args = _parse_args()
    cfg = load_config()
    base_barriers = _base_barriers(cfg, args.assets, args.timeframes)
    cur_barrier = {tf: float(cfg.trend.get_hft_barrier_pct(TF_LABEL_TO_SECONDS[tf])) for tf in args.timeframes}
    cur_max_price = {tf: float(cfg.trend.get_hft_max_price(TF_LABEL_TO_SECONDS[tf])) for tf in args.timeframes}

    markets_by_key, rows_scanned = _fetch_market_buckets(args)
    if not markets_by_key:
        print("No monitor_v2 rows found for given filters.")
        return

    report = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_hours": args.hours,
        "bucket_hours": args.bucket_hours,
        "assets": args.assets,
        "timeframes": args.timeframes,
        "rows_scanned": rows_scanned,
        "replay_params": {
            "max_bet": args.max_bet,
            "muzzle_seconds": args.muzzle_seconds,
            "start_delay_seconds": args.start_delay_seconds,
            "armed_sniper": args.armed_sniper,
            "require_depth": args.require_depth,
        },
        "results": [],
    }

    print(
        f"Bucket sweep | window={args.hours}h | bucket={args.bucket_hours}h | assets={','.join(args.assets)} | "
        f"tfs={','.join(args.timeframes)} | rows={rows_scanned}"
    )
    print("-" * 140)
    print("TF   Bucket         Mkts   Cur(b,p,pnl)                 Rec(b,p,pnl)                 Delta     Trades")
    print("-" * 140)

    for tf in args.timeframes:
        tf_keys = sorted([k for k in markets_by_key if k[0] == tf], key=lambda x: x[1])
        for _, bucket in tf_keys:
            mkts = markets_by_key[(tf, bucket)]

            cur_params = {
                "max_bet": args.max_bet,
                "max_price": cur_max_price[tf],
                "muzzle_seconds": args.muzzle_seconds,
                "start_delay_seconds": args.start_delay_seconds,
                "armed_sniper": args.armed_sniper,
                "require_depth": args.require_depth,
                "assets": args.assets,
                "tf_labels": args.timeframes,
                "tf_seconds": [TF_LABEL_TO_SECONDS[x] for x in args.timeframes],
                "barriers": _clone_barriers_with_override(base_barriers, tf, cur_barrier[tf]),
            }
            cur_summary = _summarize_markets(mkts, cur_params)

            barrier_candidates = sorted({round(float(x), 3) for x in BARRIER_GRID} | {round(cur_barrier[tf], 3)})
            price_candidates = sorted({round(float(x), 4) for x in PRICE_GRID} | {round(cur_max_price[tf], 4)})

            best_barrier = cur_barrier[tf]
            best_barrier_summary = cur_summary
            for b in barrier_candidates:
                p = dict(cur_params)
                p["barriers"] = _clone_barriers_with_override(base_barriers, tf, b)
                p["max_price"] = cur_max_price[tf]
                s = _summarize_markets(mkts, p)
                if _rank(s) > _rank(best_barrier_summary):
                    best_barrier = b
                    best_barrier_summary = s

            best_price = cur_max_price[tf]
            best_summary = best_barrier_summary
            for px in price_candidates:
                p = dict(cur_params)
                p["barriers"] = _clone_barriers_with_override(base_barriers, tf, best_barrier)
                p["max_price"] = px
                s = _summarize_markets(mkts, p)
                if _rank(s) > _rank(best_summary):
                    best_price = px
                    best_summary = s

            delta = float(best_summary["pnl"]) - float(cur_summary["pnl"])
            rec = {
                "timeframe": tf,
                "bucket": bucket,
                "markets": len(mkts),
                "current": {
                    "barrier_pct": cur_barrier[tf],
                    "max_price": cur_max_price[tf],
                    "summary": cur_summary,
                },
                "recommended": {
                    "barrier_pct": float(best_barrier),
                    "max_price": float(best_price),
                    "summary": best_summary,
                },
                "delta_pnl": delta,
            }
            report["results"].append(rec)
            print(
                f"{tf:<4} {bucket:<13} {len(mkts):>4}   "
                f"b {cur_barrier[tf]:.3f} p {cur_max_price[tf]:.2f} {_usd(cur_summary['pnl']):>8}   "
                f"b {best_barrier:.3f} p {best_price:.2f} {_usd(best_summary['pnl']):>8}   "
                f"{_usd(delta):>8}   {int(best_summary['trades']):>3}/{int(cur_summary['trades']):<3}"
            )

    print("-" * 140)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"saved -> {args.output}")


def _usd(v: float) -> str:
    n = float(v or 0.0)
    return f"{'+' if n >= 0 else '-'}${abs(n):.2f}"


if __name__ == "__main__":
    main()
