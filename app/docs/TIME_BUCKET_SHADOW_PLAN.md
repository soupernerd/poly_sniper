# Time-Bucket Shadow Optimizer Plan

## Goal
Verify whether one global HFT Mode2 setting is suboptimal across different ET times of day, then prepare a safe path toward bucket-aware auto-adjustment.

## Why this matters
- A 24h optimizer can hide regime differences by averaging.
- Busy periods may favor different barrier/max-price than calm periods.
- A global winner can still produce avoidable loss streaks in specific windows.

## Phase 1 (current): bucketed replay audit
Use ET clock buckets (default `4h`) and compare:
1. current settings per TF
2. bucket-best settings per TF from the same sweep grid

Scope now:
- assets: `BTC, ETH` (or chosen set)
- TFs: `5m, 15m, 1h`
- data window: longest available (`48h` currently)
- replay: same monitor state-machine logic used by `/api/monitor/simulate`

## Tool
`tools/analyze_shadow_time_buckets.py`

Example:
```powershell
python tools/analyze_shadow_time_buckets.py --hours 48 --bucket-hours 4 --assets bitcoin,ethereum --timeframes 5m,15m,1h
```

Optional JSON output:
```powershell
python tools/analyze_shadow_time_buckets.py --hours 48 --bucket-hours 4 --output data/shadow_bucket_report.json
```

## How to read results
Each row reports per `TF + bucket`:
- `Cur(b,p,pnl)`: current barrier/max-price and PnL
- `Rec(b,p,pnl)`: best settings in sweep for that bucket
- `Delta`: `recommended_pnl - current_pnl`
- `Trades`: `recommended_trades / current_trades`

Interpretation:
- Persistent positive `Delta` in the same bucket across days suggests time-of-day specialization is real.
- Very low market counts are low-confidence (do not auto-apply off tiny samples).

## Confidence guardrails (before any auto-adjust)
- minimum markets per bucket/TF
- minimum decided trades (won+lost)
- minimum delta threshold
- holdout validation (train on prior window, test on next window)

## Phase 2 (future): safe bucket-aware auto-adjust
Only after stable uplift is observed:
1. dry-run mode (`would_apply`)
2. canary on one TF (e.g., 15m)
3. full 3-TF apply with hard guardrails:
   - cooldown between changes
   - max step size per update
   - skip changes near boundary / while open positions active
   - full audit trail + one-click revert

