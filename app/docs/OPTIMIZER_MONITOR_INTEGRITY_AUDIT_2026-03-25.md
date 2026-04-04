# Optimizer/Monitor Integrity Audit (2026-03-25)

## Scope
- Target: Shadow Optimizer V2 correctness (main dashboard).
- Constraint: core refactor/invariant fixes only. No symptom patches.

## Optimizer Intent (contract)
1. Inputs are only:
   - `objective_mode` (`pnl` | `win_rate` | `roi`)
   - `lookback_hours`
   - `min_trades`
2. Optimizer evaluates replay data for enabled cells and returns:
   - `current` (reference only)
   - `recommended` (best candidate under objective/window/min-trades rules)
3. `Apply Recommended` copies recommended matrix values into live config.
4. W/L must be computed from resolved winners in the replay source; unresolved remain `open`.

## What failed
- Replay rows existed in `monitor_v2_samples`, but many condition_ids had no matching row in `monitor_v2_markets`.
- Winner backfill (`sweep_v2_winners`) iterates only `monitor_v2_markets`.
- Result: many replay trades stayed `open`, so optimizer displayed `Trades > 0` with `W/L = 0/0`.

## Verified evidence
- 24h linkage check showed large sample->market CID mismatch (system-wide, not BTC-only).
- Optimizer snapshot showed high `open` counts with near-zero decided W/L for affected cells.
- Root path:
  - replay joins winners via `COALESCE(s.winner, m.winner)`
  - winner sweep stamps winners from `monitor_v2_markets`
  - orphan sample CIDs never receive winner stamps

## Root cause class
- Data-integrity invariant missing between monitor tables:
  - `monitor_v2_samples.condition_id` not guaranteed to exist in `monitor_v2_markets`.

## Core refactor fix plan
1. Enforce write-time invariant:
   - before writing any `monitor_v2_samples` row, ensure a `monitor_v2_markets` row exists for that `condition_id` (`INSERT OR IGNORE`).
2. Reconcile existing orphan CIDs:
   - periodically insert missing `monitor_v2_markets` rows from distinct sample CIDs.
   - then winner sweep can resolve those rows.
3. Keep optimizer logic unchanged:
   - objective ranking/selection remains as-is.
   - fix is at data integrity layer, not scoring layer.

## Expected behavior after refactor
1. No new orphan sample CIDs.
2. Winner sweeps can resolve all sampled markets.
3. Optimizer W/L and win% reflect resolved history instead of persistent `open` inflation.
4. Works across all assets/timeframes, not just BTC 5m.

