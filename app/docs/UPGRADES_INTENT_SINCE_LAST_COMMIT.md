# Upgrade Intent Inventory Since Last Commit

Date: 2026-03-16  
Scope basis: current uncommitted diff vs last git commit in this workspace.

This document is intentionally **intent-focused** (product/behavior goals), not code walkthrough.

---

## 1) Complete changed-file inventory

The current working diff includes:

1. `app/dashboard/app.py`
2. `app/dashboard/index.html`
3. `app/dashboard/monitor.html`
4. `app/src/config.py`
5. `app/src/monitor_db.py`
6. `app/config.yaml`

---

## 2) Upgrade intents by area

## A. Scanner toggle fidelity (asset + timeframe gates)

### Intent
Make per-asset/per-timeframe enablement deterministic and safe, so toggles mean exactly what they show.

### Intended behavior
1. Boolean parsing should be robust (`on/off`, `true/false`, `1/0`, etc.) instead of naive truthy casting.
2. When an explicit toggle matrix is sent, missing cells should not silently default to ON.
3. Effective enabled cell should always be:
   - asset master ON
   - global timeframe ON
   - asset-timeframe cell ON

### Where this intent appears
1. `app/src/config.py`
2. `app/dashboard/app.py`
3. `app/dashboard/index.html`
4. `app/dashboard/monitor.html`

---

## B. Matrix-first HFT controls (barrier/max price by cell)

### Intent
Use per-cell matrix values (asset x timeframe) as first-class configuration for both runtime decisions and optimizer analysis.

### Intended behavior
1. Barrier and max-price matrix values are treated as canonical by cell.
2. Optimizer should evaluate/recommend at the same cell granularity.
3. Applying recommendations should map directly back into matrix cells.

### Where this intent appears
1. `app/dashboard/index.html` (apply recommended into matrix fields)
2. `app/dashboard/monitor.html` (optimizer display by cell)
3. `app/dashboard/app.py` (payload normalization and optimizer params)

---

## C. Optimizer V2 unification (single engine shape)

### Intent
Replace fragmented old optimizer paths with one shared V2 optimizer contract so main dashboard and monitor backtest can use the same logic model.

### Intended behavior
1. Introduce V2 endpoints:
   - main: `/api/shadow-optimizer-v2/latest`, `/api/shadow-optimizer-v2/run`
   - monitor: `/api/monitor/optimize-v2`
2. Move heavy optimization work off event loop (threaded execution for monitor optimize request path).
3. Return a normalized payload shape including:
   - `current` vs `recommended` by cell
   - `delta_pnl`
   - `rows_scanned`, `markets_sampled`, `cells_optimized`
4. Ensure current cell values are always candidates during recommendation (no accidental exclusion).

### Where this intent appears
1. `app/dashboard/app.py`
2. `app/dashboard/index.html`
3. `app/dashboard/monitor.html`

---

## D. Gate-aware optimizer evaluation surface

### Intent
Allow optimizer inputs to explicitly define exact cells to evaluate, and keep results aligned with enabled cell surface.

### Intended behavior
1. Accept `enabled_cells` list as highest-priority gate input.
2. If `enabled_cells` is present, it is authoritative for evaluation surface.
3. If matrix is provided explicitly, missing entries are treated as OFF (strict interpretation).
4. UI should filter displayed optimizer rows to currently effective cells.

### Where this intent appears
1. `app/dashboard/app.py` (`enabled_cells` handling and strict matrix behavior)
2. `app/dashboard/monitor.html` (`enabled_cells` in request payload + filtered rendering)
3. `app/dashboard/index.html` (filtered rendering against effective gate state)

---

## E. Monitor replay realism alignment

### Intent
Bring monitor replay closer to runtime constraints by respecting timing windows and reducing repeated depth parsing overhead.

### Intended behavior
1. Monitor simulation SQL applies start/muzzle temporal window bounds.
2. Depth-at-cap computation should reuse parsed asks and depth caches for speed.
3. Optimizer should still evaluate full rows in the selected window (no sampling shortcut by intent).

### Where this intent appears
1. `app/dashboard/app.py` (`api_monitor_simulate`, `_simulate_market_rows`)

---

## F. Main dashboard optimizer UX + control intent

### Intent
Provide a single actionable optimizer panel on main dashboard with direct “apply recommended” operation and clearer per-cell result columns.

### Intended behavior
1. One optimizer panel (V2) with:
   - Current (b/p/pnl)
   - Recommended (b/p/pnl)
   - Delta
   - Trades (rec/cur)
   - W/L(rec), Win%(rec), Spent(rec), ROI%(rec)
2. “Apply Recommended” writes recommended matrix values into HFT config fields and saves.
3. Optimizer display reacts to gate toggles without stale bounce from SSE.
4. Auto-refresh pulls V2 latest result on schedule.

### Where this intent appears
1. `app/dashboard/index.html`

---

## G. Monitor dashboard optimizer UX intent

### Intent
Expose the same current-vs-recommended optimizer view on monitor page (as an analysis tool), with readable columns and gate-aware filtering.

### Intended behavior
1. “Find Optimized Inputs” uses V2 monitor endpoint.
2. Optimizer card displays current vs recommended by cell, plus deltas and rec metrics.
3. Table should remain legible at narrower widths (non-clipping/non-truncating intent).
4. Backtest + optimizer should rerender with current local gate filters.

### Where this intent appears
1. `app/dashboard/monitor.html`

---

## H. Shadow worker/runtime lifecycle intent

### Intent
Run optimizer worker via V2 state/locks only, with explicit startup/scheduled/manual triggers, and cleaner shutdown wiring.

### Intended behavior
1. Dedicated V2 worker state + run locks.
2. Scheduled run at 5m boundary cadence.
3. Manual run endpoint triggers V2 run.
4. App startup/shutdown uses V2 worker events.

### Where this intent appears
1. `app/dashboard/app.py`

---

## I. Monitor DB retention intent

### Intent
Prune monitor replay tables, not only legacy tick table, so retention and storage are consistent with backtest source.

### Intended behavior
1. Prune `ticks` (legacy)
2. Prune `monitor_v2_samples` (core replay rows)
3. Delete orphan `monitor_v2_markets` rows after sample pruning

### Where this intent appears
1. `app/src/monitor_db.py`

---

## J. Runtime config edits present in diff (not structural feature work)

### Intent category
Operational tuning values (asset/timeframe toggles, barriers, max price, drawdown, etc.), not architecture upgrades.

### Note
`app/config.yaml` changes in this diff are parameter edits and environment-state adjustments, not new platform capabilities.

---

## 3) Optimizer intent contract (for clean reimplementation)

If reimplementing from scratch, these are the intended product rules:

1. Full-window processing:
   - optimizer evaluates all rows in requested time window for each enabled cell.
   - no sampling shortcut.
2. Deterministic gating:
   - use effective cell gates exactly as UI shows.
   - if explicit enabled-cells list is supplied, use that set only.
3. Current vs recommended semantics:
   - current: run replay with current matrix values for that cell.
   - recommended: best candidate pair for that same cell under same knobs/window.
   - delta: `recommended.pnl - current.pnl`.
4. Comparable metrics:
   - trades, W/L, win%, spent, pnl, ROI must all come from same replay run basis.
5. Time consistency:
   - both optimizers should use same window definition and cutoff semantics when parity is expected.
6. UI trust:
   - no hidden gate expansion.
   - no stale panel values after toggles/settings update.

---

## 4) Reimplementation checklist (intent-only)

Use this after manual revert:

1. Re-add robust bool coercion for gate parsing.
2. Re-add strict matrix semantics (missing explicit cell = OFF when explicit matrix is provided).
3. Reintroduce `enabled_cells` authoritative payload option.
4. Implement one shared optimizer core runner for both main and monitor callers.
5. Keep optimizer runs off event loop in request path.
6. Ensure monitor replay query applies same temporal constraints intended for runtime-style simulation.
7. Rebuild main optimizer panel with:
   - current vs recommended columns
   - apply recommended button
   - gate-aware filtered display
8. Rebuild monitor optimizer panel with the same semantic columns and filter behavior.
9. Preserve monitor DB prune coverage for `monitor_v2_*` tables.
10. Validate parity:
   - same payload + same cutoff timestamp => matching per-cell outputs.

---

## 5) Completion statement

This file is the complete intent-level inventory of upgrades represented by the current uncommitted diff since the last commit.

---

## 6) Addendum: Clock + Time-Gate Optimizer Intent (approved discussion)

Status: **discussion approved, implementation deferred**.

This addendum records intent only, based on the latest discussion, before clean reimplementation.

### A. Problem statement captured

1. `/clock` appears to miss some bets (user-observed), especially concern around `1h`.
2. User needs a trustworthy "when to bet / when not to bet" layer that aligns with monitor/time-machine replay.
3. Optimizer outputs must be accurate enough for real-money decisions and easy to interpret.

### B. Source-of-truth decision

1. Clock/time gating decisions should use the **same monitor replay source** as backtester:
   - `monitor_v2_samples`-driven replay path.
2. `/clock` should be treated as **informational only** unless/until proven parity with replay.
3. Time-bucket performance and optimizer recommendations must come from one consistent replay contract.

### C. Clock-gate concept (simple control, complex result)

Intent is a switchable gate that can hibernate entries during statistically poor buckets and pre-warm before good buckets.

1. One new operator control block (main dashboard, below HFT controls):
   - `Clock Gate` ON/OFF
   - `Lookback Hours`
   - `Bucket Size` (`30m`, `1h`, `4h`)
   - `Min Win%`
   - `Min Samples`
   - `Min ROI%` (floor)
   - `Prewarm Seconds` (default target: around 10s before next allowed bucket)
2. Behavior intent:
   - During blocked buckets: pause new entries only.
   - Keep settlement/redeem/maintenance alive.
   - Resume entry path automatically when bucket is allowed.

### D. Metric-priority intent (optimizer + clock gate)

Current direction to test first:

1. Primary ranking/filter: **Win%**.
2. Safety guards:
   - minimum sample size threshold
   - minimum ROI floor (configurable; default non-negative)
3. PnL as secondary/tiebreaker metric, not sole decision driver.

Rationale captured:
1. User confidence is higher in replay trade-match fidelity than in projected PnL alone.
2. Win-rate stability by bucket is expected to be more actionable for pause/allow decisions.

### E. Clock page concern tracking

1. Investigate and document why `/clock` may omit/under-report bets (notably `1h` cases).
2. Confirm if `/clock` is bounded by a hidden history window and whether that diverges from monitor replay windows.
3. Do not let `/clock` drive auto-trading decisions until parity checks pass.

### F. Implementation philosophy (explicitly recorded)

1. Keep implementation simple, switchable, and observable.
2. Avoid speculative feature bloat.
3. Favor one replay truth pipeline over multiple partially overlapping analytics paths.

### G. Process commitment for next steps

1. Commit this intent document separately first.
2. Manually revert pending code changes as planned.
3. Reimplement from intent contract (not by reusing broken code paths).
4. Verify with parity checks and clear operator-facing metrics before enabling automation.

---

## 7) Addendum: Optimizer Objective Controls (approved discussion)

Status: **discussion approved, light implementation preferred**.

### A. Scope now (must-have)

1. Add objective selection used by both optimizers via shared core:
   - `objective_mode`: `pnl` | `win_rate` | `roi`
2. Add lookback control for optimizer window:
   - `lookback_hours` supports practical values like `24`, `72`, `168`, and max.
3. Persistence:
   - Main dashboard optimizer controls persist (`config.yaml`).
   - Monitor/backtester may persist analysis knobs, but matrix values should remain non-persistent to inherit live bot matrices.

### B. Scope deferred (for auto-control phase, not display phase)

1. No hard display filtering by:
   - `min_win_rate_pct`
   - `min_roi_pct`
   - `min_samples`
2. These become relevant when optimizer is allowed to auto-update live bot settings.
3. Future guardrails expected for auto-control:
   - `min_delta_pnl`
   - `min_delta_win_rate_pct`
   - `min_delta_roi_pct`
   - `min_samples` before replacement is allowed

### C. Low-sample handling (current preference)

1. Use warning-only confidence indicator for low trade count, not exclusion.
2. Keep ranking transparent; operator can inspect W/L, trades, ROI, and PnL directly.
3. Threshold tuning should remain simple and revisited after real usage feedback.
4. Initial confidence tiers for display-only warning:
   - `LOW` if trades < 10
   - `MED` if trades is 10-29
   - `HIGH` if trades >= 30
