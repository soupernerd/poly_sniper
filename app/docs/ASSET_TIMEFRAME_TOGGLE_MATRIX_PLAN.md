# Asset-Timeframe Toggle Matrix Plan

## Goal
Add per-asset/per-timeframe enable toggles to the main dashboard/runtime, while keeping the existing global asset toggles and global timeframe toggles.

This is a core implementation plan (not a patch-layer), with strict no-regression behavior for:
- main bot execution
- shadow optimizer
- monitor/time-machine backtester

## User-facing behavior target
Layout order must follow your sketch:

1. Asset-specific timeframe rows first:
- `BTC [asset ON/OFF] [5m] [15m] [1h] [4h] [1d]`
- `ETH [asset ON/OFF] [5m] [15m] [1h] [4h] [1d]`
- `SOL [asset ON/OFF] [5m] [15m] [1h] [4h] [1d]`
- `XRP [asset ON/OFF] [5m] [15m] [1h] [4h] [1d]`

2. Existing global `Timeframes` row remains and sits below those rows:
- `[5m] [15m] [1h] [4h] [1d]`

3. No separate top-level `Assets` chip row; the asset master toggle is the first control in each asset row.

## Effective gate rule (single source of truth)
For a market cell `(asset, timeframe)` to be enabled:

`effective_enabled = global_asset_on AND global_timeframe_on AND cell_on`

This preserves your request to keep global toggles while adding higher-fidelity cell control.

## Core data model changes
Add to `ScannerConfig`:
- `asset_timeframe_enabled: dict[str, dict[str, bool]]` (default empty, normalized on read)

Allowed keys:
- assets: `bitcoin|ethereum|solana|xrp`
- tfs: `5m|15m|1h|4h|1d`

Normalization rules:
- Missing/invalid matrix -> build from current global sets (`assets`, `time_frames`).
- Missing row/cell -> default `True` only if its global asset+tf are both enabled, else `False`.
- Unknown keys are ignored.

## Backward compatibility and migration
No restart-breaking changes:

1. Existing configs with only:
- `scanner.assets`
- `scanner.time_frames`
continue to work exactly as today.

2. On first write after upgrade:
- `asset_timeframe_enabled` is persisted to `config.yaml`.

3. Fallback remains:
- if matrix missing/corrupt, runtime reverts to old cross-product behavior.

## Backend/API plan
### `/api/settings` (main runtime toggles)
GET:
- add `asset_timeframe_enabled` payload

POST:
- accept optional `asset_timeframe_enabled`
- validate + normalize + apply hot to scanner in-memory
- keep existing `assets` and `time_frames` updates unchanged
- allow zero effective-enabled cells (no forced minimum)

### `/api/hft/settings`
Include same matrix in snapshot/apply path for consistency with current HFT settings flow.

### SSE (`/api/stream`)
Include `asset_timeframe_enabled` in stream payload for front-end sync.

## Scanner/runtime plan (no patchwork)
Introduce one central scanner helper:
- `_is_cell_enabled(asset: str, duration_seconds: int) -> bool`

Replace direct checks (`asset in _assets`, `duration in _time_frames`) at all gate points with helper where execution decisions are made:
- discovery admit/filter
- tracked prune-on-toggle
- WS fast-path
- poll path
- HFT runner candidate filtering
- subscription promotion
- monitor piggyback capture path in scanner

This avoids split logic and drift.

## Shadow optimizer plan
Current optimizer iterates:
- active assets x active global timeframes

Upgrade to iterate only effective-enabled cells:
- build active cell list from scanner config via same gate rule
- keep current math/replay/ranking unchanged

Result: optimizer recommendations match actual enabled runtime universe.
If zero effective-enabled cells exist, optimizer should return an empty/idle result cleanly (no error/no synthetic fallback).

## Monitor/time-machine plan
Keep simulation logic/results model unchanged.

Monitor backtester gets the same style of asset-timeframe row controls (visual/UX parity), but remains independent from scanner runtime state:
- controls filter monitor simulation inputs only
- no coupling to main bot live execution toggles
- no behavior invention; same simulation semantics as current monitor

## Frontend plan (`dashboard/index.html`)
1. Replace separate asset chips with row-based asset master toggles.
2. Add asset-timeframe row controls as primary selector.
3. Keep existing global timeframe row below the rows.
3. New client state object for cell matrix.
4. On change, POST to `/api/settings` with `asset_timeframe_enabled`.
5. Apply visual states:
- `on`: effective-enabled
- `off`: explicitly off
- `disabled`: blocked by global asset/timeframe off (not deleting stored cell preference)

## Non-destructive toggle semantics
Recommended:
- Global asset/timeframe toggles act as master gates and do not erase saved per-cell preferences.
- Re-enabling restores prior row-cell preferences.

This prevents accidental loss of tuning and avoids hidden side effects.

## Regression checklist
1. With all cell toggles ON:
- behavior equals current production behavior.

2. With selective cells OFF:
- scanner does not discover/track/snipe those cells.
- shadow optimizer no longer includes those cells.

3. Monitor `/monitor` output:
- unchanged for same manual monitor selections.

4. Hot updates:
- toggling cells updates runtime without restart.
- YAML persistence survives restart.

5. All-off handling:
- saving zero effective-enabled cells is valid.
- scanner remains idle for new entries under that state.
- no crash/no forced auto-reenable.

## Implementation phases
Phase 1: config + normalization helpers + persistence
Phase 2: `/api/settings` + `/api/hft/settings` + SSE plumbing
Phase 3: scanner gate helper + replace call sites
Phase 4: shadow optimizer cell filtering
Phase 5: dashboard UI row controls + end-to-end validation

## Acceptance criteria
- No change to existing results when all new cells are ON.
- Per-cell toggles correctly constrain live execution scope.
- Shadow optimizer recommendations only for enabled cells.
- Monitor/time-machine behavior unchanged.
