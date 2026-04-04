# Armed WS-Instant Sniper Notes

Date: 2026-03-15

## Current behavior (confirmed)
- `Armed WS-Instant Sniper = OFF`:
  - Barrier hit follows immediate entry path.
- `Armed WS-Instant Sniper = ON`:
  - Barrier hit enters arm/watch path first.
  - Spring requires `ask <= cap` and (if enabled) `depth >= bet`.
  - No unconditional immediate first FOK before arming.

## Discussion checkpoint
- Keep depth checking for armed watch/spring behavior.
- Candidate change to revisit:
  - With `Armed = ON`, do one immediate FOK attempt first.
  - If it fails, continue with existing arm/watch behavior (no retry model change).
- Keep retry behavior unchanged for now.

## Consistency requirement
- Any armed-path behavior change in live execution must be mirrored in monitor/time-machine replay logic to preserve backtest-to-live alignment.

