# Monitor V2 (Research Pipeline)

## Purpose
- Collect independent market telemetry into `data/monitor.db` (`monitor_v2_*` tables).
- Keep monitor toggles separate from trading toggles.
- Preserve fast trading path by running monitor in its own worker loop.

## Cadence
- `5m`: `1s`
- `15m`: `1s`
- `1h`: `60s`
- `4h`: `240s`
- `1d`: `1440s`

## Dashboard APIs
- `GET /api/monitor/settings`
- `POST /api/monitor/settings`
- `GET /api/monitor/report?hours=24`
- `POST /api/control/flush-monitor` (monitor-only wipe)

## Analysis scripts
- Incremental analyzer (stateful, survives DB wipes):
```bash
python tools/analyze_monitor_v2.py
```
- Custom paths:
```bash
python tools/analyze_monitor_v2.py --db data/monitor.db --state data/monitor_report_state.json --output data/monitor_report_latest.json --hours 48
```
- Merge multiple state files:
```bash
python tools/merge_monitor_reports.py --inputs data/monitor_report_state_1.json data/monitor_report_state_2.json --output data/monitor_report_merged.json
```
