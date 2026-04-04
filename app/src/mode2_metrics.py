"""In-memory Mode2 latency metrics for runtime dashboards."""

from __future__ import annotations

import time
from collections import deque, defaultdict
from threading import Lock

_LOCK = Lock()
_RECENT = deque(maxlen=200)


def record_latency(
    *,
    stage: str,
    latency_ms: float,
    condition_id: str = "",
    event_title: str = "",
) -> None:
    """Record one latency sample."""
    sample = {
        "ts": time.time(),
        "stage": str(stage or "").strip(),
        "latency_ms": float(latency_ms),
        "condition_id": str(condition_id or ""),
        "event_title": str(event_title or ""),
    }
    with _LOCK:
        _RECENT.append(sample)


def get_snapshot() -> dict:
    """Return a compact snapshot for API/UI rendering."""
    with _LOCK:
        rows = list(_RECENT)

    latest_by_stage: dict[str, dict] = {}
    by_stage: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        stage = row["stage"]
        latest_by_stage[stage] = row
        by_stage[stage].append(float(row["latency_ms"]))

    avg_by_stage = {
        k: (sum(v) / len(v)) if v else 0.0
        for k, v in by_stage.items()
    }

    return {
        "updated_at": rows[-1]["ts"] if rows else 0,
        "latest_by_stage": latest_by_stage,
        "avg_ms_by_stage": avg_by_stage,
        "recent": rows[-25:],
    }


def reset_metrics() -> None:
    """Clear in-memory Mode2 latency samples."""
    with _LOCK:
        _RECENT.clear()
