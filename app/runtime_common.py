"""Shared runtime helpers for the standalone HFT app."""

import atexit
import logging
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from src.config import PROJECT_ROOT

logger = logging.getLogger("sniper")
ET = ZoneInfo("America/New_York")

DASHBOARD_PORT = 8898
SNIPE_VIEW_PORT = 8900
_LOCK_FILE = PROJECT_ROOT / "data" / ".sniper.lock"

LOG_TRIGGER_HOURS = 28
LOG_KEEP_HOURS = 24


def _is_bot_process(pid: int) -> bool:
    try:
        result = subprocess.run(
            [
                "wmic",
                "process",
                "where",
                f"ProcessId={pid}",
                "get",
                "CommandLine",
                "/FORMAT:LIST",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        cmd = result.stdout.lower()
        return "main.py" in cmd and "python" in cmd
    except Exception:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def _check_port_available(port: int = DASHBOARD_PORT) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _release_lock():
    try:
        if _LOCK_FILE.exists() and int(_LOCK_FILE.read_text().strip()) == os.getpid():
            _LOCK_FILE.unlink()
    except Exception:
        pass


def _acquire_lock():
    if _LOCK_FILE.exists():
        try:
            old_pid = int(_LOCK_FILE.read_text().strip())
            # Allow in-place self-restart (os.exec*) where PID is unchanged.
            if old_pid == os.getpid():
                pass
            elif _is_bot_process(old_pid):
                print(f"ERROR: HFT already running (PID {old_pid}).")
                sys.exit(1)
        except (ValueError, FileNotFoundError):
            pass
    if not _check_port_available():
        print(f"ERROR: Dashboard port {DASHBOARD_PORT} already in use.")
        sys.exit(1)
    _LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    _LOCK_FILE.write_text(str(os.getpid()))
    atexit.register(_release_lock)


def _truncate_log(log_path: Path) -> None:
    try:
        if not log_path.exists():
            return
        with open(log_path, "r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
        if not lines:
            return

        def _parse_ts(line: str) -> Optional[datetime]:
            try:
                # Current log prefix format: "MM/DD HH:MM:SS AM ET"
                stamp = line[:18]
                parsed = datetime.strptime(stamp, "%m/%d %I:%M:%S %p")
                now_et = datetime.now(ET)
                dt = parsed.replace(year=now_et.year, tzinfo=ET)
                # Handle year rollover for logs carried over New Year.
                if dt > now_et + timedelta(days=2):
                    dt = dt.replace(year=dt.year - 1)
                return dt
            except Exception:
                return None

        now_et = datetime.now(ET)

        oldest_ts = None
        for line in lines:
            oldest_ts = _parse_ts(line)
            if oldest_ts:
                break
        if not oldest_ts:
            # Fallback when timestamps are unparseable: conservative line trim.
            if len(lines) < 5000:
                return
            keep = lines[-2500:]
        else:
            age_hours = (now_et - oldest_ts).total_seconds() / 3600
            if age_hours <= LOG_TRIGGER_HOURS:
                return
            cutoff = now_et - timedelta(hours=LOG_KEEP_HOURS)
            keep_idx = None
            for i, line in enumerate(lines):
                ts = _parse_ts(line)
                if ts and ts >= cutoff:
                    keep_idx = i
                    break
            if keep_idx is None:
                return
            keep = lines[keep_idx:]

        root = logging.getLogger()
        file_handler = None
        for handler in root.handlers:
            if isinstance(handler, logging.FileHandler):
                if Path(handler.baseFilename).resolve() == log_path.resolve():
                    file_handler = handler
                    break
        if file_handler:
            file_handler.close()

        with open(log_path, "w", encoding="utf-8") as fh:
            fh.writelines(keep)

        if file_handler:
            file_handler.stream = open(str(log_path), "a", encoding="utf-8")
    except Exception as exc:
        logger.debug("Log truncate error: %s", exc)


def setup_logging(config):
    log_dir = PROJECT_ROOT / Path(config.logging.file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = PROJECT_ROOT / config.logging.file

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass

    file_handler = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    console_handler = logging.StreamHandler(sys.stdout)
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    class _ETFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created, ET)
            if datefmt:
                return dt.strftime(datefmt)
            return dt.isoformat()

    class _ChalkFormatter(_ETFormatter):
        _BLUE = "\033[38;5;111m"
        _CYAN = "\033[38;5;116m"
        _AMBER = "\033[38;5;222m"
        _RESET = "\033[0m"

        _WARN_HITS = (
            "[HFT-FAIL]",
            "[HFT-ERROR]",
            "[LOSS]",
            "[FEED: ECON-PAUSE]",
            "[FEED: ECON-HWM]",
            "FAIL-SAFE",
        )
        _HFT_HITS = (
            "[HFT-BARRIER]",
            "[HFT-ARM]",
            "[HFT-ARM-WS]",
            "[HFT-ARM-WAIT]",
            "[HFT-SPRING]",
            "[HFT-FIRE]",
            "[HFT-ACK]",
            "[HFT-PENDING]",
            "[HFT-DONE]",
            "[HFT-DISARM]",
            "[HFT-MUZZLE]",
            "[HFT-START-DELAY]",
            "[HFT-LATENCY]",
            "[HFT-EVAL]",
            "[HFT-GATE]",
            "[HFT-RECOVERY]",
            "[HFT-HOUSEKEEP]",
        )
        _REDEEM_HITS = (
            "[FEED: REDEEMED]",
            "[OK] REDEEMED",
            "[OK] EXTERNAL",
            "Instant redemption:",
            "Redeem (",
            "[ORPHAN]",
        )

        def _pick_colour(self, msg: str) -> str:
            for pat in self._WARN_HITS:
                if pat in msg:
                    return self._AMBER
            for pat in self._HFT_HITS:
                if pat in msg:
                    return self._BLUE
            for pat in self._REDEEM_HITS:
                if pat in msg:
                    return self._CYAN
            return ""

        def format(self, record: logging.LogRecord) -> str:
            line = super().format(record)
            colour = self._pick_colour(record.getMessage())
            if not colour:
                return line
            sep = " | "
            idx = line.rfind(sep)
            if idx == -1:
                return f"{colour}{line}{self._RESET}"
            prefix = line[: idx + len(sep)]
            body = line[idx + len(sep) :]
            return f"{prefix}{colour}{body}{self._RESET}"

    fmt = "%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s"
    datefmt = "%m/%d %I:%M:%S %p ET"
    file_handler.setFormatter(_ETFormatter(fmt, datefmt=datefmt))
    console_handler.setFormatter(_ChalkFormatter(fmt, datefmt=datefmt))

    root.setLevel(getattr(logging, config.logging.level, logging.INFO))
    root.addHandler(file_handler)
    root.addHandler(console_handler)

    for noisy in (
        "httpx",
        "httpcore",
        "hpack",
        "urllib3",
        "web3",
        "websockets",
        "uvicorn",
        "asyncio",
        "aiosqlite",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)
