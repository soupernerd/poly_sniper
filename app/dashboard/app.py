"""HFT dashboard backend (FastAPI).

Serves the primary HFT control panel on port 8898, with SSE status stream,
live settings updates, controls, positions/trades, and Mode2 diagnostics.
"""

import asyncio
import copy
import hashlib
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml
import json as _json
import urllib.request
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import uvicorn
from src.mode2_metrics import (
    get_snapshot as get_mode2_metrics_snapshot,
    reset_metrics as reset_mode2_metrics,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = PROJECT_ROOT.parent
DB_PATH = PROJECT_ROOT / "data" / "sniper.db"
SETUP_SCRIPT_PATH = REPO_ROOT / "scripts" / "setup_public.py"
REQUIRED_SETUP_ENV_KEYS: tuple[str, ...] = (
    "POLYMARKET_PRIVATE_KEY",
    "POLYMARKET_WALLET_ADDRESS",
    "POLYMARKET_API_KEY",
    "POLYMARKET_API_SECRET",
    "POLYMARKET_API_PASSPHRASE",
)

app = FastAPI(title="PolySnipe", docs_url=None, redoc_url=None)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("Unhandled error on %s %s: %s", request.method, request.url.path, exc)
    return JSONResponse(status_code=500, content={"ok": False, "message": str(exc)})


# -- Bot state (set by main.py) --
_bot_state = {
    "running": False,
    "mode": "UNKNOWN",
    "started_at": None,
    "poll_count": 0,
    "tracked_markets": 0,
    "scan_count": 0,
    "opportunities": 0,
    "trades": 0,
    "total_spent": 0.0,
    "redeemed": 0,
    "redeemed_pnl": 0.0,
    "paused": True,
}

_bot_controls = {
    "paused": True,
    "request_stop": False,
    "verbose_log": False,
    "econ_pause_acked": False,
    "econ_start_balance": 0.0,
    "econ_hwm": 0.0,
}

_bot_db = None
_event_loop = None
_bot_scanner = None  # Set by main.py for runtime settings access
_bot_config = None   # Set by main.py for reading/writing config values
_bot_price_feed = None  # Set by main.py for oracle health display
_bot_monitor_worker = None  # Set by main.py for monitor-v2 runtime state
_restart_lock = threading.Lock()
_restart_pending = False
_shutdown_lock = threading.Lock()
_shutdown_watchdog_started = False
_async_shutdown_timeout_s = 2.0
_dashboard_server = None
_dashboard_server_task = None
_dashboard_sock = None

_YAML_PATH = PROJECT_ROOT / "config.yaml"
_HFT_MATRIX_ASSETS: tuple[str, ...] = ("bitcoin", "ethereum", "solana", "xrp")
_HFT_MATRIX_TFS: tuple[str, ...] = ("5m", "15m", "1h")
_HFT_MATRIX_TF_SECONDS: dict[str, int] = {"5m": 300, "15m": 900, "1h": 3600}
_HFT_MATRIX_SECONDS_TO_TF: dict[int, str] = {v: k for k, v in _HFT_MATRIX_TF_SECONDS.items()}
_SCANNER_TOGGLE_ASSETS: tuple[str, ...] = ("bitcoin", "ethereum", "solana", "xrp")
_SCANNER_TOGGLE_TFS: tuple[str, ...] = ("5m", "15m", "1h", "4h", "1d")
_SCANNER_TF_LABEL_TO_SECONDS: dict[str, int] = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
_SCANNER_TF_SECONDS_TO_LABEL: dict[int, str] = {v: k for k, v in _SCANNER_TF_LABEL_TO_SECONDS.items()}
_PRICE_FEED_SOURCES: tuple[str, ...] = ("binance", "chainlink")
try:
    _ET_ZONE = ZoneInfo("America/New_York")
except Exception:
    _ET_ZONE = timezone(timedelta(hours=-5))


def _default_hft_barrier_matrix(cfg=None) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for asset in _HFT_MATRIX_ASSETS:
        out[asset] = {}
        for tf, secs in _HFT_MATRIX_TF_SECONDS.items():
            if cfg and getattr(cfg, "trend", None):
                val = float(cfg.trend.get_hft_barrier_pct_for(asset, secs))
            else:
                val = 0.1
            out[asset][tf] = round(max(0.0, min(5.0, val)), 3)
    return out


def _default_hft_max_price_matrix(cfg=None) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for asset in _HFT_MATRIX_ASSETS:
        out[asset] = {}
        for tf, secs in _HFT_MATRIX_TF_SECONDS.items():
            if cfg and getattr(cfg, "trend", None):
                val = float(cfg.trend.get_hft_max_price_for(asset, secs))
            else:
                val = 0.67
            out[asset][tf] = round(max(0.01, min(0.99, val)), 4)
    return out


def _sanitize_hft_matrix(
    incoming: object,
    defaults: dict[str, dict[str, float]],
    *,
    low: float,
    high: float,
    digits: int,
) -> dict[str, dict[str, float]]:
    out = copy.deepcopy(defaults)
    if not isinstance(incoming, dict):
        return out
    for raw_asset, raw_row in incoming.items():
        asset = str(raw_asset or "").strip().lower()
        if asset not in _HFT_MATRIX_ASSETS or not isinstance(raw_row, dict):
            continue
        for raw_tf, raw_val in raw_row.items():
            tf = str(raw_tf or "").strip().lower()
            if tf not in _HFT_MATRIX_TFS:
                continue
            try:
                out[asset][tf] = round(max(low, min(high, float(raw_val))), digits)
            except Exception:
                continue
    return out


def _parse_scanner_assets(raw: str) -> list[str]:
    return [a.strip().lower() for a in str(raw or "").split(",") if a.strip().lower() in _SCANNER_TOGGLE_ASSETS]


def _parse_scanner_tfs(raw: str) -> list[str]:
    return [tf.strip().lower() for tf in str(raw or "").split(",") if tf.strip().lower() in _SCANNER_TOGGLE_TFS]


def _normalize_price_feed_source(raw: object, default: str = "binance") -> str:
    src = str(raw or "").strip().lower()
    if src in _PRICE_FEED_SOURCES:
        return src
    return default


def _coerce_bool(raw) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return raw != 0
    if raw is None:
        return False
    s = str(raw).strip().lower()
    if s in {"1", "true", "yes", "on", "y", "t"}:
        return True
    if s in {"0", "false", "no", "off", "n", "f", "", "none", "null"}:
        return False
    return bool(raw)


def _default_scanner_asset_tf_matrix(cfg=None) -> dict[str, dict[str, bool]]:
    if cfg and getattr(cfg, "scanner", None):
        try:
            return copy.deepcopy(cfg.scanner.get_asset_timeframe_enabled_matrix())
        except Exception:
            pass
    out: dict[str, dict[str, bool]] = {}
    for asset in _SCANNER_TOGGLE_ASSETS:
        out[asset] = {tf: True for tf in _SCANNER_TOGGLE_TFS}
    return out


def _all_false_scanner_asset_tf_matrix() -> dict[str, dict[str, bool]]:
    out: dict[str, dict[str, bool]] = {}
    for asset in _SCANNER_TOGGLE_ASSETS:
        out[asset] = {tf: False for tf in _SCANNER_TOGGLE_TFS}
    return out


def _sanitize_scanner_toggle_matrix(
    incoming: object,
    defaults: dict[str, dict[str, bool]],
) -> dict[str, dict[str, bool]]:
    out = copy.deepcopy(defaults)
    if not isinstance(incoming, dict):
        return out
    for raw_asset, raw_row in incoming.items():
        asset = str(raw_asset or "").strip().lower()
        if asset not in _SCANNER_TOGGLE_ASSETS or not isinstance(raw_row, dict):
            continue
        for raw_tf, raw_val in raw_row.items():
            tf = str(raw_tf or "").strip().lower()
            if tf not in _SCANNER_TOGGLE_TFS:
                continue
            out[asset][tf] = _coerce_bool(raw_val)
    return out


def _apply_scanner_runtime_toggles():
    """Hot-apply scanner gates from config into live scanner instance."""
    cfg = _bot_config
    if not cfg or _bot_scanner is None:
        return
    assets = _parse_scanner_assets(cfg.scanner.assets)
    tfs = _parse_scanner_tfs(cfg.scanner.time_frames)
    from src.scanner import _TF_LABEL_TO_DURATION

    _bot_scanner._assets = set(assets)
    _bot_scanner._time_frames = {_TF_LABEL_TO_DURATION[tf] for tf in tfs if tf in _TF_LABEL_TO_DURATION}
    _bot_scanner._asset_tf_enabled = cfg.scanner.get_asset_timeframe_enabled_matrix()


def _active_log_path() -> Path:
    """Resolve current runtime log path (defaults to logs/sniper.log)."""
    try:
        if _bot_config and getattr(_bot_config, "logging", None):
            rel = getattr(_bot_config.logging, "file", "") or ""
            rel = str(rel).strip()
            if rel:
                return PROJECT_ROOT / rel
    except Exception:
        pass
    return PROJECT_ROOT / "logs" / "sniper.log"

# -- Wallet balance (read-only, cached) --
ETHERSCAN_API_KEY = str(os.getenv("POLYGONSCAN_API_KEY", "") or "").strip()
BALANCE_ADDRESS = str(os.getenv("POLYMARKET_WALLET_ADDRESS", "") or "").strip()
USDC_TOKEN_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_CHAIN_ID = 137
_rpc_env_raw = str(os.getenv("POLYGON_RPC_URL", "") or "").strip()
if _rpc_env_raw:
    POLYGON_RPC_URLS = [u.strip() for u in _rpc_env_raw.split(",") if u.strip()]
else:
    POLYGON_RPC_URLS = [
        "https://polygon-rpc.com",
        "https://polygon-bor-rpc.publicnode.com",
    ]
BALANCE_CACHE_SECONDS = 30

_usdc_balance_cache: float | None = None
_pol_balance_cache: float | None = None
_balance_cache_ts = 0.0


def _wallet_address() -> str:
    addr = ""
    if _bot_config is not None:
        addr = str(getattr(_bot_config, "wallet_address", "") or "").strip()
    if addr:
        return addr
    # Read live env fallback (dotenv may load after module import).
    env_addr = str(os.getenv("POLYMARKET_WALLET_ADDRESS", "") or "").strip()
    if env_addr:
        return env_addr
    file_addr = _read_env_value("POLYMARKET_WALLET_ADDRESS")
    if file_addr:
        return file_addr
    return BALANCE_ADDRESS


def _wallet_private_key() -> str:
    key = ""
    if _bot_config is not None:
        key = str(getattr(_bot_config, "private_key", "") or "").strip()
    if key:
        return key
    env_key = str(os.getenv("POLYMARKET_PRIVATE_KEY", "") or "").strip()
    if env_key:
        return env_key
    file_key = _read_env_value("POLYMARKET_PRIVATE_KEY")
    if file_key:
        return file_key
    return ""


def _read_env_value(key: str) -> str:
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return ""
    try:
        for raw in env_file.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in raw:
                continue
            k, v = raw.split("=", 1)
            if k.strip() == key:
                return v.strip()
    except Exception:
        return ""
    return ""


def _is_missing_env_value(value: str) -> bool:
    v = str(value or "").strip()
    if not v:
        return True
    low = v.lower()
    if low.startswith("<"):
        return True
    if "your_" in low or ("your" in low and "here" in low):
        return True
    if "yourpolygonwalletaddress" in low:
        return True
    return False


def _env_file_exists() -> bool:
    return (PROJECT_ROOT / ".env").exists()


def _env_setup_complete() -> bool:
    if not _env_file_exists():
        return False
    for key in REQUIRED_SETUP_ENV_KEYS:
        if _is_missing_env_value(_read_env_value(key)):
            return False
    return True


def _rpc_call(method: str, params: list) -> str | None:
    if not POLYGON_RPC_URLS:
        return None
    payload = _json.dumps({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }).encode("utf-8")
    for rpc_url in POLYGON_RPC_URLS:
        req = urllib.request.Request(
            rpc_url,
            data=payload,
            headers={
                "User-Agent": "PolySnipe/1.0",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=8) as resp:
                data = _json.loads(resp.read().decode("utf-8"))
            result = data.get("result")
            if isinstance(result, str):
                return result
        except Exception as exc:
            logger.debug("Polygon RPC call failed (%s @ %s): %s", method, rpc_url, exc)
    return None


def _fetch_pol_balance() -> float | None:
    """Fetch native POL balance via Polygon JSON-RPC (no API key required)."""
    address = _wallet_address()
    if not address:
        return None
    result = _rpc_call("eth_getBalance", [address, "latest"])
    if result:
        try:
            return int(result, 16) / 1e18
        except Exception:
            pass
    # RPC can be blocked or key-gated; fallback to Polygonscan when available.
    if not ETHERSCAN_API_KEY:
        return None
    url = (
        f"https://api.etherscan.io/v2/api?chainid={POLYGON_CHAIN_ID}"
        "&module=account&action=balance"
        f"&address={address}"
        "&tag=latest"
        f"&apikey={ETHERSCAN_API_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PolySnipe/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        if data.get("status") != "1":
            return None
        return float(data.get("result", 0)) / 1e18
    except Exception as exc:
        logger.debug("Polygonscan POL balance fetch failed: %s", exc)
        return None


def _fetch_usdc_balance_rpc() -> float | None:
    """Fetch USDC.e balance via eth_call balanceOf (no API key required)."""
    address = _wallet_address()
    if not address:
        return None
    addr = address.strip().lower()
    if addr.startswith("0x"):
        addr = addr[2:]
    if len(addr) != 40:
        return None
    try:
        int(addr, 16)
    except Exception:
        return None
    call_data = "0x70a08231" + ("0" * 24) + addr
    result = _rpc_call("eth_call", [{"to": USDC_TOKEN_ADDRESS, "data": call_data}, "latest"])
    if not result:
        return None
    try:
        return int(result, 16) / 1e6
    except Exception:
        return None


def _fetch_usdc_balance_polygonscan() -> float | None:
    """Fetch USDC.e balance via Polygonscan (fallback when RPC path fails)."""
    address = _wallet_address()
    if not ETHERSCAN_API_KEY or not address:
        return None
    url = (
        f"https://api.etherscan.io/v2/api?chainid={POLYGON_CHAIN_ID}"
        "&module=account&action=tokenbalance"
        f"&contractaddress={USDC_TOKEN_ADDRESS}"
        f"&address={address}"
        "&tag=latest"
        f"&apikey={ETHERSCAN_API_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PolySnipe/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        if data.get("status") != "1":
            return None
        raw = float(data.get("result", 0))
        return raw / 1e6
    except Exception as exc:
        logger.debug("Polygonscan balance fetch failed: %s", exc)
        return None


def _fetch_usdc_balance() -> float | None:
    bal = _fetch_usdc_balance_rpc()
    if bal is not None:
        return bal
    return _fetch_usdc_balance_polygonscan()


def _get_cached_balances() -> tuple[float | None, float | None]:
    global _usdc_balance_cache, _pol_balance_cache, _balance_cache_ts
    now = time.monotonic()
    if (now - _balance_cache_ts) < BALANCE_CACHE_SECONDS:
        return _usdc_balance_cache, _pol_balance_cache
    usdc = _fetch_usdc_balance()
    pol = _fetch_pol_balance()
    if usdc is not None:
        _usdc_balance_cache = usdc
    if pol is not None:
        _pol_balance_cache = pol
    _balance_cache_ts = now
    return _usdc_balance_cache, _pol_balance_cache


def _get_cached_balance() -> float | None:
    usdc, _ = _get_cached_balances()
    return usdc


def _get_cached_pol_balance() -> float | None:
    _, pol = _get_cached_balances()
    return pol


def _reset_balance_cache() -> None:
    global _usdc_balance_cache, _pol_balance_cache, _balance_cache_ts
    _usdc_balance_cache = None
    _pol_balance_cache = None
    _balance_cache_ts = 0.0


def _release_runtime_lock_for_current_pid() -> None:
    """Best-effort lock cleanup during restart handoff."""
    lock_path = PROJECT_ROOT / "data" / ".sniper.lock"
    try:
        if not lock_path.exists():
            return
        raw = (lock_path.read_text(encoding="utf-8") or "").strip()
        if not raw:
            return
        if int(raw) == os.getpid():
            lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def _schedule_runtime_restart(*, reason: str, delay_seconds: float = 1.2) -> bool:
    global _restart_pending
    with _restart_lock:
        if _restart_pending:
            return False
        _restart_pending = True

    def _do_restart():
        global _restart_pending
        try:
            logger.info("Runtime restart requested (%s)", reason)
            time.sleep(max(0.4, float(delay_seconds)))
            os.chdir(str(PROJECT_ROOT))
            main_script = PROJECT_ROOT / "main.py"
            if os.name == "nt":
                candidate = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
            else:
                candidate = PROJECT_ROOT / ".venv" / "bin" / "python"
            python_exec = str(candidate if candidate.exists() else Path(sys.executable))
            logger.info("Runtime relaunch using: %s", python_exec)
            subprocess.Popen(
                [python_exec, str(main_script)],
                cwd=str(PROJECT_ROOT),
            )
            # Release lock before exiting so replacement process can acquire immediately.
            _release_runtime_lock_for_current_pid()
            logger.info("Runtime restart handoff complete; exiting old process")
            os._exit(0)
        except Exception as exc:
            logger.exception("Runtime restart failed: %s", exc)
            _restart_pending = False

    threading.Thread(target=_do_restart, daemon=True).start()
    return True


def _persist_to_yaml():
    """Write current in-memory config back to config.yaml so settings survive restart."""
    cfg = _bot_config
    if cfg is None:
        return
    try:
        with open(_YAML_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        # Scanner
        data.setdefault("scanner", {})
        data["scanner"]["interval_seconds"] = cfg.scanner.interval_seconds
        data["scanner"]["max_events_per_query"] = cfg.scanner.max_events_per_query
        data["scanner"]["assets"] = cfg.scanner.assets
        data["scanner"]["time_frames"] = cfg.scanner.time_frames
        data["scanner"]["asset_timeframe_enabled"] = cfg.scanner.get_asset_timeframe_enabled_matrix()
        data["scanner"]["winner_threshold"] = cfg.scanner.winner_threshold
        data["scanner"]["pre_end_seconds"] = cfg.scanner.pre_end_seconds
        data["scanner"]["pre_end_seconds_5m"] = cfg.scanner.pre_end_seconds_5m
        data["scanner"]["pre_end_seconds_15m"] = cfg.scanner.pre_end_seconds_15m
        data["scanner"]["pre_end_seconds_1h"] = cfg.scanner.pre_end_seconds_1h
        data["scanner"]["pre_end_seconds_4h"] = cfg.scanner.pre_end_seconds_4h
        data["scanner"]["pre_end_seconds_1d"] = cfg.scanner.pre_end_seconds_1d
        data["scanner"]["lookahead_enabled"] = bool(getattr(cfg.scanner, "lookahead_enabled", False))
        data["scanner"]["lookahead_seconds"] = int(getattr(cfg.scanner, "lookahead_seconds", 60))
        data["scanner"]["pre_end_threshold"] = cfg.scanner.pre_end_threshold
        data["scanner"]["pre_end_threshold_5m"] = cfg.scanner.pre_end_threshold_5m
        data["scanner"]["pre_end_threshold_15m"] = cfg.scanner.pre_end_threshold_15m
        data["scanner"]["pre_end_threshold_1h"] = cfg.scanner.pre_end_threshold_1h
        data["scanner"]["pre_end_threshold_4h"] = cfg.scanner.pre_end_threshold_4h
        data["scanner"]["pre_end_threshold_1d"] = cfg.scanner.pre_end_threshold_1d
        data["scanner"]["stop_buffer_seconds"] = getattr(cfg.scanner, 'stop_buffer_seconds', 0)
        data["scanner"]["start_buffer_seconds"] = getattr(cfg.scanner, 'start_buffer_seconds', 0)
        data["scanner"]["ptb_capture_offset"] = cfg.scanner.ptb_capture_offset
        data["scanner"]["price_feed"] = _normalize_price_feed_source(
            getattr(cfg.scanner, "price_feed", "binance"),
            "binance",
        )
        data["scanner"]["min_conviction"] = cfg.scanner.min_conviction
        data["scanner"]["min_conviction_5m"] = cfg.scanner.min_conviction_5m
        data["scanner"]["min_conviction_15m"] = cfg.scanner.min_conviction_15m
        data["scanner"]["min_conviction_1h"] = cfg.scanner.min_conviction_1h
        data["scanner"]["min_conviction_4h"] = cfg.scanner.min_conviction_4h
        data["scanner"]["min_conviction_1d"] = cfg.scanner.min_conviction_1d
        data["scanner"]["max_gap_pct"] = cfg.scanner.max_gap_pct
        data["scanner"]["max_gap_pct_5m"] = cfg.scanner.max_gap_pct_5m
        data["scanner"]["max_gap_pct_15m"] = cfg.scanner.max_gap_pct_15m
        data["scanner"]["max_gap_pct_1h"] = cfg.scanner.max_gap_pct_1h
        data["scanner"]["max_gap_pct_4h"] = cfg.scanner.max_gap_pct_4h
        data["scanner"]["max_gap_pct_1d"] = cfg.scanner.max_gap_pct_1d
        if _bot_scanner:
            data["scanner"]["ws_cleanup_delay"] = _bot_scanner.ws_cleanup_delay
        data["scanner"]["price_gap_pct"] = cfg.scanner.price_gap_pct
        data["scanner"]["price_gap_pct_5m"] = cfg.scanner.price_gap_pct_5m
        data["scanner"]["price_gap_pct_15m"] = cfg.scanner.price_gap_pct_15m
        data["scanner"]["price_gap_pct_1h"] = cfg.scanner.price_gap_pct_1h
        data["scanner"]["price_gap_pct_4h"] = cfg.scanner.price_gap_pct_4h
        data["scanner"]["price_gap_pct_1d"] = cfg.scanner.price_gap_pct_1d
        # Remove legacy per-asset overrides if present in YAML
        data["scanner"].pop("price_gap_overrides", None)

        # Execution
        data.setdefault("execution", {})
        data["execution"]["max_buy_price"] = cfg.execution.max_buy_price
        data["execution"]["max_buy_price_5m"] = cfg.execution.max_buy_price_5m
        data["execution"]["max_buy_price_15m"] = cfg.execution.max_buy_price_15m
        data["execution"]["max_buy_price_1h"] = cfg.execution.max_buy_price_1h
        data["execution"]["max_buy_price_4h"] = cfg.execution.max_buy_price_4h
        data["execution"]["max_buy_price_1d"] = cfg.execution.max_buy_price_1d
        data["execution"]["max_bet"] = cfg.execution.max_bet
        data["execution"]["min_edge"] = cfg.execution.min_edge
        data["execution"]["min_edge_5m"] = cfg.execution.min_edge_5m
        data["execution"]["min_edge_15m"] = cfg.execution.min_edge_15m
        data["execution"]["min_edge_1h"] = cfg.execution.min_edge_1h
        data["execution"]["min_edge_4h"] = cfg.execution.min_edge_4h
        data["execution"]["min_edge_1d"] = cfg.execution.min_edge_1d
        data["execution"]["fill_fraction"] = cfg.execution.fill_fraction
        # Per-timeframe bet caps
        data["execution"]["max_bet_5m"] = cfg.execution.max_bet_5m
        data["execution"]["max_bet_15m"] = cfg.execution.max_bet_15m
        data["execution"]["max_bet_1h"] = cfg.execution.max_bet_1h
        data["execution"]["max_bet_4h"] = cfg.execution.max_bet_4h
        data["execution"]["max_bet_1d"] = cfg.execution.max_bet_1d
        data["execution"]["swing_guard_enabled"] = cfg.execution.swing_guard_enabled
        data["execution"]["swing_guard_max_pct"] = cfg.execution.swing_guard_max_pct
        data["execution"]["dynamic_max_price_enabled"] = cfg.execution.dynamic_max_price_enabled
        data["execution"]["dynamic_max_price_pct"] = cfg.execution.dynamic_max_price_pct
        data["execution"]["economic_pause_drawdown"] = cfg.execution.economic_pause_drawdown
        data["execution"]["economic_profit_target"] = cfg.execution.economic_profit_target
        data["execution"]["gtc_fallback_enabled"] = cfg.execution.gtc_fallback_enabled
        data["execution"]["oracle_consensus"] = cfg.execution.oracle_consensus

        # Remove spray_bid section if it exists in the YAML
        data.pop("spray_bid", None)

        # Blackout slots
        data["blackout"] = {
            "slots_5m": _bot_config.blackout.slots_5m,
            "slots_5m_am": _bot_config.blackout.slots_5m_am,
            "slots_5m_pm": _bot_config.blackout.slots_5m_pm,
            "slots_5m_hourly": _bot_config.blackout.slots_5m_hourly,
            "slots_15m": _bot_config.blackout.slots_15m,
            "slots_15m_am": _bot_config.blackout.slots_15m_am,
            "slots_15m_pm": _bot_config.blackout.slots_15m_pm,
            "slots_1h": _bot_config.blackout.slots_1h,
            "slots_1h_am": _bot_config.blackout.slots_1h_am,
            "slots_1h_pm": _bot_config.blackout.slots_1h_pm,
            "slots_4h": _bot_config.blackout.slots_4h,
            "slots_4h_am": _bot_config.blackout.slots_4h_am,
            "slots_4h_pm": _bot_config.blackout.slots_4h_pm,
            "slots_1d": _bot_config.blackout.slots_1d,
            "slots_1d_am": _bot_config.blackout.slots_1d_am,
            "slots_1d_pm": _bot_config.blackout.slots_1d_pm,
        }

        # HFT runtime settings (Mode2-focused)
        data["trend"] = {
            "hft_bet_amount": getattr(cfg.trend, "hft_bet_amount", 1.0),
            "hft_flip_profit_pct": getattr(cfg.trend, "hft_flip_profit_pct", 0.0),
            "hft_muzzle_seconds": getattr(cfg.trend, "hft_muzzle_seconds", 90),
            "hft_max_price": getattr(cfg.trend, "hft_max_price", 0.67),
            "hft_max_price_5m": getattr(cfg.trend, "hft_max_price_5m", 0.0),
            "hft_max_price_15m": getattr(cfg.trend, "hft_max_price_15m", 0.0),
            "hft_max_price_1h": getattr(cfg.trend, "hft_max_price_1h", 0.0),
            "hft_barrier_enabled": getattr(cfg.trend, "hft_barrier_enabled", False),
            "hft_barrier_pct": getattr(cfg.trend, "hft_barrier_pct", 0.1),
            "hft_barrier_pct_15m": getattr(cfg.trend, "hft_barrier_pct_15m", 0.0),
            "hft_barrier_pct_1h": getattr(cfg.trend, "hft_barrier_pct_1h", 0.0),
            "hft_barrier_delay": getattr(cfg.trend, "hft_barrier_delay", 10),
            "hft_armed_sniper_enabled": getattr(cfg.trend, "hft_armed_sniper_enabled", False),
            "hft_mode2_entry_mode": getattr(cfg.trend, "hft_mode2_entry_mode", ""),
            "hft_mode2_trigger_cooldown_ms": getattr(cfg.trend, "hft_mode2_trigger_cooldown_ms", 150),
            "hft_optimizer_objective_mode": _sanitize_optimizer_objective(
                getattr(cfg.trend, "hft_optimizer_objective_mode", "pnl"),
                "pnl",
            ),
            "hft_optimizer_lookback_hours": max(
                1,
                min(8760, int(getattr(cfg.trend, "hft_optimizer_lookback_hours", 24) or 24)),
            ),
            "hft_optimizer_min_trades": max(
                0,
                min(100000, int(getattr(cfg.trend, "hft_optimizer_min_trades", 0) or 0)),
            ),
            "hft_barrier_matrix": _sanitize_hft_matrix(
                getattr(cfg.trend, "hft_barrier_matrix", {}),
                _default_hft_barrier_matrix(cfg),
                low=0.0,
                high=5.0,
                digits=3,
            ),
            "hft_max_price_matrix": _sanitize_hft_matrix(
                getattr(cfg.trend, "hft_max_price_matrix", {}),
                _default_hft_max_price_matrix(cfg),
                low=0.01,
                high=0.99,
                digits=4,
            ),
        }

        # Independent research monitor settings
        data["monitor"] = {
            "enabled": bool(getattr(cfg.monitor, "enabled", True)),
            "assets": str(getattr(cfg.monitor, "assets", "bitcoin,ethereum,solana,xrp") or ""),
            "time_frames": str(getattr(cfg.monitor, "time_frames", "5m,15m,1h,4h,1d") or ""),
            "discovery_interval_seconds": int(getattr(cfg.monitor, "discovery_interval_seconds", 15)),
            "max_events_per_query": int(getattr(cfg.monitor, "max_events_per_query", 250)),
            "pre_start_capture_seconds": int(getattr(cfg.monitor, "pre_start_capture_seconds", 15)),
            "post_end_track_seconds": int(getattr(cfg.monitor, "post_end_track_seconds", 120)),
            "sample_5m_seconds": int(getattr(cfg.monitor, "sample_5m_seconds", 1)),
            "sample_15m_seconds": int(getattr(cfg.monitor, "sample_15m_seconds", 1)),
            "sample_1h_seconds": int(getattr(cfg.monitor, "sample_1h_seconds", 60)),
            "sample_4h_seconds": int(getattr(cfg.monitor, "sample_4h_seconds", 240)),
            "sample_1d_seconds": int(getattr(cfg.monitor, "sample_1d_seconds", 1440)),
        }

        with open(_YAML_PATH, "w", encoding="utf-8") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        logger.debug("Settings persisted to %s", _YAML_PATH)
    except Exception as exc:
        logger.error("Failed to persist settings to YAML: %s", exc)


def set_bot_state(**kwargs):
    _bot_state.update(kwargs)

def get_bot_controls() -> dict:
    return _bot_controls

def set_bot_db(db, loop):
    global _bot_db, _event_loop
    _bot_db = db
    _event_loop = loop

def set_bot_scanner(scanner):
    global _bot_scanner
    _bot_scanner = scanner

def set_bot_config(config):
    global _bot_config
    _bot_config = config
    try:
        _trigger_shadow_optimizer_v2_run("config_ready")
    except Exception:
        pass

def set_bot_price_feed(price_feed):
    global _bot_price_feed
    _bot_price_feed = price_feed


def set_bot_monitor_worker(worker):
    global _bot_monitor_worker
    _bot_monitor_worker = worker


# -- Read-only DB --
_read_lock = threading.Lock()
_read_db: sqlite3.Connection | None = None


def _get_read_db() -> sqlite3.Connection:
    global _read_db
    if _read_db is None:
        _read_db = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _read_db.row_factory = sqlite3.Row
        _read_db.execute("PRAGMA journal_mode=WAL")
        _read_db.execute("PRAGMA busy_timeout=5000")
    return _read_db


def _query(sql: str, params: tuple = ()) -> list[dict]:
    """Thread-safe read query."""
    with _read_lock:
        try:
            db = _get_read_db()
            cursor = db.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.debug("Read query failed: %s", e)
            return []

def _execute(sql: str, params: tuple = ()):
    """Thread-safe write query."""
    with _read_lock:
        try:
            db = _get_read_db()
            db.execute("PRAGMA busy_timeout=15000")
            db.execute(sql, params)
            db.commit()
            return True
        except Exception as e:
            logger.error("Write query failed: %s", e)
            db.rollback()
            return False


# -- Routes --

@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = PROJECT_ROOT / "dashboard" / "index.html"
    if html_path.exists():
        return HTMLResponse(
            html_path.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
        )
    return HTMLResponse("<h1>Sniper Dashboard</h1><p>index.html not found</p>")


@app.get("/hft", response_class=HTMLResponse)
async def hft_page():
    html_path = PROJECT_ROOT / "dashboard" / "index.html"
    if html_path.exists():
        return HTMLResponse(
            html_path.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
        )
    return HTMLResponse("<h1>HFT Dashboard</h1><p>dashboard/index.html not found</p>")


@app.get("/clock", response_class=HTMLResponse)
async def clock_page():
    html_path = PROJECT_ROOT / "dashboard" / "clock.html"
    if html_path.exists():
        return HTMLResponse(
            html_path.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
        )
    return HTMLResponse("<h1>Clock Blackout</h1><p>clock.html not found</p>")


@app.get("/monitor", response_class=HTMLResponse)
async def monitor_page():
    html_path = PROJECT_ROOT / "dashboard" / "monitor.html"
    if html_path.exists():
        return HTMLResponse(
            html_path.read_text(encoding="utf-8"),
            headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
        )
    return HTMLResponse("<h1>Monitor Simulator</h1><p>dashboard/monitor.html not found</p>")


@app.get("/scenarios", response_class=HTMLResponse)
async def scenarios_page():
    return HTMLResponse("<h1>Disabled</h1><p>Scenario lab is removed in HFT-only build.</p>", status_code=404)


@app.get("/trend", response_class=HTMLResponse)
async def trend_page():
    return HTMLResponse("<h1>Disabled</h1><p>Trend pages are removed in HFT-only build.</p>", status_code=404)


# -- Trend API endpoints --

@app.get("/api/trend/candles")
async def api_trend_candles(asset: str = "bitcoin", timeframe: str = "5m", count: int = 200, source: str = "chainlink"):
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)


@app.get("/api/trend/indicators")
async def api_trend_indicators(asset: str = "bitcoin", timeframe: str = "5m"):
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)


@app.get("/api/trend/signals")
async def api_trend_signals():
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)


@app.get("/api/trend/stats")
async def api_trend_stats():
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)




@app.get("/api/monitor/markets")
async def api_monitor_markets():
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)


@app.get("/api/status")
async def api_status():
    """Live bot status."""
    return {
        "ok": True,
        **_bot_state,
        "controls": {
            "paused": _bot_controls.get("paused", True),
        },
        "wallet_balance": _get_cached_balance(),
        "wallet_pol_balance": _get_cached_pol_balance(),
        "wallet_symbol": "USDC.e",
        "wallet_address": _wallet_address(),
        "wallet_private_key": _wallet_private_key(),
        "env_exists": _env_file_exists(),
        "env_setup_complete": _env_setup_complete(),
    }


@app.post("/api/setup/bootstrap")
async def api_setup_bootstrap():
    """Run bootstrap credential setup from dashboard without reinstalling deps."""
    if not SETUP_SCRIPT_PATH.exists():
        return JSONResponse({"ok": False, "message": "Setup script not found"}, status_code=500)
    try:
        proc = subprocess.run(
            [
                sys.executable,
                str(SETUP_SCRIPT_PATH),
                "--no-venv",
                "--no-pip",
                "--no-restart",
                "--overwrite-env",
                "--rotate-credentials",
            ],
            cwd=str(REPO_ROOT),
            text=True,
            capture_output=True,
            timeout=180,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return JSONResponse({"ok": False, "message": "Setup timed out"}, status_code=500)
    except Exception as exc:
        return JSONResponse({"ok": False, "message": f"Setup failed: {exc}"}, status_code=500)

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip().splitlines()
        hint = err[-1] if err else "Setup command failed"
        return JSONResponse({"ok": False, "message": hint}, status_code=500)

    # Pull freshly generated values so dashboard can show wallet immediately.
    wallet = _read_env_value("POLYMARKET_WALLET_ADDRESS")
    private_key = _read_env_value("POLYMARKET_PRIVATE_KEY")
    if wallet:
        os.environ["POLYMARKET_WALLET_ADDRESS"] = wallet
    if private_key:
        os.environ["POLYMARKET_PRIVATE_KEY"] = private_key

    _reset_balance_cache()
    return {
        "ok": True,
        "message": "Setup complete. Restart required to load new credentials.",
        "restart_required": True,
        "wallet_address": wallet or _wallet_address(),
        "wallet_private_key": private_key or _wallet_private_key(),
        "wallet_balance": _get_cached_balance(),
        "wallet_pol_balance": _get_cached_pol_balance(),
        "wallet_symbol": "USDC.e",
        "env_exists": _env_file_exists(),
        "env_setup_complete": _env_setup_complete(),
    }


def _hft_settings_snapshot() -> dict:
    """Return HFT runtime settings payload (Mode2-focused)."""
    cfg = _bot_config
    if not cfg:
        return {}
    barrier_matrix = _sanitize_hft_matrix(
        getattr(cfg.trend, "hft_barrier_matrix", {}),
        _default_hft_barrier_matrix(cfg),
        low=0.0,
        high=5.0,
        digits=3,
    )
    max_price_matrix = _sanitize_hft_matrix(
        getattr(cfg.trend, "hft_max_price_matrix", {}),
        _default_hft_max_price_matrix(cfg),
        low=0.01,
        high=0.99,
        digits=4,
    )
    return {
        "assets": cfg.scanner.assets,
        "time_frames": cfg.scanner.time_frames,
        "asset_timeframe_enabled": cfg.scanner.get_asset_timeframe_enabled_matrix(),
        "lookahead_enabled": bool(getattr(cfg.scanner, "lookahead_enabled", False)),
        "lookahead_seconds": int(getattr(cfg.scanner, "lookahead_seconds", 60)),
        "ptb_capture_offset": float(getattr(cfg.scanner, "ptb_capture_offset", 2.0)),
        "scan_interval": float(getattr(cfg.scanner, "interval_seconds", 0.6)),
        "hft_bet_amount": float(getattr(cfg.trend, "hft_bet_amount", 1.0)),
        "hft_max_price": float(getattr(cfg.trend, "hft_max_price", 0.67)),
        "hft_max_price_5m": float(getattr(cfg.trend, "hft_max_price_5m", 0.0)),
        "hft_max_price_15m": float(getattr(cfg.trend, "hft_max_price_15m", 0.0)),
        "hft_max_price_1h": float(getattr(cfg.trend, "hft_max_price_1h", 0.0)),
        "hft_flip_profit_pct": float(getattr(cfg.trend, "hft_flip_profit_pct", 0.0)),
        "hft_muzzle_seconds": int(getattr(cfg.trend, "hft_muzzle_seconds", 90)),
        "hft_barrier_enabled": bool(getattr(cfg.trend, "hft_barrier_enabled", False)),
        "hft_barrier_pct": float(getattr(cfg.trend, "hft_barrier_pct", 0.1)),
        "hft_barrier_pct_15m": float(getattr(cfg.trend, "hft_barrier_pct_15m", 0.0)),
        "hft_barrier_pct_1h": float(getattr(cfg.trend, "hft_barrier_pct_1h", 0.0)),
        "hft_barrier_delay": int(getattr(cfg.trend, "hft_barrier_delay", 10)),
        "hft_barrier_matrix": barrier_matrix,
        "hft_max_price_matrix": max_price_matrix,
        "hft_armed_sniper_enabled": bool(getattr(cfg.trend, "hft_armed_sniper_enabled", False)),
        "hft_mode2_entry_mode": str(getattr(cfg.trend, "hft_mode2_entry_mode", "") or ""),
        "hft_mode2_trigger_cooldown_ms": int(getattr(cfg.trend, "hft_mode2_trigger_cooldown_ms", 150)),
        "hft_optimizer_objective_mode": _sanitize_optimizer_objective(
            getattr(cfg.trend, "hft_optimizer_objective_mode", "pnl"),
            "pnl",
        ),
        "hft_optimizer_lookback_hours": int(getattr(cfg.trend, "hft_optimizer_lookback_hours", 24) or 24),
        "hft_optimizer_min_trades": int(getattr(cfg.trend, "hft_optimizer_min_trades", 0) or 0),
        "hft_mode2_only_runtime": bool(getattr(cfg, "hft_mode2_only_runtime", False)),
    }


def _apply_hft_settings(body: dict) -> tuple[list[str], str | None]:
    """Apply HFT settings payload to live config + runtime state."""
    cfg = _bot_config
    if not cfg:
        return [], "Config not initialized"

    updated: list[str] = []
    scanner_gate_changed = False

    if "assets" in body:
        raw = str(body["assets"]).strip().lower()
        parsed = _parse_scanner_assets(raw)
        val = ",".join(parsed)
        cfg.scanner.assets = val
        updated.append(f"assets={val}")
        scanner_gate_changed = True

    if "time_frames" in body:
        raw = str(body["time_frames"]).strip().lower()
        parsed = _parse_scanner_tfs(raw)
        val = ",".join(parsed)
        cfg.scanner.time_frames = val
        updated.append(f"time_frames={val}")
        scanner_gate_changed = True
    if "asset_timeframe_enabled" in body:
        matrix = _sanitize_scanner_toggle_matrix(
            body["asset_timeframe_enabled"],
            _all_false_scanner_asset_tf_matrix(),
        )
        cfg.scanner.asset_timeframe_enabled = matrix
        updated.append("asset_timeframe_enabled=updated")
        scanner_gate_changed = True

    if "lookahead_enabled" in body:
        val = bool(body["lookahead_enabled"])
        cfg.scanner.lookahead_enabled = val
        updated.append(f"lookahead_enabled={val}")
    if "lookahead_seconds" in body:
        val = max(0, min(300, int(body["lookahead_seconds"])))
        cfg.scanner.lookahead_seconds = val
        updated.append(f"lookahead_seconds={val}")
    if "ptb_capture_offset" in body:
        val = round(max(0.0, min(10.0, float(body["ptb_capture_offset"]))), 2)
        cfg.scanner.ptb_capture_offset = val
        updated.append(f"ptb_capture_offset={val}")
    if "scan_interval" in body:
        val = max(0.1, min(5.0, float(body["scan_interval"])))
        cfg.scanner.interval_seconds = val
        updated.append(f"scan_interval={val}s")
        if _bot_scanner is not None:
            _bot_scanner._status_log_every_scans = max(1, int(round(30.0 / val)))

    t = cfg.trend
    if "hft_bet_amount" in body:
        val = round(max(0.01, min(10000.0, float(body["hft_bet_amount"]))), 2)
        t.hft_bet_amount = val
        updated.append(f"hft_bet_amount={val}")
    if "hft_max_price" in body:
        val = round(max(0.01, min(0.99, float(body["hft_max_price"]))), 4)
        t.hft_max_price = val
        updated.append(f"hft_max_price={val}")
    if "hft_max_price_5m" in body:
        # 0 means "use global hft_max_price".
        val = round(max(0.0, min(0.99, float(body["hft_max_price_5m"]))), 4)
        t.hft_max_price_5m = val
        updated.append(f"hft_max_price_5m={val}")
    if "hft_max_price_15m" in body:
        # 0 means "use global hft_max_price".
        val = round(max(0.0, min(0.99, float(body["hft_max_price_15m"]))), 4)
        t.hft_max_price_15m = val
        updated.append(f"hft_max_price_15m={val}")
    if "hft_max_price_1h" in body:
        # 0 means "use global hft_max_price".
        val = round(max(0.0, min(0.99, float(body["hft_max_price_1h"]))), 4)
        t.hft_max_price_1h = val
        updated.append(f"hft_max_price_1h={val}")
    if "hft_flip_profit_pct" in body:
        val = round(max(0.0, min(5000.0, float(body["hft_flip_profit_pct"]))), 2)
        t.hft_flip_profit_pct = val
        updated.append(f"hft_flip_profit_pct={val}")
    if "hft_muzzle_seconds" in body:
        val = max(0, min(600, int(body["hft_muzzle_seconds"])))
        t.hft_muzzle_seconds = val
        updated.append(f"hft_muzzle_seconds={val}")
    if "hft_barrier_enabled" in body:
        val = bool(body["hft_barrier_enabled"])
        t.hft_barrier_enabled = val
        updated.append(f"hft_barrier_enabled={val}")
    if "hft_barrier_pct" in body:
        val = round(max(0.0, min(5.0, float(body["hft_barrier_pct"]))), 3)
        t.hft_barrier_pct = val
        updated.append(f"hft_barrier_pct={val}")
    if "hft_barrier_pct_15m" in body:
        # 0 means "use global hft_barrier_pct".
        val = round(max(0.0, min(5.0, float(body["hft_barrier_pct_15m"]))), 3)
        t.hft_barrier_pct_15m = val
        updated.append(f"hft_barrier_pct_15m={val}")
    if "hft_barrier_pct_1h" in body:
        # 0 means "use global hft_barrier_pct".
        val = round(max(0.0, min(5.0, float(body["hft_barrier_pct_1h"]))), 3)
        t.hft_barrier_pct_1h = val
        updated.append(f"hft_barrier_pct_1h={val}")
    if "hft_barrier_matrix" in body:
        matrix = _sanitize_hft_matrix(
            body["hft_barrier_matrix"],
            _default_hft_barrier_matrix(cfg),
            low=0.0,
            high=5.0,
            digits=3,
        )
        t.hft_barrier_matrix = matrix
        updated.append("hft_barrier_matrix=updated")
    if "hft_max_price_matrix" in body:
        matrix = _sanitize_hft_matrix(
            body["hft_max_price_matrix"],
            _default_hft_max_price_matrix(cfg),
            low=0.01,
            high=0.99,
            digits=4,
        )
        t.hft_max_price_matrix = matrix
        updated.append("hft_max_price_matrix=updated")
    if "hft_barrier_delay" in body:
        val = max(0, min(120, int(body["hft_barrier_delay"])))
        t.hft_barrier_delay = val
        updated.append(f"hft_barrier_delay={val}")
    if "hft_armed_sniper_enabled" in body:
        val = bool(body["hft_armed_sniper_enabled"])
        t.hft_armed_sniper_enabled = val
        updated.append(f"hft_armed_sniper_enabled={val}")
    if "hft_mode2_entry_mode" in body:
        raw = str(body["hft_mode2_entry_mode"] or "").strip().lower()
        allowed = {"", "immediate_fok", "watch_best_entry"}
        if raw not in allowed:
            raw = ""
        t.hft_mode2_entry_mode = raw
        updated.append(f"hft_mode2_entry_mode={raw or 'legacy-toggle'}")
    if "hft_mode2_trigger_cooldown_ms" in body:
        val = max(10, min(5000, int(body["hft_mode2_trigger_cooldown_ms"])))
        t.hft_mode2_trigger_cooldown_ms = val
        updated.append(f"hft_mode2_trigger_cooldown_ms={val}")
    if "hft_optimizer_objective_mode" in body:
        raw = str(body["hft_optimizer_objective_mode"] or "").strip().lower()
        allowed = {"pnl", "win_rate", "roi"}
        if raw not in allowed:
            raw = "pnl"
        t.hft_optimizer_objective_mode = raw
        updated.append(f"hft_optimizer_objective_mode={raw}")
    if "hft_optimizer_lookback_hours" in body:
        val = max(1, min(8760, int(body["hft_optimizer_lookback_hours"])))
        t.hft_optimizer_lookback_hours = val
        updated.append(f"hft_optimizer_lookback_hours={val}")
    if "hft_optimizer_min_trades" in body:
        val = max(0, min(100000, int(body["hft_optimizer_min_trades"])))
        t.hft_optimizer_min_trades = val
        updated.append(f"hft_optimizer_min_trades={val}")
    if updated:
        if scanner_gate_changed:
            _apply_scanner_runtime_toggles()
        _persist_to_yaml()
        logger.info("[HFT] Settings updated: %s", ", ".join(updated))

    return updated, None


def _monitor_settings_snapshot() -> dict:
    cfg = _bot_config
    if not cfg:
        return {}
    m = cfg.monitor
    return {
        "enabled": bool(getattr(m, "enabled", True)),
        "assets": str(getattr(m, "assets", "bitcoin,ethereum,solana,xrp") or ""),
        "time_frames": str(getattr(m, "time_frames", "5m,15m,1h,4h,1d") or ""),
        "discovery_interval_seconds": int(getattr(m, "discovery_interval_seconds", 15) or 15),
        "max_events_per_query": int(getattr(m, "max_events_per_query", 250) or 250),
        "pre_start_capture_seconds": int(getattr(m, "pre_start_capture_seconds", 15) or 15),
        "post_end_track_seconds": int(getattr(m, "post_end_track_seconds", 120) or 120),
        "sample_5m_seconds": int(getattr(m, "sample_5m_seconds", 1) or 1),
        "sample_15m_seconds": int(getattr(m, "sample_15m_seconds", 1) or 1),
        "sample_1h_seconds": int(getattr(m, "sample_1h_seconds", 60) or 60),
        "sample_4h_seconds": int(getattr(m, "sample_4h_seconds", 240) or 240),
        "sample_1d_seconds": int(getattr(m, "sample_1d_seconds", 1440) or 1440),
    }


def _apply_monitor_settings(body: dict) -> tuple[list[str], str | None]:
    cfg = _bot_config
    if not cfg:
        return [], "Config not initialized"

    m = cfg.monitor
    updated: list[str] = []

    if "enabled" in body:
        val = bool(body["enabled"])
        m.enabled = val
        updated.append(f"enabled={val}")

    if "assets" in body:
        raw = str(body["assets"]).strip().lower()
        known = {"bitcoin", "ethereum", "solana", "xrp"}
        parsed = [a.strip() for a in raw.split(",") if a.strip() in known]
        if not parsed:
            return updated, "Monitor must keep at least 1 valid asset"
        m.assets = ",".join(parsed)
        updated.append(f"assets={m.assets}")

    if "time_frames" in body:
        raw = str(body["time_frames"]).strip().lower()
        known = {"5m", "15m", "1h", "4h", "1d"}
        parsed = [t.strip() for t in raw.split(",") if t.strip() in known]
        if not parsed:
            return updated, "Monitor must keep at least 1 valid timeframe"
        m.time_frames = ",".join(parsed)
        updated.append(f"time_frames={m.time_frames}")

    if "discovery_interval_seconds" in body:
        val = max(5, min(300, int(body["discovery_interval_seconds"])))
        m.discovery_interval_seconds = val
        updated.append(f"discovery_interval_seconds={val}")
    if "max_events_per_query" in body:
        val = max(25, min(500, int(body["max_events_per_query"])))
        m.max_events_per_query = val
        updated.append(f"max_events_per_query={val}")
    if "pre_start_capture_seconds" in body:
        val = max(0, min(300, int(body["pre_start_capture_seconds"])))
        m.pre_start_capture_seconds = val
        updated.append(f"pre_start_capture_seconds={val}")
    if "post_end_track_seconds" in body:
        val = max(0, min(3600, int(body["post_end_track_seconds"])))
        m.post_end_track_seconds = val
        updated.append(f"post_end_track_seconds={val}")

    for key, lo, hi in (
        ("sample_5m_seconds", 1, 60),
        ("sample_15m_seconds", 1, 120),
        ("sample_1h_seconds", 10, 3600),
        ("sample_4h_seconds", 10, 7200),
        ("sample_1d_seconds", 60, 21600),
    ):
        if key in body:
            val = max(lo, min(hi, int(body[key])))
            setattr(m, key, val)
            updated.append(f"{key}={val}")

    if updated:
        _persist_to_yaml()
        logger.info("[MONITOR V2] Settings updated: %s", ", ".join(updated))

    return updated, None


def _filter_hft_log_lines(lines: list[str]) -> list[str]:
    """Drop repetitive heartbeat noise for HFT dashboard readability."""
    noisy = (
        "[SNIPE: IDLE]",
        "[SNIPE: SCANNING]",
        "Promoted ",
        "Discovered ",
        "[FEED: WS] subscribed to",
    )
    out: list[str] = []
    for ln in lines:
        if any(k in ln for k in noisy):
            continue
        out.append(ln)
    return out


@app.get("/api/hft/status")
async def api_hft_status():
    lat = get_mode2_metrics_snapshot()
    return {
        "ok": True,
        "running": bool(_bot_state.get("running", False)),
        "mode": str(_bot_state.get("mode", "")),
        "paused": bool(_bot_controls.get("paused", True)),
        "wallet_balance": _get_cached_balance(),
        "wallet_pol_balance": _get_cached_pol_balance(),
        "wallet_symbol": "USDC.e",
        "wallet_address": _wallet_address(),
        "wallet_private_key": _wallet_private_key(),
        "env_exists": _env_file_exists(),
        "env_setup_complete": _env_setup_complete(),
        "tracked_markets": int(_bot_state.get("tracked_markets", 0) or 0),
        "ws_subscribed": int(_bot_state.get("ws_subscribed", 0) or 0),
        "trades": int(_bot_state.get("trades", 0) or 0),
        "total_spent": float(_bot_state.get("total_spent", 0.0) or 0.0),
        "redeemed_pnl": float(_bot_state.get("redeemed_pnl", 0.0) or 0.0),
        "latency": lat,
    }


@app.get("/api/hft/latency")
async def api_hft_latency():
    snap = get_mode2_metrics_snapshot()
    return {"ok": True, **snap}


@app.get("/api/hft/settings")
async def api_hft_get_settings():
    payload = _hft_settings_snapshot()
    if not payload:
        return JSONResponse({"ok": False, "message": "Config not initialized"}, status_code=500)
    return {"ok": True, **payload}


@app.post("/api/hft/settings")
async def api_hft_update_settings(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "message": "Invalid JSON"}, status_code=400)
    payload = body if isinstance(body, dict) else {}
    changed, err = _apply_hft_settings(payload)
    if err:
        return JSONResponse({"ok": False, "message": err}, status_code=400)
    skip_shadow_run = _coerce_bool(payload.get("skip_shadow_optimizer_run", False))
    if changed and not skip_shadow_run:
        _trigger_shadow_optimizer_v2_run("settings_change")
    return {"ok": True, "changed": changed}


@app.get("/api/monitor/settings")
async def api_monitor_get_settings():
    payload = _monitor_settings_snapshot()
    if not payload:
        return JSONResponse({"ok": False, "message": "Config not initialized"}, status_code=500)
    worker_stats = _bot_monitor_worker.get_stats() if _bot_monitor_worker else {}
    return {"ok": True, **payload, "runtime": worker_stats}


@app.post("/api/monitor/settings")
async def api_monitor_update_settings(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "message": "Invalid JSON"}, status_code=400)
    changed, err = _apply_monitor_settings(body if isinstance(body, dict) else {})
    if err:
        return JSONResponse({"ok": False, "message": err}, status_code=400)
    return {"ok": True, "changed": changed}


@app.get("/api/monitor/report")
async def api_monitor_report(hours: int = 24):
    hrs = max(1, min(8760, int(hours)))
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=hrs)).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        with _monitor_read_lock:
            mdb = _get_monitor_db()
            if mdb is None:
                return {"ok": True, "hours": hrs, "rows": [], "totals": {}, "runtime": {}}

            rows = mdb.execute(
                """
                SELECT
                    timeframe_seconds,
                    asset,
                    COUNT(*) AS samples,
                    SUM(CASE WHEN arm_like = 1 THEN 1 ELSE 0 END) AS arm_hits,
                    AVG(gap_pct) AS avg_gap_pct,
                    MIN(gap_pct) AS min_gap_pct,
                    MAX(gap_pct) AS max_gap_pct,
                    AVG(barrier_pct) AS avg_barrier_pct,
                    SUM(CASE WHEN winner IS NOT NULL THEN 1 ELSE 0 END) AS resolved,
                    SUM(CASE WHEN winner IS NOT NULL AND winning_side = winner THEN 1 ELSE 0 END) AS right_side
                FROM monitor_v2_samples
                WHERE timestamp >= ?
                GROUP BY timeframe_seconds, asset
                ORDER BY timeframe_seconds ASC, asset ASC
                """,
                (cutoff_iso,),
            ).fetchall()

            totals = mdb.execute(
                """
                SELECT
                    COUNT(*) AS samples,
                    SUM(CASE WHEN arm_like = 1 THEN 1 ELSE 0 END) AS arm_hits,
                    COUNT(DISTINCT condition_id) AS markets
                FROM monitor_v2_samples
                WHERE timestamp >= ?
                """,
                (cutoff_iso,),
            ).fetchone()
    except Exception:
        return {"ok": True, "hours": hrs, "rows": [], "totals": {}, "runtime": {}}

    data_rows = []
    for r in rows:
        samples = int(r["samples"] or 0)
        arm_hits = int(r["arm_hits"] or 0)
        resolved = int(r["resolved"] or 0)
        right_side = int(r["right_side"] or 0)
        data_rows.append(
            {
                "timeframe_seconds": int(r["timeframe_seconds"] or 0),
                "asset": str(r["asset"] or ""),
                "samples": samples,
                "arm_hits": arm_hits,
                "arm_rate_pct": (arm_hits / samples * 100.0) if samples > 0 else 0.0,
                "avg_gap_pct": float(r["avg_gap_pct"] or 0.0),
                "min_gap_pct": float(r["min_gap_pct"] or 0.0),
                "max_gap_pct": float(r["max_gap_pct"] or 0.0),
                "avg_barrier_pct": float(r["avg_barrier_pct"] or 0.0),
                "resolved": resolved,
                "right_side": right_side,
                "right_side_pct": (right_side / resolved * 100.0) if resolved > 0 else 0.0,
            }
        )

    totals_payload = {
        "samples": int((totals["samples"] if totals else 0) or 0),
        "arm_hits": int((totals["arm_hits"] if totals else 0) or 0),
        "markets": int((totals["markets"] if totals else 0) or 0),
    }
    worker_stats = _bot_monitor_worker.get_stats() if _bot_monitor_worker else {}
    return {
        "ok": True,
        "hours": hrs,
        "rows": data_rows,
        "totals": totals_payload,
        "runtime": worker_stats,
    }


_SIM_ASSETS = ("bitcoin", "ethereum", "solana", "xrp")
_SIM_TF_LABEL_TO_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
_SIM_TF_SECONDS_TO_LABEL = {v: k for k, v in _SIM_TF_LABEL_TO_SECONDS.items()}
_SIM_TRADES_UI_LIMIT = 100


def _default_sim_barriers() -> dict:
    cfg = _bot_config
    base = float(getattr(cfg.trend, "hft_barrier_pct", 0.1)) if cfg else 0.1
    out: dict[str, dict[str, float]] = {}
    for asset in _SIM_ASSETS:
        out[asset] = {
            "5m": float(cfg.trend.get_hft_barrier_pct_for(asset, 300)) if cfg else base,
            "15m": float(cfg.trend.get_hft_barrier_pct_for(asset, 900)) if cfg else base,
            "1h": float(cfg.trend.get_hft_barrier_pct_for(asset, 3600)) if cfg else base,
            "4h": float(cfg.trend.get_hft_barrier_pct_for(asset, 14400)) if cfg else base,
            "1d": float(cfg.trend.get_hft_barrier_pct_for(asset, 86400)) if cfg else base,
        }
    return out


def _default_sim_max_prices() -> dict:
    cfg = _bot_config
    base = float(getattr(cfg.trend, "hft_max_price", 0.67)) if cfg else 0.67
    out: dict[str, dict[str, float]] = {}
    for asset in _SIM_ASSETS:
        out[asset] = {
            "5m": round(max(0.01, min(0.99, float(cfg.trend.get_hft_max_price_for(asset, 300)) if cfg else base)), 4),
            "15m": round(max(0.01, min(0.99, float(cfg.trend.get_hft_max_price_for(asset, 900)) if cfg else base)), 4),
            "1h": round(max(0.01, min(0.99, float(cfg.trend.get_hft_max_price_for(asset, 3600)) if cfg else base)), 4),
            "4h": round(max(0.01, min(0.99, float(cfg.trend.get_hft_max_price_for(asset, 14400)) if cfg else base)), 4),
            "1d": round(max(0.01, min(0.99, float(cfg.trend.get_hft_max_price_for(asset, 86400)) if cfg else base)), 4),
        }
    return out


def _default_sim_asset_tf_enabled(
    assets: list[str] | None = None,
    tfs: list[str] | None = None,
) -> dict[str, dict[str, bool]]:
    active_assets = set(assets or list(_SIM_ASSETS))
    active_tfs = set(tfs or list(_SIM_TF_LABEL_TO_SECONDS.keys()))
    out: dict[str, dict[str, bool]] = {}
    for asset in _SIM_ASSETS:
        out[asset] = {}
        for tf in _SIM_TF_LABEL_TO_SECONDS:
            out[asset][tf] = (asset in active_assets and tf in active_tfs)
    return out


def _all_false_sim_asset_tf_enabled() -> dict[str, dict[str, bool]]:
    out: dict[str, dict[str, bool]] = {}
    for asset in _SIM_ASSETS:
        out[asset] = {tf: False for tf in _SIM_TF_LABEL_TO_SECONDS}
    return out


def _sanitize_optimizer_objective(raw: object, default: str = "pnl") -> str:
    allowed = {"pnl", "win_rate", "roi"}
    val = str(raw or "").strip().lower()
    if val in allowed:
        return val
    fallback = str(default or "pnl").strip().lower()
    return fallback if fallback in allowed else "pnl"


def _sim_defaults_dict() -> dict:
    cfg = _bot_config
    lookback_hours = 24
    objective_mode = "pnl"
    min_trades = 0
    if cfg and getattr(cfg, "trend", None):
        lookback_hours = max(1, min(8760, int(getattr(cfg.trend, "hft_optimizer_lookback_hours", 24) or 24)))
        objective_mode = _sanitize_optimizer_objective(
            getattr(cfg.trend, "hft_optimizer_objective_mode", "pnl"),
            "pnl",
        )
        min_trades = max(0, min(100000, int(getattr(cfg.trend, "hft_optimizer_min_trades", 0) or 0)))
    return {
        "hours": lookback_hours,
        "min_trades": min_trades,
        "max_bet": float(getattr(cfg.trend, "hft_bet_amount", 1.0)) if cfg else 1.0,
        "max_price": float(cfg.trend.get_hft_max_price_for("bitcoin", 300)) if cfg else 0.67,
        "muzzle_seconds": int(getattr(cfg.trend, "hft_muzzle_seconds", 90)) if cfg else 90,
        "start_delay_seconds": int(getattr(cfg.trend, "hft_barrier_delay", 10)) if cfg else 10,
        "armed_sniper": bool(getattr(cfg.trend, "hft_armed_sniper_enabled", False)) if cfg else False,
        "require_depth": True,
        "objective_mode": objective_mode,
        "assets": list(_SIM_ASSETS),
        "timeframes": list(_SIM_TF_LABEL_TO_SECONDS.keys()),
        "asset_timeframe_enabled": _default_sim_asset_tf_enabled(),
        "barriers": _default_sim_barriers(),
        "max_prices": _default_sim_max_prices(),
    }


@app.get("/api/monitor/sim-defaults")
async def api_monitor_sim_defaults():
    return {
        "ok": True,
        "defaults": _sim_defaults_dict(),
    }


def _sanitize_sim_payload(body: dict, *, defaults: dict | None = None) -> dict:
    d = defaults if isinstance(defaults, dict) else _sim_defaults_dict()
    hours = max(1, min(8760, int(body.get("hours", d["hours"]))))
    objective_mode = _sanitize_optimizer_objective(body.get("objective_mode", d.get("objective_mode", "pnl")))
    min_trades = max(0, min(100000, int(body.get("min_trades", d.get("min_trades", 0)) or 0)))
    max_bet = round(max(0.01, min(100000.0, float(body.get("max_bet", d["max_bet"])))), 4)
    max_price = round(max(0.01, min(0.99, float(body.get("max_price", d["max_price"])))), 4)
    muzzle_seconds = max(0, min(600, int(body.get("muzzle_seconds", d["muzzle_seconds"])))
    )
    start_delay_seconds = max(0, min(300, int(body.get("start_delay_seconds", d["start_delay_seconds"])))
    )
    armed_sniper = bool(body.get("armed_sniper", d["armed_sniper"]))
    require_depth = bool(body.get("require_depth", d["require_depth"]))
    assets_raw = body.get("assets", d["assets"])
    tfs_raw = body.get("timeframes", d["timeframes"])

    assets: list[str] = []
    if isinstance(assets_raw, list):
        assets = [str(a).strip().lower() for a in assets_raw if str(a).strip().lower() in _SIM_ASSETS]
    if not assets:
        assets = list(_SIM_ASSETS)

    tf_labels: list[str] = []
    if isinstance(tfs_raw, list):
        for tf in tfs_raw:
            val = str(tf).strip().lower()
            if val in _SIM_TF_LABEL_TO_SECONDS:
                tf_labels.append(val)
    if not tf_labels:
        tf_labels = list(_SIM_TF_LABEL_TO_SECONDS.keys())
    tf_seconds = [_SIM_TF_LABEL_TO_SECONDS[tf] for tf in tf_labels]

    # Gate matrix precedence:
    # 1) enabled_cells list (authoritative explicit cell set)
    # 2) asset_timeframe_enabled matrix (strict; missing cells = OFF)
    # 3) default intersection of assets x timeframes.
    enabled_cells_raw = body.get("enabled_cells")
    if isinstance(enabled_cells_raw, list):
        asset_tf_enabled = _all_false_sim_asset_tf_enabled()
        valid_count = 0
        for item in enabled_cells_raw:
            asset = ""
            tf = ""
            if isinstance(item, str):
                s = str(item).strip().lower()
                if ":" in s:
                    asset, tf = [x.strip() for x in s.split(":", 1)]
                elif "|" in s:
                    asset, tf = [x.strip() for x in s.split("|", 1)]
            elif isinstance(item, dict):
                asset = str(item.get("asset", "")).strip().lower()
                tf = str(item.get("timeframe", "")).strip().lower()
            if asset in _SIM_ASSETS and tf in _SIM_TF_LABEL_TO_SECONDS:
                asset_tf_enabled[asset][tf] = True
                valid_count += 1
        if valid_count > 0:
            assets = [a for a in _SIM_ASSETS if any(asset_tf_enabled[a][tf] for tf in _SIM_TF_LABEL_TO_SECONDS)]
            tf_labels = [tf for tf in _SIM_TF_LABEL_TO_SECONDS if any(asset_tf_enabled[a][tf] for a in _SIM_ASSETS)]
        else:
            assets = []
            tf_labels = []
    elif "asset_timeframe_enabled" in body:
        # Strict explicit matrix mode: missing cells remain OFF.
        asset_tf_enabled = _all_false_sim_asset_tf_enabled()
        incoming_asset_tf = body.get("asset_timeframe_enabled", {})
        if isinstance(incoming_asset_tf, dict):
            for asset in _SIM_ASSETS:
                row = incoming_asset_tf.get(asset)
                if not isinstance(row, dict):
                    continue
                for tf in _SIM_TF_LABEL_TO_SECONDS:
                    if tf in row:
                        asset_tf_enabled[asset][tf] = _coerce_bool(row.get(tf))
    else:
        asset_tf_enabled = _default_sim_asset_tf_enabled(assets, tf_labels)

    # Recompute timeframe-seconds after any enabled-cells override.
    tf_seconds = [_SIM_TF_LABEL_TO_SECONDS[tf] for tf in tf_labels]

    barriers = _default_sim_barriers()
    incoming = body.get("barriers", {})
    if isinstance(incoming, dict):
        for asset in _SIM_ASSETS:
            row = incoming.get(asset)
            if not isinstance(row, dict):
                continue
            for tf in _SIM_TF_LABEL_TO_SECONDS:
                if tf in row:
                    try:
                        barriers[asset][tf] = round(max(0.0, min(5.0, float(row[tf]))), 4)
                    except Exception:
                        pass

    max_prices = _default_sim_max_prices()
    incoming_prices = body.get("max_prices", {})
    if isinstance(incoming_prices, dict):
        for asset in _SIM_ASSETS:
            row = incoming_prices.get(asset)
            if not isinstance(row, dict):
                continue
            for tf in _SIM_TF_LABEL_TO_SECONDS:
                if tf in row:
                    try:
                        max_prices[asset][tf] = round(max(0.01, min(0.99, float(row[tf]))), 4)
                    except Exception:
                        pass

    return {
        "hours": hours,
        "objective_mode": objective_mode,
        "min_trades": min_trades,
        "max_bet": max_bet,
        "max_price": max_price,
        "muzzle_seconds": muzzle_seconds,
        "start_delay_seconds": start_delay_seconds,
        "armed_sniper": armed_sniper,
        "require_depth": require_depth,
        "assets": assets,
        "tf_labels": tf_labels,
        "tf_seconds": tf_seconds,
        "asset_timeframe_enabled": asset_tf_enabled,
        "barriers": barriers,
        "max_prices": max_prices,
    }


def _barrier_for(barriers: dict, asset: str, timeframe_seconds: int) -> float:
    tf_label = _SIM_TF_SECONDS_TO_LABEL.get(int(timeframe_seconds), "")
    row = barriers.get(asset, {})
    if tf_label and isinstance(row, dict):
        try:
            return float(row.get(tf_label, 0.0) or 0.0)
        except Exception:
            return 0.0
    return 0.0


def _max_price_for(max_prices: dict, fallback: float, asset: str, timeframe_seconds: int) -> float:
    tf_label = _SIM_TF_SECONDS_TO_LABEL.get(int(timeframe_seconds), "")
    row = max_prices.get(asset, {})
    if tf_label and isinstance(row, dict):
        try:
            cap = float(row.get(tf_label, 0.0) or 0.0)
            if 0.01 <= cap <= 0.99:
                return cap
        except Exception:
            pass
    return float(fallback)


def _sim_cell_enabled(params: dict, asset: str, tf_label: str) -> bool:
    """Effective simulation gate = assets/timeframes globals AND per-cell matrix."""
    assets = params.get("assets")
    tf_labels = params.get("tf_labels")
    matrix = params.get("asset_timeframe_enabled")
    if not isinstance(assets, list) or not isinstance(tf_labels, list) or not isinstance(matrix, dict):
        raise ValueError("Simulation params must be normalized via _sanitize_sim_payload()")
    if asset not in assets or tf_label not in tf_labels:
        return False
    row = matrix.get(asset)
    if not isinstance(row, dict):
        return False
    return _coerce_bool(row.get(tf_label, False))


def _simulate_market_rows(
    rows: list[sqlite3.Row],
    params: dict,
    *,
    asks_json_cache: dict[str, tuple[tuple[float, float], ...]] | None = None,
    cap_depth_cache: dict[tuple[str, float], float] | None = None,
) -> dict | None:
    if not rows:
        return None
    first = rows[0]
    asset = str(first["asset"] or "")
    tf_seconds = int(first["timeframe_seconds"] or 0)
    tf_label = _SIM_TF_SECONDS_TO_LABEL.get(tf_seconds, "")
    cell_enabled = _sim_cell_enabled(params, asset, tf_label)
    if not cell_enabled:
        return None
    barrier = _barrier_for(params["barriers"], asset, tf_seconds)
    max_price = _max_price_for(params.get("max_prices", {}), float(params["max_price"]), asset, tf_seconds)
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

    # State-machine replay over collected monitor data:
    # idle -> armed -> sprung/disarmed.
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

        def _depth_to_cap(asks_raw: str | None, fallback_depth: float, ask_price: float) -> float:
            if asks_raw:
                raw = str(asks_raw)
                cache_key = (raw, round(float(max_price), 4))
                if isinstance(cap_depth_cache, dict):
                    cached = cap_depth_cache.get(cache_key)
                    if cached is not None:
                        ladder_depth = float(cached)
                    else:
                        ladder_depth = None
                else:
                    ladder_depth = None

                if ladder_depth is None:
                    levels: tuple[tuple[float, float], ...] | None = None
                    if isinstance(asks_json_cache, dict):
                        levels = asks_json_cache.get(raw)
                    if levels is None:
                        try:
                            asks = _json.loads(raw)
                        except Exception:
                            asks = None
                        parsed: list[tuple[float, float]] = []
                        if isinstance(asks, list):
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
                                parsed.append((p, s))
                        levels = tuple(parsed)
                        if isinstance(asks_json_cache, dict):
                            asks_json_cache[raw] = levels

                    ladder_depth = 0.0
                    for p, s in levels:
                        if p <= max_price:
                            ladder_depth += p * s
                    if isinstance(cap_depth_cache, dict):
                        cap_depth_cache[cache_key] = float(ladder_depth)

                # Mirror live trap spring semantics:
                # if ladder-derived cap depth is unavailable/zero but ask is already
                # within cap, fall back to aggregate ask_depth gate.
                if (
                    ladder_depth <= 0.0
                    and ask_price > 0.0
                    and ask_price <= max_price
                    and fallback_depth > 0.0
                ):
                    return float(fallback_depth)
                return float(ladder_depth)
            return fallback_depth

        def _ask_depth_for(pick_side: str) -> tuple[float, float]:
            if pick_side == "Up":
                ask = float(r["up_ask"] or 0.0)
                fallback_depth = float(r["up_depth"] or 0.0)
                asks_raw = r["up_asks_json"] if "up_asks_json" in r.keys() else None
                return ask, _depth_to_cap(asks_raw, fallback_depth, ask)
            ask = float(r["down_ask"] or 0.0)
            fallback_depth = float(r["down_depth"] or 0.0)
            asks_raw = r["down_asks_json"] if "down_asks_json" in r.keys() else None
            return ask, _depth_to_cap(asks_raw, fallback_depth, ask)

        # Armed-path replay: live logic keeps armed state independent from
        # barrier threshold until spring/disarm.
        if armed:
            if side in {"Up", "Down"} and side != armed_side:
                # Side-flip disarm.
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
                    "timeframe": _SIM_TF_SECONDS_TO_LABEL.get(tf_seconds, str(tf_seconds)),
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

        # Idle-path replay requires oracle side + barrier context to arm.
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
                    "timeframe": _SIM_TF_SECONDS_TO_LABEL.get(tf_seconds, str(tf_seconds)),
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

        # armed_mode idle path:
        # if ask already <= cap when barrier condition appears, live code can
        # arm and spring immediately on the same update.
        if ask <= max_price and _can_fill():
            arm_count += 1
            shares = max_bet / ask
            trade = {
                "condition_id": str(r["condition_id"]),
                "event_title": str(r["event_title"] or ""),
                "asset": asset,
                "timeframe_seconds": tf_seconds,
                "timeframe": _SIM_TF_SECONDS_TO_LABEL.get(tf_seconds, str(tf_seconds)),
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


def _hour_label_et(hour: int) -> str:
    h = int(hour) % 24
    end = (h + 1) % 24

    def _fmt(hh: int) -> str:
        h12 = hh % 12 or 12
        ampm = "AM" if hh < 12 else "PM"
        return f"{h12}:00 {ampm}"

    return f"{_fmt(h)}-{_fmt(end)}"


def _parse_trade_ts_utc(ts: str) -> datetime | None:
    s = str(ts or "").strip()
    if not s:
        return None
    cand = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cand)
    except Exception:
        dt = None
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                dt = datetime.strptime(s, fmt)
                break
            except Exception:
                continue
        if dt is None:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _hourly_buckets_et_from_trades(trades: list[dict]) -> list[dict]:
    buckets: list[dict] = []
    for hour in range(24):
        buckets.append(
            {
                "hour": hour,
                "label_et": _hour_label_et(hour),
                "trades": 0,
                "won": 0,
                "lost": 0,
                "open": 0,
                "spent": 0.0,
                "pnl": 0.0,
                "win_rate_pct": 0.0,
                "roi_pct": 0.0,
            }
        )

    for t in trades:
        dt_utc = _parse_trade_ts_utc(str(t.get("entry_ts") or ""))
        if dt_utc is None:
            continue
        et_hour = dt_utc.astimezone(_ET_ZONE).hour
        b = buckets[et_hour]
        b["trades"] += 1
        result = str(t.get("result") or "").lower()
        if result == "won":
            b["won"] += 1
        elif result == "lost":
            b["lost"] += 1
        else:
            b["open"] += 1
        b["spent"] += float(t.get("cost") or 0.0)
        b["pnl"] += float(t.get("pnl") or 0.0)

    for b in buckets:
        decided = int(b["won"] + b["lost"])
        b["win_rate_pct"] = (float(b["won"]) / float(decided) * 100.0) if decided > 0 else 0.0
        b["roi_pct"] = (float(b["pnl"]) / float(b["spent"]) * 100.0) if b["spent"] > 0 else 0.0
    return buckets


def _sim_active_cells(params: dict) -> list[tuple[str, str, int]]:
    """Return effective enabled simulation cells from normalized params."""
    cells: list[tuple[str, str, int]] = []
    for tf_label in params.get("tf_labels", []):
        tf_seconds = _SIM_TF_LABEL_TO_SECONDS.get(str(tf_label), 0)
        if not tf_seconds:
            continue
        for asset in params.get("assets", []):
            if _sim_cell_enabled(params, str(asset), str(tf_label)):
                cells.append((str(asset), str(tf_label), int(tf_seconds)))
    return cells


@app.post("/api/monitor/simulate")
async def api_monitor_simulate(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "message": "Invalid JSON"}, status_code=400)
    if not isinstance(body, dict):
        body = {}
    p = _sanitize_sim_payload(body)
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=p["hours"])).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not p["assets"] or not p["tf_seconds"]:
        return {
            "ok": True,
            "params": p,
            "summary": {"trades": 0, "won": 0, "lost": 0, "open": 0, "spent": 0.0, "pnl": 0.0, "win_rate_pct": 0.0, "roi_pct": 0.0},
            "by_asset_tf": [],
            "trades": [],
            "hourly_et": [],
            "hourly_timezone": "America/New_York",
            "trades_returned": 0,
            "trades_total": 0,
            "markets_sampled": 0,
            "rows_scanned": 0,
            "cutoff_utc": cutoff_iso,
        }

    with _monitor_read_lock:
        mdb = _get_monitor_db()
        if mdb is None:
            return {
                "ok": True,
                "summary": {},
                "by_asset_tf": [],
                "trades": [],
                "hourly_et": [],
                "hourly_timezone": "America/New_York",
                "params": p,
            }

        has_v2 = mdb.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='monitor_v2_samples' LIMIT 1"
        ).fetchone()
        if not has_v2:
            return {
                "ok": True,
                "summary": {},
                "by_asset_tf": [],
                "trades": [],
                "hourly_et": [],
                "hourly_timezone": "America/New_York",
                "params": p,
            }

        placeholders_assets = ",".join(["?"] * len(p["assets"]))
        placeholders_tfs = ",".join(["?"] * len(p["tf_seconds"]))
        sql = f"""
            SELECT
                s.condition_id, s.timestamp, s.asset, s.timeframe_seconds,
                COALESCE(NULLIF(s.event_title, ''), m.event_title) AS event_title,
                s.sec_to_start, s.sec_to_end,
                s.up_ask, s.down_ask, s.up_depth, s.down_depth,
                s.up_asks_json, s.down_asks_json,
                s.gap_pct, s.winning_side,
                COALESCE(s.winner, m.winner) AS winner
            FROM monitor_v2_samples s
            LEFT JOIN monitor_v2_markets m ON m.condition_id = s.condition_id
            WHERE s.timestamp >= ?
              AND s.asset IN ({placeholders_assets})
              AND s.timeframe_seconds IN ({placeholders_tfs})
              AND s.sec_to_start <= ?
              AND s.sec_to_end >= ?
            ORDER BY s.condition_id ASC, s.timestamp ASC
        """
        params_sql = [
            cutoff_iso,
            *p["assets"],
            *p["tf_seconds"],
            -int(p["start_delay_seconds"]),
            int(p["muzzle_seconds"]),
        ]
        rows = mdb.execute(sql, params_sql).fetchall()

    trades: list[dict] = []
    asks_json_cache: dict[str, tuple[tuple[float, float], ...]] = {}
    cap_depth_cache: dict[tuple[str, float], float] = {}
    current_cid = None
    bucket: list[sqlite3.Row] = []
    for r in rows:
        cid = str(r["condition_id"])
        if current_cid is None:
            current_cid = cid
        if cid != current_cid:
            sim = _simulate_market_rows(
                bucket,
                p,
                asks_json_cache=asks_json_cache,
                cap_depth_cache=cap_depth_cache,
            )
            if sim:
                trades.append(sim)
            bucket = [r]
            current_cid = cid
        else:
            bucket.append(r)
    if bucket:
        sim = _simulate_market_rows(
            bucket,
            p,
            asks_json_cache=asks_json_cache,
            cap_depth_cache=cap_depth_cache,
        )
        if sim:
            trades.append(sim)

    summary = {
        "trades": 0,
        "won": 0,
        "lost": 0,
        "open": 0,
        "spent": 0.0,
        "pnl": 0.0,
        "win_rate_pct": 0.0,
        "roi_pct": 0.0,
    }
    by_asset_tf: dict[tuple[str, int], dict] = {}
    for t in trades:
        summary["trades"] += 1
        summary["spent"] += float(t["cost"])
        summary["pnl"] += float(t["pnl"])
        result = t["result"]
        if result == "won":
            summary["won"] += 1
        elif result == "lost":
            summary["lost"] += 1
        else:
            summary["open"] += 1

        key = (str(t["asset"]), int(t["timeframe_seconds"]))
        if key not in by_asset_tf:
            by_asset_tf[key] = {
                "asset": key[0],
                "timeframe_seconds": key[1],
                "timeframe": _SIM_TF_SECONDS_TO_LABEL.get(key[1], str(key[1])),
                "trades": 0,
                "won": 0,
                "lost": 0,
                "open": 0,
                "spent": 0.0,
                "pnl": 0.0,
            }
        b = by_asset_tf[key]
        b["trades"] += 1
        b["spent"] += float(t["cost"])
        b["pnl"] += float(t["pnl"])
        if result == "won":
            b["won"] += 1
        elif result == "lost":
            b["lost"] += 1
        else:
            b["open"] += 1

    decided = summary["won"] + summary["lost"]
    if decided > 0:
        summary["win_rate_pct"] = (summary["won"] / decided) * 100.0
    if summary["spent"] > 0:
        summary["roi_pct"] = (summary["pnl"] / summary["spent"]) * 100.0

    by_rows = sorted(by_asset_tf.values(), key=lambda x: (x["timeframe_seconds"], x["asset"]))
    for b in by_rows:
        d = b["won"] + b["lost"]
        b["win_rate_pct"] = (b["won"] / d * 100.0) if d > 0 else 0.0
        b["roi_pct"] = (b["pnl"] / b["spent"] * 100.0) if b["spent"] > 0 else 0.0

    trades_sorted = sorted(trades, key=lambda x: x.get("entry_ts", ""), reverse=True)
    hourly_et = _hourly_buckets_et_from_trades(trades_sorted)

    return {
        "ok": True,
        "params": p,
        "summary": summary,
        "by_asset_tf": by_rows,
        "trades": trades_sorted[:_SIM_TRADES_UI_LIMIT],
        "hourly_et": hourly_et,
        "hourly_timezone": "America/New_York",
        "trades_returned": min(len(trades_sorted), _SIM_TRADES_UI_LIMIT),
        "trades_total": len(trades_sorted),
        "markets_sampled": len({str(r["condition_id"]) for r in rows}),
        "rows_scanned": len(rows),
        "cutoff_utc": cutoff_iso,
    }


@app.post("/api/monitor/optimize-v2")
async def api_monitor_optimize_v2(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "message": "Invalid JSON"}, status_code=400)
    if not isinstance(body, dict):
        body = {}
    params = _sanitize_sim_payload(body)
    # CPU-heavy optimization must run off the event-loop thread.
    result = await asyncio.to_thread(_optimizer_v2_run_from_params, params, source="monitor_v2")
    return {"ok": True, **(result or {})}


@app.post("/api/monitor/optimize")
async def api_monitor_optimize(request: Request):
    return await api_monitor_optimize_v2(request)


@app.post("/api/monitor/truth")
async def api_monitor_truth(request: Request):
    """
    Backward-compatible monitor route.

    Monitor dashboard backtests are sourced from collected monitor data only
    (monitor_v2_samples), independent from live execution trades/events.
    """
    return await api_monitor_simulate(request)


_SHADOW_TARGET_TFS: tuple[tuple[str, int], ...] = (("5m", 300), ("15m", 900), ("1h", 3600))
_SHADOW_V2_HOURS = 24
_OPT_V2_REQUIRE_DEPTH = True
_OPT_V2_BARRIER_GRID: tuple[float, ...] = (0.08, 0.1, 0.12, 0.15, 0.18, 0.2, 0.22, 0.25, 0.3, 0.35)
_OPT_V2_MAX_PRICE_GRID: tuple[float, ...] = (0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.92, 0.94, 0.96)
_shadow_state_lock = threading.Lock()
_shadow_worker_lock = threading.Lock()
_shadow_run_lock = threading.Lock()
_shadow_worker_thread: threading.Thread | None = None
_shadow_stop_event = threading.Event()
_shadow_optimizer_state: dict = {
    "enabled": True,
    "is_running": False,
    "schedule": "t_minus_60s_each_5m_boundary",
    "last_run_at": "",
    "next_run_at": "",
    "last_duration_ms": 0.0,
    "last_error": "",
    "run_count": 0,
    "result": {},
}


def _shadow_iso(dt: datetime | None) -> str:
    if not dt:
        return ""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _shadow_snapshot() -> dict:
    with _shadow_state_lock:
        return copy.deepcopy(_shadow_optimizer_state)


def _shadow_set_state(**kwargs):
    with _shadow_state_lock:
        _shadow_optimizer_state.update(kwargs)


def _shadow_active_assets() -> list[str]:
    assets = {asset for asset, _, _ in _shadow_active_cells()}
    return [a for a in _SIM_ASSETS if a in assets]


def _shadow_active_timeframes() -> list[tuple[str, int]]:
    tfs = {tf for _, tf, _ in _shadow_active_cells()}
    return [(tf_label, tf_seconds) for tf_label, tf_seconds in _SHADOW_TARGET_TFS if tf_label in tfs]


def _shadow_active_cells() -> list[tuple[str, str, int]]:
    """Return active (asset, tf_label, tf_seconds) cells for shadow optimizer."""
    cfg = _bot_config
    if not cfg:
        return []
    active_assets = set(_parse_scanner_assets(getattr(cfg.scanner, "assets", "")))
    active_tfs = set(_parse_scanner_tfs(getattr(cfg.scanner, "time_frames", "")))
    matrix = cfg.scanner.get_asset_timeframe_enabled_matrix()
    out: list[tuple[str, str, int]] = []
    for tf_label, tf_seconds in _SHADOW_TARGET_TFS:
        if tf_label not in active_tfs:
            continue
        for asset in _SIM_ASSETS:
            if asset not in active_assets:
                continue
            row = matrix.get(asset, {})
            if bool(row.get(tf_label, False)):
                out.append((asset, tf_label, tf_seconds))
    return out


def _shadow_current_barrier(asset: str, tf_label: str, hft: dict) -> float:
    matrix = hft.get("hft_barrier_matrix", {})
    if isinstance(matrix, dict):
        row = matrix.get(asset)
        if isinstance(row, dict):
            try:
                v = float(row.get(tf_label, 0.0))
                if 0.0 <= v <= 5.0:
                    return v
            except Exception:
                pass
    base = float(hft.get("hft_barrier_pct", 0.1) or 0.1)
    if tf_label == "15m":
        b15 = float(hft.get("hft_barrier_pct_15m", 0.0) or 0.0)
        if b15 > 0:
            return b15
    if tf_label == "1h":
        b1h = float(hft.get("hft_barrier_pct_1h", 0.0) or 0.0)
        if b1h > 0:
            return b1h
    return base


def _shadow_current_max_price(asset: str, tf_label: str, hft: dict) -> float:
    matrix = hft.get("hft_max_price_matrix", {})
    if isinstance(matrix, dict):
        row = matrix.get(asset)
        if isinstance(row, dict):
            try:
                v = float(row.get(tf_label, 0.0))
                if 0.01 <= v <= 0.99:
                    return v
            except Exception:
                pass
    base = float(hft.get("hft_max_price", 0.67) or 0.67)
    if tf_label == "5m":
        px5 = float(hft.get("hft_max_price_5m", 0.0) or 0.0)
        if px5 > 0:
            return px5
    if tf_label == "15m":
        px15 = float(hft.get("hft_max_price_15m", 0.0) or 0.0)
        if px15 > 0:
            return px15
    if tf_label == "1h":
        px1h = float(hft.get("hft_max_price_1h", 0.0) or 0.0)
        if px1h > 0:
            return px1h
    return base


def _shadow_barrier_candidates(current: float) -> list[float]:
    vals = {round(float(v), 3) for v in _OPT_V2_BARRIER_GRID}
    vals.add(round(float(current), 3))
    return sorted(vals)


def _shadow_price_candidates(current: float) -> list[float]:
    vals = {round(float(v), 4) for v in _OPT_V2_MAX_PRICE_GRID}
    vals.add(round(float(current), 4))
    return sorted(vals)


def _shadow_build_barriers_for_cell(asset: str, tf_label: str, barrier_value: float) -> dict:
    barriers = _default_sim_barriers()
    if asset in barriers and tf_label in barriers[asset]:
        barriers[asset][tf_label] = float(barrier_value)
    return barriers


def _shadow_build_max_prices_for_cell(asset: str, tf_label: str, cap: float) -> dict:
    max_prices = _default_sim_max_prices()
    if asset in max_prices and tf_label in max_prices[asset]:
        max_prices[asset][tf_label] = float(cap)
    return max_prices


def _shadow_build_sim_params(
    *,
    sim_defaults: dict,
    base_params: dict,
    asset: str,
    tf_label: str,
    barrier_value: float,
    max_price_value: float,
) -> dict:
    """Build a fully normalized simulation context for one optimizer cell."""
    return _sanitize_sim_payload(
        {
            "hours": int(sim_defaults.get("hours", _SHADOW_V2_HOURS)),
            "max_bet": float(base_params["max_bet"]),
            "max_price": float(max_price_value),
            "muzzle_seconds": int(base_params["muzzle_seconds"]),
            "start_delay_seconds": int(base_params["start_delay_seconds"]),
            "armed_sniper": bool(base_params["armed_sniper"]),
            "require_depth": bool(base_params["require_depth"]),
            "assets": [asset],
            "timeframes": [tf_label],
            "asset_timeframe_enabled": {asset: {tf_label: True}},
            "barriers": _shadow_build_barriers_for_cell(asset, tf_label, barrier_value),
            "max_prices": _shadow_build_max_prices_for_cell(asset, tf_label, max_price_value),
        },
        defaults=sim_defaults,
    )


def _shadow_fetch_buckets(
    cutoff_iso: str,
    asset: str,
    tf_seconds: int,
    *,
    start_delay_seconds: int,
    muzzle_seconds: int,
    source: str = "monitor_v2",
) -> tuple[list[list[sqlite3.Row]], int]:
    if str(source or "monitor_v2") != "monitor_v2":
        return [], 0
    with _monitor_read_lock:
        mdb = _get_monitor_db()
        if mdb is None:
            return [], 0
        sql = """
            SELECT
                s.condition_id, s.timestamp, s.asset, s.timeframe_seconds,
                COALESCE(NULLIF(s.event_title, ''), m.event_title) AS event_title,
                s.sec_to_start, s.sec_to_end,
                s.up_ask, s.down_ask, s.up_depth, s.down_depth,
                s.up_asks_json, s.down_asks_json,
                s.gap_pct, s.winning_side,
                COALESCE(s.winner, m.winner) AS winner
            FROM monitor_v2_samples s
            LEFT JOIN monitor_v2_markets m ON m.condition_id = s.condition_id
            WHERE s.timestamp >= ?
              AND s.asset = ?
              AND s.timeframe_seconds = ?
              AND s.sec_to_start <= ?
              AND s.sec_to_end >= ?
            ORDER BY s.condition_id ASC, s.timestamp ASC
        """
        rows = mdb.execute(
            sql,
            [
                cutoff_iso,
                str(asset),
                int(tf_seconds),
                -int(start_delay_seconds),
                int(muzzle_seconds),
            ],
        ).fetchall()

    buckets: list[list[sqlite3.Row]] = []
    current_cid = None
    bucket: list[sqlite3.Row] = []
    for r in rows:
        cid = str(r["condition_id"])
        if current_cid is None:
            current_cid = cid
        if cid != current_cid:
            if bucket:
                buckets.append(bucket)
            bucket = [r]
            current_cid = cid
        else:
            bucket.append(r)
    if bucket:
        buckets.append(bucket)
    return buckets, len(rows)


def _shadow_summarize_buckets(
    buckets: list[list[sqlite3.Row]],
    params: dict,
    *,
    asks_json_cache: dict[str, tuple[tuple[float, float], ...]] | None = None,
    cap_depth_cache: dict[tuple[str, float], float] | None = None,
) -> dict:
    summary = {
        "trades": 0,
        "won": 0,
        "lost": 0,
        "open": 0,
        "spent": 0.0,
        "pnl": 0.0,
        "win_rate_pct": 0.0,
        "roi_pct": 0.0,
    }
    for bucket in buckets:
        t = _simulate_market_rows(
            bucket,
            params,
            asks_json_cache=asks_json_cache,
            cap_depth_cache=cap_depth_cache,
        )
        if not t:
            continue
        summary["trades"] += 1
        summary["spent"] += float(t["cost"])
        summary["pnl"] += float(t["pnl"])
        if t["result"] == "won":
            summary["won"] += 1
        elif t["result"] == "lost":
            summary["lost"] += 1
        else:
            summary["open"] += 1
    decided = summary["won"] + summary["lost"]
    if decided > 0:
        summary["win_rate_pct"] = (summary["won"] / decided) * 100.0
    if summary["spent"] > 0:
        summary["roi_pct"] = (summary["pnl"] / summary["spent"]) * 100.0
    return summary


def _shadow_rank(summary: dict, objective_mode: str) -> tuple[float, float, float, int]:
    pnl = float(summary.get("pnl", 0.0) or 0.0)
    roi = float(summary.get("roi_pct", 0.0) or 0.0)
    win_rate = float(summary.get("win_rate_pct", 0.0) or 0.0)
    trades = int(summary.get("trades", 0) or 0)
    mode = _sanitize_optimizer_objective(objective_mode, "pnl")
    if mode == "win_rate":
        return (win_rate, float(trades), roi, pnl)
    if mode == "roi":
        return (roi, pnl, win_rate, trades)
    return (pnl, roi, win_rate, trades)


def _shadow_pick_best_candidate(runs: list[dict], *, objective_mode: str, min_trades: int) -> tuple[dict, bool]:
    if not runs:
        return {}, False
    floor = max(0, int(min_trades or 0))
    eligible = (
        [
            run
            for run in runs
            if int(((run.get("summary") or {}).get("trades", 0) or 0)) >= floor
        ]
        if floor > 0
        else list(runs)
    )
    if eligible:
        return max(eligible, key=lambda x: _shadow_rank((x.get("summary") or {}), objective_mode)), True
    return max(runs, key=lambda x: _shadow_rank((x.get("summary") or {}), objective_mode)), False


def _shadow_pick_cell(
    *,
    sim_defaults: dict,
    asset: str,
    tf_label: str,
    tf_seconds: int,
    base_params: dict,
    current_barrier: float,
    current_max_price: float,
    buckets: list[list[sqlite3.Row]],
    rows_scanned: int,
    asks_json_cache: dict[str, tuple[tuple[float, float], ...]] | None = None,
    cap_depth_cache: dict[tuple[str, float], float] | None = None,
) -> dict:
    objective_mode = _sanitize_optimizer_objective(base_params.get("objective_mode", "pnl"), "pnl")
    min_trades = max(0, int(base_params.get("min_trades", 0) or 0))
    current_params = _shadow_build_sim_params(
        sim_defaults=sim_defaults,
        base_params=base_params,
        asset=asset,
        tf_label=tf_label,
        barrier_value=current_barrier,
        max_price_value=current_max_price,
    )
    current_summary = _shadow_summarize_buckets(
        buckets,
        current_params,
        asks_json_cache=asks_json_cache,
        cap_depth_cache=cap_depth_cache,
    )

    if not buckets:
        return {
            "asset": asset,
            "timeframe": tf_label,
            "timeframe_seconds": tf_seconds,
            "markets_sampled": 0,
            "rows_scanned": rows_scanned,
            "current": {
                "barrier_pct": current_barrier,
                "max_price": current_max_price,
                "summary": current_summary,
            },
            "recommended": {
                "barrier_pct": current_barrier,
                "max_price": current_max_price,
                "summary": current_summary,
            },
            "delta_pnl": 0.0,
        }

    barrier_grid = _shadow_barrier_candidates(current_barrier)
    price_grid = _shadow_price_candidates(current_max_price)
    # Core invariant: recommendation search is driven by objective/lookback/min-trades
    # and is not anchored to current settings.
    # Keep two-stage search for runtime cost control:
    #  1) pick barrier at a neutral broad cap (highest candidate cap),
    #  2) pick max price for that barrier.
    search_cap = float(max(price_grid)) if price_grid else float(current_max_price)

    barrier_runs: list[dict] = []
    for b in barrier_grid:
        p = _shadow_build_sim_params(
            sim_defaults=sim_defaults,
            base_params=base_params,
            asset=asset,
            tf_label=tf_label,
            barrier_value=float(b),
            max_price_value=search_cap,
        )
        barrier_runs.append(
            {
                "barrier_pct": float(b),
                "summary": _shadow_summarize_buckets(
                    buckets,
                    p,
                    asks_json_cache=asks_json_cache,
                    cap_depth_cache=cap_depth_cache,
                ),
            }
        )

    best_barrier, best_barrier_meets_floor = _shadow_pick_best_candidate(
        barrier_runs,
        objective_mode=objective_mode,
        min_trades=min_trades,
    )
    if not best_barrier:
        best_barrier = {
            "barrier_pct": float(current_barrier),
            "summary": current_summary,
        }
        best_barrier_meets_floor = int(current_summary.get("trades", 0) or 0) >= min_trades

    price_runs: list[dict] = []
    for px in price_grid:
        p = _shadow_build_sim_params(
            sim_defaults=sim_defaults,
            base_params=base_params,
            asset=asset,
            tf_label=tf_label,
            barrier_value=float(best_barrier["barrier_pct"]),
            max_price_value=float(px),
        )
        price_runs.append(
            {
                "max_price": float(px),
                "summary": _shadow_summarize_buckets(
                    buckets,
                    p,
                    asks_json_cache=asks_json_cache,
                    cap_depth_cache=cap_depth_cache,
                ),
            }
        )

    best_price, best_price_meets_floor = _shadow_pick_best_candidate(
        price_runs,
        objective_mode=objective_mode,
        min_trades=min_trades,
    )
    if not best_price:
        best_price = {
            "max_price": float(current_max_price),
            "summary": current_summary,
        }
        best_price_meets_floor = int(current_summary.get("trades", 0) or 0) >= min_trades

    recommended_summary = best_price.get("summary") or {}
    meets_floor = bool(best_barrier_meets_floor and best_price_meets_floor)

    return {
        "asset": asset,
        "timeframe": tf_label,
        "timeframe_seconds": tf_seconds,
        "markets_sampled": len(buckets),
        "rows_scanned": rows_scanned,
        "current": {
            "barrier_pct": current_barrier,
            "max_price": current_max_price,
            "summary": current_summary,
        },
        "recommended": {
            "barrier_pct": float(best_barrier["barrier_pct"]),
            "max_price": float(best_price["max_price"]),
            "summary": recommended_summary,
            "min_trades_floor": int(min_trades),
            "meets_min_trades": meets_floor,
        },
        "barrier_meets_min_trades": bool(best_barrier_meets_floor),
        "delta_pnl": float(recommended_summary.get("pnl", 0.0) or 0.0)
        - float(current_summary.get("pnl", 0.0) or 0.0),
    }


def _optimizer_v2_run_from_params(params: dict, *, source: str = "monitor_v2") -> dict:
    p = _sanitize_sim_payload(params if isinstance(params, dict) else {})
    active_cells = _sim_active_cells(p)
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=int(p["hours"]))).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Keep defaults aligned with caller payload so per-cell candidate params only vary
    # in the dimensions being optimized (barrier/max-price).
    sim_defaults = _sim_defaults_dict()
    sim_defaults.update(
        {
            "hours": int(p["hours"]),
            "objective_mode": _sanitize_optimizer_objective(p.get("objective_mode", "pnl"), "pnl"),
            "min_trades": int(p["min_trades"]),
            "max_bet": float(p["max_bet"]),
            "max_price": float(p["max_price"]),
            "muzzle_seconds": int(p["muzzle_seconds"]),
            "start_delay_seconds": int(p["start_delay_seconds"]),
            "armed_sniper": bool(p["armed_sniper"]),
            "require_depth": bool(p["require_depth"]),
            "assets": list(p["assets"]),
            "timeframes": list(p["tf_labels"]),
            "asset_timeframe_enabled": copy.deepcopy(p["asset_timeframe_enabled"]),
            "barriers": copy.deepcopy(p["barriers"]),
            "max_prices": copy.deepcopy(p["max_prices"]),
        }
    )
    base_params = {
        "objective_mode": _sanitize_optimizer_objective(p.get("objective_mode", "pnl"), "pnl"),
        "min_trades": int(p["min_trades"]),
        "max_bet": float(p["max_bet"]),
        "muzzle_seconds": int(p["muzzle_seconds"]),
        "start_delay_seconds": int(p["start_delay_seconds"]),
        "armed_sniper": bool(p["armed_sniper"]),
        "require_depth": bool(p["require_depth"]),
    }

    asks_json_cache: dict[str, tuple[tuple[float, float], ...]] = {}
    cap_depth_cache: dict[tuple[str, float], float] = {}
    cell_results: list[dict] = []
    for asset, tf_label, tf_seconds in active_cells:
        buckets, rows_scanned = _shadow_fetch_buckets(
            cutoff_iso,
            asset,
            tf_seconds,
            start_delay_seconds=int(base_params["start_delay_seconds"]),
            muzzle_seconds=int(base_params["muzzle_seconds"]),
            source=source,
        )
        current_barrier = _barrier_for(p["barriers"], asset, tf_seconds)
        current_max_price = _max_price_for(p["max_prices"], float(p["max_price"]), asset, tf_seconds)
        cell_results.append(
            _shadow_pick_cell(
                sim_defaults=sim_defaults,
                asset=asset,
                tf_label=tf_label,
                tf_seconds=tf_seconds,
                base_params=base_params,
                current_barrier=float(current_barrier),
                current_max_price=float(current_max_price),
                buckets=buckets,
                rows_scanned=rows_scanned,
                asks_json_cache=asks_json_cache,
                cap_depth_cache=cap_depth_cache,
            )
        )

    cell_results = sorted(cell_results, key=lambda x: (int(x.get("timeframe_seconds", 0)), str(x.get("asset", ""))))
    total_current_pnl = sum(float((r.get("current", {}).get("summary", {}) or {}).get("pnl", 0.0) or 0.0) for r in cell_results)
    total_reco_pnl = sum(float((r.get("recommended", {}).get("summary", {}) or {}).get("pnl", 0.0) or 0.0) for r in cell_results)
    total_current_trades = sum(
        int((r.get("current", {}).get("summary", {}) or {}).get("trades", 0) or 0) for r in cell_results
    )
    total_reco_trades = sum(
        int((r.get("recommended", {}).get("summary", {}) or {}).get("trades", 0) or 0) for r in cell_results
    )
    total_rows = sum(int(r.get("rows_scanned", 0) or 0) for r in cell_results)
    total_markets = sum(int(r.get("markets_sampled", 0) or 0) for r in cell_results)
    delta_pnl = float(total_reco_pnl - total_current_pnl)

    return {
        "generated_at": _shadow_iso(datetime.now(timezone.utc)),
        "source": str(source or "monitor_v2"),
        "hours_window": int(p["hours"]),
        "objective_mode": _sanitize_optimizer_objective(p.get("objective_mode", "pnl"), "pnl"),
        "min_trades": int(p["min_trades"]),
        "assets": list(p["assets"]),
        "timeframes": list(p["tf_labels"]),
        "cells_optimized": len(cell_results),
        "current_config": {
            "objective_mode": _sanitize_optimizer_objective(p.get("objective_mode", "pnl"), "pnl"),
            "min_trades": int(p["min_trades"]),
            "max_bet": float(p["max_bet"]),
            "max_price": float(p["max_price"]),
            "muzzle_seconds": int(p["muzzle_seconds"]),
            "start_delay_seconds": int(p["start_delay_seconds"]),
            "armed_sniper": bool(p["armed_sniper"]),
            "require_depth": bool(p["require_depth"]),
            "barrier_matrix": copy.deepcopy(p["barriers"]),
            "max_price_matrix": copy.deepcopy(p["max_prices"]),
        },
        "totals": {
            "rows_scanned": total_rows,
            "markets_sampled": total_markets,
            "cells_optimized": len(cell_results),
            "current": {"trades": total_current_trades, "pnl": total_current_pnl},
            "recommended": {"trades": total_reco_trades, "pnl": total_reco_pnl},
            "delta_pnl": delta_pnl,
        },
        "results": cell_results,
    }


def _shadow_main_optimizer_params() -> dict:
    hft = _hft_settings_snapshot()
    if not hft:
        raise RuntimeError("HFT settings unavailable")

    enabled_cells = [f"{asset}:{tf_label}" for asset, tf_label, _ in _shadow_active_cells()]
    defaults = _sim_defaults_dict()
    params = _sanitize_sim_payload(
        {
            "hours": int(hft.get("hft_optimizer_lookback_hours", defaults.get("hours", _SHADOW_V2_HOURS))),
            "objective_mode": _sanitize_optimizer_objective(
                hft.get("hft_optimizer_objective_mode", defaults.get("objective_mode", "pnl")),
                defaults.get("objective_mode", "pnl"),
            ),
            "min_trades": int(hft.get("hft_optimizer_min_trades", defaults.get("min_trades", 0))),
            "max_bet": float(hft.get("hft_bet_amount", defaults.get("max_bet", 1.0))),
            "max_price": float(hft.get("hft_max_price", defaults.get("max_price", 0.67))),
            "muzzle_seconds": int(hft.get("hft_muzzle_seconds", defaults.get("muzzle_seconds", 90))),
            "start_delay_seconds": int(hft.get("hft_barrier_delay", defaults.get("start_delay_seconds", 10))),
            "armed_sniper": _coerce_bool(hft.get("hft_armed_sniper_enabled", defaults.get("armed_sniper", False))),
            "require_depth": bool(_OPT_V2_REQUIRE_DEPTH),
            "enabled_cells": enabled_cells,
            "barriers": copy.deepcopy(hft.get("hft_barrier_matrix", {})),
            "max_prices": copy.deepcopy(hft.get("hft_max_price_matrix", {})),
        },
        defaults=defaults,
    )
    return params


def _optimizer_v2_state_hash(params: dict) -> str:
    payload = {
        "hours": params.get("hours", 24),
        "objective_mode": _sanitize_optimizer_objective(params.get("objective_mode", "pnl"), "pnl"),
        "min_trades": int(params.get("min_trades", 0) or 0),
        "assets": params.get("assets", []),
        "timeframes": params.get("tf_labels", []),
        "asset_timeframe_enabled": params.get("asset_timeframe_enabled", {}),
        "barriers": params.get("barriers", {}),
        "max_prices": params.get("max_prices", {}),
        "max_bet": params.get("max_bet", 0.0),
        "muzzle_seconds": params.get("muzzle_seconds", 0),
        "start_delay_seconds": params.get("start_delay_seconds", 0),
    }
    raw = _json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _run_shadow_optimizer_once(trigger: str):
    if not _shadow_run_lock.acquire(blocking=False):
        return
    started_at = time.perf_counter()
    start_dt = datetime.now(timezone.utc)
    _shadow_set_state(is_running=True, last_error="")
    try:
        params = _shadow_main_optimizer_params()
        result = _optimizer_v2_run_from_params(params, source="monitor_v2")
        result["generated_at"] = _shadow_iso(start_dt)
        result["trigger"] = str(trigger or "")
        result["settings_hash"] = _optimizer_v2_state_hash(params)
        with _shadow_state_lock:
            _shadow_optimizer_state["result"] = result
            _shadow_optimizer_state["last_run_at"] = _shadow_iso(start_dt)
            _shadow_optimizer_state["last_duration_ms"] = round((time.perf_counter() - started_at) * 1000.0, 1)
            _shadow_optimizer_state["last_error"] = ""
            _shadow_optimizer_state["run_count"] = int(_shadow_optimizer_state.get("run_count", 0) or 0) + 1
    except Exception as exc:
        _shadow_set_state(last_error=str(exc))
        logger.debug("[SHADOW] optimizer run failed: %s", exc)
    finally:
        _shadow_set_state(is_running=False)
        _shadow_run_lock.release()


def _next_shadow_run_utc(now: datetime) -> datetime:
    now = now.astimezone(timezone.utc)
    base = now.replace(second=0, microsecond=0)
    mins_to_next_boundary = 5 - (now.minute % 5)
    if mins_to_next_boundary == 0:
        mins_to_next_boundary = 5
    boundary = base + timedelta(minutes=mins_to_next_boundary)
    run_at = boundary - timedelta(minutes=1)
    if run_at < now:
        run_at += timedelta(minutes=5)
    return run_at


def _shadow_worker_loop():
    logger.info("[SHADOW] optimizer worker started")
    _run_shadow_optimizer_once("startup")
    while not _shadow_stop_event.is_set():
        now = datetime.now(timezone.utc)
        run_at = _next_shadow_run_utc(now)
        _shadow_set_state(next_run_at=_shadow_iso(run_at))
        wait_s = max(0.0, (run_at - now).total_seconds())
        if _shadow_stop_event.wait(wait_s):
            break
        if _shadow_stop_event.is_set():
            break
        _run_shadow_optimizer_once("scheduled")
    logger.info("[SHADOW] optimizer worker stopped")


def _ensure_shadow_optimizer_running():
    global _shadow_worker_thread
    if _bot_controls.get("request_stop"):
        return
    with _shadow_worker_lock:
        if _shadow_worker_thread and _shadow_worker_thread.is_alive():
            return
        _shadow_stop_event.clear()
        _shadow_worker_thread = threading.Thread(
            target=_shadow_worker_loop,
            name="shadow-optimizer-worker",
            daemon=True,
        )
        _shadow_worker_thread.start()


def _trigger_shadow_optimizer_v2_run(trigger: str):
    if _bot_controls.get("request_stop"):
        return
    _ensure_shadow_optimizer_running()

    def _kick():
        _run_shadow_optimizer_once(str(trigger or "manual"))

    safe = str(trigger or "manual").replace(" ", "_")
    threading.Thread(target=_kick, name=f"shadow-optimizer-v2-{safe}", daemon=True).start()


@app.get("/api/shadow-optimizer-v2/latest")
async def api_shadow_optimizer_v2_latest():
    _ensure_shadow_optimizer_running()
    return {"ok": True, **_shadow_snapshot()}


@app.post("/api/shadow-optimizer-v2/run")
async def api_shadow_optimizer_v2_run():
    _trigger_shadow_optimizer_v2_run("manual")
    return {"ok": True, "queued": True}


@app.get("/api/shadow-optimizer/latest")
async def api_shadow_optimizer_latest():
    return await api_shadow_optimizer_v2_latest()


@app.post("/api/shadow-optimizer/run")
async def api_shadow_optimizer_run():
    return await api_shadow_optimizer_v2_run()


@app.get("/api/hft/positions")
async def api_hft_positions():
    rows = _query("SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC")
    return {"ok": True, "positions": rows}


@app.post("/api/hft/market/dump")
async def api_hft_market_dump(request: Request):
    return await api_market_dump(request)


@app.get("/api/hft/log")
async def api_hft_log(limit: int = 300, noisy: bool = False):
    log_path = _active_log_path()
    if not log_path.exists():
        return {"ok": True, "path": str(log_path), "lines": []}
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if not noisy:
            lines = _filter_hft_log_lines(lines)
        lim = max(50, min(2000, int(limit)))
        return {"ok": True, "path": str(log_path), "lines": lines[-lim:]}
    except Exception as e:
        return {"ok": False, "message": str(e)}


@app.post("/api/hft/pause")
async def api_hft_pause():
    return await api_pause()


@app.post("/api/hft/resume")
async def api_hft_resume():
    return await api_resume()


@app.post("/api/hft/stop")
async def api_hft_stop():
    return await api_stop()


@app.get("/api/mode2/latency")
async def api_mode2_latency():
    """Latest Mode2 latency samples for dashboard diagnostics."""
    snap = get_mode2_metrics_snapshot()
    return {"ok": True, **snap}


@app.get("/api/positions")
async def api_positions():
    """Open positions."""
    rows = _query("SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC")
    return {"ok": True, "positions": rows}


@app.post("/api/market/dump")
async def api_market_dump(request: Request):
    """Sell an open position entirely via FOK."""
    if not _bot_scanner or not hasattr(_bot_scanner, 'api'):
        return JSONResponse({"error": "API not initialized"}, status_code=500)
    api = _bot_scanner.api
    body = await request.json()
    pos_id = body.get("position_id")
    if not pos_id:
        return JSONResponse({"error": "Missing position_id"}, status_code=400)
    rows = _query("SELECT * FROM positions WHERE id = ?", (pos_id,))
    if not rows:
        return JSONResponse({"error": "Position not found"}, status_code=404)
    p = rows[0]
    if p["status"] != "open":
        return JSONResponse({"error": "Position not open"}, status_code=400)
    condition_id = p["condition_id"]
    token_id = p["token_id"]
    shares_db = float(p.get("shares", 0) or 0)
    if shares_db <= 0:
        return JSONResponse({"error": "No shares"}, status_code=400)

    # Clamp to wallet truth when available to avoid "not enough balance" dumps.
    shares = shares_db
    wallet = getattr(_bot_config, "wallet_address", "") if _bot_config else ""
    wallet = (wallet or "").strip()
    if wallet:
        try:
            chain_positions = await api.get_positions(wallet)
            chain_shares = None
            for cp in chain_positions:
                if str(cp.get("conditionId", "")) != str(condition_id):
                    continue
                cp_token = str(cp.get("tokenId") or cp.get("asset") or "")
                if cp_token and cp_token != str(token_id):
                    continue
                chain_shares = float(cp.get("size", 0) or 0)
                break
            if chain_shares is not None:
                shares = min(shares_db, chain_shares)
        except Exception as e:
            logger.debug("Dump chain-share clamp failed for %s: %s", condition_id[:16], e)

    if shares <= 0:
        return JSONResponse({"error": "No sellable wallet shares for this position"}, status_code=400)

    # Grab liquidity to execute right now
    book = await api.get_book_liquidity(token_id)
    best_bid = float(book.get("best_bid", 0) or 0)
    if best_bid <= 0:
        return JSONResponse({"error": "No bids on book, cannot FOK dump"}, status_code=400)

    # CLOB min notional is $1.00, so ensure limit price keeps order valid.
    min_price_for_size = 1.01 / shares if shares > 0 else 0.01
    price = max(min_price_for_size, best_bid * 0.95)
    price = min(0.99, max(0.01, price))

    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, api.place_fok_sell, token_id, price, shares)
        if not isinstance(result, dict):
            return JSONResponse(
                {"error": "FOK dump returned empty/invalid response"},
                status_code=502,
            )

        err = result.get("errorMsg") or result.get("error") or ""
        status = str(result.get("status", "")).lower()
        order_id = result.get("orderID") or result.get("order_id")
        if err:
            return JSONResponse({"error": str(err)}, status_code=400)
        if status not in {"matched"} or not order_id:
            return JSONResponse(
                {"error": f"FOK not filled (status={status or 'unknown'})"},
                status_code=400,
            )

        # Pull exact fill summary when available for logging.
        final = {}
        try:
            final = await loop.run_in_executor(None, api.get_order_status, order_id)
        except Exception:
            final = {}
        if not isinstance(final, dict):
            final = {}

        sold_shares = float(
            final.get("size_matched")
            or final.get("sizeMatched")
            or result.get("size_matched")
            or result.get("sizeMatched")
            or shares
        )
        sold_price = float(
            final.get("avg_price")
            or final.get("avgPrice")
            or final.get("price")
            or result.get("price")
            or price
        )

        _execute(
            "UPDATE positions SET status = 'sold', result = 'sold', closed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = ?",
            (pos_id,),
        )
        _execute(
            "UPDATE trades SET result = 'sold' WHERE condition_id = ? AND token_id = ?",
            (condition_id, token_id),
        )
        _execute(
            "INSERT INTO logs (level, message) VALUES ('INFO', ?)",
            (f"[DUMP] FOK sell {condition_id[:16]}: {sold_shares:.6f} @ ${sold_price:.4f} | {order_id[:16]}",),
        )

        return JSONResponse(
            {
                "ok": True,
                "result": {
                    "order_id": order_id,
                    "status": status,
                    "shares": sold_shares,
                    "avg_price": sold_price,
                },
            }
        )
    except Exception as e:
        if "not enough balance" in str(e).lower() or "allowance" in str(e).lower():
            return JSONResponse({"error": str(e)}, status_code=400)
        logger.error("Dump failed: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/positions/all")
async def api_all_positions():
    """All positions (open + closed)."""
    rows = _query("SELECT * FROM positions ORDER BY id DESC LIMIT 100")
    return {"ok": True, "positions": rows}


def _recent_trades_window(hours: int = 48) -> tuple[int, str, list[dict]]:
    try:
        hours = int(hours)
    except Exception:
        hours = 48
    hours = max(1, min(hours, 168))
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = _query(
        "SELECT * FROM trades WHERE timestamp >= ? ORDER BY timestamp DESC",
        (cutoff,),
    )
    return hours, cutoff, rows


@app.get("/api/trades")
async def api_trades(hours: int = 48):
    """Deprecated: use /api/trades/recent."""
    hours, cutoff, rows = _recent_trades_window(hours)
    return {
        "ok": True,
        "deprecated": True,
        "message": "Use /api/trades/recent?hours=<n>.",
        "hours": hours,
        "cutoff": cutoff,
        "trades": rows,
    }


@app.get("/api/trades/recent")
async def api_recent_trades(hours: int = 48):
    """Trades inside a trailing time window (default: last 48 hours)."""
    hours, cutoff, rows = _recent_trades_window(hours)
    return {"ok": True, "hours": hours, "cutoff": cutoff, "trades": rows}


@app.get("/api/snapshots")
async def api_snapshots(asset: str = "", limit: int = 500):
    """Snipe-window snapshots for settings-tuning analysis."""
    if asset:
        rows = _query(
            "SELECT * FROM snapshots WHERE asset = ? ORDER BY id DESC LIMIT ?",
            (asset.lower(), limit),
        )
    else:
        rows = _query("SELECT * FROM snapshots ORDER BY id DESC LIMIT ?", (limit,))
    return {"ok": True, "snapshots": rows}


@app.get("/api/snapshot-stats")
async def api_snapshot_stats():
    """Aggregate snapshot statistics for tuning analysis."""
    row = _query("""
        SELECT
            COUNT(*) as total_snapshots,
            COUNT(CASE WHEN actual_winner IS NOT NULL THEN 1 END) as resolved,
            COUNT(CASE WHEN would_have_won = 1 THEN 1 END) as correct_side,
            COUNT(CASE WHEN passed_all = 1 THEN 1 END) as passed_all_gates,
            COUNT(CASE WHEN passed_all = 1 AND would_have_won = 1 THEN 1 END) as passed_and_won,
            AVG(best_mid) as avg_best_mid,
            AVG(CASE WHEN best_ask > 0 THEN best_ask END) as avg_best_ask,
            AVG(best_depth) as avg_best_depth,
            AVG(CASE WHEN edge > 0 THEN edge END) as avg_edge,
            AVG(gap_pct) as avg_gap_pct,
            AVG(seconds_to_end) as avg_seconds_to_end,
            MIN(seconds_to_end) as min_seconds_to_end,
            MAX(gap_pct) as max_gap_pct
        FROM snapshots
    """)
    stats = row[0] if row else {}

    # Per-asset breakdown
    per_asset = _query("""
        SELECT asset,
            COUNT(*) as total,
            COUNT(CASE WHEN actual_winner IS NOT NULL THEN 1 END) as resolved,
            COUNT(CASE WHEN would_have_won = 1 THEN 1 END) as correct_side,
            AVG(gap_pct) as avg_gap,
            MAX(gap_pct) as max_gap,
            AVG(best_mid) as avg_mid,
            AVG(best_depth) as avg_depth
        FROM snapshots
        GROUP BY asset
    """)

    return {"ok": True, "stats": stats, "per_asset": per_asset}


# -- Legacy monitor/scenario APIs removed in HFT-only build --
_MONITOR_DB_PATH = PROJECT_ROOT / "data" / "monitor.db"
_monitor_read_lock = threading.Lock()
_monitor_read_db: sqlite3.Connection | None = None
_monitor_schema_ready = False


def _get_monitor_db() -> sqlite3.Connection | None:
    global _monitor_read_db, _monitor_schema_ready
    if not _MONITOR_DB_PATH.exists():
        return None
    if _monitor_read_db is None:
        _monitor_read_db = sqlite3.connect(str(_MONITOR_DB_PATH), check_same_thread=False)
        _monitor_read_db.row_factory = sqlite3.Row
        _monitor_read_db.execute("PRAGMA journal_mode=WAL")
        _monitor_read_db.execute("PRAGMA busy_timeout=5000")
    if not _monitor_schema_ready:
        # Backward-compatible migration for replay depth-at-cap support.
        for col_sql in (
            "up_asks_json TEXT NOT NULL DEFAULT ''",
            "down_asks_json TEXT NOT NULL DEFAULT ''",
        ):
            try:
                _monitor_read_db.execute(f"ALTER TABLE monitor_v2_samples ADD COLUMN {col_sql}")
            except Exception:
                pass
        try:
            _monitor_read_db.commit()
        except Exception:
            pass
        _monitor_schema_ready = True
    return _monitor_read_db


@app.get("/api/scenarios/analyze")
async def api_scenarios_analyze():
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)


@app.get("/api/scenarios/parameter-sweep")
async def api_parameter_sweep():
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)


@app.get("/api/monitor/ticks")
async def api_monitor_ticks():
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)


@app.get("/api/monitor/stats")
async def api_monitor_stats():
    return JSONResponse({"ok": False, "message": "Disabled in HFT-only build"}, status_code=404)

# -- Market slug resolution (for Polymarket links) --

_slug_cache: dict[str, str] = {}  # condition_id -> event_slug
_slug_retried: set[str] = set()   # CIDs already retried


def _resolve_event_slug(cid: str) -> str:
    """Resolve condition_id -> event_slug via CLOB + Gamma APIs.

    Mirrors the copytrader's approach: CLOB -> token_id -> Gamma -> event slug.
    """
    try:
        url = f"https://clob.polymarket.com/markets/{cid}"
        req = urllib.request.Request(url, headers={
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 PolymarketSniper/1.0',
        })
        with urllib.request.urlopen(req, timeout=5) as resp:
            clob_data = _json.loads(resp.read())

        tokens = clob_data.get('tokens', [])
        if not tokens:
            return clob_data.get('market_slug', '')

        token_id = tokens[0].get('token_id', '')
        if not token_id:
            return clob_data.get('market_slug', '')

        url2 = f"https://gamma-api.polymarket.com/markets?clob_token_ids={token_id}"
        req2 = urllib.request.Request(url2, headers={
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 PolymarketSniper/1.0',
        })
        with urllib.request.urlopen(req2, timeout=5) as resp2:
            gamma_data = _json.loads(resp2.read())

        if gamma_data and isinstance(gamma_data, list) and len(gamma_data) > 0:
            events = gamma_data[0].get('events', [])
            if events and isinstance(events, list) and len(events) > 0:
                event_slug = events[0].get('slug', '')
                if event_slug:
                    return event_slug

        return clob_data.get('market_slug', '')
    except Exception:
        return ''


@app.get("/api/market-slugs")
async def api_market_slugs():
    """Return {condition_id: event_slug} mapping for Polymarket links.

    Sources (in priority order):
      1. DB event_slug column (populated on insert for new trades)
      2. Live scanner tracked markets (always have slugs from Gamma)
      3. CLOB + Gamma API fallback (for old trades without slugs)
    """
    # Collect all condition_ids we need slugs for
    rows = _query(
        "SELECT DISTINCT condition_id, event_slug FROM positions "
        "UNION SELECT DISTINCT condition_id, event_slug FROM trades"
    )
    result: dict[str, str] = {}
    need_resolve: list[str] = []

    for r in rows:
        cid = r.get('condition_id', '')
        if not cid:
            continue
        slug = r.get('event_slug', '') or ''
        if slug:
            result[cid] = slug
            _slug_cache[cid] = slug
        elif cid in _slug_cache and _slug_cache[cid]:
            result[cid] = _slug_cache[cid]
        else:
            need_resolve.append(cid)

    # Check live scanner for slugs
    if _bot_scanner and need_resolve:
        still_need = []
        for cid in need_resolve:
            for m in _bot_scanner.tracked_markets:
                if m.condition_id == cid and m.event_slug:
                    result[cid] = m.event_slug
                    _slug_cache[cid] = m.event_slug
                    break
            else:
                still_need.append(cid)
        need_resolve = still_need

    # Resolve remaining via API (max 5 per call to avoid slowdown)
    to_resolve = [
        cid for cid in need_resolve[:5]
        if cid not in _slug_retried
    ]
    if to_resolve:
        import asyncio as _aio
        results = await _aio.gather(
            *(_aio.to_thread(_resolve_event_slug, cid) for cid in to_resolve),
            return_exceptions=True,
        )
        for cid, slug in zip(to_resolve, results):
            if isinstance(slug, Exception):
                slug = ''
            _slug_cache[cid] = slug
            if slug:
                result[cid] = slug
            else:
                _slug_retried.add(cid)

    return result


@app.get("/api/stats")
async def api_stats():
    """Aggregated stats."""
    trades = _query("""
        SELECT COUNT(*) as total_trades,
               SUM(CASE WHEN dry_run = 0 THEN 1 ELSE 0 END) as live_trades,
               SUM(CASE WHEN dry_run = 0 THEN amount ELSE 0 END) as total_spent
        FROM trades
    """)
    positions = _query("""
        SELECT COUNT(*) as open_pos, SUM(total_cost) as cost
        FROM positions WHERE status = 'open'
    """)
    redeemed = _query("""
        SELECT COUNT(*) as count, SUM(pnl) as pnl
        FROM positions WHERE status IN ('redeemed', 'sold')
    """)
    return {
        "ok": True,
        "trades": trades[0] if trades else {},
        "positions": positions[0] if positions else {},
        "redeemed": redeemed[0] if redeemed else {},
    }


@app.get("/api/log")
async def api_log():
    """Recent log lines."""
    log_path = _active_log_path()
    if not log_path.exists():
        return {"ok": True, "lines": []}
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        return {"ok": True, "path": str(log_path), "lines": lines[-1000:]}
    except Exception as e:
        return {"ok": False, "message": str(e)}


@app.get("/api/db-logs")
async def api_db_logs():
    """Fetch logs from the DB logs table."""
    try:
        logs = _query("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 200")
        return {"ok": True, "logs": logs}
    except Exception as e:
        return {"ok": False, "message": str(e)}


# -- SSE live stream --

def _build_sse_payload() -> dict:
    """Assemble all dashboard data into a single dict for SSE push."""
    _ensure_shadow_optimizer_running()
    # Status
    status = {**_bot_state}
    status["controls"] = {"paused": _bot_controls.get("paused", True)}
    status["wallet_address"] = _wallet_address()
    status["wallet_private_key"] = _wallet_private_key()

    # Positions
    positions = _query("SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC")

    # PnL stats (all-time + current session)
    pnl_row = _query("""
        SELECT COALESCE(SUM(pnl), 0) as total_pnl
        FROM trades WHERE dry_run = 0 AND pnl IS NOT NULL
    """)
    total_pnl = pnl_row[0]["total_pnl"] if pnl_row else 0.0
    session_pnl = 0.0
    started_at_raw = str(status.get("started_at") or "").strip()
    if started_at_raw:
        try:
            started_at = datetime.fromisoformat(started_at_raw.replace("Z", "+00:00"))
            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)
            started_at_utc = started_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            session_row = _query(
                "SELECT COALESCE(SUM(pnl), 0) as session_pnl "
                "FROM trades WHERE dry_run = 0 AND pnl IS NOT NULL AND timestamp >= ?",
                (started_at_utc,),
            )
            session_pnl = session_row[0]["session_pnl"] if session_row else 0.0
        except Exception:
            session_pnl = 0.0

    # Log (last 1000 lines)
    log_lines = []
    log_path = _active_log_path()
    if log_path.exists():
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
            log_lines = text.splitlines()[-1000:]
        except Exception:
            pass

    # Slugs (cached + DB, no external API calls)
    slugs = dict(_slug_cache)
    slug_rows = _query(
        "SELECT DISTINCT condition_id, event_slug FROM positions "
        "WHERE event_slug IS NOT NULL AND event_slug != '' "
        "UNION SELECT DISTINCT condition_id, event_slug FROM trades "
        "WHERE event_slug IS NOT NULL AND event_slug != ''"
    )
    for r in slug_rows:
        cid = r.get("condition_id", "")
        slug = r.get("event_slug", "")
        if cid and slug:
            slugs[cid] = slug

    # Active assets (for toggle sync)
    cfg = _bot_config
    active_assets = cfg.scanner.assets if cfg else ""

    # Oracle health (Chainlink + Binance status)
    oracle_health = None
    if _bot_price_feed:
        try:
            oracle_health = _bot_price_feed.get_oracle_health()
        except Exception:
            pass

    return {
        "status": status,
        "positions": positions,
        "total_pnl": total_pnl,
        "session_pnl": session_pnl,
        "log": log_lines,
        "log_path": str(log_path),
        "slugs": slugs,
        "assets": active_assets,
        "time_frames": cfg.scanner.time_frames if cfg else "5m,15m,1h,4h,1d",
        "asset_timeframe_enabled": cfg.scanner.get_asset_timeframe_enabled_matrix() if cfg else _default_scanner_asset_tf_matrix(),
        "price_feed": _normalize_price_feed_source(getattr(cfg.scanner, "price_feed", "binance"), "binance") if cfg else "binance",
        "verbose_log": _bot_controls.get("verbose_log", False),
        "oracle_health": oracle_health,
        "wallet_balance": _get_cached_balance(),
        "wallet_pol_balance": _get_cached_pol_balance(),
        "wallet_symbol": "USDC.e",
        "wallet_address": _wallet_address(),
        "wallet_private_key": _wallet_private_key(),
        "env_exists": _env_file_exists(),
        "env_setup_complete": _env_setup_complete(),
        "econ_start_balance": _bot_controls.get("econ_start_balance", 0.0),
        "econ_hwm": _bot_controls.get("econ_hwm", 0.0),
        "economic_pause_drawdown": _bot_config.execution.economic_pause_drawdown if _bot_config else 0.0,
        "economic_profit_target": _bot_config.execution.economic_profit_target if _bot_config else 0.0,
        "shadow_optimizer": _shadow_snapshot(),
    }


@app.get("/api/stream")
async def api_stream(request: Request):
    """Server-Sent Events stream; pushes all dashboard data every ~1.5s."""
    async def generate():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = _build_sse_payload()
                    yield f"data: {_json.dumps(payload)}\n\n"
                except Exception as exc:
                    logger.debug("SSE payload error: %s", exc)
                await asyncio.sleep(1.5)
        except asyncio.CancelledError:
            return

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/pause")
async def api_pause():
    _bot_controls["paused"] = True
    return {"ok": True, "paused": True}


@app.post("/api/resume")
async def api_resume():
    _bot_controls["paused"] = False
    _bot_controls["econ_pause_acked"] = True
    # Reset high-water mark to current balance (fresh protection window)
    bal = _get_cached_balance()
    if bal is not None and bal > 0:
        _bot_controls["econ_hwm"] = bal

    return {"ok": True, "paused": False}


@app.post("/api/control/restart")
async def api_control_restart():
    """Restart the current runtime process in-place."""
    queued = _schedule_runtime_restart(reason="api_control_restart")
    if queued:
        return {"ok": True, "message": "Runtime restart queued"}
    return {"ok": True, "message": "Runtime restart already in progress"}


@app.post("/api/verbose")
async def api_toggle_verbose():
    """Toggle verbose logging mode."""
    current = _bot_controls.get("verbose_log", False)
    _bot_controls["verbose_log"] = not current
    # Update scanner's verbose flag in real-time
    if _bot_scanner is not None:
        _bot_scanner.verbose = not current
    mode = "ON" if not current else "OFF"
    logger.info("Verbose logging %s", mode)
    return {"ok": True, "verbose": not current}


@app.post("/api/stop")
async def api_stop():
    _bot_controls["request_stop"] = True
    return {"ok": True, "message": "Stop requested"}


@app.get("/api/settings")
async def api_get_settings():
    """Get runtime-adjustable core scanner settings (HFT-only dashboard)."""
    cfg = _bot_config
    cleanup_delay = _bot_scanner.ws_cleanup_delay if _bot_scanner else 60
    return {
        "ok": True,
        # Scanner core
        "scan_interval": float(cfg.scanner.interval_seconds) if cfg else 10.0,
        "max_events": int(cfg.scanner.max_events_per_query) if cfg else 100,
        "assets": str(cfg.scanner.assets) if cfg else "",
        "time_frames": str(cfg.scanner.time_frames) if cfg else "5m,15m,1h,4h,1d",
        "asset_timeframe_enabled": cfg.scanner.get_asset_timeframe_enabled_matrix() if cfg else _default_scanner_asset_tf_matrix(),
        # WS housekeeping
        "ws_cleanup_delay": cleanup_delay,
        # PTB/risk
        "ptb_capture_offset": float(cfg.scanner.ptb_capture_offset) if cfg else 2.0,
        "price_feed": _normalize_price_feed_source(getattr(cfg.scanner, "price_feed", "binance"), "binance") if cfg else "binance",
        "economic_pause_drawdown": float(cfg.execution.economic_pause_drawdown) if cfg else 0.0,
        "economic_profit_target": float(cfg.execution.economic_profit_target) if cfg else 0.0,
    }


@app.post("/api/settings")
async def api_update_settings(request: Request):
    """Update scanner core settings used by the HFT dashboard."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "message": "Invalid JSON"})
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"ok": False, "message": "Invalid payload"})

    cfg = _bot_config
    if not cfg:
        return {"ok": False, "message": "Config not initialized"}

    updated = []
    scanner_gate_changed = False

    # Scanner settings
    if "scan_interval" in body:
        val = max(0.1, float(body["scan_interval"]))
        cfg.scanner.interval_seconds = val
        updated.append(f"scan_interval={val}s")

    if "max_events" in body:
        val = max(10, min(200, int(body["max_events"])))
        cfg.scanner.max_events_per_query = val
        updated.append(f"max_events={val}")

    # WS
    if "ws_cleanup_delay" in body:
        val = max(10, int(body["ws_cleanup_delay"]))
        if _bot_scanner:
            _bot_scanner.ws_cleanup_delay = val
            updated.append(f"ws_cleanup_delay={val}s")

    if "ptb_capture_offset" in body:
        val = round(max(0.0, min(10.0, float(body["ptb_capture_offset"]))), 1)
        cfg.scanner.ptb_capture_offset = val
        updated.append(f"ptb_capture_offset={val}s")

    if "price_feed" in body:
        val = _normalize_price_feed_source(body.get("price_feed"), getattr(cfg.scanner, "price_feed", "binance"))
        cfg.scanner.price_feed = val
        updated.append(f"price_feed={val}")
        if _bot_price_feed is not None:
            try:
                _bot_price_feed.set_primary_source(val)
            except Exception as exc:
                logger.warning("Failed to hot-apply price_feed=%s: %s", val, exc)

    # Economic pause (drawdown guard)
    if "economic_pause_drawdown" in body:
        val = round(max(0.0, float(body["economic_pause_drawdown"])), 2)
        cfg.execution.economic_pause_drawdown = val
        label = f"${val}" if val > 0 else "OFF"
        updated.append(f"economic_pause_drawdown={label}")
    if "economic_profit_target" in body:
        val = round(max(0.0, float(body["economic_profit_target"])), 2)
        cfg.execution.economic_profit_target = val
        label = f"${val}" if val > 0 else "OFF"
        updated.append(f"economic_profit_target={label}")

    # Asset toggles -- update config string AND scanner._assets set for immediate effect
    if "assets" in body:
        raw = str(body["assets"]).strip().lower()
        parsed = _parse_scanner_assets(raw)
        val = ",".join(parsed)
        cfg.scanner.assets = val
        updated.append(f"assets={val}")
        scanner_gate_changed = True

    # Timeframe toggles -- update config string AND scanner._time_frames set
    if "time_frames" in body:
        raw = str(body["time_frames"]).strip().lower()
        parsed = _parse_scanner_tfs(raw)
        val = ",".join(parsed)
        cfg.scanner.time_frames = val
        updated.append(f"time_frames={val}")
        scanner_gate_changed = True
    if "asset_timeframe_enabled" in body:
        matrix = _sanitize_scanner_toggle_matrix(
            body["asset_timeframe_enabled"],
            _all_false_scanner_asset_tf_matrix(),
        )
        cfg.scanner.asset_timeframe_enabled = matrix
        updated.append("asset_timeframe_enabled=updated")
        scanner_gate_changed = True

    if updated:
        if scanner_gate_changed:
            _apply_scanner_runtime_toggles()
            logger.info("Scanner gates hot-updated: assets=%s tfs=%s", cfg.scanner.assets, cfg.scanner.time_frames)
        logger.info("Dashboard settings changed: %s", ", ".join(updated))
        _persist_to_yaml()
        _trigger_shadow_optimizer_v2_run("settings_change")

    return {"ok": True, "message": "Updated: " + ", ".join(updated) if updated else "No changes"}


@app.post("/api/control/flush-all-data")
async def api_flush_all_data():
    """Wipe HFT runtime data: sniper DB and runtime logs (monitor DB excluded)."""
    try:
        deleted_trades = 0
        deleted_positions = 0
        deleted_snapshots = 0
        deleted_hft_events = 0
        with _read_lock:
            db = _get_read_db()
            deleted_trades = db.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            deleted_positions = db.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
            deleted_snapshots = db.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            try:
                deleted_hft_events = db.execute("SELECT COUNT(*) FROM hft_events").fetchone()[0]
            except Exception:
                deleted_hft_events = 0
            db.execute("DELETE FROM trades")
            db.execute("DELETE FROM positions")
            db.execute("DELETE FROM snapshots")
            try:
                db.execute("DELETE FROM hft_events")
            except Exception:
                pass
            db.execute("DELETE FROM bot_state")
            db.execute("DELETE FROM sqlite_sequence")
            db.commit()
            db.execute("VACUUM")

        log_paths = {
            _active_log_path(),
            PROJECT_ROOT / "logs" / "mode2.log",
            PROJECT_ROOT / "logs" / "sniper.log",
        }
        truncated_logs = 0
        for lp in log_paths:
            try:
                lp.parent.mkdir(parents=True, exist_ok=True)
                lp.write_text("", encoding="utf-8")
                truncated_logs += 1
            except Exception:
                pass

        reset_mode2_metrics()

        # Reset in-memory counters
        _bot_state["trades"] = 0
        _bot_state["dry_runs"] = 0
        _bot_state["total_spent"] = 0.0
        _bot_state["redeemed"] = 0
        _bot_state["redeemed_pnl"] = 0.0
        _bot_state["opportunities"] = 0
        msg = (
            f"{deleted_trades} trades, {deleted_positions} positions, {deleted_snapshots} snapshots, "
            f"{deleted_hft_events} hft events removed; {truncated_logs} log file(s) truncated; "
            "monitor DB untouched"
        )
        logger.info("HFT cleanup via dashboard: %s", msg)
        return {"ok": True, "message": msg}
    except Exception as e:
        logger.error("HFT cleanup failed: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})


@app.post("/api/control/flush-monitor")
async def api_flush_monitor():
    """Wipe monitor data tables from monitor.db."""
    try:
        with _monitor_read_lock:
            db = _get_monitor_db()
            if db is None:
                return {"ok": False, "message": "monitor.db not found"}
            deleted_ticks = 0
            deleted_samples = 0
            deleted_markets = 0
            try:
                deleted_ticks = db.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
                db.execute("DELETE FROM ticks")
            except Exception:
                deleted_ticks = 0
            try:
                deleted_samples = db.execute("SELECT COUNT(*) FROM monitor_v2_samples").fetchone()[0]
                deleted_markets = db.execute("SELECT COUNT(*) FROM monitor_v2_markets").fetchone()[0]
                db.execute("DELETE FROM monitor_v2_samples")
                db.execute("DELETE FROM monitor_v2_markets")
            except Exception:
                deleted_samples = 0
                deleted_markets = 0
            db.execute("DELETE FROM sqlite_sequence")
            db.commit()
            db.execute("VACUUM")
        msg = (
            f"{deleted_ticks} monitor ticks deleted; "
            f"{deleted_samples} monitor samples deleted; "
            f"{deleted_markets} monitor markets deleted"
        )
        logger.info("Monitor DB wiped via dashboard: %s", msg)
        return {"ok": True, "message": msg}
    except Exception as e:
        logger.error("Monitor DB wipe failed: %s", e)
        return JSONResponse(status_code=500, content={"ok": False, "message": str(e)})


@app.post("/api/shutdown")
async def api_shutdown():
    """Hard shutdown request.

    Graceful-first: ask runtime loop to stop cleanly.
    Force fallback: if process is still alive after timeout, exit hard.
    """
    global _shutdown_watchdog_started
    _bot_controls["request_stop"] = True
    set_bot_state(running=False)
    _shadow_stop_event.set()

    # Watchdog fallback protects against rare hangs during graceful shutdown.
    with _shutdown_lock:
        if not _shutdown_watchdog_started:
            _shutdown_watchdog_started = True

            def _force_kill_watchdog():
                time.sleep(10.0)
                try:
                    logger.warning("Shutdown watchdog forcing process exit")
                except Exception:
                    pass
                os._exit(0)

            t = threading.Thread(target=_force_kill_watchdog, daemon=True)
            t.start()

    logger.info("Shutdown requested via dashboard (graceful-first)")
    return {"ok": True, "message": "Sniper shutting down"}


# -- Clock Blackout API --

@app.get("/api/blackout")
async def get_blackout():
    cfg = _bot_config.blackout
    return {
        "5m": {
            "all": sorted(cfg.blocked_slots(300, "all")),
            "am": sorted(cfg.blocked_slots(300, "am")),
            "pm": sorted(cfg.blocked_slots(300, "pm")),
            "hourly": cfg.blocked_slots_5m_hourly(),
        },
        "15m": {
            "all": sorted(cfg.blocked_slots(900, "all")),
            "am": sorted(cfg.blocked_slots(900, "am")),
            "pm": sorted(cfg.blocked_slots(900, "pm")),
        },
        "1h": {
            "all": sorted(cfg.blocked_slots(3600, "all")),
            "am": sorted(cfg.blocked_slots(3600, "am")),
            "pm": sorted(cfg.blocked_slots(3600, "pm")),
        },
        "4h": {
            "all": sorted(cfg.blocked_slots(14400, "all")),
            "am": sorted(cfg.blocked_slots(14400, "am")),
            "pm": sorted(cfg.blocked_slots(14400, "pm")),
        },
        "1d": {
            "all": sorted(cfg.blocked_slots(86400, "all")),
            "am": sorted(cfg.blocked_slots(86400, "am")),
            "pm": sorted(cfg.blocked_slots(86400, "pm")),
        },
    }


@app.post("/api/blackout")
async def set_blackout(req: Request):
    body = await req.json()
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"ok": False, "message": "Invalid payload"})
    cfg = _bot_config.blackout
    _TF_MAP = {"5m": "slots_5m", "15m": "slots_15m", "1h": "slots_1h",
               "4h": "slots_4h", "1d": "slots_1d"}
    _TF_RULES = {
        "5m": (5, 55),
        "15m": (15, 45),
        "1h": (60, 0),
        "4h": (60, 0),
        "1d": (60, 0),
    }

    def _sanitize_slots(values, tf: str) -> list[int]:
        if not isinstance(values, list):
            return []
        step, max_slot = _TF_RULES.get(tf, (5, 55))
        clean: set[int] = set()
        for raw in values:
            try:
                slot = int(raw)
            except Exception:
                continue
            if slot < 0 or slot > max_slot:
                continue
            if slot % step != 0:
                continue
            clean.add(slot)
        return sorted(clean)

    changed = []
    for tf, base_field in _TF_MAP.items():
        if tf not in body:
            continue
        raw = body.get(tf)
        # Backward-compatible payload: { "5m": [0,5] }
        if isinstance(raw, list):
            slots = _sanitize_slots(raw, tf)
            csv = ",".join(str(s) for s in slots)
            setattr(cfg, base_field, csv)
            changed.append(f"{tf}.all=[{csv}]")
            continue
        # New payload: { "5m": { "all": [...], "am": [...], "pm": [...] } }
        if isinstance(raw, dict):
            for sess, suffix in (("all", ""), ("am", "_am"), ("pm", "_pm")):
                if sess not in raw or not isinstance(raw.get(sess), list):
                    continue
                slots = _sanitize_slots(raw[sess], tf)
                csv = ",".join(str(s) for s in slots)
                setattr(cfg, base_field + suffix, csv)
                changed.append(f"{tf}.{sess}=[{csv}]")
            # Optional 5m per-hour overrides:
            # {"5m":{"hourly":{"10":{"am":[0,5],"pm":[10]}}}}
            if tf == "5m" and "hourly" in raw:
                hourly_raw = raw.get("hourly")
                clean_hourly: dict[str, dict[str, list[int]]] = {}
                if isinstance(hourly_raw, dict):
                    for hour_12 in range(1, 13):
                        row = hourly_raw.get(str(hour_12), hourly_raw.get(hour_12))
                        if not isinstance(row, dict):
                            continue
                        am_slots = _sanitize_slots(row.get("am"), "5m")
                        pm_slots = _sanitize_slots(row.get("pm"), "5m")
                        if am_slots or pm_slots:
                            clean_hourly[str(hour_12)] = {"am": am_slots, "pm": pm_slots}
                encoded = _json.dumps(clean_hourly, separators=(",", ":"), sort_keys=True) if clean_hourly else ""
                setattr(cfg, "slots_5m_hourly", encoded)
                changed.append(f"5m.hourly={len(clean_hourly)}h")
    if changed:
        _persist_to_yaml()
        logger.info("Blackout slots updated: %s", ", ".join(changed))
    return {"ok": True}


@app.get("/api/blackout/stats")
async def blackout_stats(hours: int = 0):
    """Historical win/loss stats per slot, bucketed by timeframe and end-minute."""
    import re as _re
    window_hours = max(0, min(8760, int(hours or 0)))
    if window_hours > 0:
        rows = _query(
            "SELECT event_title, event_slug, result, pnl, timeframe_seconds "
            "FROM trades "
            "WHERE result IN ('won','lost') "
            "AND REPLACE(timestamp, 'T', ' ') >= datetime('now', ?)",
            (f"-{window_hours} hours",),
        )
    else:
        rows = _query(
            "SELECT event_title, event_slug, result, pnl, timeframe_seconds "
            "FROM trades WHERE result IN ('won','lost')"
        )
    # {
    #   "5m": { 0: {"all":{...}, "am":{...}, "pm":{...}}, ... },
    #   "5m_hourly": { "10": { 0: {"all":...,"am":...,"pm":...}, ... } },
    #   "15m": ...
    # }
    stats = {"5m_hourly": {}}

    def _bump(bucket: dict, result: str, pnl: float):
        if result == "won":
            bucket["wins"] += 1
        else:
            bucket["losses"] += 1
        bucket["net"] = round(bucket["net"] + pnl, 2)

    for r in rows:
        slug = r["event_slug"] or ""
        title = r["event_title"] or ""
        tf_seconds = int(r.get("timeframe_seconds") or 0)
        if tf_seconds == 300:
            tf = "5m"
        elif tf_seconds == 900:
            tf = "15m"
        elif tf_seconds == 3600:
            tf = "1h"
        elif tf_seconds == 14400:
            tf = "4h"
        elif tf_seconds == 86400:
            tf = "1d"
        else:
            # Backward compatibility for older rows without timeframe_seconds.
            tf_match = _re.search(r'-(\d+[mhd])-', slug)
            if not tf_match:
                continue
            tf = tf_match.group(1)
        # Extract end time from title.
        # Supports both "...9:55PM-10:00PM ET" and "...9AM-10AM ET".
        time_match = _re.search(r'-(\d{1,2})(?::(\d{2}))?(AM|PM)\s*ET', title)
        if not time_match:
            continue
        end_hour_12 = int(time_match.group(1) or 0)
        if end_hour_12 < 1 or end_hour_12 > 12:
            continue
        minute = int(time_match.group(2) or 0)
        ampm = str(time_match.group(3)).upper()
        # We just need the minute-of-hour
        slot = (minute // 5) * 5 if tf == "5m" else (minute // 15) * 15 if tf == "15m" else 0
        stats.setdefault(tf, {})
        bucket = stats[tf].setdefault(
            slot,
            {
                "all": {"wins": 0, "losses": 0, "net": 0.0},
                "am": {"wins": 0, "losses": 0, "net": 0.0},
                "pm": {"wins": 0, "losses": 0, "net": 0.0},
            },
        )
        pnl = r["pnl"] or 0.0
        _bump(bucket["all"], r["result"], pnl)
        _bump(bucket["am" if ampm == "AM" else "pm"], r["result"], pnl)
        if tf == "5m":
            hour_key = str(end_hour_12)
            stats["5m_hourly"].setdefault(hour_key, {})
            hour_bucket = stats["5m_hourly"][hour_key].setdefault(
                slot,
                {
                    "all": {"wins": 0, "losses": 0, "net": 0.0},
                    "am": {"wins": 0, "losses": 0, "net": 0.0},
                    "pm": {"wins": 0, "losses": 0, "net": 0.0},
                },
            )
            _bump(hour_bucket["all"], r["result"], pnl)
            _bump(hour_bucket["am" if ampm == "AM" else "pm"], r["result"], pnl)
    return stats


# -- Dashboard startup --

async def start_dashboard(port: int = 8898):
    """Start the dashboard as a background async task."""
    global _dashboard_server, _dashboard_server_task, _dashboard_sock
    if _dashboard_server_task is not None and not _dashboard_server_task.done():
        return

    _ensure_shadow_optimizer_running()
    import socket as _socket
    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))
    sock.listen(8)
    sock.setblocking(False)

    config = uvicorn.Config(
        app, host="127.0.0.1", port=port,
        log_level="warning", access_log=False,
    )
    server = uvicorn.Server(config)
    server.config.setup_event_loop = lambda: None

    loop = asyncio.get_event_loop()

    async def _serve():
        try:
            config.load()
            server.lifespan = config.lifespan_class(config)
            await server.startup(sockets=[sock])
            while server.started and not server.should_exit:
                await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            try:
                if server.started:
                    await server.shutdown(sockets=[sock])
            except Exception:
                pass
            raise
        finally:
            try:
                sock.close()
            except Exception:
                pass

    _dashboard_server = server
    _dashboard_sock = sock
    _dashboard_server_task = loop.create_task(_serve())


async def stop_dashboard():
    """Gracefully stop background dashboard server started by start_dashboard()."""
    global _dashboard_server, _dashboard_server_task, _dashboard_sock
    server = _dashboard_server
    task = _dashboard_server_task
    sock = _dashboard_sock
    _dashboard_server = None
    _dashboard_server_task = None
    _dashboard_sock = None
    if server is None and task is None:
        return
    try:
        if server is not None:
            server.should_exit = True
            if getattr(server, "started", False):
                try:
                    await asyncio.wait_for(
                        server.shutdown(sockets=[sock] if sock is not None else None),
                        timeout=_async_shutdown_timeout_s,
                    )
                except asyncio.TimeoutError:
                    logger.debug("Dashboard shutdown timed out; continuing teardown")
    except Exception:
        pass
    try:
        if task is not None and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(task, return_exceptions=True),
                    timeout=_async_shutdown_timeout_s,
                )
            except asyncio.TimeoutError:
                logger.debug("Dashboard task cancel timed out; continuing teardown")
    except Exception:
        pass
    try:
        if sock is not None:
            sock.close()
    except Exception:
        pass


