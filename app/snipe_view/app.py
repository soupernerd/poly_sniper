"""Snipe-View -- READ-ONLY public dashboard.

Standalone viewer backend for PolySnipe. Serves snipe_view/index.html on port 8900.
Zero POST routes, zero bot controls, zero config writes.

Key responsibilities:
  - SSE stream (/api/stream): Pushes full state every ~1.5s.
    Payload: status (from shared _bot_state dict), positions, trades, total_pnl,
    log (last 1000 lines), slugs, visitors (active count + IPs), wallet_balance,
    wallet_symbol.
  - Wallet balance: Fetched via Etherscan v2 API (Polygon USDC.e), cached 30s.
  - Visitor tracking: Counts active SSE connections and exposes connected IPs.
  - PIN auth: Simple PIN gate via signed cookie (HMAC). No sessions, no tokens.

Database: data/sniper.db (trades + positions tables, SELECT only).
Frontend: snipe_view/index.html (read-only viewer with PIN gate).

Related: dashboard/app.py is the full-control operator dashboard on port 8898.
"""

import asyncio
import hashlib
import hmac
import json as _json
import logging
import os
import sqlite3
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import re

import uvicorn
from fastapi import FastAPI, Request, HTTPException, Cookie
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

logger = logging.getLogger(__name__)

# -- PIN Auth --
VIEWER_PIN = "1337"
# Derive a token from the PIN so the cookie value isn't the raw PIN
_PIN_TOKEN = hashlib.sha256(f"snipe-view-{VIEWER_PIN}".encode()).hexdigest()[:32]


def _verify_pin_cookie(request: Request) -> None:
    """Check the session cookie. Raises 401 if missing/invalid."""
    token = request.cookies.get("sv_auth")
    if not token or not hmac.compare_digest(token, _PIN_TOKEN):
        raise HTTPException(status_code=401, detail="Not authenticated")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "sniper.db"
_view_server = None
_view_server_task = None
_view_sock = None

# -- Active viewer tracking (SSE connections) --
_viewer_lock = threading.Lock()
_viewer_sessions: dict[int, str] = {}   # {session_id: ip_address}
_viewer_next_id: int = 0


def _extract_ip(request: Request) -> str:
    """Get the real client IP, respecting Cloudflare / proxy headers."""
    for hdr in ("CF-Connecting-IP", "X-Real-IP"):
        val = request.headers.get(hdr)
        if val:
            return val.strip()
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _viewer_connect(request: Request) -> int:
    """Register a new SSE viewer. Returns a session id for later disconnect."""
    global _viewer_next_id
    ip = _extract_ip(request)
    with _viewer_lock:
        sid = _viewer_next_id
        _viewer_next_id += 1
        _viewer_sessions[sid] = ip
    return sid


def _viewer_disconnect(sid: int) -> None:
    """Remove a viewer session."""
    with _viewer_lock:
        _viewer_sessions.pop(sid, None)


def _get_viewer_count() -> dict:
    """Return active viewer count + connected IPs."""
    with _viewer_lock:
        ips = list(_viewer_sessions.values())
    return {"active": len(ips), "ips": ips}

ETHERSCAN_API_KEY = str(os.getenv("POLYGONSCAN_API_KEY", "") or "").strip()
BALANCE_ADDRESS = str(os.getenv("POLYMARKET_WALLET_ADDRESS", "") or "").strip()
USDC_TOKEN_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_CHAIN_ID = 137
BALANCE_CACHE_SECONDS = 30

_balance_cache: float | None = None
_balance_cache_ts = 0.0

view_app = FastAPI(title="Snipe-View", docs_url=None, redoc_url=None)


@view_app.exception_handler(Exception)
async def _exc(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"ok": False, "message": str(exc)})


# ── Shared bot state (set by main.py, read-only here) ──
_bot_state: dict = {}
_view_scanner = None


def set_view_state(state: dict):
    """Called by main.py to share the bot state reference."""
    global _bot_state
    _bot_state = state


def set_view_scanner(scanner):
    global _view_scanner
    _view_scanner = scanner


# ── Read-only DB ──
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
    with _read_lock:
        try:
            db = _get_read_db()
            cursor = db.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
        except Exception:
            return []


# ── Slug cache (condition_id -> event_slug) ──
_slug_cache: dict[str, str] = {}


def _load_slugs() -> dict[str, str]:
    rows = _query(
        "SELECT DISTINCT condition_id, event_slug FROM positions "
        "WHERE event_slug IS NOT NULL AND event_slug != '' "
        "UNION SELECT DISTINCT condition_id, event_slug FROM trades "
        "WHERE event_slug IS NOT NULL AND event_slug != ''"
    )
    for r in rows:
        cid = r.get("condition_id", "")
        slug = r.get("event_slug", "")
        if cid and slug:
            _slug_cache[cid] = slug
    return dict(_slug_cache)


def _fetch_usdc_balance() -> float | None:
    """Fetch USDC.e balance on Polygon via Etherscan v2 API (read-only)."""
    if not ETHERSCAN_API_KEY or not BALANCE_ADDRESS:
        return None
    url = (
        f"https://api.etherscan.io/v2/api?chainid={POLYGON_CHAIN_ID}"
        "&module=account&action=tokenbalance"
        f"&contractaddress={USDC_TOKEN_ADDRESS}"
        f"&address={BALANCE_ADDRESS}"
        "&tag=latest"
        f"&apikey={ETHERSCAN_API_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Snipe-View/1.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = _json.loads(resp.read().decode("utf-8"))
        if data.get("status") != "1":
            return None
        raw = float(data.get("result", 0))
        return raw / 1e6
    except Exception as exc:
        logger.debug("Polygonscan balance fetch failed: %s", exc)
        return None


def _get_cached_balance() -> float | None:
    global _balance_cache, _balance_cache_ts
    now = time.monotonic()
    if (now - _balance_cache_ts) < BALANCE_CACHE_SECONDS:
        return _balance_cache
    bal = _fetch_usdc_balance()
    if bal is not None:
        _balance_cache = bal
        _balance_cache_ts = now
    return _balance_cache


# ── Routes (ALL read-only) ──

@view_app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Serve the dashboard HTML (PIN gate is client-side)."""
    html_path = Path(__file__).resolve().parent / "index.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>Snipe-View</h1><p>index.html not found</p>")


@view_app.post("/api/auth")
async def api_auth(request: Request):
    """Validate PIN and set session cookie."""
    try:
        body = await request.json()
        pin = str(body.get("pin", "")).strip()
    except Exception:
        pin = ""
    if not hmac.compare_digest(pin, VIEWER_PIN):
        raise HTTPException(status_code=401, detail="Wrong PIN")
    resp = JSONResponse({"ok": True})
    resp.set_cookie(
        "sv_auth", _PIN_TOKEN,
        httponly=True, samesite="lax", max_age=86400 * 30,  # 30 days
    )
    return resp


@view_app.get("/api/auth/check")
async def api_auth_check(request: Request):
    """Lightweight cookie validation (no SSE, no side-effects)."""
    _verify_pin_cookie(request)
    return JSONResponse({"ok": True})


def _build_payload() -> dict:
    """Assemble read-only dashboard data."""
    # Positions
    positions = _query("SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC")

    # Trades + PnL
    trades = _query("SELECT * FROM trades ORDER BY id DESC LIMIT 1000")
    pnl_row = _query("""
        SELECT COALESCE(SUM(pnl), 0) as total_pnl
        FROM trades WHERE dry_run = 0 AND pnl IS NOT NULL
    """)
    total_pnl = pnl_row[0]["total_pnl"] if pnl_row else 0.0

    # Log (last 1000 lines)
    log_lines: list[str] = []
    log_path = PROJECT_ROOT / "logs" / "sniper.log"
    if log_path.exists():
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace")
            log_lines = text.splitlines()[-1000:]
        except Exception:
            pass

    slugs = _load_slugs()
    wallet_balance = _get_cached_balance()

    # -- Sanitize logs for public viewer --
    # Remove lines that leak strategy details (gap %, thresholds, settings, oracle gate)
    _SENSITIVE_PATTERNS = [
        r"\bBINANCE GATE\b",
        r"\bPRICE GATE\b",
        r"\bORACLE DISAGREES\b",
        r"\bgap=\d",
        r"\bprice_to_beat\b",
        r"\bprice_gap_overrides\b",
        r"Dashboard settings changed",
        r"\bwinner_threshold\b",
        r"\bpre_end_threshold\b",
        r"\bmin_edge\b",
        r"\bpre_end_seconds\b",
        r"\bGAP PRE-CHECK\b",
        r"\bGAP-OBS\b",
        r"(?:Binance|Chainlink|Oracle).*disagrees",
        r"gap=.*above|gap=.*below",
        r"Max buy:.*Max bet:",
        r"Price feed:.*Gap:",
        r"\[V\]",               # Verbose-mode lines (strategy internals)
        r"\[SCAN\].*(?:mid|ask|edge|threshold|depth|floor|max \$)",  # Gate detail lines
        r"Verbose logging",     # Toggle notifications
    ]
    _sensitive_re = re.compile("|".join(_SENSITIVE_PATTERNS), re.IGNORECASE)

    def _redact_line(line: str) -> str | None:
        """Return None to drop the line, or the line with secrets scrubbed."""
        if _sensitive_re.search(line):
            return None
        # Scrub trailing gap info from SNIPING/SNIPED lines (e.g. "| chainlink bitcoin=... gap=0.1% above")
        line = re.sub(r"\s*\|\s*(?:BINANCE|chainlink|binance)\s+\S+=\$[\d.]+\s+gap=[\d.]+%\s+\w+\s*$", "", line, flags=re.IGNORECASE)
        return line

    log_lines = [r for line in log_lines if (r := _redact_line(line)) is not None]

    return {
        "status": dict(_bot_state) if _bot_state else {"running": False, "mode": "UNKNOWN"},
        "positions": positions,
        "trades": trades,
        "total_pnl": total_pnl,
        "log": log_lines,
        "slugs": slugs,
        "wallet_balance": wallet_balance,
        "wallet_symbol": "USDC.e",
        "wallet_address": BALANCE_ADDRESS,
        "visitors": _get_viewer_count(),
    }


@view_app.get("/api/stream")
async def api_stream(request: Request):
    """SSE stream -- pushes read-only dashboard data every 2s."""
    _verify_pin_cookie(request)
    sid = _viewer_connect(request)
    async def generate():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = _build_payload()
                    yield f"data: {_json.dumps(payload)}\n\n"
                except Exception:
                    pass
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass
        finally:
            _viewer_disconnect(sid)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── Startup ──

async def start_snipe_view(port: int = 8900):
    """Launch the read-only viewer as a background async task."""
    global _view_server, _view_server_task, _view_sock
    if _view_server_task is not None and not _view_server_task.done():
        return

    import socket as _socket
    sock = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
    sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", port))
    sock.listen(8)
    sock.setblocking(False)

    config = uvicorn.Config(
        view_app, host="0.0.0.0", port=port,
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

    _view_server = server
    _view_sock = sock
    _view_server_task = loop.create_task(_serve())


async def stop_snipe_view():
    """Gracefully stop background Snipe-View server started by start_snipe_view()."""
    global _view_server, _view_server_task, _view_sock
    server = _view_server
    task = _view_server_task
    sock = _view_sock
    _view_server = None
    _view_server_task = None
    _view_sock = None
    if server is None and task is None:
        return
    try:
        if server is not None:
            server.should_exit = True
            if getattr(server, "started", False):
                await server.shutdown(sockets=[sock] if sock is not None else None)
    except Exception:
        pass
    try:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    except Exception:
        pass
    try:
        if sock is not None:
            sock.close()
    except Exception:
        pass
