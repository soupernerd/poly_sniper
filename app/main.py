"""HFT-only runtime entrypoint.

Runs only HFT barrier-breaker flow:
  - price-feed PTB gap trigger
  - immediate_fok or watch_best_entry arm/spring
  - HFT executor fast path

Keeps dashboard/DB compatibility with main runtime.
"""

import asyncio
import logging
import re
import signal
import sys
import time
from datetime import datetime, timezone

from src.api import PolymarketAPI
from src.config import PROJECT_ROOT, load_config
from src.database import Database
from src.executor import Executor
from src.hft_log import format_market_label, short_cid
from src.market_runtime import MarketRuntime
from src.monitor_db import MonitorDB
from src.monitor_worker import MonitorWorker
from src.price_feed import ASSET_SYMBOL_MAP, BinancePriceFeed
from src.redeemer import Redeemer
from src.scanner import Scanner
from src.ws_client import MarketWSClient
from dashboard.app import (
    _bot_state as _dashboard_bot_state,
    get_bot_controls,
    set_bot_config,
    set_bot_db,
    set_bot_monitor_worker,
    set_bot_price_feed,
    set_bot_scanner,
    set_bot_state,
    stop_dashboard,
    start_dashboard,
)
from snipe_view.app import set_view_scanner, set_view_state, start_snipe_view, stop_snipe_view
from runtime_common import (
    DASHBOARD_PORT,
    SNIPE_VIEW_PORT,
    _acquire_lock,
    _release_lock,
    _truncate_log,
    setup_logging,
)

logger = logging.getLogger("sniper")


async def main():
    config = load_config()
    # Enforce HFT-only behavior for this runtime.
    config.trend.enabled = False
    config.trend.hft_autobet_enabled = False
    config.trend.hft_flash_enabled = False
    config.offload.sell_price = 0.0
    config.offload.sell_price_t1 = 0.0
    config.offload.sell_price_t2 = 0.0
    config.offload.sell_price_t3 = 0.0
    config.offload.sell_price_t4 = 0.0
    config.offload.pct_offload = 0
    setattr(config, "hft_mode2_only_runtime", True)
    # Keep HFT runtime logs isolated from the full-runtime log stream.
    config.logging.file = "logs/mode2.log"
    setup_logging(config)

    logger.info("=" * 60)
    logger.info("  Polymarket Sniper starting -- HFT ONLY")
    logger.info("  Wallet: %s...", config.wallet_address[:10] if config.wallet_address else "NONE")
    logger.info("  Assets: %s", config.scanner.assets)
    logger.info("  HFT Entry: %s | Armed fallback: %s",
                (config.trend.hft_mode2_entry_mode or "legacy-toggle"),
                config.trend.hft_armed_sniper_enabled)
    logger.info("  HFT Trap monitor: ws_push lifecycle")
    logger.info(
        "  Mode profile: Mode1=OFF, Mode3=OFF, TrendStrategy=OFF, LegacyOffloaders=OFF"
    )
    logger.info(
        "  Barrier: enabled=%s 5m=%.3f 15m=%.3f 1h=%.3f delay=%ss | Max price 5m=%.4f 15m=%.4f 1h=%.4f",
        config.trend.hft_barrier_enabled,
        config.trend.get_hft_barrier_pct(300),
        config.trend.get_hft_barrier_pct(900),
        config.trend.get_hft_barrier_pct(3600),
        config.trend.hft_barrier_delay,
        config.trend.get_hft_max_price(300),
        config.trend.get_hft_max_price(900),
        config.trend.get_hft_max_price(3600),
    )
    logger.info("  Dashboard: http://127.0.0.1:%d", DASHBOARD_PORT)
    logger.info("=" * 60)

    _acquire_lock()

    api = PolymarketAPI(config)
    db = Database(config)
    ws = MarketWSClient()
    monitor_db = MonitorDB()

    # Oracle collection is runtime-wide and independent from scanner trade toggles.
    # Keep feed scope stable so PTB data is collected consistently for all core assets.
    price_feed_assets = list(ASSET_SYMBOL_MAP.keys())
    price_feed = BinancePriceFeed(assets=price_feed_assets)
    market_runtime = MarketRuntime(
        config=config,
        api=api,
        ws=ws,
        price_feed=price_feed,
    )
    monitor_worker = MonitorWorker(
        config=config,
        api=api,
        ws=ws,
        price_feed=price_feed,
        monitor_db=monitor_db,
        market_runtime=market_runtime,
    )
    scanner = Scanner(
        config,
        api,
        ws,
        price_feed=price_feed,
        db=db,
        market_runtime=market_runtime,
    )
    monitor_enabled = bool(getattr(config.monitor, "enabled", True))
    # Reduce heartbeat spam for HFT runtime while keeping event logs.
    _scan_iv = max(0.1, float(getattr(config.scanner, "interval_seconds", 0.6) or 0.6))
    scanner._status_log_every_scans = max(1, int(round(30.0 / _scan_iv)))
    executor = Executor(
        config,
        api,
        db,
        price_feed=price_feed,
        is_market_terminal=scanner.is_terminal_market,
    )
    redeemer = Redeemer(config, api, db)
    redeem_lock = asyncio.Lock()

    async def _run_redeem(reason: str, window_minute: int | None = None):
        """Serialized redeemer runner for periodic + instant resolution checks."""
        if redeem_lock.locked():
            logger.debug("Redeem skipped (in-flight): %s", reason)
            return {"checked": 0, "redeemed": 0, "pnl": 0.0, "lost": 0}
        async with redeem_lock:
            run_finalizer = (reason == "scheduled-window")
            result = await redeemer.check_and_redeem(
                run_finalizer=run_finalizer,
                finalizer_max_rows=80,
            )
            if result.get("redeemed", 0) > 0 or result.get("lost", 0) > 0:
                if window_minute is None:
                    logger.info(
                        "Redeem (%s): %d redeemed ($%.2f pnl), %d lost",
                        reason, result.get("redeemed", 0), result.get("pnl", 0.0), result.get("lost", 0),
                    )
                else:
                    logger.info(
                        "Redeem (%s @ :%02d): %d redeemed ($%.2f pnl), %d lost",
                        reason, window_minute, result.get("redeemed", 0), result.get("pnl", 0.0), result.get("lost", 0),
                    )
            return result

    await db.start()
    await api.start()
    await ws.start()
    await monitor_db.init()

    price_feed.set_db(db._conn)
    await price_feed.load_ptb_from_db()
    await price_feed.start()
    await market_runtime.start()

    # HFT trigger callback with per-asset cooldown.
    _hft_m2_last_by_asset: dict[str, float] = {}

    def _on_hft_mode2_tick(asset, price, ts, source="binance"):
        if not getattr(config.trend, "hft_barrier_enabled", False):
            return
        cooldown_ms = max(
            10,
            int(getattr(config.trend, "hft_mode2_trigger_cooldown_ms", 150)),
        )
        now_ts = float(ts or time.time())
        last = _hft_m2_last_by_asset.get(asset, 0.0)
        if (now_ts - last) < (cooldown_ms / 1000.0):
            return
        _hft_m2_last_by_asset[asset] = now_ts
        asyncio.create_task(scanner.evaluate_hft_runner(asset, mode="MODE2", price=price))

    # Startup in-flight reconciliation.
    _ASSET_RE = re.compile(r"(bitcoin|ethereum|solana|xrp|btc|eth|sol)\b", re.IGNORECASE)
    _ALIAS = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana"}
    _SPORT_RE = re.compile(
        r"\b(nba|nfl|mlb|nhl|mma|ufc|epl|mls|ncaa|wnba|soccer|football|basketball|baseball|hockey)\b",
        re.IGNORECASE,
    )

    def _parse_asset_from_title(title: str) -> str:
        m = _ASSET_RE.search(title)
        if m:
            raw = m.group(1).lower()
            return _ALIAS.get(raw, raw)
        m = _SPORT_RE.search(title)
        if m:
            return m.group(1).lower()
        if " vs " in title.lower() or " @ " in title.lower():
            return "sports"
        return "unknown"

    try:
        inflight_cids = await db.get_inflight_cids()
        if inflight_cids:
            wallet = config.wallet_address
            chain_positions = await api.get_all_positions(wallet, page_size=500) if wallet else []
            chain_map = {cp.get("conditionId", ""): cp for cp in chain_positions}
            db_open = {p["condition_id"] for p in await db.get_open_positions()}
            recovered = 0
            for cid in inflight_cids:
                if cid in db_open:
                    continue
                cp = chain_map.get(cid)
                if not cp:
                    continue
                c_shares = float(cp.get("size", 0))
                c_avg = float(cp.get("avgPrice", 0))
                c_cost = float(cp.get("initialValue", 0))
                if c_cost <= 0 and c_shares > 0 and c_avg > 0:
                    c_cost = round(c_shares * c_avg, 6)
                if c_avg <= 0 and c_shares > 0 and c_cost > 0:
                    c_avg = c_cost / c_shares
                if c_shares <= 0 or c_avg <= 0:
                    continue
                chain_title = cp.get("title", "")
                recovered_asset = _parse_asset_from_title(chain_title)
                await db.record_confirmed_fill(
                    condition_id=cid,
                    token_id=cp.get("asset", ""),
                    event_title=chain_title or "Recovered position",
                    outcome=cp.get("outcome", "Unknown"),
                    asset=recovered_asset,
                    action="SNIPE",
                    shares=c_shares,
                    avg_price=c_avg,
                    total_cost=c_cost,
                    outcome_index=int(cp.get("outcomeIndex", 0)),
                    neg_risk=bool(cp.get("negativeRisk", False)),
                )
                recovered += 1
                logger.info(
                    "[HFT-RECOVERY] cid=%s shares=%.4f avg=%.4f cost=%.4f",
                    cid[:16], c_shares, c_avg, c_cost,
                )
            await db.clear_all_inflight()
            if recovered:
                logger.info("Startup: recovered %d inflight position(s)", recovered)
    except Exception as e:
        logger.warning("Startup chain reconciliation error: %s", e)

    # Startup resolution sync for held positions.
    try:
        open_positions = await db.get_open_positions()
        resolved_count = 0
        for pos in open_positions:
            if pos.get("result"):
                continue
            cid = pos["condition_id"]
            market = await api.get_market(cid)
            if not market:
                continue
            for tok in market.get("tokens", []):
                if tok.get("winner") and tok.get("outcome"):
                    await db.mark_resolution(cid, tok.get("outcome", ""))
                    resolved_count += 1
                    break
        if resolved_count:
            logger.info("Startup: resolved %d positions from CLOB API", resolved_count)
    except Exception as e:
        logger.warning("Startup resolution check error: %s", e)

    try:
        hist = await db.get_historical_stats()
        executor.trades_executed = hist.get("live_trades", 0)
        executor.total_spent = hist.get("total_spent", 0.0)
        redeemer.redeemed_count = hist.get("redeemed_count", 0)
        redeemer.total_pnl = hist.get("redeemed_pnl", 0.0)
    except Exception as e:
        logger.warning("Could not load historical stats: %s", e)

    # Startup warmup (central runtime):
    # 1) Discover markets
    # 2) Ensure PTB (live/backfill as needed)
    # Only after this do we enable HFT trigger callbacks.
    try:
        await scanner.startup_warmup()
    except Exception as e:
        logger.warning("HFT startup warmup error: %s", e)

    if monitor_enabled:
        try:
            await monitor_worker.start()
        except Exception as e:
            logger.warning("Monitor worker startup error: %s", e)
    else:
        logger.info("[MONITOR V2] disabled by config; running pure main-bot scope")

    price_feed._on_price_callbacks.append(_on_hft_mode2_tick)
    logger.info("[HFT-BOOT] Price trigger callback enabled")

    async def _on_market_resolved(condition_id: str, winning_outcome: str):
        try:
            scanner.on_ws_resolution(condition_id, winning_outcome)
        except Exception as e:
            logger.error("Scanner resolution forward error: %s", e)
        try:
            await db.resolve_snapshots(condition_id, winning_outcome)
        except Exception as e:
            logger.error("Resolution snapshot backfill error: %s", e)
        try:
            if monitor_enabled:
                await monitor_db.backfill_winner(condition_id, winning_outcome)
        except Exception as e:
            logger.debug("Monitor backfill error: %s", e)
        try:
            has_position = await db.count_open_positions_for(condition_id)
            if has_position:
                await db.mark_resolution(condition_id, winning_outcome)
                logger.info("WS resolution event for held position -> instant redemption check")
                try:
                    await _run_redeem("instant-resolution")
                except Exception as redeem_err:
                    logger.error("Instant redemption error: %s", redeem_err)
        except Exception as e:
            logger.error("Instant result annotation error: %s", e)

    ws.set_resolution_callback(_on_market_resolved)

    async def _on_ws_snipe(opp):
        controls = get_bot_controls()
        if controls.get("paused", False):
            return
        if not getattr(opp, "is_hft_runner", False):
            return

        t0 = time.perf_counter()
        result = await executor.execute(opp)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        mkt = format_market_label(opp.market)
        cid_short = short_cid(opp.market.condition_id)
        if result.get("ok"):
            scanner.record_cooldown(opp.market.condition_id)
            if result.get("action") == "SNIPED_PENDING":
                logger.info(
                    "[HFT-PENDING] mkt=%s cid=%s amt=%.2f exec_ms=%.1f",
                    mkt,
                    cid_short,
                    result.get("amount", 0),
                    elapsed_ms,
                )
            else:
                if monitor_enabled:
                    await monitor_db.mark_sniped(opp.market.condition_id)
                logger.info(
                    "[HFT-DONE] mkt=%s cid=%s amt=%.2f avg=%.4f exec_ms=%.1f",
                    mkt,
                    cid_short,
                    result.get("amount", 0),
                    result.get("avg_price", 0), elapsed_ms,
                )
        elif result.get("reason"):
            logger.info(
                "[HFT-FAIL] mkt=%s cid=%s reason=%s",
                mkt,
                cid_short,
                result["reason"],
            )

    scanner.set_snipe_callback(_on_ws_snipe)
    ws.set_price_callback(scanner.on_ws_price_update)

    try:
        balance = await api.refresh_balance()
        logger.info("Wallet balance: $%.2f", balance)
        executor.prime_balance_cache(balance)
        get_bot_controls()["econ_hwm"] = balance if balance and balance > 0 else 0.0
        if balance < 1.0:
            logger.warning("Balance $%.2f < $1.00 minimum", balance)
    except Exception as e:
        logger.warning("Balance check failed: %s", e)

    loop = asyncio.get_event_loop()
    set_bot_db(db, loop)
    set_bot_scanner(scanner)
    set_bot_config(config)
    set_bot_price_feed(price_feed)
    set_bot_monitor_worker(monitor_worker if monitor_enabled else None)
    set_bot_state(
        running=True,
        mode="MODE2_ONLY",
        started_at=datetime.now(timezone.utc).isoformat(),
        wallet_address=config.wallet_address or "",
    )
    await start_dashboard(port=DASHBOARD_PORT)
    logger.info("Dashboard running on http://127.0.0.1:%d", DASHBOARD_PORT)

    set_view_state(_dashboard_bot_state)
    set_view_scanner(scanner)
    try:
        await start_snipe_view(port=SNIPE_VIEW_PORT)
        logger.info("Snipe-View running on http://0.0.0.0:%d", SNIPE_VIEW_PORT)
    except Exception as e:
        logger.warning("Snipe-View failed to start: %s", e)

    shutdown_event = asyncio.Event()

    def _signal_handler(*_):
        logger.info("Shutdown signal received")
        shutdown_event.set()

    if sys.platform == "win32":
        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)
    else:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)

    controls = get_bot_controls()
    scan_interval = config.scanner.interval_seconds
    cycle = 0
    last_redeem_window = -1
    last_housekeep_window = -1

    # Redeem schedule: run once per 5-minute block at :X2.
    _REDEEM_OFFSET_MIN = 2

    def _in_redeem_window(now_utc: datetime) -> tuple[bool, int]:
        minute = now_utc.minute
        slot_offset = minute % 5
        window_id = now_utc.hour * 12 + minute // 5
        return (slot_offset == _REDEEM_OFFSET_MIN, window_id)

    # Housekeeping schedule: run once per 5-min block at :X2:30.
    _HOUSEKEEP_OFFSET_MIN = 2
    _HOUSEKEEP_OFFSET_SEC = 30

    def _in_housekeep_window(now_utc: datetime) -> tuple[bool, int]:
        minute = now_utc.minute
        slot_offset = minute % 5
        window_id = now_utc.hour * 12 + minute // 5
        return (
            slot_offset == _HOUSEKEEP_OFFSET_MIN and now_utc.second >= _HOUSEKEEP_OFFSET_SEC,
            window_id,
        )

    logger.info("Entering HFT-only loop (scan every %.2fs, redeem at :X2)", scan_interval)
    logger.info(
        "Runtime lanes: execution(scan/trigger/buy/relist), settlement(redeem/sync), maintenance(housekeep/finalizer)"
    )
    while not shutdown_event.is_set():
        cycle += 1
        cycle_start = time.monotonic()

        if controls.get("request_stop"):
            logger.info("Stop requested via dashboard")
            break

        paused = controls.get("paused", False)
        ws_stats = ws.get_stats()
        pf_stats = price_feed.get_stats()
        mon_stats = monitor_worker.get_stats()
        runtime_stats = market_runtime.get_stats()
        set_bot_state(
            poll_count=cycle,
            tracked_markets=scanner.active_count,
            runtime_tracked=runtime_stats.get("tracked_markets", 0),
            runtime_ws_claimed=runtime_stats.get("claimed_tokens", 0),
            monitor_tracked=mon_stats.get("tracked_markets", 0),
            monitor_active=mon_stats.get("active_markets", 0),
            monitor_samples_written=mon_stats.get("samples_written", 0),
            monitor_ws_tokens=mon_stats.get("ws_tokens", 0),
            scan_count=scanner.scan_count,
            opportunities=scanner.opportunities_found,
            trades=executor.trades_executed,
            total_spent=executor.total_spent,
            redeemed=redeemer.redeemed_count,
            redeemed_pnl=redeemer.total_pnl,
            paused=paused,
            ws_connected=ws_stats["connected"],
            ws_subscribed=ws_stats["subscribed_tokens"],
            ws_messages=ws_stats["messages_received"],
            binance_connected=pf_stats["connected"],
            binance_ticks=pf_stats["ticks"],
            binance_prices=pf_stats.get("prices", {}),
            chainlink_connected=pf_stats.get("chainlink_connected", False),
            chainlink_ticks=pf_stats.get("chainlink_ticks", 0),
            chainlink_prices=pf_stats.get("chainlink_prices", {}),
            prices_to_beat=pf_stats.get("prices_to_beat", 0),
        )

        # Economic drawdown guard: auto-pause if balance falls below (HWM - configured drawdown).
        try:
            dd = float(getattr(config.execution, "economic_pause_drawdown", 0.0) or 0.0)
            if dd > 0:
                current_bal = executor._get_cached_balance()
                hwm = float(controls.get("econ_hwm", 0.0) or 0.0)
                if hwm <= 0 and current_bal > 0:
                    controls["econ_hwm"] = current_bal
                    hwm = current_bal
                floor = (hwm - dd) if hwm > 0 else None
                if floor is not None and current_bal < floor:
                    if not paused and not controls.get("econ_pause_acked", False):
                        controls["paused"] = True
                        controls["econ_pause_acked"] = True
                        paused = True
                        logger.warning(
                            "[FEED: ECON-PAUSE] Balance $%.2f < floor $%.2f (HWM $%.2f - $%.2f drawdown) -- pausing.",
                            current_bal, floor, hwm, dd,
                        )
                elif floor is not None and current_bal >= floor:
                    controls["econ_pause_acked"] = False
            else:
                controls["econ_pause_acked"] = False
        except Exception as e:
            logger.debug("Economic pause check error: %s", e)

        now_utc = datetime.now(timezone.utc)
        should_redeem, redeem_window_id = _in_redeem_window(now_utc)

        # Execution lane first (latency-critical): keep entry path ahead of
        # settlement/maintenance tasks inside each cycle.
        if not paused:
            try:
                # Keep discovery/tracking/PTB warmups active; ignore poll opportunities.
                opportunities = await scanner.scan()
                if opportunities:
                    logger.debug("HFT-only runtime ignored %d poll opportunities", len(opportunities))
            except Exception as e:
                logger.error("Scan cycle error: %s", e)

        # Settlement lane (scheduled): run after scan so cycle hot path is
        # less likely to be delayed by redemption/reconciliation work.
        if should_redeem and redeem_window_id != last_redeem_window:
            last_redeem_window = redeem_window_id
            try:
                await _run_redeem("scheduled-window", window_minute=now_utc.minute)
            except Exception as e:
                logger.error("Redemption error: %s", e)

        # Maintenance lane: defer when cycle budget is already mostly consumed.
        now_utc = datetime.now(timezone.utc)
        should_housekeep, hk_window_id = _in_housekeep_window(now_utc)
        if should_housekeep and hk_window_id != last_housekeep_window:
            last_housekeep_window = hk_window_id
            elapsed_pre_hk = time.monotonic() - cycle_start
            hk_budget = max(0.10, float(scan_interval) * 0.60)
            do_housekeep = True
            if not paused and elapsed_pre_hk > hk_budget:
                logger.info(
                    "[HFT-HOUSEKEEP] deferred this window (elapsed=%.3fs budget=%.3fs)",
                    elapsed_pre_hk,
                    hk_budget,
                )
                do_housekeep = False

            if do_housekeep:
                try:
                    _truncate_log(PROJECT_ROOT / config.logging.file)
                except Exception as e:
                    logger.debug("Housekeep truncate error: %s", e)

                # 1) PTB prune/persist backstop
                try:
                    pruned = price_feed._prune_ptb()
                    ptb_count = len(price_feed._price_to_beat)
                    if pruned:
                        await price_feed._save_ptb_to_db()
                except Exception as e:
                    pruned, ptb_count = 0, -1
                    logger.debug("PTB prune error: %s", e)

                # 2) WS cache cleanup backstop.
                # Subscription ownership lives in MarketRuntime.
                stale_cache = 0
                orphan_count = 0
                try:
                    cleanup_secs = scanner.ws_cleanup_delay
                    tracked_tokens = set(market_runtime.subscribed_tokens())

                    stale_tokens = [
                        tid for tid, entry in list(ws.book_cache.items())
                        if tid not in tracked_tokens
                        and (time.time() - entry.get("updated_at", 0)) > cleanup_secs
                    ]
                    for tid in stale_tokens:
                        ws.book_cache.pop(tid, None)
                    stale_cache = len(stale_tokens)
                except Exception as e:
                    logger.debug("WS cleanup error: %s", e)

                # 3) Scanner dict sweep backstop
                swept = 0
                try:
                    tracked_cids = set(scanner._tracked.keys())

                    def _key_is_live(k: object) -> bool:
                        if k in tracked_cids:
                            return True
                        if isinstance(k, str) and k.startswith("mode2:"):
                            return k.split(":", 1)[1] in tracked_cids
                        return False

                    for d in (
                        scanner._cooldowns,
                        scanner._hft_cooldowns,
                        scanner._snapshot_ts,
                        scanner._ws_rest_ts,
                        scanner._monitor_ts,
                    ):
                        stale_keys = [k for k in d if not _key_is_live(k)]
                        for k in stale_keys:
                            del d[k]
                            swept += 1
                    swept += scanner.sweep_orphan_hft_traps()
                except Exception as e:
                    logger.debug("Scanner dict sweep error: %s", e)

                # 4) Monitor DB prune
                if monitor_enabled:
                    try:
                        await monitor_db.prune()
                    except Exception as e:
                        logger.debug("Monitor DB prune error: %s", e)

                # 5) Monitor DB winner sweep
                if monitor_enabled:
                    try:
                        await monitor_db.sweep_winners(api)
                    except Exception as e:
                        logger.debug("Monitor DB sweep error: %s", e)

                # 6) Economic high-water mark ratchet
                try:
                    dd = float(getattr(config.execution, "economic_pause_drawdown", 0.0) or 0.0)
                    if dd > 0:
                        current_bal = executor._get_cached_balance()
                        hwm = float(controls.get("econ_hwm", 0.0) or 0.0)
                        if current_bal > hwm:
                            controls["econ_hwm"] = current_bal
                            logger.info(
                                "[FEED: ECON-HWM] High-water mark ratcheted: $%.2f -> $%.2f (floor now $%.2f)",
                                hwm, current_bal, current_bal - dd,
                            )
                except Exception as e:
                    logger.debug("Economic HWM ratchet error: %s", e)

                ws_stats_hk = ws.get_stats()
                logger.info(
                    "[HFT-HOUSEKEEP] PTB:%d(%+d) | WS:subs=%d,cache=%d,stale=%d,orphan=%d | tracked:%d | swept:%d",
                    ptb_count,
                    -pruned,
                    ws_stats_hk["subscribed_tokens"],
                    ws_stats_hk["cached_books"],
                    stale_cache,
                    orphan_count,
                    scanner.active_count,
                    swept,
                )

        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0, scan_interval - elapsed)
        if sleep_time > 0:
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=sleep_time)
            except asyncio.TimeoutError:
                pass

    logger.info("Shutting down HFT-only runtime...")
    set_bot_state(running=False)
    try:
        await stop_dashboard()
    except Exception:
        pass
    try:
        await stop_snipe_view()
    except Exception:
        pass
    if monitor_enabled:
        try:
            await monitor_worker.stop()
        except Exception:
            pass
    try:
        await market_runtime.stop()
    except Exception:
        pass
    await price_feed.stop()
    await ws.stop()
    await monitor_db.close()
    await api.stop()
    await db.stop()
    _release_lock()
    logger.info("HFT-only runtime stopped cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nHFT-only runtime stopped by user")
