"""Sniper configuration -- config.yaml + .env -> Config dataclass."""

import json
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional
from zoneinfo import ZoneInfo

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Duration → field-name maps (avoids rebuilding dicts per call)
_PRE_END_FIELDS = {
    300: "pre_end_seconds_5m", 900: "pre_end_seconds_15m",
    3600: "pre_end_seconds_1h", 14400: "pre_end_seconds_4h",
    86400: "pre_end_seconds_1d",
}
_BET_FIELDS = {
    300: "max_bet_5m", 900: "max_bet_15m",
    3600: "max_bet_1h", 14400: "max_bet_4h",
    86400: "max_bet_1d",
}
_PRICE_FIELDS = {
    300: "max_buy_price_5m", 900: "max_buy_price_15m",
    3600: "max_buy_price_1h", 14400: "max_buy_price_4h",
    86400: "max_buy_price_1d",
}
_EDGE_FIELDS = {
    300: "min_edge_5m", 900: "min_edge_15m",
    3600: "min_edge_1h", 14400: "min_edge_4h",
    86400: "min_edge_1d",
}
_GAP_FIELDS = {
    300: "price_gap_pct_5m", 900: "price_gap_pct_15m",
    3600: "price_gap_pct_1h", 14400: "price_gap_pct_4h",
    86400: "price_gap_pct_1d",
}
_MAX_GAP_FIELDS = {
    300: "max_gap_pct_5m", 900: "max_gap_pct_15m",
    3600: "max_gap_pct_1h", 14400: "max_gap_pct_4h",
    86400: "max_gap_pct_1d",
}
_THRESHOLD_FIELDS = {
    300: "pre_end_threshold_5m", 900: "pre_end_threshold_15m",
    3600: "pre_end_threshold_1h", 14400: "pre_end_threshold_4h",
    86400: "pre_end_threshold_1d",
}
_CONV_FIELDS = {
    300: "min_conviction_5m", 900: "min_conviction_15m",
    3600: "min_conviction_1h", 14400: "min_conviction_4h",
    86400: "min_conviction_1d",
}
_SCANNER_TOGGLE_ASSETS = ("bitcoin", "ethereum", "solana", "xrp")
_SCANNER_TOGGLE_TFS = ("5m", "15m", "1h", "4h", "1d")
_SCANNER_TF_LABEL_TO_SECONDS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
_SCANNER_SECONDS_TO_TF_LABEL = {v: k for k, v in _SCANNER_TF_LABEL_TO_SECONDS.items()}


@dataclass
class ApiConfig:
    clob: str = "https://clob.polymarket.com"
    data: str = "https://data-api.polymarket.com"
    gamma: str = "https://gamma-api.polymarket.com"
    chain_id: int = 137
    signature_type: int = 0


@dataclass
class ScannerConfig:
    interval_seconds: float = 10        # How often to scan for opportunities
    assets: str = "bitcoin,ethereum,solana,xrp"  # Crypto assets to track
    time_frames: str = "5m,15m,1h,4h,1d"    # Active timeframes (csv, toggleable via dashboard)
    asset_timeframe_enabled: dict[str, dict[str, bool]] = field(default_factory=dict)
    pre_window_seconds: int = 30        # Start watching N sec before market ends
    post_window_seconds: int = 120      # Keep watching N sec after market ends
    ws_cleanup_delay: int = 60           # Unsub WS + drop tracking N sec after post-window
    winner_threshold: float = 0.80      # Price above this = likely winner
    max_events_per_query: int = 100     # Events per Gamma API page
    pre_end_seconds: int = 15           # Pre-end sniping window (seconds before end)
    pre_end_threshold: float = 0.92     # Pre-end: midpoint must exceed this to buy
    lookahead_enabled: bool = False     # Allow pre-start sniping on next market(s)
    lookahead_seconds: int = 60         # Evaluate markets up to N seconds before start
    post_end_bet_seconds: int = 10      # Bet up to N sec AFTER market end (0 = no post-end bets)
    stop_buffer_seconds: int = 0          # Stop betting N sec BEFORE market end (anti-dump shield, 0 = disabled)
    start_buffer_seconds: int = 0         # Only bet within first N sec AFTER market start (0 = disabled)
    min_conviction: float = 0.30            # Min |up_mid - down_mid| spread (global fallback, 0 = disabled)
    max_gap_pct: float = 0.0                  # Max oracle gap% allowed (global fallback, 0 = disabled)

    # Per-timeframe max-gap caps (0 = use global max_gap_pct)
    max_gap_pct_5m: float = 0.0
    max_gap_pct_15m: float = 0.0
    max_gap_pct_1h: float = 0.0
    max_gap_pct_4h: float = 0.0
    max_gap_pct_1d: float = 0.0

    # Per-timeframe conviction overrides (0 = use global min_conviction)
    min_conviction_5m: float = 0.0
    min_conviction_15m: float = 0.0
    min_conviction_1h: float = 0.0
    min_conviction_4h: float = 0.0
    min_conviction_1d: float = 0.0

    # Per-timeframe pre-end windows (override pre_end_seconds for specific durations)
    pre_end_seconds_5m: int = 0         # 0 = use pre_end_seconds default
    pre_end_seconds_15m: int = 0
    pre_end_seconds_1h: int = 0
    pre_end_seconds_4h: int = 0
    pre_end_seconds_1d: int = 0

    # Per-timeframe pre-end threshold overrides (0 = use global pre_end_threshold)
    # Allows higher thresholds for longer TFs (e.g. 0.93 for 15m = "wait until 93c")
    pre_end_threshold_5m: float = 0.0
    pre_end_threshold_15m: float = 0.0
    pre_end_threshold_1h: float = 0.0
    pre_end_threshold_4h: float = 0.0
    pre_end_threshold_1d: float = 0.0

    def get_pre_end_seconds(self, duration_seconds: int = 0) -> int:
        """Return the pre-end window for a given market duration, falling back to global pre_end_seconds."""
        field_name = _PRE_END_FIELDS.get(duration_seconds)
        override = getattr(self, field_name, 0) if field_name else 0
        return override if override > 0 else self.pre_end_seconds

    def get_pre_end_threshold(self, duration_seconds: int = 0) -> float:
        """Return per-TF pre-end threshold if set, else global fallback."""
        field_name = _THRESHOLD_FIELDS.get(duration_seconds)
        override = getattr(self, field_name, 0.0) if field_name else 0.0
        return override if override > 0 else self.pre_end_threshold

    def max_pre_end_seconds(self) -> int:
        """Return the widest pre_end_seconds across all timeframes (for scan window sizing)."""
        return max(
            self.pre_end_seconds,
            self.pre_end_seconds_5m or 0,
            self.pre_end_seconds_15m or 0,
            self.pre_end_seconds_1h or 0,
            self.pre_end_seconds_4h or 0,
            self.pre_end_seconds_1d or 0,
        )

    # -- Price-gap safety gate (Binance feed) --
    price_feed: str = "binance"          # Price data source
    price_gap_pct: float = 0.30          # Default min % gap from price-to-beat (global fallback)
    ptb_capture_offset: float = 1.5      # Capture PTB this many seconds BEFORE market_start
                                          # Aligns with Polymarket's PTB (end of prior candle)

    # Per-timeframe gap % overrides (0 = use global fallback)
    price_gap_pct_5m: float = 0.0
    price_gap_pct_15m: float = 0.0
    price_gap_pct_1h: float = 0.0
    price_gap_pct_4h: float = 0.0
    price_gap_pct_1d: float = 0.0

    def get_price_gap_pct(self, duration_seconds: int = 0) -> float:
        """Return per-timeframe gap % if set, else global fallback."""
        field_name = _GAP_FIELDS.get(duration_seconds)
        override = getattr(self, field_name, 0.0) if field_name else 0.0
        return override if override > 0 else self.price_gap_pct

    def get_min_conviction(self, duration_seconds: int = 0) -> float:
        """Return per-timeframe conviction if set, else global fallback."""
        field_name = _CONV_FIELDS.get(duration_seconds)
        override = getattr(self, field_name, 0.0) if field_name else 0.0
        return override if override > 0 else self.min_conviction

    def get_max_gap_pct(self, duration_seconds: int = 0) -> float:
        """Return per-timeframe max-gap cap if set, else global fallback."""
        field_name = _MAX_GAP_FIELDS.get(duration_seconds)
        override = getattr(self, field_name, 0.0) if field_name else 0.0
        return override if override > 0 else self.max_gap_pct

    @staticmethod
    def _parse_assets_csv(raw: str) -> set[str]:
        return {
            a.strip().lower()
            for a in str(raw or "").split(",")
            if a.strip().lower() in _SCANNER_TOGGLE_ASSETS
        }

    @staticmethod
    def _parse_tfs_csv(raw: str) -> set[str]:
        return {
            tf.strip().lower()
            for tf in str(raw or "").split(",")
            if tf.strip().lower() in _SCANNER_TOGGLE_TFS
        }

    @staticmethod
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

    def get_asset_timeframe_enabled_matrix(self) -> dict[str, dict[str, bool]]:
        """Return normalized per-asset/per-timeframe toggle matrix.

        This matrix stores user preferences and is combined with global
        scanner assets/time_frames gates at runtime.
        """
        active_assets = self._parse_assets_csv(self.assets)
        active_tfs = self._parse_tfs_csv(self.time_frames)
        defaults: dict[str, dict[str, bool]] = {}
        for asset in _SCANNER_TOGGLE_ASSETS:
            defaults[asset] = {}
            for tf in _SCANNER_TOGGLE_TFS:
                defaults[asset][tf] = (asset in active_assets and tf in active_tfs)

        incoming = self.asset_timeframe_enabled
        if not isinstance(incoming, dict):
            return defaults

        out: dict[str, dict[str, bool]] = {}
        for asset in _SCANNER_TOGGLE_ASSETS:
            row = incoming.get(asset)
            out[asset] = {}
            if not isinstance(row, dict):
                out[asset] = defaults[asset].copy()
                continue
            for tf in _SCANNER_TOGGLE_TFS:
                if tf in row:
                    out[asset][tf] = self._coerce_bool(row.get(tf))
                else:
                    out[asset][tf] = defaults[asset][tf]
        return out

    def is_asset_timeframe_enabled(self, asset: str, duration_seconds: int) -> bool:
        """Return effective runtime gate for a market cell.

        Effective gate = global asset gate AND global timeframe gate AND
        per-cell preference from asset_timeframe_enabled matrix.
        """
        a = str(asset or "").strip().lower()
        tf = _SCANNER_SECONDS_TO_TF_LABEL.get(int(duration_seconds or 0), "")
        if a not in _SCANNER_TOGGLE_ASSETS or not tf:
            return False
        active_assets = self._parse_assets_csv(self.assets)
        active_tfs = self._parse_tfs_csv(self.time_frames)
        if a not in active_assets or tf not in active_tfs:
            return False
        matrix = self.get_asset_timeframe_enabled_matrix()
        row = matrix.get(a, {})
        return bool(row.get(tf, False))


@dataclass
class ExecutionConfig:
    max_buy_price: float = 0.97         # Max price for winning shares (profit = 1 - price)
    min_edge: float = 0.02              # Min edge (global fallback, 0 = disabled)
    max_bet: float = 10000.00           # Default upper-bound dollar amount per snipe
    fill_fraction: float = 0.30         # Fraction of ask-side depth to take (0.05-1.0)
    max_concurrent: int = 5             # Max simultaneous open positions
    cooldown_seconds: float = 10.0      # Block repeat buys on same market
    oracle_consensus: bool = True        # Require Chainlink + Binance to agree on direction before buying
    swing_guard_enabled: bool = False   # Pre-execution book re-check: abort if price moved too far since eval
    swing_guard_max_pct: float = 0.05   # Max % price move tolerated between eval and execution (0.05 = 5%)
    dynamic_max_price_enabled: bool = False  # Cap execution ceiling relative to eval best_ask
    dynamic_max_price_pct: float = 0.10      # Max allowed rise vs eval ask (0.10 = +10%)
    economic_pause_drawdown: float = 0.0     # Auto-turn OFF Mode 2 if balance drops this many USD below HWM (0 = off)
    economic_profit_target: float = 0.0      # Auto-turn OFF Mode 2 once balance gains this many USD above startup (0 = off)
    gtc_fallback_enabled: bool = True        # Allow GTC limit-order fallback when FOK fails (False = FOK-only)
    no_bet_assets: str = ""              # CSV of assets to watch-only (no betting). e.g. "solana,ripple"

    @property
    def no_bet_set(self) -> set:
        """Parsed set of watch-only asset names (lowercase)."""
        raw = self.no_bet_assets
        if not raw:
            return set()
        return {a.strip().lower() for a in raw.split(",") if a.strip()}

    # Per-timeframe buy-price caps (override max_buy_price for specific durations)
    max_buy_price_5m: float = 0.0       # 0 = use max_buy_price default
    max_buy_price_15m: float = 0.0
    max_buy_price_1h: float = 0.0
    max_buy_price_4h: float = 0.0
    max_buy_price_1d: float = 0.0

    # Per-timeframe edge floors (override min_edge for specific durations)
    min_edge_5m: float = 0.0            # 0 = use min_edge default
    min_edge_15m: float = 0.0
    min_edge_1h: float = 0.0
    min_edge_4h: float = 0.0
    min_edge_1d: float = 0.0

    # Per-timeframe bet caps (override max_bet for specific durations)
    max_bet_5m: float = 0.0             # 0 = use max_bet default
    max_bet_15m: float = 0.0
    max_bet_1h: float = 0.0
    max_bet_4h: float = 0.0
    max_bet_1d: float = 0.0

    def get_max_buy_price(self, duration_seconds: int = 0) -> float:
        """Return max buy price for a market duration.

        Resolution: per-TF override > 0 wins, else global max_buy_price.
        Global 0 = feature OFF (no ceiling enforced).
        Per-TF 0 = inherit global.  Per-TF > 0 always takes priority.
        """
        field_name = _PRICE_FIELDS.get(duration_seconds)
        override = getattr(self, field_name, 0.0) if field_name else 0.0
        return override if override > 0 else self.max_buy_price

    def get_max_bet(self, duration_seconds: int = 0) -> float:
        """Return the max bet for a given market duration, falling back to global max_bet."""
        field_name = _BET_FIELDS.get(duration_seconds)
        override = getattr(self, field_name, 0.0) if field_name else 0.0
        return override if override > 0 else self.max_bet

    def get_min_edge(self, duration_seconds: int = 0) -> float:
        """Return min edge for a market duration, falling back to global min_edge.

        Resolution: per-TF override > 0 wins, else global min_edge.
        Global 0 = feature OFF.
        Per-TF 0 = inherit global.
        """
        field_name = _EDGE_FIELDS.get(duration_seconds)
        override = getattr(self, field_name, 0.0) if field_name else 0.0
        return override if override > 0 else self.min_edge


@dataclass
class PostEndConfig:
    """Post-end MAKER sniper — posts GTC LIMIT BUY for the winning token after resolution.

    Strategy (confirmed on-chain from @lesstidy):
      1. Market ends → determine winner via Binance/Chainlink oracle
      2. Post resting GTC LIMIT BUY at `maker_price` (e.g. $0.99)
      3. Sellers who want instant liquidity fill against our bid
      4. Redeemer handles $1.00 redemption → $0.01/share profit
    """
    enabled: bool = False               # Master toggle
    max_bet: float = 50.0               # $ per maker order (independent from main bot)
    maker_price: float = 0.99           # GTC LIMIT BUY price (bid for winning token)
    window_seconds: int = 120           # How long to leave resting order before cancel
    poll_interval: float = 2.0          # Seconds between fill-check polls
    fetch_leadtime: float = 1.0         # Seconds BEFORE boundary to fire settlement HTTP call
                                        # (HTTP round-trip ~700ms acts as natural timer)

    @property
    def cooldown_seconds(self) -> float:
        """Auto-cooldown = window + 5s.  Prevents re-entry on same market."""
        return self.window_seconds + 5.0

    @property
    def max_buy_price(self) -> float:
        """Safety cap — always capped at 0.995 (hardcoded, no UI knob needed)."""
        return min(self.maker_price, 0.995)


@dataclass
class OffloadConfig:
    """Post-fill offload — immediately list purchased shares for sale via GTC LIMIT SELL.

    After a successful snipe fill, post a GTC sell at a tiered sell price.
    If a buyer takes our ask, we lock in profit before resolution.
    If nobody buys, Polymarket auto-cancels the order at resolution and we redeem normally.

    Sell price tiers are based on buy price (chain-confirmed avg_price).
    Setting sell_price (global) to 0 disables the feature entirely.
    Tier fields at 0 inherit from global.  All 0 = feature off.
    """
    sell_price: float = 0.0             # Global default sell price (0 = feature OFF)
    sell_price_t1: float = 0.0          # Bought ≤ $0.65  (0 = use global)
    sell_price_t2: float = 0.0          # Bought $0.66–$0.75  (0 = use global)
    sell_price_t3: float = 0.0          # Bought $0.76–$0.82  (0 = use global)
    sell_price_t4: float = 0.0          # Bought ≥ $0.83  (0 = use global)
    pct_offload: int = 0                # Whole-number % above fill (0 = OFF)

    def resolve_sell_price(self, fill_price: float) -> float | None:
        """Determine sell price based on fill price tier. Returns None if off."""
        if fill_price <= 0.65:
            tier_price = self.sell_price_t1
        elif fill_price <= 0.75:
            tier_price = self.sell_price_t2
        elif fill_price <= 0.82:
            tier_price = self.sell_price_t3
        else:
            tier_price = self.sell_price_t4

        # Inheritance: tier > 0 uses tier, else fall back to global
        effective = tier_price if tier_price > 0 else self.sell_price

        # 0 means off (unless tier overrides)
        if effective <= 0:
            return None

        # Cap at $0.99 — selling at $1.00+ makes no sense
        return min(effective, 0.99)

    def resolve_pct_sell_price(self, fill_price: float) -> float | None:
        """Determine sell price from percent markup over fill. Returns None if off."""
        pct = int(self.pct_offload)
        if pct <= 0 or fill_price <= 0:
            return None

        # Whole-number percent markup over the chain-confirmed fill price.
        target = round(fill_price * (1.0 + pct / 100.0), 2)
        target = min(target, 0.99)

        # If capped target is not above fill, skip posting.
        if target <= fill_price:
            return None
        return target


@dataclass
class TrendConfig:
    """Trend Analysis — Bollinger Band mean-reversion betting.

    Independent system: direction comes from technical indicators,
    not from the Polymarket book.  Toggle on/off without affecting
    existing lookahead / pre-end features.
    """
    engine_enabled: bool = True         # Background engine: collect candles, indicators, backfill gaps
    enabled: bool = False               # Strategy active: allow placing bets based on trend signals
    ensemble_enabled: bool = True       # Layer A: BB Mean-Reversion Ensemble
    price_action_enabled: bool = False  # Layer B: High-Low Sequencing Momentum
    crossover_enabled: bool = False     # Layer C: EMA/SMA Crossovers
    adx_enabled: bool = False           # Layer D: ADX Trend Measurement

    ensemble_invert: bool = False       # Layer A Invert toggle
    price_action_invert: bool = False   # Layer B Invert toggle
    crossover_invert: bool = False      # Layer C Invert toggle
    adx_invert: bool = False            # Layer D Invert toggle

    ensemble_invert_5m: Optional[bool] = None
    ensemble_invert_15m: Optional[bool] = None
    ensemble_invert_1h: Optional[bool] = None
    ensemble_invert_4h: Optional[bool] = None
    ensemble_invert_1d: Optional[bool] = None

    price_action_invert_5m: Optional[bool] = None
    price_action_invert_15m: Optional[bool] = None
    price_action_invert_1h: Optional[bool] = None
    price_action_invert_4h: Optional[bool] = None
    price_action_invert_1d: Optional[bool] = None

    crossover_invert_5m: Optional[bool] = None
    crossover_invert_15m: Optional[bool] = None
    crossover_invert_1h: Optional[bool] = None
    crossover_invert_4h: Optional[bool] = None
    crossover_invert_1d: Optional[bool] = None

    adx_invert_5m: Optional[bool] = None
    adx_invert_15m: Optional[bool] = None
    adx_invert_1h: Optional[bool] = None
    adx_invert_4h: Optional[bool] = None
    adx_invert_1d: Optional[bool] = None

    invert_signal: bool = False         # If true, flips standard BB betting logic (Bet Opposite)
    invert_signal_5m: Optional[bool] = None
    invert_signal_15m: Optional[bool] = None
    invert_signal_1h: Optional[bool] = None
    invert_signal_4h: Optional[bool] = None
    invert_signal_1d: Optional[bool] = None
    bb_period: int = 20                 # Bollinger Band period (candle count)
    bb_period_5m: int = 0
    bb_period_15m: int = 0
    bb_period_1h: int = 0
    bb_period_4h: int = 0
    bb_period_1d: int = 0
    bb_std_dev: float = 2.0             # BB standard deviation multiplier
    bb_std_dev_5m: float = 0.0
    bb_std_dev_15m: float = 0.0
    bb_std_dev_1h: float = 0.0
    bb_std_dev_4h: float = 0.0
    bb_std_dev_1d: float = 0.0
    min_confidence: float = 0.7         # Minimum signal confidence to place a bet
    bet_amount: float = 0.98            # Bet size for trend-mode bets (USDC)
    timeframes: str = "5m,15m,1h"       # Timeframes to generate signals for
    bootstrap_hours: int = 168          # Hours of kline history to fetch on startup

    regime_guard_enabled: bool = False  # If true, overrides inversion during high volume/ADX
    post_start_window: int = 0          # Bet up to N sec AFTER market start (Trend system only)

    # HFT Runner (Sub-second price movement detection + Instant Flip)
    hft_autobet_enabled: bool = False   # If true, watch for fast moves and auto-buy
    hft_trigger_pct: float = 0.05       # Price move (%) within window to trigger
    hft_bet_amount: float = 5.0         # USDC amount for HFT bets
    hft_refresh_ms: int = 1000          # Dashboard refresh speed in ms
    hft_window_ms: int = 500            # Detection window (ms) for velocity
    hft_flip_profit_pct: float = 2.0    # Target profit to immediately list for flip
    hft_muzzle_seconds: int = 30        # Stop betting N seconds before market end
    hft_max_price: float = 0.94         # Don't buy if share price is above this
    hft_max_price_5m: float = 0.0       # 5m override (0 = use hft_max_price)
    hft_max_price_15m: float = 0.0      # 15m override (0 = use hft_max_price)
    hft_max_price_1h: float = 0.0       # 1h override (0 = use hft_max_price)
    hft_max_price_matrix: dict[str, dict[str, float]] = field(default_factory=dict)
    hft_cooldown_seconds: int = 60      # Per-market cooldown for HFT re-entry
    hft_max_concurrent: int = 10        # Protects against rate limit bans

    # HFT Mode 2: Barrier-Breaker (Distance from starting price)
    hft_barrier_enabled: bool = False   # If true, trigger buy when price moves X% from PTB
    hft_barrier_pct: float = 0.5        # Minimum distance from PTB (%) to trigger
    hft_barrier_pct_15m: float = 0.0    # 15m override (%). 0 = use hft_barrier_pct
    hft_barrier_pct_1h: float = 0.0     # 1h override (%). 0 = use hft_barrier_pct
    hft_barrier_matrix: dict[str, dict[str, float]] = field(default_factory=dict)
    hft_barrier_delay: int = 10         # Seconds to wait post-start before Mode 2 can fire
    hft_armed_sniper_enabled: bool = False # If true, use WS orderbook hook for Mode 2 execution instead of FOK
    hft_mode2_entry_mode: str = ""  # immediate_fok | watch_best_entry (empty = use toggle)
    hft_mode2_trigger_cooldown_ms: int = 150     # Asset-level Mode 2 callback throttle
    hft_optimizer_objective_mode: str = "pnl"    # pnl | win_rate | roi (shared main+monitor optimizer objective)
    hft_optimizer_lookback_hours: int = 24       # Shared default optimizer lookback window
    hft_optimizer_min_trades: int = 0            # Minimum recommended-trade floor for optimizer candidate selection (0 = disabled)

    # HFT Mode 3: Flash-Pulse (Candle magnitude in short window)
    hft_flash_enabled: bool = False     # If true, trigger buy on large single-candle moves
    hft_flash_pct: float = 1.0          # Minimum candle magnitude (%) to trigger
    hft_flash_window_ms: int = 1000     # Detection window for flash-pulse (ms)

    def mode2_watch_enabled(self) -> bool:
        """Resolve Mode 2 entry submode with backward compatibility."""
        mode = (self.hft_mode2_entry_mode or "").strip().lower()
        if mode in {"watch_best_entry", "watch", "armed_watch"}:
            return True
        if mode in {"immediate_fok", "immediate", "fok"}:
            return False
        # Legacy fallback if mode string is unset/invalid.
        return bool(self.hft_armed_sniper_enabled)

    def get_hft_barrier_pct(self, duration_seconds: int) -> float:
        """Return effective Mode2 barrier for a timeframe.

        15m and 1h markets can use dedicated overrides. All other
        timeframes use the global hft_barrier_pct.
        """
        tf = int(duration_seconds or 0)
        if tf == 900:
            override = float(getattr(self, "hft_barrier_pct_15m", 0.0) or 0.0)
            if override > 0:
                return override
        if tf == 3600:
            override = float(getattr(self, "hft_barrier_pct_1h", 0.0) or 0.0)
            if override > 0:
                return override
        return float(getattr(self, "hft_barrier_pct", 0.5))

    @staticmethod
    def _hft_tf_label(duration_seconds: int) -> str:
        return {
            300: "5m",
            900: "15m",
            3600: "1h",
            14400: "4h",
            86400: "1d",
        }.get(int(duration_seconds or 0), "")

    @staticmethod
    def _hft_matrix_value(
        matrix: object,
        asset: str,
        tf_label: str,
        *,
        low: float,
        high: float,
    ) -> float | None:
        if not tf_label or not isinstance(matrix, dict):
            return None
        key = str(asset or "").strip().lower()
        row = matrix.get(key)
        if not isinstance(row, dict):
            return None
        raw = row.get(tf_label)
        if raw is None:
            return None
        try:
            val = float(raw)
        except Exception:
            return None
        if low <= val <= high:
            return val
        return None

    def get_hft_barrier_pct_for(self, asset: str, duration_seconds: int) -> float:
        """Return effective Mode2 barrier for an asset/timeframe cell.

        Resolution:
        1) matrix cell (if present/valid)
        2) legacy per-TF/global fallback via get_hft_barrier_pct()
        """
        tf_label = self._hft_tf_label(duration_seconds)
        cell = self._hft_matrix_value(
            getattr(self, "hft_barrier_matrix", {}),
            asset,
            tf_label,
            low=0.0,
            high=5.0,
        )
        if cell is not None:
            return cell
        return self.get_hft_barrier_pct(duration_seconds)

    def get_hft_max_price(self, duration_seconds: int) -> float:
        """Return effective Mode2 max-entry price for a timeframe.

        Resolution: per-TF override (>0) wins, else global hft_max_price.
        """
        tf = int(duration_seconds or 0)
        if tf == 300:
            override = float(getattr(self, "hft_max_price_5m", 0.0) or 0.0)
            if override > 0:
                return override
        elif tf == 900:
            override = float(getattr(self, "hft_max_price_15m", 0.0) or 0.0)
            if override > 0:
                return override
        elif tf == 3600:
            override = float(getattr(self, "hft_max_price_1h", 0.0) or 0.0)
            if override > 0:
                return override
        return float(getattr(self, "hft_max_price", 0.94))

    def get_hft_max_price_for(self, asset: str, duration_seconds: int) -> float:
        """Return effective Mode2 max-entry cap for an asset/timeframe cell.

        Resolution:
        1) matrix cell (if present/valid)
        2) legacy per-TF/global fallback via get_hft_max_price()
        """
        tf_label = self._hft_tf_label(duration_seconds)
        cell = self._hft_matrix_value(
            getattr(self, "hft_max_price_matrix", {}),
            asset,
            tf_label,
            low=0.01,
            high=0.99,
        )
        if cell is not None:
            return cell
        return self.get_hft_max_price(duration_seconds)

    def get_invert_signal(self, duration_seconds: int) -> bool:
        TF_MAP = {300: "invert_signal_5m", 900: "invert_signal_15m", 3600: "invert_signal_1h", 14400: "invert_signal_4h", 86400: "invert_signal_1d"}
        key = TF_MAP.get(duration_seconds, "")
        val = getattr(self, key, None)
        return val if val is not None else getattr(self, "invert_signal", False)

    def get_layer_invert(self, layer: str, duration_seconds: int) -> bool:
        TF_MAP = {300: "5m", 900: "15m", 3600: "1h", 14400: "4h", 86400: "1d"}
        tf_str = TF_MAP.get(duration_seconds, "")
        if tf_str:
            key = f"{layer}_invert_{tf_str}"
            val = getattr(self, key, None)
            if val is not None:
                return val
        return getattr(self, f"{layer}_invert", False)

    def get_bb_period(self, duration_seconds: int) -> int:
        TF_MAP = {300: "bb_period_5m", 900: "bb_period_15m", 3600: "bb_period_1h", 14400: "bb_period_4h", 86400: "bb_period_1d"}
        key = TF_MAP.get(duration_seconds, "")
        val = getattr(self, key, 0)
        return val if val > 0 else self.bb_period

    def get_bb_std_dev(self, duration_seconds: int) -> float:
        TF_MAP = {300: "bb_std_dev_5m", 900: "bb_std_dev_15m", 3600: "bb_std_dev_1h", 14400: "bb_std_dev_4h", 86400: "bb_std_dev_1d"}
        key = TF_MAP.get(duration_seconds, "")
        val = getattr(self, key, 0.0)
        return val if val > 0.0 else self.bb_std_dev


@dataclass
class BlackoutConfig:
    """Clock Blackout — block specific minute-of-hour settlement slots per timeframe."""
    slots_5m: str = ""       # e.g. "0,55" — blocked end-minute values (csv)
    slots_15m: str = ""
    slots_1h: str = ""
    slots_4h: str = ""
    slots_1d: str = ""
    # Optional session-specific overrides (ET session):
    # if present, these are checked in addition to the global slots_* list.
    slots_5m_am: str = ""
    slots_5m_pm: str = ""
    slots_15m_am: str = ""
    slots_15m_pm: str = ""
    slots_1h_am: str = ""
    slots_1h_pm: str = ""
    slots_4h_am: str = ""
    slots_4h_pm: str = ""
    slots_1d_am: str = ""
    slots_1d_pm: str = ""
    # Optional 5m hour-scoped overrides, JSON string:
    # {"10":{"am":[5,10],"pm":[15]}}
    # Keys are 12-hour clock values 1..12; values are AM/PM minute slots.
    slots_5m_hourly: str = ""

    _DURATION_KEY = {300: "slots_5m", 900: "slots_15m", 3600: "slots_1h",
                     14400: "slots_4h", 86400: "slots_1d"}
    _DURATION_KEY_AM = {300: "slots_5m_am", 900: "slots_15m_am", 3600: "slots_1h_am",
                        14400: "slots_4h_am", 86400: "slots_1d_am"}
    _DURATION_KEY_PM = {300: "slots_5m_pm", 900: "slots_15m_pm", 3600: "slots_1h_pm",
                        14400: "slots_4h_pm", 86400: "slots_1d_pm"}
    _ET = ZoneInfo("America/New_York")
    _SLOTS_5M_HOURLY_CACHE_RAW: str = field(default="", init=False, repr=False)
    _SLOTS_5M_HOURLY_CACHE: dict[int, dict[str, set[int]]] = field(default_factory=dict, init=False, repr=False)

    @staticmethod
    def _parse_slots(csv: str) -> set:
        if not csv:
            return set()
        return {int(m.strip()) for m in csv.split(",") if m.strip().isdigit()}

    @staticmethod
    def _sanitize_slot_values(values, *, step: int, max_slot: int) -> set[int]:
        if not isinstance(values, (list, tuple, set)):
            return set()
        out: set[int] = set()
        for raw in values:
            try:
                slot = int(raw)
            except Exception:
                continue
            if slot < 0 or slot > max_slot:
                continue
            if slot % step != 0:
                continue
            out.add(slot)
        return out

    def _parse_5m_hourly(self) -> dict[int, dict[str, set[int]]]:
        raw = str(getattr(self, "slots_5m_hourly", "") or "").strip()
        if raw == self._SLOTS_5M_HOURLY_CACHE_RAW:
            return self._SLOTS_5M_HOURLY_CACHE

        parsed: dict[int, dict[str, set[int]]] = {}
        if raw:
            try:
                obj = json.loads(raw)
            except Exception:
                obj = {}
            if isinstance(obj, dict):
                for hour_raw, row in obj.items():
                    try:
                        hour_12 = int(hour_raw)
                    except Exception:
                        continue
                    if hour_12 < 1 or hour_12 > 12 or not isinstance(row, dict):
                        continue
                    am_slots = self._sanitize_slot_values(row.get("am"), step=5, max_slot=55)
                    pm_slots = self._sanitize_slot_values(row.get("pm"), step=5, max_slot=55)
                    if am_slots or pm_slots:
                        parsed[hour_12] = {"am": am_slots, "pm": pm_slots}

        self._SLOTS_5M_HOURLY_CACHE_RAW = raw
        self._SLOTS_5M_HOURLY_CACHE = parsed
        return parsed

    def blocked_slots_5m_hourly(self) -> dict[str, dict[str, list[int]]]:
        parsed = self._parse_5m_hourly()
        out: dict[str, dict[str, list[int]]] = {}
        for hour_12 in sorted(parsed):
            row = parsed.get(hour_12, {})
            am_slots = sorted(row.get("am", set()))
            pm_slots = sorted(row.get("pm", set()))
            if am_slots or pm_slots:
                out[str(hour_12)] = {"am": am_slots, "pm": pm_slots}
        return out

    def blocked_slots_5m_for_hour(self, hour_12: int, session: str) -> set[int]:
        if session not in ("am", "pm"):
            return set()
        try:
            h = int(hour_12)
        except Exception:
            return set()
        if h < 1 or h > 12:
            return set()
        parsed = self._parse_5m_hourly()
        return set((parsed.get(h) or {}).get(session, set()))

    def blocked_slots(self, duration_seconds: int, session: str = "all") -> set:
        """Return set of blacklisted minute-of-hour values for a given market duration."""
        if session == "am":
            field_name = self._DURATION_KEY_AM.get(duration_seconds, "")
        elif session == "pm":
            field_name = self._DURATION_KEY_PM.get(duration_seconds, "")
        else:
            field_name = self._DURATION_KEY.get(duration_seconds, "")
        csv = getattr(self, field_name, "") if field_name else ""
        return self._parse_slots(csv)

    def is_blocked(self, duration_seconds: int, end_minute: int, end_dt=None) -> bool:
        """Check if a settlement slot is blacklisted for this timeframe.

        Applies:
        1) global slot blacklist (`slots_*`)
        2) optional session-specific blacklist (`slots_*_am` / `slots_*_pm`)
           when `end_dt` is provided.
        """
        if end_minute in self.blocked_slots(duration_seconds, "all"):
            return True

        am_slots = self.blocked_slots(duration_seconds, "am")
        pm_slots = self.blocked_slots(duration_seconds, "pm")
        has_hourly_5m = bool(self._parse_5m_hourly()) if int(duration_seconds or 0) == 300 else False
        if not am_slots and not pm_slots and not has_hourly_5m:
            return False
        if end_dt is None:
            return False
        try:
            end_et = end_dt.astimezone(self._ET)
        except Exception:
            return False
        session = "am" if end_et.hour < 12 else "pm"
        if session == "am" and end_minute in am_slots:
            return True
        if session == "pm" and end_minute in pm_slots:
            return True
        if int(duration_seconds or 0) == 300:
            hour_12 = end_et.hour % 12 or 12
            if end_minute in self.blocked_slots_5m_for_hour(hour_12, session):
                return True
        return False


@dataclass
class MonitorConfig:
    """Independent research monitor (separate from trading toggles)."""
    enabled: bool = True
    assets: str = "bitcoin,ethereum,solana,xrp"
    time_frames: str = "5m,15m,1h,4h,1d"
    discovery_interval_seconds: int = 15
    max_events_per_query: int = 250
    pre_start_capture_seconds: int = 15
    post_end_track_seconds: int = 120
    sample_5m_seconds: int = 1
    sample_15m_seconds: int = 1
    sample_1h_seconds: int = 60
    sample_4h_seconds: int = 240
    sample_1d_seconds: int = 1440

    def sample_interval_for(self, duration_seconds: int) -> int:
        if duration_seconds == 300:
            return max(1, int(self.sample_5m_seconds))
        if duration_seconds == 900:
            return max(1, int(self.sample_15m_seconds))
        if duration_seconds == 3600:
            return max(5, int(self.sample_1h_seconds))
        if duration_seconds == 14400:
            return max(5, int(self.sample_4h_seconds))
        if duration_seconds == 86400:
            return max(5, int(self.sample_1d_seconds))
        return 1


@dataclass
class CategoriesConfig:
    """Sports post-market settings.

    Controls the Sports WS feed-based post-market maker that places
    GTC LIMIT BUYs on winning game-outcome tokens.
    """
    sports_enabled: bool = False         # Master toggle for sports post-market
    sports_max_bet: float = 50.0         # $ per sports maker order
    sports_maker_price: float = 0.99     # GTC bid price for sports winners
    sports_window_seconds: int = 120     # How long to leave sports resting order
    sports_poll_interval: float = 2.0    # Seconds between fill-check polls
    sports_leagues: str = "all"          # "all" or csv filter: "nba,nfl,mlb"

    def is_league_allowed(self, league: str) -> bool:
        """Check if a specific sports league is allowed."""
        raw = self.sports_leagues.strip().lower()
        if raw == "all":
            return True
        allowed = {l.strip() for l in raw.split(",") if l.strip()}
        return league.lower() in allowed


@dataclass
class RedemptionConfig:
    auto_redeem: bool = True
    check_interval: int = 60            # Check resolved markets every N seconds
    stale_sell_after_seconds: int = 3600  # Auto-sell stale positions after N seconds (0 = disabled)
    stale_sell_min_price: float = 0.99    # Minimum sell price ($0.99 = sell at $0.99 or better)


@dataclass
class DatabaseConfig:
    path: str = "data/sniper.db"


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: str = "logs/sniper.log"


@dataclass
class Config:
    """Top-level sniper configuration."""
    private_key: str = ""
    wallet_address: str = ""
    api_key: str = ""
    api_secret: str = ""
    api_passphrase: str = ""

    api: ApiConfig = field(default_factory=ApiConfig)
    scanner: ScannerConfig = field(default_factory=ScannerConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    post_end: PostEndConfig = field(default_factory=PostEndConfig)
    offload: OffloadConfig = field(default_factory=OffloadConfig)
    trend: TrendConfig = field(default_factory=TrendConfig)
    blackout: BlackoutConfig = field(default_factory=BlackoutConfig)
    monitor: MonitorConfig = field(default_factory=MonitorConfig)
    categories: CategoriesConfig = field(default_factory=CategoriesConfig)
    redemption: RedemptionConfig = field(default_factory=RedemptionConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def _load_section(data: dict, key: str, cls):
    """Load a YAML section into a dataclass, ignoring unknown keys."""
    section = data.get(key) or {}
    valid = {f.name for f in cls.__dataclass_fields__.values()}
    return cls(**{k: v for k, v in section.items() if k in valid})


def load_config(config_path: Optional[str] = None, env_path: Optional[str] = None) -> "Config":
    """Load config from config.yaml + .env -> Config object."""
    env_file = Path(env_path) if env_path else PROJECT_ROOT / ".env"
    if env_file.exists():
        load_dotenv(env_file)

    yaml_file = Path(config_path) if config_path else PROJECT_ROOT / "config.yaml"
    data = {}
    if yaml_file.exists():
        with open(yaml_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

    _s = lambda k, c: _load_section(data, k, c)

    return Config(
        private_key=os.getenv("POLYMARKET_PRIVATE_KEY", ""),
        wallet_address=os.getenv("POLYMARKET_WALLET_ADDRESS", ""),
        api_key=os.getenv("POLYMARKET_API_KEY", ""),
        api_secret=os.getenv("POLYMARKET_API_SECRET", ""),
        api_passphrase=os.getenv("POLYMARKET_API_PASSPHRASE", ""),
        api=_s("api", ApiConfig),
        scanner=_s("scanner", ScannerConfig),
        execution=_s("execution", ExecutionConfig),
        post_end=_s("post_end", PostEndConfig),
        offload=_s("offload", OffloadConfig),
        trend=_s("trend", TrendConfig),
        blackout=_s("blackout", BlackoutConfig),
        monitor=_s("monitor", MonitorConfig),
        categories=_s("categories", CategoriesConfig),
        redemption=_s("redemption", RedemptionConfig),
        database=_s("database", DatabaseConfig),
        logging=_s("logging", LoggingConfig),
    )
