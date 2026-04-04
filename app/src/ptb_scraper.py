"""PTB Scraper — fetch settlement/PTB data from Polymarket.

Three-stage fallback:
  0. Crypto-price API (/api/crypto/crypto-price) — ~1.2s, ~133 bytes, ALL assets
  1. Direct JSON API  (/api/past-results)         — ~0.8s, ~800 bytes
  2. Page scrape       (SSR dehydratedState)       — ~2s, 0.5-2MB

The crypto-price endpoint (discovered 2026-02-22) is the fastest path.
It returns {openPrice, closePrice, completed} for ANY asset/timeframe combo
in ~133 bytes — 15,000x smaller than a page scrape.

The direct API works for: BTC 5m, ALL 15m, ALL 4h.
For everything else (ETH/SOL/XRP 5m, ALL 1h) the API returns HTTP 400.
The page scrape bypasses this — PM's SSR server fetches the data internally
and embeds it in the HTML as a React Query dehydrated cache entry.

Provides:
  - get_settlement(): Get settlement price for a just-ended market
  - pre_fetch_current_ptb(): Pre-fetch the PTB (available NOW)
  - get_cached_ptb(): Read from pre-fetch cache (0ms)

Data chain (verified):
  Each market's openPrice = previous market's closePrice (exact match, 0 diff).
  past-results returns the 4 most recent completed periods BEFORE the requested
  currentEventStartTime.  Data is available within ~1s of period end.

Coverage (verified 2026-02-21):
  Crypto-price: ALL assets, ALL timeframes (BTC/ETH/SOL/XRP 5m/15m/1h/4h/1d) ✓
  Direct API:   BTC 5m ✓ | ALL 15m ✓ | ALL 4h ✓
  Page scrape:  ETH/SOL/XRP 5m ✓ | ALL 1h ✓  (Binance 451 geo / "only BTC")
  Slug format:  5m/15m → {asset}-updown-{tf}-{epoch}  (computable)
                1h+    → human slug from Gamma        (must be passed in)
"""

import asyncio
import logging
import re
import time
import urllib.error
import urllib.request
import json
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# Asset name → API symbol
_ASSET_TO_SYMBOL = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "xrp": "XRP",
}

# Duration (seconds) → API variant string
# Verified 2026-02-21: fiveminute (BTC only for 5m), fifteen (all), fourhour (all)
# hourly is valid variant name but hits Binance 451 geo-restriction
_DURATION_TO_VARIANT = {
    300: "fiveminute",   # verified: BTC only (ETH/SOL/XRP → 400)
    900: "fifteen",      # verified: all assets
    3600: "hourly",      # verified name, but Binance-backed → 451 geo-block
    14400: "fourhour",   # verified: all assets
    86400: "daily",      # verified 2026-02-21: all assets
}

_API_URL = "https://polymarket.com/api/past-results"
_CRYPTO_PRICE_URL = "https://polymarket.com/api/crypto/crypto-price"
_PAGE_URL = "https://polymarket.com/event/"   # + slug
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json",
}
_PAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html",
}
_TIMEOUT = 10  # seconds

# Slug construction for epoch-based markets (5m, 15m)
_ASSET_SLUG = {
    "bitcoin": "btc",
    "ethereum": "eth",
    "solana": "sol",
    "xrp": "xrp",
}
_DURATION_SLUG = {
    300: "5m",
    900: "15m",
    # 3600 (1h) and 14400 (4h) use human-readable slugs — can't compute
}


class PTBScraper:
    """Fetches settlement/PTB data from Polymarket.

    Three-stage fallback per request:
      0. Crypto-price API (/api/crypto/crypto-price) — fastest (~1.2s), tiny
         payload (~133 bytes), works for ALL assets + timeframes.  Returns
         both openPrice (PTB) and closePrice (settlement) in one call.
      1. Direct API call (/api/past-results)  — fast (~0.8s), small payload
      2. Page scrape (SSR dehydratedState)    — slower (~2s), but works for ALL
         market combos because PM's server fetches internally, bypassing the
         public API restrictions.

    Maintains a cache keyed by (asset, duration_seconds, start_epoch) so the
    post-end maker can read settlement data with 0ms latency.
    """

    def __init__(self):
        # Cache: (asset, duration_seconds, start_epoch) -> {
        #   "ptb": float,          # openPrice of the queried period
        #   "settlement": float,   # closePrice of the last completed period
        #   "outcome": str,        # "up" or "down"
        #   "fetched_at": float,   # timestamp
        #   "results": list,       # raw results array (API path only)
        #   "source": str,         # "api" or "page_scrape"
        # }
        self._cache: dict[tuple, dict] = {}
        # Track inflight fetches to prevent duplicate concurrent requests
        self._inflight: dict[tuple, asyncio.Event] = {}
        # Negative cache: (asset_lower, duration_seconds) combos where the
        # direct API returns HTTP 400.  These go straight to page scrape.
        self._unsupported: set[tuple[str, int]] = set()

    def _build_url(self, asset: str, duration_seconds: int,
                   start_time_iso: str) -> Optional[str]:
        """Build the past-results API URL."""
        symbol = _ASSET_TO_SYMBOL.get(asset.lower())
        if not symbol:
            logger.warning("[FEED: PTB] Unknown asset: %s", asset)
            return None

        variant = _DURATION_TO_VARIANT.get(duration_seconds)
        if not variant:
            logger.warning("[FEED: PTB] Unknown duration: %ds", duration_seconds)
            return None

        return (
            f"{_API_URL}?symbol={symbol}&variant={variant}"
            f"&assetType=crypto&currentEventStartTime={start_time_iso}"
        )

    @staticmethod
    def _epoch_to_iso(epoch: int) -> str:
        """Convert epoch seconds to ISO 8601 UTC string."""
        return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    @staticmethod
    def _iso_to_epoch(iso: str) -> int:
        """Convert ISO 8601 UTC string to epoch seconds."""
        iso = iso.replace("Z", "+00:00")
        return int(datetime.fromisoformat(iso).timestamp())

    @staticmethod
    def _compute_slug(asset: str, duration_seconds: int, epoch: int) -> str:
        """Compute event slug for epoch-based markets (5m, 15m).

        Returns empty string for durations that use human-readable slugs.
        """
        asset_short = _ASSET_SLUG.get(asset.lower(), "")
        tf_label = _DURATION_SLUG.get(duration_seconds, "")
        if asset_short and tf_label:
            return f"{asset_short}-updown-{tf_label}-{epoch}"
        return ""

    # ── Crypto-price API (lightweight, all assets) ────────────────────

    async def _fetch_via_crypto_price(
        self, asset: str, duration_seconds: int, start_epoch: int,
    ) -> Optional[dict]:
        """Stage 0: /api/crypto/crypto-price — ~133 bytes, ALL assets.

        Returns raw response dict {openPrice, closePrice, completed,
        incomplete, cached} or None on failure.

        For ended markets: completed=True, both openPrice + closePrice present.
        For running markets: completed=False, closePrice may be null.
        """
        symbol = _ASSET_TO_SYMBOL.get(asset.lower())
        variant = _DURATION_TO_VARIANT.get(duration_seconds)
        if not symbol or not variant:
            return None

        start_iso = self._epoch_to_iso(start_epoch)
        end_iso = self._epoch_to_iso(start_epoch + duration_seconds)
        url = (
            f"{_CRYPTO_PRICE_URL}?symbol={symbol}"
            f"&eventStartTime={start_iso}"
            f"&variant={variant}"
            f"&endDate={end_iso}"
        )

        loop = asyncio.get_running_loop()
        t0 = time.time()
        try:
            data = await loop.run_in_executor(None, self._http_get, url)
        except urllib.error.HTTPError as he:
            logger.debug(
                "[CRYPTO-PRICE] HTTP %d for %s %ds @%s",
                he.code, asset.upper(), duration_seconds, start_iso,
            )
            return None
        except Exception as e:
            logger.debug(
                "[CRYPTO-PRICE] Failed for %s %ds @%s: %s",
                asset.upper(), duration_seconds, start_iso, e,
            )
            return None

        elapsed_ms = int((time.time() - t0) * 1000)
        if data and data.get("openPrice") is not None:
            data["_fetch_ms"] = elapsed_ms
            logger.debug(
                "[CRYPTO-PRICE] %s %ds @%s in %dms | open=%.6f close=%s %s",
                asset.upper(), duration_seconds, start_iso, elapsed_ms,
                data["openPrice"],
                f'{data["closePrice"]:.6f}' if data.get("closePrice") is not None else "null",
                "completed" if data.get("completed") else "running",
            )
            return data
        return None

    # ── Page scrape (SSR fallback) ──────────────────────────────────────

    async def _scrape_page_data(
        self, event_slug: str, asset: str, duration_seconds: int,
    ) -> Optional[dict]:
        """Scrape SSR past-results data from the event page.

        The PM server-side renderer calls /api/past-results internally
        (bypassing public 400 restrictions) and embeds the result in the
        HTML as a React Query dehydrated cache entry.

        Extracts {openPrice, closePrice} from the SSR data.
        """
        symbol = _ASSET_TO_SYMBOL.get(asset.lower())
        variant = _DURATION_TO_VARIANT.get(duration_seconds)
        if not symbol or not variant or not event_slug:
            return None

        url = f"{_PAGE_URL}{event_slug}"
        loop = asyncio.get_running_loop()
        try:
            html = await loop.run_in_executor(None, self._http_get_page, url)
        except Exception as e:
            logger.warning("[FEED: PTB SCRAPE] Page fetch failed for %s: %s", event_slug, e)
            return None

        # Pattern: "past-results","ETH","fiveminute","2026-..."
        # followed by "openPrice":VALUE,"closePrice":VALUE
        # GREEDY (.*) to match the LAST result (most recent period).
        # Non-greedy (.*?) matched the FIRST/oldest result — wrong by
        # hours for 1h markets, causing incorrect winner determination.
        pattern = (
            rf'"past-results","{re.escape(symbol)}","{re.escape(variant)}"'
            r'.*"openPrice":(\d+\.?\d*),"closePrice":(\w+\.?\d*)'
        )
        m = re.search(pattern, html)
        if not m:
            logger.debug("[FEED: PTB SCRAPE] No past-results match in %s (%d bytes)",
                         event_slug, len(html))
            return None

        open_price = float(m.group(1))
        close_raw = m.group(2)
        close_price = None if close_raw == "null" else float(close_raw)
        return {"openPrice": open_price, "closePrice": close_price}

    async def _scrape_settlement(
        self, event_slug: str, asset: str, duration_seconds: int,
        ended_end_epoch: int,
    ) -> Optional[dict]:
        """Scrape definitive settlement data from a PM event page.

        Two extraction methods tried in order:

          1. /api/series results (BTC) — full results array with
             openPrice + closePrice per completed period.  Find the entry
             matching the ended market and return both values.

          2. past-results flat openPrice (ETH/SOL/XRP) — the openPrice
             on page N = PTB of period N = closePrice of period N-1
             = settlement of the previous period.
             Verified: openPrice(N+1) == closePrice(N) for all assets.

        Returns dict with at least 'settlement_price' key, or None.
        """
        url = f"{_PAGE_URL}{event_slug}"
        loop = asyncio.get_running_loop()
        try:
            html = await loop.run_in_executor(None, self._http_get_page, url)
        except Exception as e:
            logger.warning("[FEED: PTB SCRAPE] Settlement fetch failed for %s: %s",
                           event_slug, e)
            return None

        # ── Method 1: /api/series results (BTC) ──
        # SSR embeds: "results":[{...openPrice, closePrice, outcome...},...]
        series_m = re.search(
            r'/api/series.*?"results":(\[\{.*?\}\])\}', html,
        )
        if series_m:
            try:
                results = json.loads(series_m.group(1))
                # Find entry whose endTime matches the ended market
                for r in reversed(results):
                    r_end = self._iso_to_epoch(r["endTime"])
                    if abs(r_end - ended_end_epoch) <= 5:
                        logger.info(
                            "[FEED: PTB SCRAPE] Settlement via /api/series for %s: "
                            "PTB=%.6f settle=%.6f %s",
                            event_slug, r["openPrice"], r["closePrice"],
                            r.get("outcome", "?"),
                        )
                        return {
                            "settlement_price": r["closePrice"],
                            "ptb": r["openPrice"],
                            "winner": r.get("outcome"),
                            "percent_change": r.get("percentChange", 0),
                            "fetch_time_ms": 0,
                        }
                logger.debug(
                    "[FEED: PTB SCRAPE] /api/series has %d results but none "
                    "match ended_end=%d", len(results), ended_end_epoch,
                )
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.debug("[FEED: PTB SCRAPE] /api/series parse error: %s", e)

        # ── Method 2: past-results flat openPrice (ETH/SOL/XRP) ──
        # The flat data {"openPrice":X,"closePrice":null} appears in each
        # page's SSR.  openPrice = PTB of THIS page's period.
        # When scraping the NEXT period's page, this openPrice
        # = settlement of the PREVIOUS (ended) period.
        symbol = _ASSET_TO_SYMBOL.get(asset.lower())
        variant = _DURATION_TO_VARIANT.get(duration_seconds)
        if symbol and variant:
            pattern = (
                rf'"past-results","{re.escape(symbol)}","{re.escape(variant)}"'
                r'.*?"openPrice":(\d+\.?\d*)'
            )
            m = re.search(pattern, html)
            if m:
                settlement = float(m.group(1))
                logger.info(
                    "[FEED: PTB SCRAPE] Settlement via past-results openPrice "
                    "for %s: %.6f", event_slug, settlement,
                )
                return {
                    "settlement_price": settlement,
                    "ptb": None,
                    "winner": None,
                    "percent_change": 0,
                    "fetch_time_ms": 0,
                }

        logger.warning(
            "[FEED: PTB SCRAPE] No settlement data for %s in %d bytes",
            event_slug, len(html),
        )
        return None

    @staticmethod
    def _http_get_page(url: str) -> str:
        """Synchronous HTML GET (runs in executor thread)."""
        req = urllib.request.Request(url, headers=_PAGE_HEADERS)
        resp = urllib.request.urlopen(req, timeout=_TIMEOUT + 5)  # pages are larger
        return resp.read().decode()

    # ── FAST settlement (single scrape + cached PTB) ────────────────────

    async def fetch_settlement_fast(
        self, asset: str, duration_seconds: int,
        ended_market_start_epoch: int,
        event_slug: str = "",
    ) -> Optional[dict]:
        """Fastest path to settlement: crypto-price API or page scrape + cached PTB.

        Called from the boundary launcher at T+0.05s.  Skips the slow
        two-stage pipeline and returns {settlement_price, ptb, winner,
        percent_change, fetch_time_ms} or None.

        Strategy:
          0. Try crypto-price API (~133 bytes) — returns BOTH openPrice (PTB)
             and closePrice (settlement) for the ended market in one call.
          0.5 NEXT period's crypto-price openPrice = ended market's settlement.
              The next period's openPrice appears faster than the ended
              period's closePrice (same number, faster data path).
          1. Get PTB from pre-fetch cache (0ms — pre-fetched at T-10s).
          2. Compute next slug and scrape NEXT market's page for settlement.
             - BTC: /api/series results → direct closePrice per period.
             - All: past-results flat openPrice → settlement of ended period.
          3. winner = "up" if settlement > ptb else "down".
          4. Return immediately — caller places the bid.
        """
        t0 = time.time()
        market_end_epoch = ended_market_start_epoch + duration_seconds
        cur_key = (asset.lower(), duration_seconds, ended_market_start_epoch)
        next_start_epoch = ended_market_start_epoch + duration_seconds

        # ── Step 0: Parallel crypto-price for ENDED + NEXT period ─────
        # Fire both calls simultaneously (~0.85s instead of ~1.6s sequential).
        # Ended period: if completed → openPrice=PTB, closePrice=settlement.
        # Next period:  openPrice = ended market's settlement (appears faster).
        crypto_ended, crypto_next = await asyncio.gather(
            self._fetch_via_crypto_price(asset, duration_seconds, ended_market_start_epoch),
            self._fetch_via_crypto_price(asset, duration_seconds, next_start_epoch),
            return_exceptions=True,
        )
        if isinstance(crypto_ended, Exception):
            crypto_ended = None
        if isinstance(crypto_next, Exception):
            crypto_next = None

        # Prefer ended period's completed result (both PTB + settlement in one)
        if crypto_ended and crypto_ended.get("completed"):
            open_p = crypto_ended.get("openPrice")
            close_p = crypto_ended.get("closePrice")
            if open_p is not None and close_p is not None:
                fetch_ms = int((time.time() - t0) * 1000)
                # Safety: settle == PTB exactly → stale data
                if abs(close_p - open_p) < 1e-6:
                    logger.warning(
                        "[POST: PRICE] %s — settle==PTB (%.6f) via crypto-price "
                        "— stale, falling through",
                        asset.upper(), open_p,
                    )
                else:
                    result = self._build_settlement_result(open_p, close_p, fetch_ms)
                    self._cache[cur_key] = {
                        "ptb": open_p,
                        "settlement": close_p,
                        "outcome": result["winner"],
                        "fetched_at": time.time(),
                        "fetch_time_ms": fetch_ms,
                        "results": [],
                        "source": "crypto_price",
                    }
                    logger.info(
                        "[POST: PRICE] %s — PTB=%.6f settle=%.6f %s (%.4f%%) "
                        "%dms via crypto-price",
                        asset.upper(), open_p, close_p,
                        result["winner"], abs(result["percent_change"]),
                        fetch_ms,
                    )
                    return result

        # ── Step 0.5: NEXT period's openPrice = ended market's settlement ──
        # The next period's openPrice appears faster on PM's backend than
        # the ended period's closePrice (same number, different source).
        ptb_from_crypto = crypto_ended.get("openPrice") if crypto_ended else None

        if crypto_next and crypto_next.get("openPrice") is not None:
            settle_p = crypto_next["openPrice"]
            ptb_p = ptb_from_crypto  # ended period's openPrice
            if ptb_p is None:
                # Fall back to cache
                cached_c = self._cache.get(cur_key)
                ptb_p = cached_c["ptb"] if cached_c and cached_c.get("ptb") is not None else None
            if ptb_p is not None and abs(settle_p - ptb_p) >= 1e-6:
                fetch_ms = int((time.time() - t0) * 1000)
                result = self._build_settlement_result(ptb_p, settle_p, fetch_ms)
                self._cache[cur_key] = {
                    "ptb": ptb_p,
                    "settlement": settle_p,
                    "outcome": result["winner"],
                    "fetched_at": time.time(),
                    "fetch_time_ms": fetch_ms,
                    "results": [],
                    "source": "crypto_price_next",
                }
                logger.info(
                    "[POST: PRICE] %s — PTB=%.6f settle=%.6f %s (%.4f%%) "
                    "%dms via NEXT period crypto-price",
                    asset.upper(), ptb_p, settle_p,
                    result["winner"], abs(result["percent_change"]),
                    fetch_ms,
                )
                return result

        # ── Step 1: Get cached PTB (pre-fetched at T-10s) ──
        cached = self._cache.get(cur_key)
        ptb = cached["ptb"] if cached and cached.get("ptb") is not None else None
        if ptb is None and ptb_from_crypto is not None:
            ptb = ptb_from_crypto

        # ── Step 2: Scrape NEXT market's page for settlement ──
        next_slug = self._compute_slug(asset, duration_seconds, next_start_epoch)
        if not next_slug:
            next_slug = event_slug  # fallback
        if not next_slug:
            return None

        # Invalidate stale pre-fetch for next period
        next_key = (asset.lower(), duration_seconds, next_start_epoch)
        self._cache.pop(next_key, None)

        scraped = await self._scrape_settlement(
            next_slug, asset, duration_seconds, market_end_epoch,
        )
        fetch_ms = int((time.time() - t0) * 1000)

        if not scraped:
            logger.debug("[POST: PRICE] %s — no settlement from page scrape", asset.upper())
            return None

        settlement = scraped.get("settlement_price")
        if settlement is None:
            return None

        # /api/series path already returns ptb + winner
        if scraped.get("ptb") is not None and scraped.get("winner") is not None:
            scraped["fetch_time_ms"] = fetch_ms
            # Cache for Phase 0 fallback
            self._cache[cur_key] = {
                "ptb": scraped["ptb"],
                "settlement": settlement,
                "outcome": scraped["winner"],
                "fetched_at": time.time(),
                "fetch_time_ms": fetch_ms,
                "results": [],
                "source": "settlement_scrape",
            }
            return scraped

        # past-results path: settlement only, need PTB from cache
        if ptb is None:
            # Last resort: scrape the CURRENT market's page for PTB
            cur_slug = event_slug or self._compute_slug(
                asset, duration_seconds, ended_market_start_epoch,
            )
            if cur_slug:
                cur_scraped = await self._scrape_page_data(
                    cur_slug, asset, duration_seconds,
                )
                if cur_scraped and cur_scraped.get("openPrice") is not None:
                    ptb = cur_scraped["openPrice"]
            fetch_ms = int((time.time() - t0) * 1000)

        if ptb is None:
            logger.debug("[POST: PRICE] %s — settlement=%.6f but no PTB",
                         asset.upper(), settlement)
            return None

        # Safety: settle == PTB exactly → stale data
        if abs(settlement - ptb) < 1e-6:
            logger.warning(
                "[POST: PRICE] %s — settle==PTB (%.6f) — stale, returning None",
                asset.upper(), ptb,
            )
            return None

        result = self._build_settlement_result(ptb, settlement, fetch_ms)
        # Cache for Phase 0 fallback
        self._cache[cur_key] = {
            "ptb": ptb,
            "settlement": settlement,
            "outcome": result["winner"],
            "fetched_at": time.time(),
            "fetch_time_ms": fetch_ms,
            "results": [],
            "source": "settlement_scrape",
        }
        return result

    # ── Main fetch (API + scrape fallback) ──────────────────────────────

    async def fetch_past_results(
        self, asset: str, duration_seconds: int, current_start_epoch: int,
        event_slug: str = "",
    ) -> Optional[dict]:
        """Fetch past-results for a market period.

        Tries direct API first; on HTTP 400 falls back to page scrape.

        Args:
            asset: Asset name (e.g. "bitcoin", "ethereum")
            duration_seconds: Market duration (300, 900, 3600)
            current_start_epoch: Unix epoch of the market window's START time
            event_slug: Event page slug (required for 1h+ page scrape;
                        computed automatically for 5m/15m).

        Returns:
            Dict with 'ptb', 'settlement', 'outcome', etc. or None on failure.
        """
        cache_key = (asset.lower(), duration_seconds, current_start_epoch)

        # Return cached if available
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Deduplicate concurrent fetches
        if cache_key in self._inflight:
            await self._inflight[cache_key].wait()
            return self._cache.get(cache_key)

        event = asyncio.Event()
        self._inflight[cache_key] = event

        try:
            start_iso = self._epoch_to_iso(current_start_epoch)
            combo_key = (asset.lower(), duration_seconds)
            result = None

            # ── Stage 0: Crypto-price API (133 bytes, all assets) ──
            crypto = await self._fetch_via_crypto_price(
                asset, duration_seconds, current_start_epoch,
            )
            if crypto and crypto.get("openPrice") is not None:
                open_p = crypto["openPrice"]
                close_p = crypto.get("closePrice")
                fetch_ms = crypto.get("_fetch_ms", 0)
                outcome = None
                pct_change = 0.0
                if close_p is not None and open_p is not None:
                    outcome = "up" if close_p > open_p else "down"
                    pct_change = ((close_p - open_p) / open_p) * 100 if open_p else 0.0
                result = {
                    "ptb": open_p,
                    "settlement": None,
                    "outcome": outcome,
                    "settlement_start": None,
                    "settlement_end": None,
                    "fetched_at": time.time(),
                    "fetch_time_ms": fetch_ms,
                    "results": [],
                    "source": "crypto_price",
                    "percent_change": pct_change,
                }
                logger.info(
                    "[FEED: PTB] crypto-price %s %ds @%s in %dms | PTB=%.4f close=%s %s",
                    asset.upper(), duration_seconds, start_iso, fetch_ms,
                    open_p,
                    f"{close_p:.6f}" if close_p is not None else "null",
                    outcome or "running",
                )

            # ── Stage 1: Direct API (skip if known-unsupported) ──
            if result is None and combo_key not in self._unsupported:
                result = await self._fetch_via_api(
                    asset, duration_seconds, start_iso, combo_key,
                )

            # ── Stage 2: Page scrape fallback ──
            if result is None:
                slug = event_slug or self._compute_slug(
                    asset, duration_seconds, current_start_epoch,
                )
                if slug:
                    result = await self._fetch_via_scrape(
                        slug, asset, duration_seconds, start_iso,
                    )

            if result:
                self._cache[cache_key] = result
            return result

        finally:
            event.set()
            self._inflight.pop(cache_key, None)

    async def _fetch_via_api(
        self, asset: str, duration_seconds: int, start_iso: str,
        combo_key: tuple,
    ) -> Optional[dict]:
        """Stage 1: direct /api/past-results call."""
        url = self._build_url(asset, duration_seconds, start_iso)
        if not url:
            return None

        t0 = time.time()
        loop = asyncio.get_running_loop()
        try:
            data = await loop.run_in_executor(None, self._http_get, url)
        except urllib.error.HTTPError as he:
            if he.code == 400:
                self._unsupported.add(combo_key)
                body = ""
                try:
                    body = he.read().decode()[:120]
                except Exception:
                    pass
                logger.info(
                    "[FEED: PTB] %s %ds API returns 400 — will use page scrape: %s",
                    asset.upper(), duration_seconds, body,
                )
            else:
                logger.warning("[FEED: PTB] API failed for %s %ds @%s: HTTP %d",
                               asset, duration_seconds, start_iso, he.code)
            return None
        except Exception as e:
            logger.warning("[FEED: PTB] API failed for %s %ds @%s: %s",
                           asset, duration_seconds, start_iso, e)
            return None
        elapsed = time.time() - t0

        if not data or data.get("status") != "success":
            logger.warning("[FEED: PTB] Bad API response for %s: %s",
                           asset, str(data)[:200])
            return None

        results = data.get("data", {}).get("results", [])
        if not results:
            logger.debug("[FEED: PTB] No results for %s %ds @%s",
                         asset, duration_seconds, start_iso)
            return None

        last = results[-1]
        result = {
            "ptb": last["closePrice"],
            "settlement": None,  # API returns PTB, NOT settlement (settlement unknown until market ends)
            "outcome": last["outcome"],
            "settlement_start": last["startTime"],
            "settlement_end": last["endTime"],
            "fetched_at": time.time(),
            "fetch_time_ms": int(elapsed * 1000),
            "results": results,
            "source": "api",
        }
        logger.info(
            "[FEED: PTB] API %s %ds @%s in %dms | PTB=%.4f outcome=%s (%d results)",
            asset.upper(), duration_seconds, start_iso,
            int(elapsed * 1000), last["closePrice"], last["outcome"],
            len(results),
        )
        return result

    async def _fetch_via_scrape(
        self, slug: str, asset: str, duration_seconds: int, start_iso: str,
    ) -> Optional[dict]:
        """Stage 2: scrape event page SSR data."""
        t0 = time.time()
        scraped = await self._scrape_page_data(slug, asset, duration_seconds)
        elapsed = time.time() - t0

        if not scraped or scraped.get("openPrice") is None:
            logger.debug("[FEED: PTB SCRAPE] No data from page %s", slug)
            return None

        open_price = scraped["openPrice"]
        close_price = scraped.get("closePrice")  # None if market still running

        # Determine outcome from the matched result (informational only)
        outcome = None
        pct_change = 0.0
        if close_price is not None and open_price:
            outcome = "up" if close_price > open_price else "down"
            pct_change = ((close_price - open_price) / open_price) * 100

        result = {
            "ptb": open_price,
            "settlement": None,  # Past-results are NOT the current market's settlement
            "outcome": outcome,
            "settlement_start": None,
            "settlement_end": None,
            "fetched_at": time.time(),
            "fetch_time_ms": int(elapsed * 1000),
            "results": [],  # page scrape doesn't return full results
            "source": "page_scrape",
            "percent_change": pct_change,
        }
        logger.info(
            "[FEED: PTB SCRAPE] %s %ds @%s via page %s in %dms | "
            "open=%.6f close=%s outcome=%s",
            asset.upper(), duration_seconds, start_iso, slug,
            int(elapsed * 1000), open_price,
            f"{close_price:.6f}" if close_price is not None else "null",
            outcome or "pending",
        )
        return result

    async def get_settlement(
        self, asset: str, duration_seconds: int,
        ended_market_start_epoch: int,
        event_slug: str = "",
    ) -> Optional[dict]:
        """Get settlement data for a market that just ended.

        Combines multiple data sources to determine settlement AND winner:

          Stage 0 — Crypto-price API: returns BOTH openPrice (PTB) and
                    closePrice (settlement) in ~133 bytes.  Fastest path.
          Stage 0.5 — NEXT period's crypto-price openPrice = settlement.
                    Appears faster than ended period's closePrice.
          Stage 1 — NEXT period's openPrice = current market's settlement.
                    (via API or page scrape of next market's page)
          Stage 2 — CURRENT (ended) market's page: PTB from openPrice,
                    settlement from closePrice (if PM has processed it).

        Args:
            asset: Asset name
            duration_seconds: Market duration
            ended_market_start_epoch: Start epoch of the market that just ended
            event_slug: Slug of the CURRENT (just-ended) market's event page.
                        Used for Stage 2 page scrape.
                        For 5m/15m, auto-computed; for 1h+, must be provided.

        Returns:
            Dict with 'settlement_price', 'ptb', 'winner', 'percent_change',
            'fetch_time_ms' — or None if data unavailable yet.
        """
        # ── Stage 0: Try crypto-price API (133 bytes, all assets) ──────
        crypto = await self._fetch_via_crypto_price(
            asset, duration_seconds, ended_market_start_epoch,
        )
        if crypto and crypto.get("completed"):
            open_p = crypto.get("openPrice")
            close_p = crypto.get("closePrice")
            if (open_p is not None and close_p is not None
                    and abs(close_p - open_p) >= 1e-6):
                fetch_ms = crypto.get("_fetch_ms", 0)
                result = self._build_settlement_result(open_p, close_p, fetch_ms)
                logger.info(
                    "[POST: PRICE] %s — PTB=%.6f settle=%.6f %s (%.4f%%) "
                    "%dms via crypto-price",
                    asset.upper(), open_p, close_p,
                    result["winner"], abs(result["percent_change"]),
                    fetch_ms,
                )
                return result

        # ── Stage 0.5: NEXT period's crypto-price openPrice = settlement ──
        # The next period's openPrice appears faster than the ended
        # period's closePrice (same number, different data path).
        next_start_epoch = ended_market_start_epoch + duration_seconds
        ptb_from_crypto = crypto.get("openPrice") if crypto else None

        next_crypto = await self._fetch_via_crypto_price(
            asset, duration_seconds, next_start_epoch,
        )
        if next_crypto and next_crypto.get("openPrice") is not None:
            settle_p = next_crypto["openPrice"]
            ptb_p = ptb_from_crypto  # ended period's openPrice
            if ptb_p is not None and abs(settle_p - ptb_p) >= 1e-6:
                fetch_ms = next_crypto.get("_fetch_ms", 0)
                result = self._build_settlement_result(ptb_p, settle_p, fetch_ms)
                logger.info(
                    "[POST: PRICE] %s — PTB=%.6f settle=%.6f %s (%.4f%%) "
                    "%dms via NEXT period crypto-price",
                    asset.upper(), ptb_p, settle_p,
                    result["winner"], abs(result["percent_change"]),
                    fetch_ms,
                )
                return result

        # ── Stage 1: Fetch settlement from NEXT period's page ──
        # Two methods (tried by _scrape_settlement):
        #   BTC:          /api/series results → direct closePrice per period
        #   ETH/SOL/XRP:  past-results flat openPrice on NEXT page
        #                 = PTB of next period = closePrice of ended period
        #
        # Both are available IMMEDIATELY when the next period starts (~2s).
        # API fallback kept for resilience (slow, ~90s, but reliable).
        next_data = None
        next_settlement = None
        next_fetch_ms = 0

        next_slug = self._compute_slug(asset, duration_seconds, next_start_epoch)
        market_end_epoch = ended_market_start_epoch + duration_seconds
        if next_slug:
            next_cache_key = (asset.lower(), duration_seconds, next_start_epoch)
            # Use cached ONLY if fetched AFTER market ended AND from
            # a settlement scrape (not a pre-fetch, which has stale data).
            if next_cache_key in self._cache:
                cached_next = self._cache[next_cache_key]
                fetched_at = cached_next.get("fetched_at", 0)
                source = cached_next.get("source", "")
                if (fetched_at >= market_end_epoch
                        and source == "settlement_scrape"
                        and cached_next.get("ptb") is not None):
                    next_settlement = cached_next["ptb"]
                    next_fetch_ms = cached_next.get("fetch_time_ms", 0)
                else:
                    # Stale pre-fetch or wrong source — discard
                    self._cache.pop(next_cache_key, None)
            if next_settlement is None:
                t0 = time.time()
                scraped = await self._scrape_settlement(
                    next_slug, asset, duration_seconds, market_end_epoch,
                )
                fetch_ms = int((time.time() - t0) * 1000)
                if scraped:
                    # /api/series returns full result → can return directly
                    if scraped.get("ptb") is not None and scraped.get("winner") is not None:
                        scraped["fetch_time_ms"] = fetch_ms
                        logger.info(
                            "[POST: PRICE] %s | PTB=%.6f settle=%.6f %s (%.4f%%) %dms "
                            "(via series)",
                            asset.upper(), scraped["ptb"],
                            scraped["settlement_price"],
                            scraped["winner"],
                            abs(scraped["percent_change"]), fetch_ms,
                        )
                        return scraped
                    # past-results path: only settlement, need PTB from Stage 2
                    next_settlement = scraped["settlement_price"]
                    next_fetch_ms = fetch_ms
                    self._cache[next_cache_key] = {
                        "ptb": next_settlement,
                        "settlement": None,
                        "outcome": None,
                        "fetched_at": time.time(),
                        "fetch_time_ms": fetch_ms,
                        "results": [],
                        "source": "settlement_scrape",
                    }

        # 1b. Fall back to API if page scrape didn't yield settlement
        if next_settlement is None:
            next_data = await self.fetch_past_results(
                asset, duration_seconds, next_start_epoch,
            )
            if next_data and next_data.get("source") == "api":
                result = self._settlement_from_api(
                    next_data, ended_market_start_epoch, duration_seconds,
                )
                if result:
                    return result

        # ── Stage 2: Get PTB from CURRENT market (cache or scrape) ──
        cur_key = (asset.lower(), duration_seconds, ended_market_start_epoch)
        cached = self._cache.get(cur_key)

        # Fast path: cached current market has BOTH PTB and settlement
        if cached and cached.get("ptb") is not None and cached.get("settlement") is not None:
            return self._build_settlement_result(
                cached["ptb"], cached["settlement"], cached.get("fetch_time_ms", 0),
            )

        # Fast path: cached PTB + settlement from Stage 1
        if cached and cached.get("ptb") is not None and next_settlement is not None:
            return self._build_settlement_result(
                cached["ptb"], next_settlement, next_fetch_ms,
            )

        # Need to scrape the current (ended) market's page
        slug = event_slug or self._compute_slug(
            asset, duration_seconds, ended_market_start_epoch,
        )
        if slug:
            t0 = time.time()
            scraped = await self._scrape_page_data(slug, asset, duration_seconds)
            elapsed_ms = int((time.time() - t0) * 1000)

            if scraped and scraped.get("openPrice") is not None:
                ptb = scraped["openPrice"]

                # Cache PTB only — past-results closePrice is NOT settlement
                self._cache[cur_key] = {
                    "ptb": ptb,
                    "settlement": None,
                    "outcome": None,
                    "fetched_at": time.time(),
                    "fetch_time_ms": elapsed_ms,
                    "results": [],
                    "source": "page_scrape",
                }

                # Settlement can only come from Stage 1 (next period data)
                if ptb is not None and next_settlement is not None:
                    # Safety: if settle == PTB exactly (0.0000% gap),
                    # the data is almost certainly stale (regex hit
                    # the wrong value or SSR not updated yet).
                    # Crypto 5m markets virtually never close at exactly
                    # the open price to 4+ decimal places.
                    if abs(next_settlement - ptb) < 1e-6:
                        logger.warning(
                            "[POST: PRICE] %s | settle==PTB (%.6f) — "
                            "stale data, returning None to force retry",
                            asset.upper(), ptb,
                        )
                        return None
                    result = self._build_settlement_result(ptb, next_settlement, elapsed_ms)
                    # Enrich cache with full data
                    self._cache[cur_key]["settlement"] = next_settlement
                    self._cache[cur_key]["outcome"] = result["winner"]
                    self._cache[cur_key]["percent_change"] = result["percent_change"]
                    logger.info(
                        "[POST: PRICE] %s | PTB=%.6f settle=%.6f %s (%.4f%%) %dms",
                        asset.upper(), ptb, next_settlement,
                        result["winner"], abs(result["percent_change"]), elapsed_ms,
                    )
                    return result

                logger.debug(
                    "[POST: PRICE] %s: PTB=%.6f but no settlement yet "
                    "(closePrice=null, no next-period data)",
                    asset.upper(), ptb,
                )

        # Last resort: settlement from Stage 1 without PTB (winner unknown)
        if next_settlement is not None:
            logger.debug(
                "[POST: PRICE] %s: settlement=%.6f but no PTB (winner unknown)",
                asset.upper(), next_settlement,
            )
            return {
                "settlement_price": next_settlement,
                "ptb": None,
                "winner": None,
                "percent_change": 0,
                "fetch_time_ms": next_fetch_ms,
            }

        return None

    @staticmethod
    def _build_settlement_result(
        ptb: float, settlement_price: float, fetch_ms: int,
    ) -> dict:
        """Build a settlement result dict with winner computed from PTB vs settlement.

        Per Polymarket rules: "Up" = close > open (strictly above).
        Equal (0% change) resolves as "Down".
        """
        winner = "up" if settlement_price > ptb else "down"
        pct = ((settlement_price - ptb) / ptb) * 100 if ptb else 0
        return {
            "settlement_price": settlement_price,
            "ptb": ptb,
            "winner": winner,
            "percent_change": pct,
            "fetch_time_ms": fetch_ms,
        }

    def _settlement_from_api(self, data: dict, ended_start: int, dur: int) -> Optional[dict]:
        """Extract settlement from API-sourced past-results."""
        results = data.get("results", [])
        if not results:
            return None
        last = results[-1]
        last_end_epoch = self._iso_to_epoch(last["endTime"])
        expected_end = ended_start + dur
        if abs(last_end_epoch - expected_end) > 5:
            logger.debug(
                "[FEED: PTB] Settlement not ready: last result ends %s, expected %s",
                last["endTime"], self._epoch_to_iso(expected_end),
            )
            return None
        return {
            "settlement_price": last["closePrice"],
            "ptb": last["openPrice"],
            "winner": last["outcome"],
            "percent_change": last.get("percentChange", 0),
            "fetch_time_ms": data.get("fetch_time_ms", 0),
        }

    async def pre_fetch_current_ptb(
        self, asset: str, duration_seconds: int, current_start_epoch: int,
        event_slug: str = "",
    ) -> Optional[float]:
        """Pre-fetch and cache the PTB for a currently-running market.

        Call this BEFORE the market ends. The past-results for the current
        period return the previous periods, and the last result's closePrice
        is the PTB for the current period.

        Returns the PTB value, or None on failure.
        """
        data = await self.fetch_past_results(
            asset, duration_seconds, current_start_epoch, event_slug=event_slug,
        )
        if data:
            return data["ptb"]
        return None

    def get_cached_ptb(
        self, asset: str, duration_seconds: int, start_epoch: int
    ) -> Optional[float]:
        """Read PTB from cache (0ms). Returns None if not pre-fetched."""
        cache_key = (asset.lower(), duration_seconds, start_epoch)
        entry = self._cache.get(cache_key)
        return entry["ptb"] if entry else None

    def invalidate(
        self, asset: str, duration_seconds: int, start_epoch: int
    ) -> None:
        """Remove a cache entry (e.g., to force re-fetch after settlement)."""
        cache_key = (asset.lower(), duration_seconds, start_epoch)
        self._cache.pop(cache_key, None)

    def prune_stale(self, max_age_seconds: float = 600) -> int:
        """Remove cache entries older than max_age_seconds. Returns count pruned."""
        now = time.time()
        stale = [k for k, v in self._cache.items()
                 if (now - v.get("fetched_at", 0)) > max_age_seconds]
        for k in stale:
            del self._cache[k]
        return len(stale)

    @staticmethod
    def _http_get(url: str) -> dict:
        """Synchronous HTTP GET (runs in executor thread)."""
        req = urllib.request.Request(url, headers=_HEADERS)
        resp = urllib.request.urlopen(req, timeout=_TIMEOUT)
        return json.loads(resp.read().decode())
