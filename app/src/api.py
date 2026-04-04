"""Polymarket API client -- focused on sniper operations.

Uses py-clob-client + aiohttp. Wallet credentials loaded from .env.
"""

import asyncio
import logging
from typing import Optional

import aiohttp
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds, AssetType, BalanceAllowanceParams,
    MarketOrderArgs, OrderArgs, OrderType, TradeParams,
)
from py_clob_client.order_builder.constants import BUY, SELL
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware

from src.config import Config

logger = logging.getLogger(__name__)

# -- Patch py-clob-client UA --
from py_clob_client.http_helpers import helpers as _clob_helpers

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)
_orig_overload = _clob_helpers.overloadHeaders


def _patched_overload(method: str, headers: dict) -> dict:
    headers = _orig_overload(method, headers)
    headers["User-Agent"] = _BROWSER_UA
    if method == "POST":
        headers["Accept-Encoding"] = "gzip, deflate"
    return headers


_clob_helpers.overloadHeaders = _patched_overload

_TIMEOUT = aiohttp.ClientTimeout(total=15)

# -- On-chain constants --
POLYGON_RPC = "https://polygon-bor-rpc.publicnode.com"
CTF_CONTRACT = Web3.to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
USDC_E = Web3.to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
NEGRISK_ADAPTER = Web3.to_checksum_address("0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296")
CTF_EXCHANGE = Web3.to_checksum_address("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E")
NEGRISK_CTF_EXCHANGE = Web3.to_checksum_address("0xC5d563A36AE78145C45a50134d48A1215220f80a")

# ERC-20 Transfer(address indexed from, address indexed to, uint256 value)
_ERC20_TRANSFER_TOPIC = Web3.keccak(text="Transfer(address,address,uint256)")

_CTF_ABI = [
    {
        "name": "redeemPositions", "type": "function", "stateMutability": "nonpayable",
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "outputs": [],
    },
    {
        "name": "isApprovedForAll", "type": "function", "stateMutability": "view",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
    {
        "name": "setApprovalForAll", "type": "function", "stateMutability": "nonpayable",
        "inputs": [
            {"name": "operator", "type": "address"},
            {"name": "approved", "type": "bool"},
        ],
        "outputs": [],
    },
    {
        "name": "balanceOf", "type": "function", "stateMutability": "view",
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

_NEGRISK_ADAPTER_ABI = [
    {
        "name": "redeemPositions", "type": "function", "stateMutability": "nonpayable",
        "inputs": [
            {"name": "_conditionId", "type": "bytes32"},
            {"name": "_amounts", "type": "uint256[]"},
        ],
        "outputs": [],
    },
]


class PolymarketAPI:
    """Async Polymarket API client for the sniper."""

    _REQUEST_CONCURRENCY = 5
    _RETRY_STATUSES = {429, 500, 502, 503, 504}
    _MAX_RETRIES = 3
    _INITIAL_BACKOFF = 1.0

    def __init__(self, config: Config):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._clob_client: Optional[ClobClient] = None
        self._request_sem: Optional[asyncio.Semaphore] = None

    async def start(self):
        """Initialize HTTP session and CLOB client."""
        headers = {"User-Agent": _BROWSER_UA, "Accept-Encoding": "gzip, deflate"}
        # DNS cache: avoid repeated lookups for the same CLOB/Gamma/Data hosts
        connector = aiohttp.TCPConnector(
            ttl_dns_cache=300,   # cache DNS results for 5 minutes
            limit=20,            # connection pool size
            keepalive_timeout=30,
        )
        self._session = aiohttp.ClientSession(
            timeout=_TIMEOUT, headers=headers, connector=connector,
        )
        self._request_sem = asyncio.Semaphore(self._REQUEST_CONCURRENCY)

        if self.config.private_key:
            self._clob_client = ClobClient(
                self.config.api.clob,
                key=self.config.private_key,
                chain_id=self.config.api.chain_id,
                signature_type=self.config.api.signature_type,
                funder=self.config.wallet_address,
            )
            if self.config.api_key:
                self._clob_client.set_api_creds(ApiCreds(
                    api_key=self.config.api_key,
                    api_secret=self.config.api_secret,
                    api_passphrase=self.config.api_passphrase,
                ))
            try:
                self._clob_client.update_balance_allowance(
                    BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                )
                logger.info("CLOB collateral allowance set")
            except Exception as e:
                logger.warning("CLOB allowance failed (non-fatal): %s", e)

            # ERC-1155 approval for exchange operators
            for _attempt in range(3):
                try:
                    self._ensure_conditional_approval()
                    break
                except Exception as e:
                    logger.warning("Conditional approval attempt %d/3: %s", _attempt + 1, e)
                    if _attempt < 2:
                        await asyncio.sleep(2 ** _attempt)

            logger.info("API initialized (wallet: %s...)", self.config.wallet_address[:10])
        else:
            logger.warning("No private key -- read-only mode")

    async def stop(self):
        """Clean up HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("API stopped")

    # -- HTTP helpers --

    async def _get(self, url: str, params: dict | None = None) -> dict | list:
        """GET with retry + exponential backoff + concurrency gate."""
        backoff = self._INITIAL_BACKOFF
        last_error = None
        for attempt in range(self._MAX_RETRIES + 1):
            need_retry = False
            try:
                async with self._request_sem:
                    async with self._session.get(url, params=params) as resp:
                        if resp.status in self._RETRY_STATUSES:
                            body = await resp.text()
                            last_error = aiohttp.ClientResponseError(
                                resp.request_info, resp.history,
                                status=resp.status, message=body[:200],
                            )
                            if attempt < self._MAX_RETRIES:
                                need_retry = True
                            else:
                                resp.raise_for_status()
                        else:
                            resp.raise_for_status()
                            return await resp.json()
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < self._MAX_RETRIES:
                    need_retry = True
            if need_retry:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
        if last_error:
            raise last_error
        raise RuntimeError(f"GET {url} failed after {self._MAX_RETRIES} retries")

    # -- Market Discovery (Gamma API) --

    async def search_events(
        self,
        tag: str | None = None,
        tag_id: str | int | None = None,
        slug: str | None = None,
        active: bool = True,
        closed: bool = False,
        limit: int = 50,
        offset: int = 0,
        end_date_min: str | None = None,
        end_date_max: str | None = None,
        order: str | None = None,
        ascending: bool = True,
    ) -> list[dict]:
        """Search Gamma API for events matching criteria.

        Returns list of event dicts, each with embedded `markets` array.
        Each market has: conditionId, question, outcomes, outcomePrices,
        clobTokenIds, endDate, closed, active, etc.

        Args:
            tag:           Filter by tag label (e.g. "Crypto").
                           NOTE: tag labels are currently unreliable on Gamma;
                           prefer ``tag_id`` for accurate filtering.
            tag_id:        Filter by numeric tag ID (e.g. 1 for "Sports",
                           102127 for "Up or Down").
            slug:          Exact event slug for direct lookup.
            end_date_min:  ISO-8601 min end date filter.
            end_date_max:  ISO-8601 max end date filter.
            order:         Sort field (e.g. "endDate", "createdAt").
            ascending:     Sort direction (default ascending).
        """
        url = f"{self.config.api.gamma}/events"
        params: dict[str, str | int] = {"limit": limit, "offset": offset}
        # When doing a slug lookup, skip active/closed filters so we
        # find the event regardless of its current state.
        if not slug:
            params["active"] = str(active).lower()
            params["closed"] = str(closed).lower()
        if slug:
            params["slug"] = slug
        if tag:
            params["tag"] = tag
        if tag_id is not None:
            params["tag_id"] = str(tag_id)
        if end_date_min:
            params["end_date_min"] = end_date_min
        if end_date_max:
            params["end_date_max"] = end_date_max
        if order:
            params["order"] = order
            params["ascending"] = str(ascending).lower()
        try:
            result = await self._get(url, params)
            return result if isinstance(result, list) else []
        except Exception as e:
            logger.warning("Event search failed: %s", e)
            return []

    async def get_market(self, condition_id: str) -> dict | None:
        """Get a single market from the CLOB API (authoritative source)."""
        try:
            url = f"{self.config.api.clob}/markets/{condition_id}"
            result = await self._get(url)
            return result if isinstance(result, dict) else None
        except Exception as e:
            logger.debug("CLOB market lookup failed for %s: %s", condition_id[:16], e)
            return None

    async def get_gamma_market(self, condition_id: str) -> dict | None:
        """Get a single market from Gamma API (volume/metadata)."""
        try:
            url = f"{self.config.api.gamma}/markets"
            params = {"condition_id": condition_id, "limit": 1}
            result = await self._get(url, params)
            if isinstance(result, list) and result:
                m = result[0]
                if m.get("conditionId", "").lower() == condition_id.lower():
                    return m
            return None
        except Exception as e:
            logger.debug("Gamma market lookup failed for %s: %s", condition_id[:16], e)
            return None

    # -- Orderbook (CLOB API) --

    async def get_book_liquidity(self, token_id: str) -> dict:
        """Get orderbook liquidity stats for a token.

        Returns: {bid_depth, ask_depth, best_bid, best_ask, spread_pct, midpoint}
        """
        _EMPTY = {"bid_depth": 0, "ask_depth": 0, "best_bid": 0, "best_ask": 0,
                   "spread_pct": 1.0, "midpoint": 0}
        try:
            url = f"{self.config.api.clob}/book"
            params = {"token_id": token_id}
            book = await self._get(url, params)
            if not book:
                return _EMPTY

            bids = book.get("bids", [])
            asks = book.get("asks", [])

            bid_depth = sum(float(b["price"]) * float(b["size"]) for b in bids)
            ask_depth = sum(float(a["price"]) * float(a["size"]) for a in asks)
            best_bid = max((float(b["price"]) for b in bids), default=0)
            best_ask = min((float(a["price"]) for a in asks), default=0)

            if best_bid > 0 and best_ask > 0:
                midpoint = (best_bid + best_ask) / 2
                spread_pct = (best_ask - best_bid) / midpoint if midpoint > 0 else 1.0
            else:
                midpoint = best_bid or best_ask or 0
                spread_pct = 1.0

            return {
                "bid_depth": round(bid_depth, 2),
                "ask_depth": round(ask_depth, 2),
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread_pct": round(spread_pct, 4),
                "midpoint": midpoint,
                "asks": asks,
            }
        except Exception as e:
            logger.debug("Book liquidity fetch failed for %s: %s", token_id[:16], e)
            return _EMPTY

    async def get_book_bids(self, token_id: str, min_price: float = 0.99) -> dict:
        """Get bid-side liquidity at or above min_price for a token.

        Returns: {total_size: float, best_bid: float, bids_at_min: float}
            total_size = total shares bid at >= min_price
            best_bid = highest bid price
            bids_at_min = number of distinct bid levels at >= min_price
        """
        _EMPTY = {"total_size": 0.0, "best_bid": 0.0, "bids_at_min": 0}
        try:
            url = f"{self.config.api.clob}/book"
            params = {"token_id": token_id}
            book = await self._get(url, params)
            if not book:
                return _EMPTY

            bids = book.get("bids", [])
            total_size = 0.0
            best_bid = 0.0
            levels = 0
            for b in bids:
                p = float(b["price"])
                s = float(b["size"])
                if p >= min_price:
                    total_size += s
                    levels += 1
                    if p > best_bid:
                        best_bid = p
            return {"total_size": total_size, "best_bid": best_bid, "bids_at_min": levels}
        except Exception as e:
            logger.debug("Book bids fetch failed for %s: %s", token_id[:16], e)
            return _EMPTY

    # -- Trading (CLOB API) --

    def get_balance(self) -> float:
        """USDC.e balance in dollars."""
        if not self._clob_client:
            return 0.0
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            ba = self._clob_client.get_balance_allowance(params)
            raw = float(ba.get("balance", 0)) if isinstance(ba, dict) else 0.0
            return raw / 1e6  # USDC.e uses 6 decimals on Polygon
        except Exception as e:
            logger.debug("get_balance failed: %s", e)
            return 0.0

    async def refresh_balance(self) -> float:
        """Force re-read on-chain balance."""
        if not self._clob_client:
            return 0.0
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            self._clob_client.update_balance_allowance(params)
            await asyncio.sleep(2)
            return self.get_balance()
        except Exception as e:
            logger.warning("Balance refresh failed: %s", e)
            return self.get_balance()

    def place_market_buy(self, token_id: str, amount: float,
                         max_price: float = 0) -> dict:
        """Place a FOK market BUY order.

        Args:
            token_id: The outcome token to buy.
            amount: Dollar amount to spend.
            max_price: Price ceiling per share.  When >0 the CLOB will reject
                       any fill above this price (enforced on-chain via the
                       signed order amounts).  When 0 the client falls back to
                       sweeping the full book (original behaviour).

        Returns:
            Order result dict from CLOB.
        """
        if not self._clob_client:
            raise RuntimeError("CLOB client not initialized -- no private key")
        # Pre-round to 2 dp: the CLOB server enforces maker ≤ 2 decimals
        # for FOK orders, and py-clob-client may not round sufficiently
        # for tokens with tick_size < 0.01.
        import math
        amount = math.floor(amount * 100) / 100
        order = MarketOrderArgs(
            token_id=token_id,
            amount=amount,
            side=BUY,
            order_type=OrderType.FOK,
            price=max_price,
        )
        signed = self._clob_client.create_market_order(order)
        result = self._clob_client.post_order(signed, OrderType.FOK)
        logger.info("SNIPE BUY: $%.2f @ ceil $%.4f of token %s...",
                    amount, max_price, token_id[:16])
        return result

    def place_limit_buy(self, token_id: str, price: float, amount: float) -> dict:
        """Place a GTC limit BUY order (used by executor's FOK→GTC fallback on thin books).

        Args:
            token_id: The outcome token to buy.
            price: Limit price per share (edge-capped by executor).
            amount: Dollar amount to spend.

        Returns:
            Order result dict from CLOB (includes 'orderID' if accepted).
        """
        if not self._clob_client:
            raise RuntimeError("CLOB client not initialized -- no private key")
        import math
        size = math.ceil(amount / price * 100) / 100  # Round UP to ensure total >= $1 CLOB min
        order = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=BUY,
        )
        signed = self._clob_client.create_order(order)
        result = self._clob_client.post_order(signed, OrderType.GTC)
        logger.info("GTC BUY: $%.2f for %.2f shares @ $%.2f of token %s...",
                     amount, size, price, token_id[:16])
        return result

    def place_limit_sell(self, token_id: str, price: float, size: float) -> dict:
        """Place a GTC limit SELL order (used by stale-position auto-seller).

        Args:
            token_id: The outcome token to sell.
            price: Minimum acceptable price per share.
            size: Number of shares to sell.

        Returns:
            Order result dict from CLOB (includes 'orderID' if accepted).
        """
        if not self._clob_client:
            raise RuntimeError("CLOB client not initialized -- no private key")
        import math
        size = math.floor(size * 1000000) / 1000000  # Round DOWN to avoid selling more than we have
        order = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=SELL,
        )
        signed = self._clob_client.create_order(order)
        result = self._clob_client.post_order(signed, OrderType.GTC)
        logger.info("GTC SELL: %.2f shares @ $%.2f of token %s...",
                     size, price, token_id[:16])
        return result

    def place_fok_sell(self, token_id: str, price: float, size: float) -> dict:
        """Place a FOK limit SELL order (used for instant dumping)."""
        if not self._clob_client:
            raise RuntimeError("CLOB client not initialized -- no private key")
        import math
        size = math.floor(size * 1000000) / 1000000  # Round DOWN to avoid selling more than we have
        order = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=SELL,
        )
        signed = self._clob_client.create_order(order)
        result = self._clob_client.post_order(signed, OrderType.FOK)
        logger.info("FOK SELL DUMP: %.2f shares @ min $%.2f of token %s...",
                     size, price, token_id[:16])
        return result

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open CLOB order. Returns True on success."""
        if not self._clob_client:
            return False
        try:
            self._clob_client.cancel(order_id)
            return True
        except Exception as e:
            logger.debug("cancel_order(%s) failed: %s", order_id[:16], e)
            return False

    def get_order_status(self, order_id: str) -> dict:
        """Get current status of a CLOB order."""
        if not self._clob_client:
            return {}
        try:
            return self._clob_client.get_order(order_id)
        except Exception as e:
            logger.debug("get_order(%s) failed: %s", order_id[:16], e)
            return {}

    def get_order_trades(
        self,
        order_id: str,
        *,
        token_id: str | None = None,
        lookback_seconds: int = 900,
    ) -> list[dict]:
        """Fetch recent user trades and return rows linked to order_id.

        This is used as a fill-truth fallback when order status payloads do not
        include expanded trade details.
        """
        if not self._clob_client:
            return []
        try:
            import time
            after_ts = max(0, int(time.time()) - max(60, int(lookback_seconds)))
            params = TradeParams(
                asset_id=token_id,
                maker_address=self.config.wallet_address or None,
                after=after_ts,
            )
            rows = self._clob_client.get_trades(params=params)
            if not isinstance(rows, list):
                return []

            oid = str(order_id or "")
            if not oid:
                return []

            matched: list[dict] = []
            for tr in rows:
                if not isinstance(tr, dict):
                    continue
                taker = str(
                    tr.get("taker_order_id")
                    or tr.get("takerOrderId")
                    or ""
                )
                if taker == oid:
                    matched.append(tr)
                    continue
                makers = tr.get("maker_orders") or tr.get("makerOrders") or []
                if isinstance(makers, str):
                    makers = [makers]
                if isinstance(makers, list) and oid in {str(x) for x in makers}:
                    matched.append(tr)
            return matched
        except Exception as e:
            logger.debug("get_order_trades(%s) failed: %s", order_id[:16], e)
            return []

    # -- Positions (Data API) --

    async def get_positions(self, wallet_address: str, limit: int = 100, offset: int = 0) -> list[dict]:
        """Fetch positions for a wallet."""
        url = f"{self.config.api.data}/positions"
        params = {"user": wallet_address, "sizeThreshold": 0, "limit": limit, "offset": offset}
        return await self._get(url, params)

    async def get_all_positions(self, wallet_address: str, page_size: int = 500) -> list[dict]:
        """Fetch ALL positions (auto-paginating)."""
        all_pos: list[dict] = []
        offset = 0
        while True:
            page = await self.get_positions(wallet_address, limit=page_size, offset=offset)
            all_pos.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
        return all_pos

    # -- On-chain Redemption --

    def _web3_ctf(self):
        w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        acct = w3.eth.account.from_key(self.config.private_key)
        ctf = w3.eth.contract(address=CTF_CONTRACT, abi=_CTF_ABI)
        return w3, acct, ctf

    def _ensure_conditional_approval(self) -> None:
        """Set ERC-1155 approval for exchange operators."""
        w3, acct, ctf = self._web3_ctf()
        for label, operator in [
            ("CTF Exchange", CTF_EXCHANGE),
            ("NegRisk CTF Exchange", NEGRISK_CTF_EXCHANGE),
            ("NegRisk Adapter", NEGRISK_ADAPTER),
        ]:
            if ctf.functions.isApprovedForAll(acct.address, operator).call():
                continue
            tx = ctf.functions.setApprovalForAll(operator, True).build_transaction({
                "from": acct.address,
                "nonce": w3.eth.get_transaction_count(acct.address),
                "gas": 100_000, "gasPrice": w3.eth.gas_price, "chainId": 137,
            })
            signed = acct.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
            if receipt["status"] != 1:
                raise RuntimeError(f"setApprovalForAll reverted for {label}")
            logger.info("ERC-1155 approval set for %s | tx: %s", label, tx_hash.hex())

    @staticmethod
    def _parse_usdc_payout(receipt, wallet_address: str) -> float:
        """Parse USDC.e Transfer logs from a redemption receipt.

        Returns the total USDC received by wallet_address (float, 6-decimal adjusted).
        This is the on-chain source of truth for won/lost: >0 means won, 0 means lost.
        """
        payout_raw = 0
        wallet_lower = wallet_address.lower()
        for log in receipt.get("logs", []):
            addr = log.get("address", "")
            if addr.lower() != USDC_E.lower():
                continue
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            if topics[0] != _ERC20_TRANSFER_TOPIC:
                continue
            # topics[2] = 'to' address (right-padded 32 bytes)
            to_addr = "0x" + topics[2].hex()[-40:]
            if to_addr.lower() == wallet_lower:
                value = int(log["data"].hex(), 16) if isinstance(log["data"], bytes) else int(log["data"], 16)
                payout_raw += value
        return payout_raw / 1e6  # USDC.e has 6 decimals

    def _check_pending_clear(self, w3, acct) -> int:
        """Return safe nonce.  Raises if a prior tx is still pending.

        Root-cause fix: when a previous redeem tx is stuck in the mempool,
        sending another with the same nonce causes 'replacement transaction
        underpriced' or piles up stuck txs.  By comparing 'latest' vs
        'pending' nonce we detect this and bail out — the outer redeemer
        loop retries next cycle (60 s) by which time the tx has usually mined.
        """
        latest = w3.eth.get_transaction_count(acct.address, "latest")
        try:
            pending = w3.eth.get_transaction_count(acct.address, "pending")
        except Exception:
            pending = latest  # RPC doesn't support 'pending' — assume clear
        if pending > latest:
            raise RuntimeError(
                f"Skipping redeem: {pending - latest} pending tx(s) in mempool, "
                "waiting for chain to clear"
            )
        return latest

    def redeem_positions_onchain(self, condition_id: str, index_sets: list[int] | None = None) -> dict:
        """Redeem resolved positions via CTF contract.

        Returns: {"tx_hash": str, "payout_usdc": float}
            payout_usdc > 0 means we won, == 0 means we lost.
        """
        if not self.config.private_key:
            raise RuntimeError("No private key -- cannot redeem")
        if index_sets is None:
            index_sets = [1, 2]
        w3, acct, ctf = self._web3_ctf()
        nonce = self._check_pending_clear(w3, acct)
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        gas_price = max(w3.eth.gas_price, w3.to_wei(30, "gwei"))
        gas_price = int(gas_price * 1.2)  # 20% premium to reduce timeout risk
        tx = ctf.functions.redeemPositions(
            USDC_E, b'\x00' * 32, cid_bytes, index_sets,
        ).build_transaction({
            "from": acct.address,
            "nonce": nonce,
            "gas": 300_000, "gasPrice": gas_price, "chainId": 137,
        })
        signed = acct.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
        if receipt["status"] != 1:
            raise RuntimeError(f"Redemption tx reverted: {tx_hash.hex()}")
        payout = self._parse_usdc_payout(receipt, acct.address)
        logger.info("[FEED: REDEEMED] %s... | payout $%.6f | tx: %s",
                    condition_id[:18], payout, tx_hash.hex())
        return {"tx_hash": tx_hash.hex(), "payout_usdc": payout}

    def redeem_negrisk_onchain(self, condition_id: str, token_id: str, outcome_index: int) -> dict | str:
        """Redeem neg-risk position via NegRiskAdapter.

        Returns: {"tx_hash": str, "payout_usdc": float} or "already_redeemed".
        """
        if not self.config.private_key:
            raise RuntimeError("No private key -- cannot redeem")
        w3, acct, ctf = self._web3_ctf()
        token_id_int = int(token_id)
        balance = ctf.functions.balanceOf(acct.address, token_id_int).call()
        if balance == 0:
            logger.info("[FEED: REDEEMED] NegRisk balance 0 for token %s -- already redeemed", str(token_id)[:16])
            return "already_redeemed"
        nonce = self._check_pending_clear(w3, acct)
        amounts = [balance, 0] if outcome_index == 0 else [0, balance]
        cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
        adapter = w3.eth.contract(address=NEGRISK_ADAPTER, abi=_NEGRISK_ADAPTER_ABI)
        gas_price = max(w3.eth.gas_price, w3.to_wei(30, "gwei"))
        gas_price = int(gas_price * 1.2)  # 20% premium to reduce timeout risk
        tx = adapter.functions.redeemPositions(cid_bytes, amounts).build_transaction({
            "from": acct.address,
            "nonce": nonce,
            "gas": 300_000, "gasPrice": gas_price, "chainId": 137,
        })
        signed = acct.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
        if receipt["status"] != 1:
            raise RuntimeError(f"NegRisk redemption reverted: {tx_hash.hex()}")
        payout = self._parse_usdc_payout(receipt, acct.address)
        logger.info("[FEED: REDEEMED] NegRisk %s... outcome=%d | payout $%.6f | tx: %s",
                     condition_id[:18], outcome_index, payout, tx_hash.hex())
        return {"tx_hash": tx_hash.hex(), "payout_usdc": payout}
