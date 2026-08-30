"""
Clip bot — Kalshi 15-minute Bitcoin (KXBTC15M).

One position. Always on the BTC-agreed favorite. If the 1-tick take
price is still 80–96¢, IOC-sweep it. Otherwise post-only join the
touch. Never open outside that band. Crowded perpetual funding vetoes
joining the packed side (not a 15m price forecast). KXBTC15M is
quadratic M=1 — maker fills are free; taker pays 0.07×C×P×(1−P).
reduce_only is IOC-only. Never dump at 1¢. Never place from ws.recv().
"""

import asyncio
import base64
import contextlib
import json
import logging
import math
import os
import signal
import statistics
import sys
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Optional, TypedDict
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


# ============================================================
# CONFIG
# ============================================================

@dataclass
class Config:
    access_key: str = (
            os.environ.get("KALSHI_ACCESS_KEY")
            or os.environ.get("KALSHI_API_KEY_ID")
            or os.environ.get("KALSHI_KEY_ID")
            or ""
    )
    private_key_path: str = os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")

    rate_read: int = 300
    rate_write: int = 300
    read_cost: int = 10
    write_cost: int = 10
    cancel_cost: int = 2

    # 78 skipped every real lead this window: BTC $75–$156 below, NO already
    # 83–91¢. 62 still refuses coin-flips. 94 sat out $150 NO at 95¢ until
    # it faded to 90 — 96 still refuses 1–3¢ pennies.
    entry_lo: int = 62
    entry_hi: int = 96
    # Stop NEW joins just before flatten. The old 180s cutoff sat out the
    # last 3 minutes of every window. The 29¢ late blowup was outside 62–78.
    entry_cutoff_secs: float = 15.0
    # Rotate only once BTC is through by the same floor used to enter.
    # Hairline with_gap<=0 dumped 10 NO while WATCH still had $3.33 below
    # (−$3.07) then 10 YES while still $5.60 above (−$3.07), then skipped
    # re-entry at $2 vs $25. A clip already this cheap rides expiry
    # (overnight 7¢ NO → 72¢ YES was −$6.69).
    rotate_cheap: float = 15.0
    # Gap must beat recent 5-second noise, not a random-walk to expiry.
    # σ × sqrt(secs_left/5) asked $90–$250 with 12m left and filtered the
    # live $76 NO clip (YES 30–31¢ → 18¢ eight minutes later). 1.5σ of
    # 5s moves was ~$25 today and never bound vs min_btc_gap.
    vol_gap_mult: float = 2.0
    # Hysteresis on the vol bar: an existing rest only needs this fraction
    # of the entry bar to stay up, so the quote doesn't churn on every
    # borderline tick (join→pull cycles fill nothing and burn write tokens).
    vol_keep_frac: float = 0.75
    # A fresh rest lives at least this long before "signal gone" may pull
    # it — a 50ms-old rest never fills. Side flips pull instantly.
    min_rest_age_s: float = 1.5
    # Pacing comes from GET /account/limits at boot. Live 2026-08-27:
    # Advanced, read/write refill 300, bucket 900. Create 10, cancel 2.
    reject_cooldown_s: float = 0.2
    quote_min_gap_s: float = 0.02
    # Fraction of the remaining distance to 100 the maker exit demands.
    # Live records: losses run 10–17¢/contract no matter how tight the stop
    # (binaries reprice in jumps), so 3–5¢ winners can never carry them —
    # winners must ride toward convergence to earn the entry's 80% edge.
    clip_x: float = 0.45
    clip_maker_secs: float = 90.0
    # Green taker only in the last 25s. The 90s/240s age take cut +2¢ out of
    # clips that then paid 15¢+ on the maker rest (overnight 77→79 +$0.08).
    clip_taker_only_secs: float = 25.0
    flatten_secs: float = 8.0
    # BTC side is the filter. 2.0 blocked joining NO at 69¢ on a $76 gap
    # because cheap YES still had 1.85 bid pressure. 0.7 only skips a
    # steamroller against the quote.
    mom_min_pressure: float = 0.7
    # Keep-side pressure tolerance. Book pressure whipsaws 0.4→3.2 within a
    # minute on these thin markets; live pulls showed quotes dying to
    # pressure 0.8 while the BTC edge was 2× the bar. A rest survives until
    # pressure is meaningfully AGAINST it, not merely not-with-it.
    keep_pressure: float = 0.5
    # Once BTC has cleared the floor, trust BTC over the book. 1.5× still
    # sat out a $40 ITM 95¢ YES on 0.59 pressure. The floor is the filter.
    pressure_free_mult: float = 1.0
    # Floor so 2σ can still bind in chop. $40 sat out live 63–68¢ favorites
    # with a $27–$38 BTC lead — the clip — then swept 67–74¢ and fade-dumped.
    min_btc_gap: float = 25.0
    # Crowded-perp veto, not a 15m direction call. 0.55 ann = 0.05%/8h
    # (Kraken/literature crowded-long bar). Hyperliquid BTC funds hourly
    # (verified 60m history). Fail-open if the feed is stale.
    funding_crowd_ann: float = 0.55
    funding_max_age_s: float = 30.0
    clip_size: int = 10
    max_position_per_market: int = 10
    # Max notional per clip as a fraction of shard cash. 10 lots on ~$10
    # was ~30% of the stack per dump. Do not ask for a top-up: size down
    # until cash can eat a full 10-lot (about $50 at 70¢ × 0.2).
    clip_risk_frac: float = 0.2
    dump_cooldown_s: float = 0.08
    max_open_markets: int = 1
    max_daily_loss: float = 500.0
    # Stop for the day once realized PnL is down this fraction of the cash
    # the day started with (absolute max_daily_loss still caps it above).
    # 0.4 of a $12 shard died on one 10-lot fade rotate (~$4.80).
    daily_loss_frac: float = 0.75

    series_ticker: str = "KXBTC15M"
    cycle_min_secs: float = 5.0
    cycle_max_secs: float = 930.0

    heartbeat_s: float = 10.0
    # Memory-only pulse so the log shows the 50Hz loop, not a 10s REST wait.
    watch_s: float = 0.4
    fire_idle_s: float = 0.008
    book_rest_gap_s: float = 0.4
    book_stale_ok_s: float = 2.0
    book_max_age_s: float = 0.6
    btc_max_age_s: float = 1.5
    btc_max_spread: float = 150.0
    read_timeout_s: float = 4.0
    trade_read_timeout_s: float = 0.75
    write_timeout_s: float = 1.25
    ws_url: str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    base_url: str = "https://external-api.kalshi.com/trade-api/v2"
    btc_urls: list = field(default_factory=lambda: [
        "https://api.coinbase.com/v2/prices/BTC-USD/spot",
        "https://api.kraken.com/0/public/Ticker?pair=XBTUSD",
    ])


class MarketCandidate(TypedDict):
    ticker: str
    strike: float
    expiry: datetime
    secs: float
    exchange_index: Optional[int]


cfg = Config()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler("rogue.log", mode="a"), logging.StreamHandler()],
)
log = logging.getLogger("rogue")
# The log carries balances, positions, and PnL — keep it owner-only.
with contextlib.suppress(OSError):
    os.chmod("rogue.log", 0o600)


# ============================================================
# PLAIN-ENGLISH LOG HELPERS
# ============================================================

def _cents_to_dollars(cents: float) -> str:
    return f"{float(cents) / 100:.4f}"


def _dollars_to_cents(value) -> Optional[float]:
    if value is None:
        return None
    return round(float(value) * 100.0, 2)


def _complement(cents: float) -> float:
    return round(100.0 - float(cents), 2)


def _fp_to_float(value) -> float:
    if value is None:
        return 0.0
    return round(float(value), 2)


def _optional_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _order_body(result: Optional[dict]) -> dict:
    # Create/amend responses nest the order under "order" with fp-string
    # fields (fill_count_fp: "10.00", taker_fees_dollars: "0.1750", ...).
    if not isinstance(result, dict):
        return {}
    body = result.get("order")
    return body if isinstance(body, dict) else result


def _order_id(result: Optional[dict]) -> Optional[str]:
    oid = _order_body(result).get("order_id")
    return oid if isinstance(oid, str) else None


def _order_fill(result: Optional[dict]) -> Optional[float]:
    o = _order_body(result)
    value = o.get("fill_count_fp")
    if value is None:
        value = o.get("fill_count")
    return _optional_float(value)


def _order_remaining(result: Optional[dict]) -> Optional[float]:
    o = _order_body(result)
    value = o.get("remaining_count_fp")
    if value is None:
        value = o.get("remaining_count")
    return _optional_float(value)


def _rejected(result: Optional[dict]) -> bool:
    return isinstance(result, dict) and result.get("rejected") is True


def _fmt_usd(value: float) -> str:
    return f"${value:,.2f}"


def _fmt_pnl(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{_fmt_usd(value)}"


def _fmt_secs(secs: float) -> str:
    total = max(0, int(secs))
    minutes, seconds = divmod(total, 60)
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _fmt_strike(strike: float) -> str:
    return f"${strike:,.0f}"


def _fmt_qty(n: float) -> str:
    return f"{n:.2f}".rstrip("0").rstrip(".")


def _pos_words(n: float) -> str:
    if abs(n) < 0.005:
        return "flat"
    if n > 0:
        return f"{_fmt_qty(n)} YES"
    return f"{_fmt_qty(abs(n))} NO"


_fail_log: dict[str, float] = {}


def _log_throttled(key: str, msg: str, every: float = 3.0):
    now = time.monotonic()
    if now - _fail_log.get(key, 0) < every:
        return
    _fail_log[key] = now
    log.info(msg)


def _market_strike(m: dict) -> Optional[float]:
    for key in ("floor_strike", "strike_price", "floor_price"):
        if m.get(key) is not None:
            return float(m[key])
    return None


def _ceil_centicent(x: float) -> float:
    return math.ceil(x * 10000.0 - 1e-12) / 10000.0


def _taker_fee_dollars(count: float, p: float, multiplier: float = 1.0) -> float:
    # kalshi-fee-schedule.pdf: round up(M × 0.07 × C × P × (1−P)) to a centicent
    p = min(0.99, max(0.01, float(p)))
    return _ceil_centicent(multiplier * 0.07 * float(count) * p * (1.0 - p))


def _maker_target_yes(avg: float) -> int:
    p = avg / 100.0
    t = p + cfg.clip_x * (1.0 - p)
    return max(1, min(99, int(math.ceil(t * 100.0))))


def _pick_btc_underlying(tickers) -> Optional[str]:
    scored = []
    for t in tickers or []:
        u = str(t).upper()
        if "BTC" not in u and "XBT" not in u:
            continue
        scored.append((0 if "USD" in u else 1, t))
    scored.sort()
    return scored[0][1] if scored else None


# ============================================================
# RATE LIMITER — Kalshi token buckets (read vs write)
# ============================================================

class TokenBucket:
    def __init__(self, refill_rate: float, capacity: float):
        self.refill_rate = float(refill_rate)
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.ts = time.monotonic()
        self._lock = asyncio.Lock()

    def configure(self, refill_rate: float, capacity: float):
        self.refill_rate = float(refill_rate)
        self.capacity = float(capacity)
        self.tokens = min(self.tokens, self.capacity)

    def _refill(self):
        now = time.monotonic()
        self.tokens = min(
            self.capacity,
            self.tokens + (now - self.ts) * self.refill_rate,
        )
        self.ts = now

    async def acquire(self, n: int) -> bool:
        async with self._lock:
            self._refill()
            cost = float(n)
            if self.tokens >= cost:
                self.tokens -= cost
                return True
            return False

    async def refund(self, n: int):
        async with self._lock:
            self._refill()
            self.tokens = min(self.capacity, self.tokens + float(n))

    async def wait_acquire(self, n: int, timeout: float = 1.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if await self.acquire(n):
                return True
            await asyncio.sleep(0.005)
        return False


read_rl = TokenBucket(cfg.rate_read, cfg.rate_read * 2)
write_rl = TokenBucket(cfg.rate_write, cfg.rate_write * 2)


# ============================================================
# KALSHI CLIENT — RSA Signature Auth
# ============================================================

class Kalshi:
    def __init__(self):
        self.sess: Optional[aiohttp.ClientSession] = None
        self._private_key = None
        self.cash: Optional[float] = None
        # Per-shard balances from balance_breakdown. Orders spend the cash
        # sitting on their market's shard, not the account-wide total.
        self.cash_by_shard: dict[int, float] = {}

    async def connect(self):
        self.sess = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=150, ttl_dns_cache=300),
            timeout=aiohttp.ClientTimeout(total=4),
        )
        self._load_key()

    def _load_key(self):
        with open(cfg.private_key_path, "rb") as f:
            self._private_key = serialization.load_pem_private_key(
                f.read(), password=None
            )

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        message = f"{timestamp_ms}{method}{path}"
        signature = self._private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _headers(self, method: str, path: str, sign_path: Optional[str] = None) -> dict:
        ts = str(int(time.time() * 1000))
        to_sign = sign_path if sign_path is not None else f"/trade-api/v2{path}"
        sig = self._sign(ts, method, to_sign)
        return {
            "KALSHI-ACCESS-KEY": cfg.access_key,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "Content-Type": "application/json",
        }

    def websocket_headers(self) -> dict:
        return self._headers("GET", "/ws", sign_path="/trade-api/ws/v2")

    async def close(self):
        if self.sess:
            await self.sess.close()
            self.sess = None
        await asyncio.sleep(0)

    def can_spend(
            self, count: float, price_cents: float,
            exchange_index: Optional[int] = None,
    ) -> bool:
        cash = self.cash_on(exchange_index)
        if cash is None:
            return True
        p = float(price_cents) / 100.0
        # Reserve the taker fee too — an order that clears the contract cost
        # but not the fee bounces with insufficient_balance.
        need = float(count) * p + _taker_fee_dollars(count, p) + 0.05
        return cash >= need

    async def _call(
            self,
            method: str,
            path: str,
            params: Optional[dict] = None,
            data: Optional[dict] = None,
            write: bool = False,
            cost: int = 10,
            timeout_s: Optional[float] = None,
    ) -> Optional[dict]:
        session = self.sess
        if session is None:
            return None
        bucket = write_rl if write else read_rl
        backoff = 0.033
        wait_s = 2.5 if write else 1.0
        for _ in range(4):
            if not await bucket.wait_acquire(cost, timeout=wait_s):
                _log_throttled(
                    f"rate-budget-{method}-{path}",
                    f"Kalshi {method} {path} skipped: local rate budget unavailable.",
                )
                return None
            h = self._headers(method, path)
            try:
                kwargs = {"headers": h}
                if params:
                    kwargs["params"] = params
                if data is not None and method != "GET":
                    kwargs["json"] = data
                req = getattr(session, method.lower())
                kwargs["timeout"] = aiohttp.ClientTimeout(
                    total=(
                        timeout_s if timeout_s is not None
                        else cfg.write_timeout_s if write
                        else cfg.read_timeout_s
                    )
                )
                async with req(f"{cfg.base_url}{path}", **kwargs) as r:
                    if r.status == 429:
                        await bucket.refund(cost)
                        await asyncio.sleep(backoff)
                        backoff = min(0.25, backoff * 2)
                        continue
                    if r.status >= 400:
                        detail = (await r.text())[:500].replace("\n", " ")
                        log.warning(
                            f"Kalshi {method} {path} failed: HTTP {r.status} {detail}"
                        )
                        # A non-retryable 4xx on a write is a definitive "no
                        # order happened" — unlike a timeout, where the order
                        # may exist. Callers use this to skip the uncertainty
                        # backoff and to drop ghost resting orders on cancel.
                        if write and 400 <= r.status < 500 and r.status not in (408, 429):
                            return {"rejected": True, "status": r.status}
                        return None
                    payload = await r.json()
                    return payload if isinstance(payload, dict) else None
            except (
                    aiohttp.ClientError,
                    asyncio.TimeoutError,
                    OSError,
                    TypeError,
                    ValueError,
            ) as exc:
                log.warning(
                    f"Kalshi {method} {path} failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                return None
        log.warning(f"Kalshi {method} {path} exhausted rate-limit retries.")
        return None

    async def get(
            self, path: str, params: Optional[dict] = None,
            timeout_s: Optional[float] = None,
    ) -> Optional[dict]:
        return await self._call(
            "GET", path, params=params, write=False, cost=cfg.read_cost,
            timeout_s=timeout_s,
        )

    async def post(self, path: str, data: dict) -> Optional[dict]:
        return await self._call(
            "POST", path, data=data, write=True, cost=cfg.write_cost
        )

    async def delete(
            self,
            path: str,
            params: Optional[dict] = None,
            data: Optional[dict] = None,
    ) -> Optional[dict]:
        return await self._call(
            "DELETE", path, params=params, data=data,
            write=True, cost=cfg.cancel_cost,
        )

    async def load_limits(self):
        d = await self.get("/account/limits")
        if not isinstance(d, dict):
            return
        read = d.get("read") or {}
        write = d.get("write") or {}
        rr = _optional_float(read.get("refill_rate"))
        rc = _optional_float(read.get("bucket_capacity"))
        wr = _optional_float(write.get("refill_rate"))
        wc = _optional_float(write.get("bucket_capacity"))
        if rr is not None and rc is not None:
            read_rl.configure(rr, rc)
        if wr is not None and wc is not None:
            write_rl.configure(wr, wc)
        tier = (d.get("usage_tier") or "unknown").replace("_", " ")
        grants = [
            f"{g.get('exchange_instance')}={g.get('level')}"
            for g in (d.get("grants") or [])
            if isinstance(g, dict)
        ]
        grant_s = f" grants {', '.join(grants)}" if grants else ""
        log.info(
            f"Account is on {tier.title()} "
            f"({int(read_rl.refill_rate)} read and {int(write_rl.refill_rate)} "
            f"write tokens per second, buckets "
            f"{int(read_rl.capacity)}/{int(write_rl.capacity)}).{grant_s}"
        )

    async def load_endpoint_costs(self):
        d = await self.get("/account/endpoint_costs")
        if not isinstance(d, dict):
            return
        rows = d.get("endpoint_costs") or d.get("costs") or []
        for row in rows:
            path = str(row.get("path") or row.get("endpoint") or "")
            method = str(row.get("method") or "").upper()
            cost = row.get("cost")
            if cost is None:
                continue
            if method == "DELETE" and "orders" in path and "batched" not in path:
                cfg.cancel_cost = int(cost)
                break

    async def get_series(self, series: Optional[str] = None) -> Optional[dict]:
        series = series or cfg.series_ticker
        return await self.get(f"/series/{series}")

    async def get_markets(
            self,
            series: Optional[str] = None,
            status: str = "open",
    ) -> Optional[list[dict]]:
        series = series or cfg.series_ticker
        out: list[dict] = []
        cursor: Optional[str] = None
        first = True
        while True:
            p: dict[str, str | int] = {
                "series_ticker": series,
                "status": status,
                "limit": 200,
            }
            if cursor:
                p["cursor"] = cursor
            d = await self.get("/markets", p)
            if not isinstance(d, dict):
                if first:
                    return None
                break
            first = False
            out.extend(d.get("markets", []))
            cursor_value = d.get("cursor")
            cursor = cursor_value if isinstance(cursor_value, str) else None
            if not cursor:
                break
        return out

    async def get_book(self, ticker: str) -> Optional[dict]:
        return await self.get(
            f"/markets/{ticker}/orderbook",
            timeout_s=cfg.trade_read_timeout_s,
        )

    async def balance(self) -> Optional[dict]:
        d = await self.get("/portfolio/balance")
        if isinstance(d, dict):
            if d.get("balance_dollars") is not None:
                self.cash = float(d["balance_dollars"])
            elif d.get("balance") is not None:
                self.cash = float(d["balance"]) / 100.0
            rows = d.get("balance_breakdown")
            if isinstance(rows, list):
                by_shard: dict[int, float] = {}
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    idx = _optional_float(row.get("exchange_index"))
                    amt = _optional_float(row.get("balance"))
                    if idx is not None and amt is not None:
                        by_shard[int(idx)] = amt
                if by_shard:
                    self.cash_by_shard = by_shard
        return d

    async def transfer_to_shard(
            self, dest_shard: int, amount_dollars: float, source_shard: int,
    ) -> bool:
        """Move cash between exchange shards (amount API unit: centicents).
        Kalshi routes each series to a specific shard and orders there fail
        without local collateral — this keeps the money where the bot trades,
        including after a deposit or if Kalshi re-shards a series."""
        d = {
            "source": "event_contract",
            "destination": "event_contract",
            "amount": int(round(float(amount_dollars) * 10000)),
            "source_exchange_shard": int(source_shard),
            "destination_exchange_shard": int(dest_shard),
        }
        r = await self.post("/portfolio/intra_exchange_instance_transfer", d)
        ok = isinstance(r, dict) and not _rejected(r)
        if ok:
            log.info(
                f"MOVED {_fmt_usd(amount_dollars)} from shard {source_shard} "
                f"to shard {dest_shard} so it can trade."
            )
        else:
            log.warning(
                f"Shard transfer of {_fmt_usd(amount_dollars)} "
                f"{source_shard}→{dest_shard} did not confirm."
            )
        return ok

    def cash_on(self, exchange_index: Optional[int]) -> Optional[float]:
        """Spendable cash for orders routed to this shard. Falls back to the
        account-wide total when the shard is unknown or unreported."""
        if exchange_index is not None and int(exchange_index) >= 0:
            shard = self.cash_by_shard.get(int(exchange_index))
            if shard is not None:
                return shard
        return self.cash

    async def place(
            self,
            ticker,
            side,
            count,
            price,
            tif: str = "good_till_canceled",
            reduce_only: bool = False,
            post_only: bool = False,
            expiration_time: Optional[int] = None,
            exchange_index: Optional[int] = None,
            closing: bool = False,
    ):
        # Kalshi hard rule: reduce_only is only valid on IOC orders.
        if reduce_only and tif != "immediate_or_cancel":
            raise ValueError("reduce_only requires an immediate_or_cancel order")
        # Closing orders net against the held position on the exchange and
        # need no fresh cash — skip the local cash check for them.
        if (
                not reduce_only and not closing
                and not self.can_spend(count, price, exchange_index)
        ):
            return None
        if side == "yes":
            book_side, yes_cents = "bid", price
        else:
            book_side, yes_cents = "ask", _complement(price)
        d = {
            "ticker": ticker,
            "side": book_side,
            "count": f"{float(count):.2f}",
            "price": _cents_to_dollars(yes_cents),
            "time_in_force": tif,
            "self_trade_prevention_type": "taker_at_cross",
            "client_order_id": str(uuid.uuid4()),
            "reduce_only": bool(reduce_only),
            "post_only": bool(post_only),
            "cancel_order_on_pause": True,
        }
        if expiration_time is not None:
            if tif != "good_till_canceled":
                raise ValueError("expiration_time only with good_till_canceled")
            d["expiration_time"] = int(expiration_time)
        # Kalshi shards its matching engines (crypto = shard 2) and orders
        # must route to the market's shard — with collateral pre-placed
        # there, or the order service rejects with "user not found".
        # -1 = auto-route by ticker when the market metadata gave no index.
        d["exchange_index"] = (
            int(exchange_index) if exchange_index is not None else -1
        )
        return await self.post("/portfolio/events/orders", d)

    async def amend(
            self, oid, ticker, book_side, count, yes_cents,
            exchange_index: Optional[int] = None,
    ):
        d = {
            "ticker": ticker,
            "side": book_side,
            "count": f"{float(count):.2f}",
            "price": _cents_to_dollars(yes_cents),
            "exchange_index": (
                int(exchange_index) if exchange_index is not None else -1
            ),
        }
        return await self.post(f"/portfolio/events/orders/{oid}/amend", d)

    async def cancel(
            self, oid, exchange_index: Optional[int] = None,
            market_ticker: Optional[str] = None,
    ):
        params: Optional[dict[str, str | int]] = None
        if exchange_index is not None:
            params = {"exchange_index": int(exchange_index)}
        elif market_ticker:
            # Auto-route by ticker; -1 alone has nothing to route on.
            params = {"exchange_index": -1, "market_ticker": market_ticker}
        return await self.delete(
            f"/portfolio/events/orders/{oid}", params=params
        )

    async def open_orders(self, ticker: str) -> Optional[list[dict]]:
        d = await self.get(
            "/portfolio/orders",
            {"ticker": ticker, "status": "resting"},
            timeout_s=cfg.trade_read_timeout_s,
        )
        if not isinstance(d, dict):
            return None
        rows = d.get("orders")
        return rows if isinstance(rows, list) else []

    async def positions(self, ticker: Optional[str] = None) -> Optional[dict]:
        params: dict[str, str | int] = {
            "limit": 1000,
            "count_filter": "position,total_traded",
        }
        if ticker:
            params["ticker"] = ticker
            params["count_filter"] = "position"
        return await self.get(
            "/portfolio/positions",
            params,
            timeout_s=cfg.trade_read_timeout_s if ticker else None,
        )


# ============================================================
# ORDERBOOK STATE (from WebSocket + REST fallback)
# ============================================================

@dataclass
class BookLevel:
    price: float
    size: float


@dataclass
class BookState:
    bids: list[BookLevel] = field(default_factory=list)
    asks: list[BookLevel] = field(default_factory=list)
    ts: float = 0.0

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None

    @property
    def mid(self) -> Optional[float]:
        if self.bids and self.asks:
            return (self.bids[0].price + self.asks[0].price) / 2.0
        return None

    def depth_bid(self, cents_from_mid: int = 5) -> float:
        if not self.bids:
            return 0
        top = self.bids[0].price
        return sum(
            level.size
            for level in self.bids
            if level.price >= top - cents_from_mid
        )

    def depth_ask(self, cents_from_mid: int = 5) -> float:
        if not self.asks:
            return 0
        top = self.asks[0].price
        return sum(
            level.size
            for level in self.asks
            if level.price <= top + cents_from_mid
        )

    @property
    def pressure(self) -> float:
        db = self.depth_bid(4)
        da = self.depth_ask(4)
        if da == 0 and db == 0:
            return 1.0
        # A fully swept side is extreme pressure, not a neutral book.
        if da == 0:
            return 100.0
        if db == 0:
            return 0.01
        return db / da

    @property
    def is_crossed(self) -> bool:
        if not self.bids or not self.asks:
            return False
        return self.bids[0].price >= self.asks[0].price


class BookTracker:
    def __init__(self):
        self.books: dict[str, BookState] = {}
        self._yes: dict[str, dict[float, float]] = defaultdict(dict)
        self._no: dict[str, dict[float, float]] = defaultdict(dict)
        self._seq: dict[str, int] = {}

    @staticmethod
    def _levels_from_fp(rows) -> dict[float, float]:
        out = {}
        if not rows:
            return out
        for row in rows:
            if not row or len(row) < 2:
                continue
            cents = _dollars_to_cents(row[0])
            size = _fp_to_float(row[1])
            if cents is None or size <= 0:
                continue
            out[cents] = size
        return out

    def drop(self, ticker: str):
        self.books.pop(ticker, None)
        self._yes.pop(ticker, None)
        self._no.pop(ticker, None)
        self._seq.pop(ticker, None)

    def _publish(self, ticker: str):
        yes = {
            p: sz for p, sz in self._yes.get(ticker, {}).items()
            if 0 < p < 100
        }
        no = {
            p: sz for p, sz in self._no.get(ticker, {}).items()
            if 0 < p < 100
        }
        book = BookState(ts=time.monotonic())
        book.bids = [BookLevel(p, yes[p]) for p in sorted(yes, reverse=True)]
        book.asks = [
            BookLevel(_complement(p), no[p])
            for p in sorted(no, reverse=True)
            if 0 < _complement(p) < 100
        ]
        book.asks.sort(key=lambda x: x.price)
        self.books[ticker] = book

    def apply_snapshot(
            self, ticker: str, body: dict, yes_leg: bool = False,
            seq: Optional[int] = None,
    ):
        yes_rows = body.get("yes_dollars_fp") or body.get("yes_dollars") or []
        no_rows = body.get("no_dollars_fp") or body.get("no_dollars") or []
        self._yes[ticker] = self._levels_from_fp(yes_rows)
        no_map = self._levels_from_fp(no_rows)
        if yes_leg:
            converted = {}
            for p, sz in no_map.items():
                no_leg = _complement(p)
                if 0 < no_leg < 100:
                    converted[no_leg] = sz
            no_map = converted
        self._no[ticker] = no_map
        if seq is not None:
            self._seq[ticker] = seq
        self._publish(ticker)

    def apply_delta(
            self, ticker: str, body: dict, yes_leg: bool = False,
            seq: Optional[int] = None,
    ) -> bool:
        previous = self._seq.get(ticker)
        if seq is None:
            self.drop(ticker)
            return False
        if previous is None:
            # No snapshot yet — one is already on the way after subscribe or
            # a requested resync; skip deltas instead of re-requesting.
            return True
        if seq <= previous:
            return True
        if seq != previous + 1:
            self.drop(ticker)
            return False
        cents = _dollars_to_cents(body.get("price_dollars"))
        if cents is None:
            self.drop(ticker)
            return False
        delta = _fp_to_float(body.get("delta_fp"))
        side = body.get("side")
        if side == "yes":
            book = self._yes[ticker]
        elif side == "no":
            if yes_leg:
                cents = _complement(cents)
            book = self._no[ticker]
        else:
            self.drop(ticker)
            return False
        nxt = round(book.get(cents, 0.0) + delta, 2)
        if nxt <= 0.0:
            book.pop(cents, None)
        else:
            book[cents] = nxt
        self._seq[ticker] = seq
        self._publish(ticker)
        return True

    def update_from_rest(self, ticker: str, raw: dict):
        body = raw.get("orderbook_fp") or raw
        self._seq.pop(ticker, None)
        self.apply_snapshot(ticker, body, yes_leg=False)

    def get(self, ticker: str) -> Optional[BookState]:
        return self.live(ticker, cfg.book_max_age_s)

    def live(self, ticker: str, max_age: float) -> Optional[BookState]:
        b = self.books.get(ticker)
        if not b or time.monotonic() - b.ts >= max_age:
            return None
        if b.is_crossed:
            return None
        return b

    def peek(self, ticker: str) -> Optional[BookState]:
        return self.books.get(ticker)


tracker = BookTracker()


# ============================================================
# BTC SPOT — Pyth WS first, Coinbase/Kraken REST as fallback
# ============================================================

class BTCSpot:
    def __init__(self):
        self.prices: dict[str, tuple[float, float]] = {}
        # Rolling (monotonic ts, price) samples, ~2/s max, for realized-vol
        # sizing of the entry gap. 600 samples ≈ the last 5 minutes.
        self.hist: deque[tuple[float, float]] = deque(maxlen=600)
        self.sess: Optional[aiohttp.ClientSession] = None
        self.running = False

    async def connect(self):
        self.sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3))
        self.running = True

    async def close(self):
        self.running = False
        if self.sess:
            await self.sess.close()
            self.sess = None
        await asyncio.sleep(0)

    def note(self, price: float, source: str):
        # NaN passes every <=/> guard downstream, silently disabling the
        # disagreement and direction checks — reject non-finite outright.
        if not price or price <= 0 or not math.isfinite(price):
            return
        now = time.monotonic()
        self.prices[source] = (now, float(price))
        if not self.hist or now - self.hist[-1][0] >= 0.5:
            self.hist.append((now, float(price)))

    def required_gap(self, secs_left: float) -> Optional[float]:
        """How far BTC must be from the strike before an entry is worth it:
        recent 5-second noise, not remaining cycle time. A clip holds tens
        of seconds; scaling σ to expiry starved every live window."""
        now = time.monotonic()
        window = [(t, p) for t, p in self.hist if now - t <= 300.0]
        if len(window) < 20 or window[-1][0] - window[0][0] < 60.0:
            return None
        # Resample to ~5s spacing so per-tick noise doesn't drown the signal.
        moves = []
        last_t, last_p = window[0]
        for t, p in window[1:]:
            dt = t - last_t
            if dt >= 5.0:
                # A span across a feed outage is not a 5-second move —
                # counting it would inflate σ and freeze entries for minutes.
                if dt <= 12.0:
                    moves.append(p - last_p)
                last_t, last_p = t, p
        if len(moves) < 8:
            return None
        sigma5 = statistics.pstdev(moves)
        if sigma5 <= 0:
            return None
        return cfg.vol_gap_mult * sigma5

    async def poll(self):
        while self.running:
            if not self.sess:
                break
            await asyncio.gather(*(self._poll_one(url) for url in cfg.btc_urls))
            await asyncio.sleep(0.2)

    async def _poll_one(self, url: str):
        if not self.running or not self.sess:
            return
        try:
            async with self.sess.get(url) as r:
                if r.status != 200:
                    _log_throttled(
                        f"btc-http-{url}",
                        f"Bitcoin source returned HTTP {r.status}: {url}",
                        every=5.0,
                    )
                    return
                d = await r.json()
                if "coinbase" in url:
                    p = float(d["data"]["amount"])
                    source = "coinbase"
                else:
                    res = d.get("result", {})
                    k = list(res.keys())[0] if res else None
                    p = float(res[k]["c"][0]) if k else None
                    source = "kraken"
                if p:
                    self.note(p, source)
        except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                IndexError,
                KeyError,
                OSError,
                TypeError,
                ValueError,
        ) as exc:
            _log_throttled(
                f"btc-error-{url}",
                f"Bitcoin source failed ({url}): "
                f"{type(exc).__name__}: {exc}",
                every=5.0,
            )

    def _fresh(self) -> list[tuple[str, float, float]]:
        now = time.monotonic()
        out = []
        for src, (ts, price) in self.prices.items():
            age = now - ts
            if age < cfg.btc_max_age_s:
                out.append((src, price, age))
        return out

    def spot(self) -> Optional[float]:
        recent = self._fresh()
        if not recent:
            return None
        prices = [p for _, p, _ in recent]
        pyth_fast = any(src == "pyth" and age < 0.8 for src, _, age in recent)
        if len(recent) < 2 and not pyth_fast:
            return None
        if len(prices) >= 2 and max(prices) - min(prices) > cfg.btc_max_spread:
            _log_throttled(
                "btc-disagreement",
                f"Bitcoin sources disagree by {_fmt_usd(max(prices) - min(prices))}; "
                "waiting before trading.",
                every=2.0,
            )
            return None
        return float(statistics.median(prices))

    def all_on_side(self, strike: float, yes_side: bool) -> bool:
        recent = self._fresh()
        if not recent:
            return False
        prices = [p for _, p, _ in recent]
        pyth_fast = any(src == "pyth" and age < 0.8 for src, _, age in recent)
        if len(recent) < 2 and not pyth_fast:
            return False
        if len(prices) >= 2 and max(prices) - min(prices) > cfg.btc_max_spread:
            return False
        if yes_side:
            return min(prices) > strike + cfg.min_btc_gap
        return max(prices) < strike - cfg.min_btc_gap


# ============================================================
# PERP FUNDING — crowded-side veto. Not a 15-minute price oracle.
# ============================================================

class Funding:
    """Hyperliquid BTC-USD hourly funding + Kraken PF_XBTUSD basis.

    Extreme positive funding = crowded longs → do not join YES.
    Extreme negative = crowded shorts → do not join NO.
    Stale or missing data does not block a clip.
    """

    def __init__(self):
        self.sess: Optional[aiohttp.ClientSession] = None
        self.running = False
        self.ann: Optional[float] = None
        self.premium: Optional[float] = None
        self.ts: float = 0.0

    async def connect(self):
        self.sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4))
        self.running = True

    async def close(self):
        self.running = False
        if self.sess:
            await self.sess.close()
            self.sess = None
        await asyncio.sleep(0)

    def fresh(self) -> bool:
        return (
            self.ann is not None
            and time.monotonic() - self.ts < cfg.funding_max_age_s
        )

    def crowded(self) -> Optional[str]:
        """'long' or 'short' when perps are packed; None if normal or unknown."""
        if not self.fresh() or self.ann is None:
            return None
        if self.ann > cfg.funding_crowd_ann:
            return "long"
        if self.ann < -cfg.funding_crowd_ann:
            return "short"
        return None

    def words(self) -> str:
        if not self.fresh() or self.ann is None:
            return "waiting"
        crowd = self.crowded()
        crowd_s = crowd if crowd else "neutral"
        prem = self.premium
        prem_s = f"{prem * 1e4:+.1f}bp" if prem is not None else "?"
        return f"{self.ann * 100:+.0f}% ann  |  prem {prem_s}  |  {crowd_s}"

    async def poll(self):
        while self.running:
            if not self.sess:
                break
            await asyncio.gather(self._poll_hl(), self._poll_kraken())
            await asyncio.sleep(5.0)

    async def _poll_hl(self):
        if not self.sess:
            return
        try:
            async with self.sess.post(
                    "https://api.hyperliquid.xyz/info",
                    json={"type": "metaAndAssetCtxs"},
            ) as r:
                if r.status != 200:
                    _log_throttled(
                        "funding-hl-http",
                        f"Hyperliquid funding returned HTTP {r.status}",
                        every=15.0,
                    )
                    return
                d = await r.json()
            if not (isinstance(d, list) and len(d) >= 2):
                return
            universe = d[0].get("universe") if isinstance(d[0], dict) else None
            ctxs = d[1]
            if not isinstance(universe, list) or not isinstance(ctxs, list):
                return
            for i, u in enumerate(universe):
                if not isinstance(u, dict) or u.get("name") != "BTC":
                    continue
                if i >= len(ctxs) or not isinstance(ctxs[i], dict):
                    return
                rate = _optional_float(ctxs[i].get("funding"))
                if rate is None or not math.isfinite(rate):
                    return
                # History prints are 60 minutes apart.
                self.ann = float(rate) * 24.0 * 365.0
                prem = _optional_float(ctxs[i].get("premium"))
                if prem is not None and math.isfinite(prem):
                    self.premium = float(prem)
                self.ts = time.monotonic()
                return
        except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                OSError,
                TypeError,
                ValueError,
        ) as exc:
            _log_throttled(
                "funding-hl",
                f"Hyperliquid funding failed: {type(exc).__name__}: {exc}",
                every=15.0,
            )

    async def _poll_kraken(self):
        """Basis-only. Kraken's absolute fundingRate is not a fraction."""
        if not self.sess:
            return
        try:
            async with self.sess.get(
                    "https://futures.kraken.com/derivatives/api/v3/tickers",
            ) as r:
                if r.status != 200:
                    return
                d = await r.json()
            for t in d.get("tickers") or []:
                if not isinstance(t, dict) or t.get("symbol") != "PF_XBTUSD":
                    continue
                mark = _optional_float(t.get("markPrice"))
                index = _optional_float(t.get("indexPrice"))
                if (
                        mark is None or index is None or index <= 0
                        or not math.isfinite(mark) or not math.isfinite(index)
                ):
                    return
                if self.premium is None:
                    self.premium = (mark - index) / index
                    if self.ann is None:
                        self.ts = time.monotonic()
                return
        except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                OSError,
                TypeError,
                ValueError,
        ):
            return


# ============================================================
# CLIP — one position, maker in, maker out (taker only to exit)
# ============================================================

@dataclass
class Resting:
    oid: str
    price: float  # always YES cents — where the order sits on the YES book
    size: float
    filled: float = 0.0
    exchange_index: Optional[int] = None
    side: str = ""  # "yes" or "no" — the side we sent to place()
    entry: bool = False  # True = opening rest, False = exit rest
    placed: float = 0.0  # monotonic time the order first went up


class Clip:
    """One position. Sweep the 1-tick offer only at 80–96¢; else maker join."""

    def __init__(self, client: Kalshi, btc: BTCSpot, funding: Funding):
        self.client = client
        self.btc = btc
        self.funding = funding
        self.inventory: dict[str, float] = defaultdict(float)
        self.avg_yes: dict[str, float] = {}
        self.entry_fees: dict[str, float] = {}
        self.opened_at: dict[str, float] = {}
        # BTC gap in our favor at fill — used only to log a cross, not a
        # fade scratch. Winners ride the maker rest toward $1.
        self.entry_gap: dict[str, float] = {}
        self.rest: dict[str, Resting] = {}
        self.fee_mult: float = 1.0
        # Fraction of the taker fee charged to makers on this series:
        # 0 on plain quadratic/flat series, 0.25 with maker fees, 0.5 on
        # combo maker fees. Conservative (taker rate) until the series loads.
        self.maker_fee_frac: float = 1.0
        # Open-trade bookkeeping for the per-trade expectancy record.
        self.trade_open: dict[str, dict] = {}
        self.exit_reason: dict[str, str] = {}
        self._quote_ts: dict[str, float] = {}
        self._cooldown_until: dict[str, float] = {}
        self._rest_dirty: set[str] = set()
        self._flat_ts: dict[str, float] = {}
        self.quote_skip: str = ""
        self._skip_kind: str = ""

    def _cool(self, ticker: str, secs: Optional[float] = None):
        self._cooldown_until[ticker] = time.monotonic() + (
            secs if secs is not None else cfg.reject_cooldown_s
        )

    def _cooling(self, ticker: str) -> bool:
        return time.monotonic() < self._cooldown_until.get(ticker, 0.0)

    def _size_for(
            self, side_px: float, exchange_index: Optional[int] = None,
    ) -> float:
        """Largest clip the shard's cash can cover at this price, fees
        included, without putting more than clip_risk_frac of cash on one
        clip."""
        want = float(min(cfg.clip_size, cfg.max_position_per_market))
        cash = self.client.cash_on(exchange_index)
        if cash is None:
            return 0.0
        p = float(side_px) / 100.0
        per = p + _taker_fee_dollars(1, p, self.fee_mult)
        if per <= 0:
            return 0.0
        afford = math.floor(max(0.0, cash - 0.05) / per)
        risk = math.floor(max(0.0, cash) * cfg.clip_risk_frac / per)
        return float(min(want, afford, risk))

    def _set_skip(self, kind: str, msg: str) -> None:
        self.quote_skip = msg
        if kind != self._skip_kind:
            self._skip_kind = kind
            if msg:
                log.info(f"SKIP {msg}")

    def flat(self, ticker: str) -> bool:
        return abs(self.inventory.get(ticker, 0.0)) < 0.005

    def has_any_position(self) -> bool:
        return any(abs(qty) >= 0.005 for qty in self.inventory.values())

    def resting_words(self, ticker: str) -> str:
        r = self.rest.get(ticker)
        if not r:
            return "none"
        return f"{_fmt_qty(r.size)} at {r.price:g}¢"

    def forget(self, ticker: str):
        self.inventory.pop(ticker, None)
        self.avg_yes.pop(ticker, None)
        self.entry_fees.pop(ticker, None)
        self.opened_at.pop(ticker, None)
        self.entry_gap.pop(ticker, None)
        self.rest.pop(ticker, None)
        self._quote_ts.pop(ticker, None)
        self._cooldown_until.pop(ticker, None)
        self._rest_dirty.discard(ticker)
        self._flat_ts.pop(ticker, None)

    def drop_rest(self, oid: str):
        for ticker, r in list(self.rest.items()):
            if r.oid == oid:
                self.rest.pop(ticker, None)

    def apply_user_order(self, body: dict):
        oid = body.get("order_id")
        if not isinstance(oid, str):
            return
        for ticker, resting in list(self.rest.items()):
            if resting.oid != oid:
                continue
            status = body.get("status")
            remaining = _optional_float(body.get("remaining_count_fp"))
            filled = _optional_float(body.get("fill_count_fp"))
            if status in ("canceled", "executed") or remaining == 0:
                self.rest.pop(ticker, None)
            else:
                if remaining is not None:
                    resting.size = remaining
                if filled is not None:
                    resting.filled = filled
            return

    def apply_position(
            self,
            ticker: str,
            qty: float,
            avg_cents: Optional[float] = None,
            cost_dollars: Optional[float] = None,
            fees: Optional[float] = None,
            opening: bool = False,
            exit_yes: Optional[float] = None,
    ):
        old = self.inventory.get(ticker, 0.0)
        qty = round(float(qty), 2)
        if abs(qty) < 0.005:
            qty = 0.0
        self.inventory[ticker] = qty
        if qty == 0.0:
            if abs(old) >= 0.005:
                self._record_trade(ticker, old, exit_yes)
            self.avg_yes.pop(ticker, None)
            self.entry_fees.pop(ticker, None)
            self.opened_at.pop(ticker, None)
            self.entry_gap.pop(ticker, None)
            self.trade_open.pop(ticker, None)
            self.exit_reason.pop(ticker, None)
            # Deliberately keep self.rest: rests are NOT reduce_only, so a
            # still-live one must be canceled by tick's flat branch, never
            # silently forgotten.
            return
        if old == 0 or opening:
            self.opened_at[ticker] = time.monotonic()
            if fees is not None:
                self.entry_fees[ticker] = float(fees)
        elif fees:
            self.entry_fees[ticker] = self.entry_fees.get(ticker, 0.0) + float(fees)
        if cost_dollars is not None:
            if qty > 0:
                self.avg_yes[ticker] = 100.0 * float(cost_dollars) / qty
            else:
                self.avg_yes[ticker] = 100.0 - 100.0 * float(cost_dollars) / abs(qty)
        elif avg_cents is not None and (opening or ticker not in self.avg_yes):
            self.avg_yes[ticker] = float(avg_cents)
        if (old == 0 or opening) and ticker not in self.trade_open:
            self.trade_open[ticker] = {
                "ts": time.time(),
                "mono": time.monotonic(),
                "qty": qty,
                "avg_yes": self.avg_yes.get(ticker),
            }
        # Leftover entry rests are not reduce_only. If we are already at
        # the cap, cancel them before a second 10-lot fills (20 YES −$6.86).
        cap = float(cfg.max_position_per_market)
        rest = self.rest.get(ticker)
        if abs(qty) >= cap - 0.005 and rest is not None and rest.entry:
            self._rest_dirty.add(ticker)

    def _record_trade(
            self, ticker: str, old_qty: float, exit_yes: Optional[float],
    ):
        """Append one closed round trip to trades.jsonl and the log. This is
        the measurement layer — expectancy gets tuned from these records, so
        a trade must never close without leaving one."""
        info = self.trade_open.pop(ticker, {})
        reason = self.exit_reason.pop(ticker, "maker exit filled")
        avg = info.get("avg_yes") or self.avg_yes.get(ticker)
        held = (
            round(time.monotonic() - info["mono"], 1)
            if info.get("mono") else None
        )
        qty = abs(old_qty)
        pnl = None
        if avg is not None and exit_yes is not None:
            gross = (exit_yes - avg) if old_qty > 0 else (avg - exit_yes)
            # Taker exits pay the full quadratic fee; maker-exit fills pay
            # the series' maker fraction (zero on KXBTC15M). Entry was maker.
            fee = _taker_fee_dollars(qty, exit_yes / 100.0, self.fee_mult)
            if reason == "maker exit filled":
                fee *= self.maker_fee_frac
            entry_fee = self.entry_fees.get(
                ticker,
                self.maker_fee_frac * _taker_fee_dollars(
                    qty, avg / 100.0, self.fee_mult
                ),
            )
            pnl = round(gross * qty / 100.0 - fee - entry_fee, 4)
        rec = {
            "ts": round(time.time(), 3),
            "ticker": ticker,
            "dir": "yes" if old_qty > 0 else "no",
            "qty": qty,
            "avg_in_yes": round(avg, 2) if avg is not None else None,
            "exit_yes": round(exit_yes, 2) if exit_yes is not None else None,
            "held_s": held,
            "reason": reason,
            "est_pnl": pnl,
        }
        try:
            with open("trades.jsonl", "a") as f:
                f.write(json.dumps(rec) + "\n")
        except OSError:
            pass
        pnl_words = _fmt_pnl(pnl) if pnl is not None else "unknown"
        held_words = _fmt_secs(held) if held is not None else "?"
        log.info(
            f"TRADE CLOSED {rec['dir'].upper()} {_fmt_qty(qty)} {ticker}: "
            f"{avg if avg is None else round(avg, 1)}¢ → "
            f"{exit_yes if exit_yes is None else round(exit_yes, 1)}¢ "
            f"held {held_words} — {reason} — est {pnl_words}"
        )

    @staticmethod
    def _touch_yes(inv: float, book: BookState) -> Optional[float]:
        mark = book.best_bid if inv > 0 else book.best_ask
        if mark is None:
            return None
        return float(mark)

    def _green_after_fee(
            self, inv: float, exit_yes: float, qty: float, ticker: str,
    ) -> bool:
        avg = self.avg_yes.get(ticker)
        if avg is None:
            return False
        fee = _taker_fee_dollars(qty, exit_yes / 100.0, self.fee_mult)
        paid = self.entry_fees.get(ticker)
        if paid is None:
            # Entries are maker-only, so the entry fee is the series' maker
            # rate — zero on plain quadratic series like KXBTC15M. Demanding
            # phantom taker-fee headroom here held losers longer for no gain.
            paid = self.maker_fee_frac * _taker_fee_dollars(
                qty, avg / 100.0, self.fee_mult
            )
        if inv > 0:
            gross = (exit_yes - avg) * qty / 100.0
        else:
            gross = (avg - exit_yes) * qty / 100.0
        return gross - fee - paid > 0.0

    async def pull(self, reason: str = "") -> bool:
        had = False
        for ticker, r in list(self.rest.items()):
            had = True
            result = await self.client.cancel(r.oid, r.exchange_index, ticker)
            if isinstance(result, dict):
                # A definitive 4xx means the order is already gone on the
                # exchange (expired/canceled/executed) — drop the ghost.
                if _rejected(result):
                    log.info(
                        f"Resting order {r.oid} already gone on the exchange."
                    )
                self.rest.pop(ticker, None)
            else:
                # Timeout — the order may still be live. Rests are not
                # reduce_only, so this MUST be resolved before new orders.
                self._rest_dirty.add(ticker)
                self._cool(ticker)
                log.critical(
                    f"Could not cancel our resting order {r.oid}; "
                    "refusing to place a crossing replacement."
                )
        if had:
            why = f" — {reason}" if reason else ""
            if self.rest:
                log.critical(f"PULL INCOMPLETE{why}")
            else:
                log.info(f"PULLED resting clip{why}")
        return not self.rest

    async def resync_rest(self, ticker: str, exchange_index: Optional[int]) -> bool:
        """When we can't be sure what's resting, cancel everything on the
        market and start clean. Returns True once nothing is live."""
        rows = await self.client.open_orders(ticker)
        if rows is None:
            return False
        ok = True
        for o in rows:
            oid = o.get("order_id")
            if not isinstance(oid, str):
                continue
            result = await self.client.cancel(oid, exchange_index, ticker)
            if not isinstance(result, dict):
                ok = False
        if ok:
            self.rest.pop(ticker, None)
        return ok

    async def ensure_clean(
            self, ticker: str, exchange_index: Optional[int],
    ) -> bool:
        """True once no unknown orders can be live on this market."""
        if ticker not in self._rest_dirty:
            return True
        if await self.resync_rest(ticker, exchange_index):
            self._rest_dirty.discard(ticker)
            return True
        return False

    async def reconcile_position(self, ticker: str) -> bool:
        data = await self.client.positions(ticker)
        if not isinstance(data, dict):
            return False
        rows = data.get("market_positions") or []
        row = next(
            (p for p in rows if isinstance(p, dict) and p.get("ticker") == ticker),
            None,
        )
        if row is None:
            self.apply_position(ticker, 0.0)
            return True
        qty = _optional_float(row.get("position_fp"))
        if qty is None:
            return False
        cost = _optional_float(row.get("position_cost_dollars"))
        self.apply_position(ticker, round(qty, 2), cost_dollars=cost)
        return True

    async def flatten(
            self,
            ticker: str,
            reason: str,
            book: BookState,
            limit_yes: float,
            exchange_index: Optional[int] = None,
            force: bool = False,
    ) -> bool:
        inv = self.inventory.get(ticker, 0.0)
        if abs(inv) < 0.005:
            return True
        if limit_yes <= 0 or limit_yes >= 100:
            return False
        sale_price = limit_yes if inv > 0 else _complement(limit_yes)
        if sale_price <= 1.0:
            _log_throttled(
                f"no-one-cent-{ticker}",
                f"REFUSED 1¢-or-worse IOC while holding {_pos_words(inv)}.",
            )
            return False
        # IOC only when it's going to happen: size the order to what the
        # book actually shows within our limit. Nobody there → no order.
        if inv > 0:
            avail = sum(
                level.size for level in book.bids if level.price >= limit_yes
            )
        else:
            avail = sum(
                level.size for level in book.asks if level.price <= limit_yes
            )
        if avail < 0.005:
            _log_throttled(
                f"no-liquidity-{ticker}",
                f"HOLDING {_pos_words(inv)}: nobody in the book within "
                f"{limit_yes:g}¢ — not sending an IOC into air.",
            )
            return False
        qty = round(min(abs(inv), avail), 2)
        full = qty >= abs(inv) - 0.005
        now = time.monotonic()
        if not force and now - self._flat_ts.get(ticker, 0.0) < cfg.dump_cooldown_s:
            return False
        if not await self.pull(reason):
            # Can't confirm our rest is gone — cancel everything and retry
            # next round rather than risk a double exit.
            await self.resync_rest(ticker, exchange_index)
            return False
        # Stamp the cooldown only once a dump is actually going out, so a
        # failed cancel doesn't lock the scratch out for a whole cooldown.
        self._flat_ts[ticker] = time.monotonic()
        self.exit_reason[ticker] = reason
        if inv > 0:
            log.info(
                f"DUMP {_fmt_qty(qty)} YES at ≥{limit_yes:g}¢ — {reason}"
            )
            result = await self.client.place(
                ticker, "no", qty, _complement(limit_yes),
                tif="immediate_or_cancel",
                reduce_only=True,
                exchange_index=exchange_index,
            )
        else:
            log.info(
                f"DUMP {_fmt_qty(qty)} NO (buy YES ≤{limit_yes:g}¢) — {reason}"
            )
            result = await self.client.place(
                ticker, "yes", qty, limit_yes,
                tif="immediate_or_cancel",
                reduce_only=True,
                exchange_index=exchange_index,
            )
        if _rejected(result):
            self._cool(ticker)
            return False
        if not isinstance(result, dict):
            await self.reconcile_position(ticker)
            return self.flat(ticker)
        filled = _order_fill(result)
        if full and filled is not None and filled >= qty - 0.005:
            self.apply_position(ticker, 0.0, exit_yes=limit_yes)
            return True
        await self.reconcile_position(ticker)
        if not self.flat(ticker):
            remaining_words = _pos_words(self.inventory.get(ticker, 0.0))
            if full:
                log.warning(f"PARTIAL EXIT: still holding {remaining_words}.")
            else:
                log.info(
                    f"Took the displayed size; still holding {remaining_words}."
                )
        return self.flat(ticker)

    async def _place_rest(
            self,
            ticker: str,
            side: str,
            count: float,
            yes_cents: float,
            expire_s: float,
            exchange_index: Optional[int],
            entry: bool,
    ) -> bool:
        # Maker rests are post-only GTC and NEVER reduce_only — Kalshi
        # rejects reduce_only on anything but IOC.
        side_px = yes_cents if side == "yes" else _complement(yes_cents)
        result = await self.client.place(
            ticker, side, count, side_px,
            tif="good_till_canceled",
            post_only=True,
            expiration_time=int(time.time() + expire_s),
            exchange_index=exchange_index,
            closing=not entry,
        )
        if result is None:
            # Timeout — the order may be live without us knowing its id.
            self._rest_dirty.add(ticker)
            self._cool(ticker)
            return False
        if _rejected(result):
            self._cool(ticker)
            return False
        oid = _order_id(result)
        if not oid:
            self._rest_dirty.add(ticker)
            self._cool(ticker)
            return False
        filled = _order_fill(result) or 0.0
        remaining = _order_remaining(result)
        if remaining is None:
            remaining = max(0.0, count - filled)
        if remaining > 0.005:
            self.rest[ticker] = Resting(
                oid, yes_cents, remaining, filled, exchange_index, side, entry,
                placed=time.monotonic(),
            )
            return True
        # Post-only that would have crossed comes back canceled at once.
        return False

    async def _sweep_in(
            self,
            ticker: str,
            side: str,
            count: float,
            take_yes: float,
            book: BookState,
            exchange_index: Optional[int],
    ) -> bool:
        """IOC the 1-tick offer. Only used when that take price is still in-band."""
        take_px = take_yes if side == "yes" else _complement(take_yes)
        if take_px <= 1.0 or take_px >= 99.0:
            return False
        if side == "yes":
            avail = sum(
                level.size for level in book.asks if level.price <= take_yes
            )
        else:
            avail = sum(
                level.size for level in book.bids if level.price >= take_yes
            )
        qty = round(min(count, avail), 2)
        if qty < 1:
            return False
        log.info(
            f"SWEEP {side.upper()} {_fmt_qty(qty)} at {take_px:g}¢ "
            "(taker — offer was still in band)"
        )
        result = await self.client.place(
            ticker, side, qty, take_px,
            tif="immediate_or_cancel",
            post_only=False,
            exchange_index=exchange_index,
        )
        if result is None or _rejected(result):
            self._cool(ticker)
            return False
        filled = _order_fill(result) or 0.0
        if filled < 1:
            return False
        signed = filled if side == "yes" else -filled
        taker_fee = _taker_fee_dollars(filled, take_yes / 100.0, self.fee_mult)
        self.apply_position(
            ticker, signed,
            avg_cents=take_yes,
            fees=taker_fee,
            opening=True,
        )
        return True

    async def _amend_rest(
            self,
            ticker: str,
            r: Resting,
            count_total: float,
            yes_cents: float,
            exchange_index: Optional[int],
    ) -> bool:
        book_side = "bid" if r.side == "yes" else "ask"
        result = await self.client.amend(
            r.oid, ticker, book_side, count_total, yes_cents, exchange_index
        )
        if result is None:
            self._rest_dirty.add(ticker)
            self._cool(ticker)
            return False
        if _rejected(result):
            # Order likely already gone (filled/expired); confirm and clear.
            canceled = await self.client.cancel(r.oid, exchange_index, ticker)
            if isinstance(canceled, dict):
                self.rest.pop(ticker, None)
            else:
                self._rest_dirty.add(ticker)
            self._cool(ticker)
            return False
        # fill_count_fp is the order's TOTAL fills, not an increment.
        filled_total = _order_fill(result)
        if filled_total is None:
            filled_total = r.filled
        remaining = _order_remaining(result)
        if remaining is None:
            remaining = max(0.0, count_total - filled_total)
        if remaining > 0.005:
            # An amend keeps the original placement time — the age gate is
            # about how long we've been quoting, not the last price move.
            self.rest[ticker] = Resting(
                _order_id(result) or r.oid, yes_cents, remaining,
                filled_total, exchange_index, r.side, r.entry,
                placed=r.placed or time.monotonic(),
            )
        else:
            self.rest.pop(ticker, None)
        return True

    async def _rest_or_raise(
            self,
            ticker: str,
            inv: float,
            target: float,
            qty: float,
            exchange_index: Optional[int],
    ):
        cur = self.rest.get(ticker)
        if cur and cur.entry:
            # Leftover opening order from before the fill — clear it before
            # quoting the exit; amending across sides would be nonsense.
            if not self._cooling(ticker):
                await self.pull("clearing entry order before the exit quote")
            return
        if cur and cur.price == target and cur.size == qty:
            return
        if self._cooling(ticker):
            return
        now = time.monotonic()
        if now - self._quote_ts.get(ticker, 0.0) < cfg.quote_min_gap_s:
            return
        self._quote_ts[ticker] = now
        if cur:
            if await self._amend_rest(
                    ticker, cur, cur.filled + qty, target, exchange_index
            ):
                log.info(f"RAISED maker rest to {target:g}¢")
            return
        if await self._place_rest(
                ticker, "no" if inv > 0 else "yes", qty, target,
                cfg.clip_maker_secs, exchange_index, entry=False,
        ):
            log.info(f"REST maker exit {_fmt_qty(qty)} at {target:g}¢")

    @staticmethod
    def _cushioned(mark: float, inv: float, cushion: float) -> float:
        # A worse-than-touch limit so urgent exits can sweep a few levels.
        limit = mark - cushion if inv > 0 else mark + cushion
        return max(1.0, min(99.0, limit))

    async def tick(
            self,
            ticker: str,
            book: BookState,
            strike: float,
            spot: Optional[float],
            secs: float,
            exchange_index: Optional[int],
    ):
        # If we lost track of what's resting, cancel everything on this
        # market before another order goes anywhere near it.
        if not await self.ensure_clean(ticker, exchange_index):
            return
        inv = self.inventory.get(ticker, 0.0)
        qty = abs(inv)
        mark = self._touch_yes(inv, book) if qty >= 0.005 else None
        if qty >= 0.005:
            self.quote_skip = ""
            self._skip_kind = "held"

        if qty >= 0.005 and secs <= cfg.flatten_secs:
            if mark is not None:
                await self.flatten(
                    ticker, "cycle almost over", book,
                    self._cushioned(mark, inv, 5.0), exchange_index,
                )
            return

        if qty >= 0.005 and spot is None:
            # A 1.5s Coinbase/Kraken hitch is not a sell. Flattening on it
            # dumped 10 YES at 84¢ tonight (−$1.25). Keep the maker rest.
            return

        if qty >= 0.005 and spot is not None:
            with_gap = (spot - strike) if inv > 0 else (strike - spot)
            avg = self.avg_yes.get(ticker)
            fav = None
            if avg is not None:
                fav = avg if inv > 0 else _complement(avg)
            cheap = fav is not None and fav <= cfg.rotate_cheap
            vol_bar = self.btc.required_gap(secs)
            through = with_gap <= -max(
                cfg.min_btc_gap, vol_bar if vol_bar is not None else cfg.min_btc_gap
            )
            if through and not cheap:
                if mark is not None:
                    what = "YES" if inv > 0 else "NO"
                    await self.flatten(
                        ticker, f"BTC crossed — rotate off {what}", book,
                        self._cushioned(mark, inv, 5.0), exchange_index,
                    )
                if not self.flat(ticker):
                    return
                inv = 0.0
                qty = 0.0
                mark = None
                self._quote_ts[ticker] = 0.0

        if qty < 0.005:
            await self._manage_entry(
                ticker, book, strike, spot, secs, exchange_index
            )
            return

        avg = self.avg_yes.get(ticker)
        if avg is None or mark is None:
            return
        if inv > 0:
            target = _maker_target_yes(avg)
        else:
            target = _complement(_maker_target_yes(_complement(avg)))
        last5 = secs <= cfg.clip_taker_only_secs
        # Soft book alone must not cut a winner while BTC is still with us.
        take_if_green = last5

        if take_if_green and self._green_after_fee(inv, mark, qty, ticker):
            await self.flatten(
                ticker, "first green taker", book, mark, exchange_index
            )
            return

        live = mark
        raised = max(target, live + 1) if inv > 0 else min(target, live - 1)
        raised = max(1, min(99, raised))
        await self._rest_or_raise(ticker, inv, raised, qty, exchange_index)

    def _entry_quote(
            self,
            ticker: str,
            book: BookState,
            strike: float,
            spot: Optional[float],
            secs: float,
            keeping: bool,
            exchange_index: Optional[int] = None,
    ) -> Optional[tuple[str, float, float]]:
        """Where our opening maker order should sit, or None if we shouldn't
        have one. Returns (side, yes_cents to join, size)."""
        if secs <= cfg.entry_cutoff_secs:
            self._set_skip("late", f"too late ({_fmt_secs(secs)} left)")
            return None
        if spot is None:
            self._set_skip("nobtc", "no bitcoin yet")
            return None
        if self.has_any_position():
            return None
        vol_bar = self.btc.required_gap(secs)
        if vol_bar is None:
            vol_bar = cfg.min_btc_gap
        if keeping:
            vol_bar *= cfg.vol_keep_frac
        gap = abs(spot - strike)
        need = max(cfg.min_btc_gap, vol_bar)
        if gap < need:
            self._set_skip("gap", f"gap {_fmt_usd(gap)} vs {_fmt_usd(need)}")
            return None
        pressure = book.pressure
        # Wide BTC lead: the book lag IS the clip. Thin lead still needs
        # the book not steamrolling against us.
        wide = gap >= cfg.min_btc_gap * cfg.pressure_free_mult
        if wide:
            long_ok = short_ok = True
        else:
            long_ok = pressure >= (
                cfg.keep_pressure if keeping else cfg.mom_min_pressure
            )
            short_ok = pressure <= (
                1.0 / cfg.keep_pressure if keeping else 1.0 / cfg.mom_min_pressure
            )
        # Placing demands every fresh source beyond the line; keeping only
        # needs this tick's median spot there — re-reading per-source
        # freshness milliseconds later was the last silent quote-killer.
        if keeping:
            yes_dir = spot > strike + cfg.min_btc_gap
            no_dir = spot < strike - cfg.min_btc_gap
        else:
            yes_dir = self.btc.all_on_side(strike, yes_side=True)
            no_dir = self.btc.all_on_side(strike, yes_side=False)
        if long_ok and yes_dir:
            join_yes = book.best_bid
            if join_yes is None:
                self._set_skip("book", "empty book")
                return None
            side, side_px = "yes", join_yes
        elif short_ok and no_dir:
            join_yes = book.best_ask
            if join_yes is None:
                self._set_skip("book", "empty book")
                return None
            side, side_px = "no", _complement(join_yes)
        else:
            self._set_skip(
                "disagree",
                f"btc/pressure disagree (pressure {pressure:.2f})",
            )
            return None
        if not (cfg.entry_lo <= side_px <= cfg.entry_hi):
            self._set_skip(
                "band",
                f"{side.upper()} {side_px:g}¢ outside "
                f"{cfg.entry_lo}–{cfg.entry_hi}",
            )
            return None
        crowd = self.funding.crowded()
        if crowd == "long" and side == "yes":
            self._set_skip("funding", "funding crowded longs — skip YES")
            return None
        if crowd == "short" and side == "no":
            self._set_skip("funding", "funding crowded shorts — skip NO")
            return None
        size = self._size_for(side_px, exchange_index)
        if size < 1:
            self._set_skip(
                "cash",
                f"cash {_fmt_usd(self.client.cash_on(exchange_index) or 0)} "
                f"too small for {side_px:g}¢",
            )
            return None
        self._set_skip("", "")
        return side, float(join_yes), size

    async def _manage_entry(
            self,
            ticker: str,
            book: BookState,
            strike: float,
            spot: Optional[float],
            secs: float,
            exchange_index: Optional[int],
    ):
        r = self.rest.get(ticker)
        if r and not r.entry:
            # Position is gone but its exit order may still be live — and
            # without reduce_only it could open a fresh position. Cancel it.
            if not self._cooling(ticker):
                await self.pull("order left over after exit")
            return
        want = self._entry_quote(
            ticker, book, strike, spot, secs,
            keeping=r is not None, exchange_index=exchange_index,
        )
        if want is None:
            # Missing data is not a sell signal: a momentary BTC-feed gap
            # nulls the spot, and pulling on it churns quotes for nothing.
            # The server-side expiration bounds a blind rest.
            if spot is None:
                return
            # Soft signal loss: let a young rest sit — it was priced when
            # the signal was on, the server expiry backstops it, and a
            # 50ms-old quote never fills. Hard flips still pull instantly
            # below (side change) and post-fill the soft stop takes over.
            if (
                    r and not self._cooling(ticker)
                    and time.monotonic() - r.placed >= cfg.min_rest_age_s
            ):
                bar = self.btc.required_gap(secs)
                keep_bar = (
                    bar * cfg.vol_keep_frac if bar is not None else None
                )
                detail = (
                    f"gap {_fmt_usd(abs(spot - strike))} vs keep "
                    f"{_fmt_usd(keep_bar) if keep_bar is not None else '?'}"
                    f", pressure {book.pressure:.2f}"
                )
                await self.pull(f"entry signal gone ({detail})")
            return
        side, join_yes, size = want
        if r and r.side != side:
            if self._cooling(ticker):
                return
            if not await self.pull("entry side flipped"):
                return
            r = None
            self._quote_ts[ticker] = 0.0
        if self._cooling(ticker):
            return
        now = time.monotonic()
        if now - self._quote_ts.get(ticker, 0.0) < cfg.quote_min_gap_s:
            return
        if r:
            if abs(r.price - join_yes) >= 1.0:
                self._quote_ts[ticker] = now
                await self._amend_rest(
                    ticker, r, r.filled + r.size, join_yes, exchange_index
                )
            return
        self._quote_ts[ticker] = now
        take_yes = book.best_ask if side == "yes" else book.best_bid
        if take_yes is not None:
            take_px = take_yes if side == "yes" else _complement(take_yes)
            if (
                    cfg.entry_lo <= take_px <= cfg.entry_hi
                    and take_px >= 80.0
            ):
                if await self._sweep_in(
                        ticker, side, size, take_yes, book, exchange_index,
                ):
                    return
        expire = min(cfg.clip_maker_secs, max(5.0, secs - cfg.flatten_secs - 5.0))
        if await self._place_rest(
                ticker, side, size, join_yes, expire, exchange_index, entry=True,
        ):
            side_px = join_yes if side == "yes" else _complement(join_yes)
            log.info(
                f"JOIN {side.upper()} queue: {_fmt_qty(size)} at {side_px:g}¢ "
                "(maker — waiting to be filled)"
            )


# ============================================================
# WEBSOCKET FEED — mutate + wake. Never place from recv.
# ============================================================

class WSFeed:
    def __init__(self, engine: "Engine"):
        self.engine = engine
        self.running = False
        self.ticker: Optional[str] = None
        self._sub_id = 1
        self._sids: dict[str, int] = {}
        self._pyth_btc: Optional[str] = None
        self._listed = False

    def set_ticker(self, ticker: Optional[str]):
        previous_ticker: str = self.ticker or ""
        self.ticker = ticker
        if previous_ticker and previous_ticker != ticker:
            tracker.drop(previous_ticker)

    async def _send(self, ws, payload: dict):
        self._sub_id += 1
        payload["id"] = self._sub_id
        await ws.send(json.dumps(payload))

    async def _boot(self, ws):
        self._sids.clear()
        self._listed = False
        await self._send(ws, {
            "cmd": "subscribe",
            "params": {"channels": ["fill", "market_positions", "user_orders"]},
        })
        await self._send(ws, {
            "cmd": "subscribe",
            "params": {"channels": ["pyth_value"]},
        })
        if self.ticker:
            await self._send(ws, {
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_ticker": self.ticker,
                    "use_yes_price": True,
                },
            })

    async def _sync_book(self, ws, last_sub: Optional[str]) -> Optional[str]:
        if self.ticker == last_sub:
            return last_sub
        sid = self._sids.get("orderbook_delta")
        if last_sub and sid:
            await self._send(ws, {
                "cmd": "update_subscription",
                "params": {
                    "sid": sid,
                    "action": "delete_markets",
                    "market_tickers": [last_sub],
                },
            })
        if self.ticker and sid:
            await self._send(ws, {
                "cmd": "update_subscription",
                "params": {
                    "sid": sid,
                    "action": "add_markets",
                    "market_tickers": [self.ticker],
                },
            })
        elif self.ticker and not sid:
            await self._send(ws, {
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_ticker": self.ticker,
                    "use_yes_price": True,
                },
            })
        return self.ticker

    def _wake(self):
        self.engine.wake.set()

    def _on_message(self, ws_msg: dict) -> Optional[str]:
        typ = ws_msg.get("type")
        raw_body = ws_msg.get("msg")
        body = raw_body if isinstance(raw_body, dict) else {}
        if typ == "error":
            code = body.get("code") or ws_msg.get("code")
            if code == 25 or code == "25":
                return "overflow"
            log.warning(f"Live feed error: {body.get('message') or ws_msg}")
            return None
        if typ == "subscribed":
            channel = body.get("channel")
            sid = body.get("sid") or ws_msg.get("sid")
            if isinstance(channel, str) and sid is not None:
                sid_value = _optional_float(sid)
                if sid_value is not None:
                    self._sids[channel] = int(sid_value)
            return None
        ticker_value = body.get("market_ticker") or body.get("ticker")
        ticker = ticker_value if isinstance(ticker_value, str) else ""
        seq_value = _optional_float(ws_msg.get("seq"))
        seq = int(seq_value) if seq_value is not None else None
        if typ in ("orderbook_snapshot", "orderbook_delta") and ticker != self.ticker:
            return None
        if typ == "orderbook_snapshot" and ticker:
            tracker.apply_snapshot(ticker, body, yes_leg=True, seq=seq)
            self._wake()
        elif typ == "orderbook_delta" and ticker:
            if not tracker.apply_delta(ticker, body, yes_leg=True, seq=seq):
                log.warning(
                    f"Orderbook sequence gap for {ticker}; requesting a fresh snapshot."
                )
                return f"resync:{ticker}"
            self._wake()
        elif typ == "fill" and ticker:
            qty_value = _optional_float(body.get("post_position_fp"))
            if qty_value is None:
                # Never let a missing field read as "flat" — that would wipe
                # live position state while contracts are still held.
                _log_throttled(
                    "fill-no-position",
                    "Fill message without post_position_fp ignored.",
                )
                return None
            qty = round(qty_value, 2)
            clip = self.engine.clip
            old_qty = clip.inventory.get(ticker, 0.0)
            dumped_ago = time.monotonic() - clip._flat_ts.get(ticker, 0.0)
            if dumped_ago < 2.0 and abs(qty) > abs(old_qty) + 0.005:
                # A fill that GROWS the position right after a dump is a
                # late, out-of-order message from the dump's partial fills —
                # applying it resurrects phantom inventory (and phantom
                # trade records). Confirm with REST instead of trusting it.
                log.warning(
                    f"Out-of-order fill for {ticker} "
                    f"({_pos_words(old_qty)} → {_pos_words(qty)} right after "
                    "a dump) — reconciling with REST instead."
                )
                return f"reconcile:{ticker}"
            avg_value = _optional_float(body.get("yes_price_dollars"))
            avg = avg_value * 100.0 if avg_value is not None else None
            have = ticker in self.engine.clip.avg_yes
            old = self.engine.clip.inventory.get(ticker, 0)
            if old != qty:
                log.info(
                    f"FILLED: we now hold {_pos_words(qty)} "
                    f"(was {_pos_words(old)})"
                )
            self.engine.clip.apply_position(
                ticker, qty,
                avg_cents=None if have else avg,
                exit_yes=avg if qty == 0 else None,
            )
            self._wake()
        elif typ in ("market_position", "market_positions"):
            if ticker:
                qty_value = _optional_float(body.get("position_fp"))
                if qty_value is not None:
                    cost = _optional_float(body.get("position_cost_dollars"))
                    self.engine.clip.apply_position(
                        ticker, round(qty_value, 2),
                        cost_dollars=cost,
                    )
            pnl = _optional_float(body.get("realized_pnl_dollars"))
            fees = _optional_float(body.get("fees_paid_dollars")) or 0.0
            if pnl is not None:
                self.engine.note_realized(ticker, pnl - fees)
            self._wake()
        elif typ == "user_order":
            self.engine.clip.apply_user_order(body)
            self._wake()
        elif typ == "pyth_value":
            value = _optional_float(body.get("value_usd"))
            if value is not None:
                self.engine.btc.note(value, "pyth")
                self._wake()
        elif typ == "pyth_value_underlying_list":
            if self._pyth_btc:
                return None
            picked = _pick_btc_underlying(body.get("underlying_tickers"))
            if picked:
                self._pyth_btc = picked
                log.info(f"Pyth Bitcoin feed: {picked}")
        return None

    async def run(self):
        self.running = True
        while self.running:
            try:
                h = self.engine.client.websocket_headers()
                async with websockets.connect(
                        cfg.ws_url,
                        additional_headers=h,
                        ping_interval=15,
                        ping_timeout=8,
                ) as ws:
                    if self.ticker:
                        tracker.drop(self.ticker)
                    await self._boot(ws)
                    last_sub = self.ticker
                    overflow = False
                    while self.running:
                        last_sub = await self._sync_book(ws, last_sub)
                        pyth_sid = self._sids.get("pyth_value")
                        if pyth_sid and not self._listed:
                            self._listed = True
                            await self._send(ws, {
                                "cmd": "update_subscription",
                                "params": {
                                    "sid": pyth_sid,
                                    "action": "underlying_list",
                                },
                            })
                        if pyth_sid and self._pyth_btc:
                            btc = self._pyth_btc
                            self._pyth_btc = None
                            await self._send(ws, {
                                "cmd": "update_subscription",
                                "params": {
                                    "sid": pyth_sid,
                                    "action": "subscribe_underlyings",
                                    "underlying_tickers": [btc],
                                },
                            })
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=0.05)
                        except asyncio.TimeoutError:
                            continue
                        batch = [raw]
                        while len(batch) < 64:
                            try:
                                batch.append(
                                    await asyncio.wait_for(ws.recv(), timeout=0.001)
                                )
                            except asyncio.TimeoutError:
                                break
                        for raw in batch:
                            try:
                                msg = json.loads(raw)
                                if not isinstance(msg, dict):
                                    continue
                                flag = self._on_message(msg)
                                if flag == "overflow":
                                    overflow = True
                                    break
                                if flag and flag.startswith("resync:"):
                                    sid = self._sids.get("orderbook_delta")
                                    resync_ticker = flag.split(":", 1)[1]
                                    if sid is None:
                                        overflow = True
                                        break
                                    await self._send(ws, {
                                        "cmd": "update_subscription",
                                        "params": {
                                            "sid": sid,
                                            "action": "get_snapshot",
                                            "market_tickers": [resync_ticker],
                                        },
                                    })
                                if flag and flag.startswith("reconcile:"):
                                    await self.engine.clip.reconcile_position(
                                        flag.split(":", 1)[1]
                                    )
                            except (
                                    json.JSONDecodeError,
                                    KeyError,
                                    OverflowError,
                                    TypeError,
                                    ValueError,
                            ) as exc:
                                _log_throttled(
                                    "ws-malformed",
                                    f"Ignored malformed live-feed message: "
                                    f"{type(exc).__name__}: {exc}",
                                    every=3.0,
                                )
                        if overflow:
                            break
                    if overflow:
                        log.warning("Live book feed overflowed. Reconnecting.")
            except (
                    asyncio.TimeoutError,
                    OSError,
                    websockets.WebSocketException,
            ):
                log.warning("Live book feed dropped. Reconnecting.")
            if self.running:
                await asyncio.sleep(0.5)

    def stop(self):
        self.running = False


# ============================================================
# MAIN ENGINE
# ============================================================

class Engine:
    def __init__(self):
        self.client = Kalshi()
        self.btc = BTCSpot()
        self.funding = Funding()
        self.clip = Clip(self.client, self.btc, self.funding)
        self.ws = WSFeed(self)
        self.markets: dict[str, dict] = {}
        self.daily_pnl = 0.0
        self._realized: dict[str, float] = {}
        self._pnl_day: Optional[date] = None
        self.day_start_cash: Optional[float] = None
        self._last_shard_sweep: float = 0.0
        self.running = False
        self._shutting_down = False
        self.cycle_lock = asyncio.Lock()
        self._locked_ticker: Optional[str] = None
        self._tasks: list = []
        self.wake = asyncio.Event()
        self._fire_n: int = 0
        self._fire_window: float = 0.0
        self.fire_hz: float = 0.0
        self._last_book_rest: float = 0.0

    def note_realized(self, ticker: Optional[str], pnl: float):
        if ticker:
            self._realized[ticker] = pnl
        if self._realized:
            self.daily_pnl = sum(self._realized.values())

    def request_stop(self):
        self.running = False
        self.btc.running = False
        self.funding.running = False
        self.ws.stop()
        self.wake.set()
        for task in self._tasks:
            if task and not task.done():
                task.cancel()

    async def start(self):
        log.info("======== live trading started ========")
        log.info(
            "Starting live clip trading. Real money. One 15-minute Bitcoin "
            f"market at a time ({cfg.series_ticker})."
        )

        await self.client.connect()
        await self.btc.connect()
        await self.funding.connect()
        await self.client.load_limits()
        await self.client.load_endpoint_costs()
        await self._load_series_fees()
        if not await self._sync_positions():
            log.error("Could not reconcile live positions. Stopping before any orders.")
            await self.client.close()
            await self.btc.close()
            await self.funding.close()
            self._shutting_down = True
            return
        if self.clip.has_any_position():
            log.warning(
                "Existing live Kalshi position detected; new entries remain blocked "
                "until the account is flat."
            )

        poll_task = asyncio.create_task(self.btc.poll())
        fund_task = asyncio.create_task(self.funding.poll())
        spot = self.btc.spot()
        for _ in range(30):
            spot = self.btc.spot()
            if spot is not None:
                break
            await asyncio.sleep(0.3)
        if spot is None:
            log.error("No live Bitcoin price. Stopping before any orders.")
            poll_task.cancel()
            fund_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await poll_task
            with contextlib.suppress(asyncio.CancelledError):
                await fund_task
            await self.client.close()
            await self.btc.close()
            await self.funding.close()
            self._shutting_down = True
            return

        log.info(f"Bitcoin is {_fmt_usd(spot)}.")
        await self.funding._poll_hl()
        await self.funding._poll_kraken()
        log.info(f"Perp funding: {self.funding.words()}.")
        self.running = True

        self._tasks = [
            asyncio.create_task(self.ws.run()),
            poll_task,
            fund_task,
            asyncio.create_task(self._scan_markets()),
            asyncio.create_task(self._fire_loop()),
            asyncio.create_task(self._watch()),
            asyncio.create_task(self._heartbeat()),
        ]

        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def _load_series_fees(self):
        d = await self.client.get_series(cfg.series_ticker)
        if not isinstance(d, dict):
            log.info("Could not read series fees. Using the standard quadratic taker formula.")
            return
        series_data = d.get("series")
        series = series_data if isinstance(series_data, dict) else d
        fee_type_value = series.get("fee_type")
        fee_type = fee_type_value if isinstance(fee_type_value, str) else "quadratic"
        self.clip.fee_mult = _optional_float(series.get("fee_multiplier")) or 1.0
        self.clip.maker_fee_frac = {
            "quadratic": 0.0,
            "flat": 0.0,
            "quadratic_with_maker_fees": 0.25,
            "quadratic_with_combo_maker_fees": 0.5,
        }.get(fee_type, 1.0)
        words = fee_type.replace("_", " ")
        log.info(
            f"{cfg.series_ticker} fees: {words}, multiplier {self.clip.fee_mult:g}, "
            f"maker fraction {self.clip.maker_fee_frac:g}."
        )

    async def _force_flatten(self, ticker: str, reason: str):
        info = self.markets.get(ticker) or {}
        exchange_index = info.get("exchange_index")
        await self.clip.ensure_clean(ticker, exchange_index)
        for _ in range(8):
            inv = self.clip.inventory.get(ticker, 0.0)
            if abs(inv) < 0.005:
                break
            book = tracker.get(ticker)
            if book is None:
                raw = await self.client.get_book(ticker)
                if raw:
                    tracker.update_from_rest(ticker, raw)
                    book = tracker.get(ticker)
            mark = Clip._touch_yes(inv, book) if book is not None else None
            if mark is None or book is None:
                break
            if await self.clip.flatten(
                    ticker, reason, book,
                    Clip._cushioned(mark, inv, 5.0), exchange_index,
                    force=True,
            ):
                break
            await asyncio.sleep(0.05)
        await self.clip.reconcile_position(ticker)

    async def shutdown(self):
        if self._shutting_down:
            return
        self._shutting_down = True
        self.running = False
        self.btc.running = False
        self.funding.running = False
        self.ws.stop()
        open_positions: list[str] = []
        for ticker in list(self.clip.inventory):
            await self._force_flatten(ticker, "shutting down")
            if not self.clip.flat(ticker):
                remaining = self.clip.inventory.get(ticker, 0.0)
                open_positions.append(f"{ticker} {_pos_words(remaining)}")
        pending = [t for t in self._tasks if t and not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        pulled = await self.clip.pull("shutting down")
        await self.client.close()
        await self.btc.close()
        await self.funding.close()
        if open_positions or not pulled:
            details = ", ".join(open_positions) or "resting cancellation failed"
            log.critical(f"STOPPED WITH LIVE EXPOSURE: {details}")
        else:
            log.info("Stopped flat. All bot-owned resting orders pulled.")

    def _pick_cycle(
            self,
            candidates: list[MarketCandidate],
    ) -> Optional[MarketCandidate]:
        if not candidates:
            return None
        spot = self.btc.spot()
        if spot is not None:
            current_spot = float(spot)
            return min(
                candidates,
                key=lambda candidate: abs(candidate["strike"] - current_spot),
            )
        return min(candidates, key=lambda c: c["secs"])

    async def _roll_to(
            self,
            chosen: Optional[MarketCandidate],
            reason: str,
    ):
        async with self.cycle_lock:
            if self._locked_ticker:
                await self._force_flatten(
                    self._locked_ticker, "rolling off this cycle"
                )
                if not self.clip.flat(self._locked_ticker):
                    log.critical(
                        f"ROLLOVER BLOCKED: still holding "
                        f"{_pos_words(self.clip.inventory.get(self._locked_ticker, 0.0))} "
                        f"in {self._locked_ticker}."
                    )
                    return
                if not await self.clip.pull(reason):
                    log.critical("ROLLOVER BLOCKED: bot-owned order cancellation failed.")
                    return
                self.clip.forget(self._locked_ticker)
            if chosen:
                self.markets = {
                    chosen["ticker"]: {
                        "strike": chosen["strike"],
                        "expiry": chosen["expiry"],
                        "secs": chosen["secs"],
                        "exchange_index": chosen.get("exchange_index"),
                    }
                }
                self._locked_ticker = chosen["ticker"]
                self.ws.set_ticker(chosen["ticker"])
                log.info(
                    f"LOCKED new market: BTC above {_fmt_strike(chosen['strike'])}, "
                    f"expires in {_fmt_secs(chosen['secs'])} ({chosen['ticker']})"
                )
            else:
                self.markets = {}
                self._locked_ticker = None
                self.ws.set_ticker(None)
                log.info("UNLOCKED — waiting for the next 15-minute Bitcoin cycle")
            self.wake.set()

    async def _scan_markets(self):
        while self.running:
            markets = await self.client.get_markets(cfg.series_ticker, "open")
            now = datetime.now(timezone.utc)

            if markets is None:
                await asyncio.sleep(2.0)
                continue

            candidates: list[MarketCandidate] = []
            for m in markets:
                ticker = m.get("ticker", "")
                close_str = m.get("close_time", "")
                strike = _market_strike(m)
                if not ticker or not close_str or strike is None:
                    continue
                try:
                    expiry = datetime.fromisoformat(
                        close_str.replace("Z", "+00:00")
                    )
                    secs = (expiry - now).total_seconds()
                except (TypeError, ValueError):
                    continue
                if secs < cfg.cycle_min_secs or secs > cfg.cycle_max_secs:
                    continue
                idx_value = _optional_float(m.get("exchange_index"))
                candidates.append({
                    "ticker": ticker,
                    "strike": strike,
                    "expiry": expiry,
                    "secs": secs,
                    "exchange_index": (
                        int(idx_value) if idx_value is not None else None
                    ),
                })

            locked = self._locked_ticker
            still = next((c for c in candidates if c["ticker"] == locked), None)

            if (
                    still is not None
                    and locked is not None
                    and still["secs"] >= cfg.cycle_min_secs
            ):
                self.markets[locked]["secs"] = still["secs"]
                self.markets[locked]["expiry"] = still["expiry"]
                if still.get("exchange_index") is not None:
                    self.markets[locked]["exchange_index"] = still["exchange_index"]
            elif locked:
                nxt = self._pick_cycle(
                    [c for c in candidates if c["ticker"] != locked]
                )
                await self._roll_to(
                    nxt,
                    "rolling to the next 15-minute cycle"
                    if nxt else "this 15-minute cycle ended",
                )
            else:
                nxt = self._pick_cycle(candidates)
                if nxt:
                    await self._roll_to(nxt, "locking first live cycle")

            await asyncio.sleep(2.0)

    async def _fire_loop(self):
        while self.running:
            self.wake.clear()
            loss_limit = cfg.max_daily_loss
            if self.day_start_cash is not None and self.day_start_cash > 0:
                loss_limit = min(
                    loss_limit,
                    max(1.0, cfg.daily_loss_frac * self.day_start_cash),
                )
            if self.daily_pnl <= -loss_limit:
                log.critical(
                    f"STOPPED trading — down {_fmt_usd(abs(self.daily_pnl))} "
                    f"today (limit {_fmt_usd(loss_limit)})"
                )
                self.request_stop()
                return
            spot = self.btc.spot()
            now = datetime.now(timezone.utc)
            self._note_fire()
            async with self.cycle_lock:
                for ticker, info in list(self.markets.items())[:cfg.max_open_markets]:
                    secs = (info["expiry"] - now).total_seconds()
                    # No orders in the final 3s. Invariant: this must stay
                    # below cfg.flatten_secs, or the end-of-cycle dump in
                    # Clip.tick never gets a chance to run.
                    if secs < 3:
                        continue
                    book = await self._book_for(ticker)
                    if book is None:
                        continue
                    await self.clip.tick(
                        ticker, book, info["strike"], spot, secs,
                        info.get("exchange_index"),
                    )
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=cfg.fire_idle_s)
            except asyncio.TimeoutError:
                pass

    def _note_fire(self):
        now = time.monotonic()
        if self._fire_window <= 0:
            self._fire_window = now
        self._fire_n += 1
        elapsed = now - self._fire_window
        if elapsed >= 1.0:
            self.fire_hz = self._fire_n / elapsed
            self._fire_n = 0
            self._fire_window = now

    async def _book_for(self, ticker: str) -> Optional[BookState]:
        book = tracker.live(ticker, cfg.book_max_age_s)
        if book is not None:
            return book
        stale = tracker.peek(ticker)
        if (
                stale is not None
                and not stale.is_crossed
                and time.monotonic() - stale.ts < cfg.book_stale_ok_s
        ):
            return stale
        now = time.monotonic()
        if now - self._last_book_rest < cfg.book_rest_gap_s:
            return None
        self._last_book_rest = now
        raw = await self.client.get_book(ticker)
        if raw:
            tracker.update_from_rest(ticker, raw)
            return tracker.live(ticker, cfg.book_stale_ok_s)
        return None

    async def _auto_fund_shard(self):
        """Sweep idle cash from other shards onto the locked market's shard.
        Covers a fresh deposit landing on the wrong shard and Kalshi
        re-sharding the series — either would otherwise strand the money.
        Throttled: transfers run in non-atomic steps, so give each one a
        minute to land before judging the balances again."""
        if not self._locked_ticker:
            return
        info = self.markets.get(self._locked_ticker) or {}
        target = info.get("exchange_index")
        if target is None or int(target) < 0:
            return
        target = int(target)
        by_shard = self.client.cash_by_shard
        if not by_shard:
            return
        now = time.monotonic()
        if now - self._last_shard_sweep < 60.0:
            return
        for shard, amount in by_shard.items():
            if shard != target and amount >= 0.01:
                self._last_shard_sweep = now
                await self.client.transfer_to_shard(target, amount, shard)

    async def _sync_positions(self) -> bool:
        data = await self.client.positions()
        if not isinstance(data, dict):
            return False
        rows = data.get("market_positions") or data.get("positions") or []
        current_realized: dict[str, float] = {}
        today = datetime.now(ZoneInfo("America/New_York")).date()
        if self._pnl_day != today:
            # New trading day — yesterday's markets no longer count, and the
            # loss limit re-anchors to whatever cash the new day starts with.
            self._pnl_day = today
            self._realized = {}
            self.day_start_cash = None
        for p in rows:
            if not isinstance(p, dict):
                continue
            ticker_value = p.get("ticker")
            ticker = ticker_value if isinstance(ticker_value, str) else None
            qty = _optional_float(p.get("position_fp"))
            if (
                    ticker is not None
                    and qty is not None
                    and not (self.running and ticker == self._locked_ticker)
            ):
                # While live, the locked market's inventory is owned by the
                # WS fill feed and targeted reconciles; a 10s-old REST
                # snapshot here can race a fresh fill back to zero.
                cost = _optional_float(p.get("position_cost_dollars"))
                self.clip.apply_position(
                    ticker, round(qty, 2),
                    cost_dollars=cost,
                )
            pnl_dollars = _optional_float(p.get("realized_pnl_dollars"))
            fees = _optional_float(p.get("fees_paid_dollars")) or 0.0
            updated = p.get("last_updated_ts")
            updated_date = None
            if isinstance(updated, str):
                try:
                    updated_date = datetime.fromisoformat(
                        updated.replace("Z", "+00:00")
                    ).astimezone(ZoneInfo("America/New_York")).date()
                except ValueError:
                    continue
            elif isinstance(updated, (int, float)):
                # The API may serve epoch seconds here — without this branch
                # the day filter silently never applied to such rows.
                updated_date = datetime.fromtimestamp(
                    float(updated), tz=timezone.utc
                ).astimezone(ZoneInfo("America/New_York")).date()
            if updated_date is not None and updated_date != today:
                continue
            if pnl_dollars is not None and ticker:
                current_realized[ticker] = pnl_dollars - fees
        # Merge, never replace: Kalshi archives settled positions out of
        # GET /portfolio/positions (into /historical/positions) once the
        # event wraps up, so a market that vanishes from this response must
        # keep its realized PnL for the rest of the day.
        self._realized.update(current_realized)
        self.daily_pnl = sum(self._realized.values())
        return True

    def _watch_line(self) -> str:
        now = datetime.now(timezone.utc)
        spot = self.btc.spot()
        hz = f"{self.fire_hz:.0f}Hz" if self.fire_hz else "…"
        locked = self._locked_ticker
        if not locked:
            spot_s = _fmt_usd(spot) if spot is not None else "waiting"
            return f"WATCH {hz}  unlocked  BTC {spot_s}"
        info = self.markets.get(locked) or {}
        strike = info.get("strike")
        expiry = info.get("expiry")
        secs = (expiry - now).total_seconds() if expiry is not None else 0.0
        book = tracker.peek(locked)
        book_age = (
            f"{(time.monotonic() - book.ts) * 1000:.0f}ms"
            if book else "none"
        )
        btc_age = "none"
        fresh = self.btc._fresh()
        if fresh:
            btc_age = f"{min(a for _, _, a in fresh) * 1000:.0f}ms"
        if spot is not None and strike is not None:
            gap = spot - strike
            vs = (
                f"{_fmt_usd(gap)} above"
                if gap >= 0
                else f"{_fmt_usd(abs(gap))} below"
            )
            spot_s = f"{_fmt_usd(spot)}  {vs}"
        else:
            spot_s = "waiting"
        if book and book.best_bid is not None and book.best_ask is not None:
            no_px = _complement(book.best_ask)
            book_s = (
                f"YES {book.best_bid:g}¢/{book.best_ask:g}¢  "
                f"NO {no_px:g}¢  p {book.pressure:.1f}"
            )
        else:
            book_s = "book waiting"
        inv = self.clip.inventory.get(locked, 0.0)
        rest = self.clip.resting_words(locked)
        skip = self.clip.quote_skip
        why = f"  skip {skip}" if skip else "  live"
        pos = _pos_words(inv)
        return (
            f"WATCH {hz}  fire  book {book_age}  btc {btc_age}  "
            f"{_fmt_secs(secs)} left  BTC {spot_s}  {book_s}  "
            f"{pos}  rest {rest}{why}"
        )

    async def _watch(self):
        await asyncio.sleep(0.2)
        while self.running:
            log.info(self._watch_line())
            await asyncio.sleep(cfg.watch_s)

    async def _heartbeat(self):
        await asyncio.sleep(1.0)
        while self.running:
            cash_s = "waiting"
            value_s = "waiting"
            bal = await self.client.balance()
            if isinstance(bal, dict):
                cash_dollars = _optional_float(bal.get("balance_dollars"))
                cash_cents = _optional_float(bal.get("balance"))
                portfolio_cents = _optional_float(bal.get("portfolio_value"))
                if cash_dollars is not None:
                    cash_s = _fmt_usd(cash_dollars)
                elif cash_cents is not None:
                    cash_s = _fmt_usd(cash_cents / 100.0)
                if portfolio_cents is not None:
                    value_s = _fmt_usd(portfolio_cents / 100.0)
                if self.day_start_cash is None and self.client.cash is not None:
                    # Available cash alone is $0.01 with a live clip on —
                    # that made tonight's loss cap $1.00 and killed a $9 stack.
                    port = (
                        portfolio_cents / 100.0
                        if portfolio_cents is not None else 0.0
                    )
                    self.day_start_cash = self.client.cash + max(0.0, port)
            await self._auto_fund_shard()
            await self._sync_positions()

            now = datetime.now(timezone.utc)
            active = list(self.markets.items())[:cfg.max_open_markets]
            spot = self.btc.spot()
            spot_s = _fmt_usd(spot) if spot is not None else "waiting"

            if not active:
                log.info(
                    "HEARTBEAT\n"
                    f"  Cash: {cash_s} available  |  account value: {value_s}  |  "
                    f"realized today: {_fmt_pnl(self.daily_pnl)}\n"
                    "  Locked market: none — waiting for the next 15-minute Bitcoin contract\n"
                    f"  Bitcoin now: {spot_s}\n"
                    "  Book: none\n"
                    "  Position: flat  |  our resting orders: none"
                )
            else:
                ticker, info = active[0]
                secs = (info["expiry"] - now).total_seconds()
                strike = info["strike"]
                vs = "waiting"
                if spot is not None:
                    gap = spot - strike
                    if gap >= 0:
                        vs = f"{_fmt_usd(gap)} above the line"
                    else:
                        vs = f"{_fmt_usd(abs(gap))} below the line"
                book = tracker.peek(ticker)
                if book and book.is_crossed:
                    book_s = "book looks backwards — ignoring until it uncrosses"
                elif book:
                    best_bid = book.best_bid
                    best_ask = book.best_ask
                    if best_bid is None or best_ask is None:
                        book_s = "waiting for live prices"
                    else:
                        flow = f"{book.pressure:.1f} to 1"
                        book_s = (
                            f"best buy {best_bid}¢  |  best sell {best_ask}¢  |  "
                            f"buyers vs sellers: {flow}"
                        )
                else:
                    book_s = "waiting for live prices"
                inv = self.clip.inventory.get(ticker, 0)
                avg = self.clip.avg_yes.get(ticker)
                avg_s = f"  |  avg {avg:.1f}¢" if avg is not None else ""
                rest_s = self.clip.resting_words(ticker)
                skip = self.clip.quote_skip
                why = (
                    f"  |  {skip}"
                    if rest_s == "none" and skip else ""
                )
                log.info(
                    "HEARTBEAT\n"
                    f"  Cash: {cash_s} available  |  account value: {value_s}  |  "
                    f"realized today: {_fmt_pnl(self.daily_pnl)}\n"
                    f"  Locked market: {ticker}  |  "
                    f"Will BTC be above {_fmt_strike(strike)}?  |  {_fmt_secs(secs)} left\n"
                    f"  Bitcoin now: {spot_s}  |  {vs}  |  clip\n"
                    f"  Funding: {self.funding.words()}\n"
                    f"  Book: {book_s}\n"
                    f"  Position: {_pos_words(inv)} on this market{avg_s}  |  "
                    f"our resting orders: {rest_s}{why}"
                )
            await asyncio.sleep(cfg.heartbeat_s)


# ============================================================
# ENTRY
# ============================================================

def main():
    if not cfg.access_key or not cfg.private_key_path:
        print("Set KALSHI_ACCESS_KEY and KALSHI_PRIVATE_KEY_PATH")
        sys.exit(1)
    if not os.path.isfile(cfg.private_key_path):
        print(f"Private key file not found: {cfg.private_key_path}")
        sys.exit(1)

    engine = Engine()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, engine.request_stop)

    try:
        loop.run_until_complete(engine.start())
    except KeyboardInterrupt:
        engine.request_stop()
        loop.run_until_complete(engine.shutdown())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
