#!/usr/bin/env python3
"""
Momentum bot — offensive rewrite of flow.py trade logic.

Strategy: pure momentum. Enter on strong BTC direction via IOC sweep,
ride to expiry, exit on reversal or final-second IOC. No defense
rotation, no maker entries waiting to be picked off.

Math:
  - Entry: BTC must be $5+ through strike AND moving at >= 2USD/s
    (beats 5s noise on 15-min BTC binary). IOC sweep the 1-tick offer.
  - Exit (reversal): BTC crosses back through strike by vol bar.
    IOC sweep out to stop losses before they accelerate.
  - Exit (time): Last 10s — IOC sweep toward 0/100 for convergence edge.
  - Sizing: full clip_risk_frac (20% of shard cash) — momentum needs
    big positions to outpace fees.

Usage:
  python momentum.py            # Syntax/imports check
  python momentum.py --live     # Run with real Kalshi API
"""

import asyncio
import logging
import math
import os
import sys
from datetime import datetime, timezone
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import flow
from flow import (
    cfg, log, _order_fill, _rejected, _complement, _taker_fee_dollars,
    _fmt_qty,
)
from flow import BTCSpot, BookState, Kalshi, Funding, tracker

# --- Momentum config ---
MOM_ENTRY_VOL_MULT = 2.0    # 2x vol bar for entry velocity
MOM_EXIT_SECS = 10.0        # IOC exit window at end of cycle
MOM_BTC_HISTORY = 60        # Keep last ~24s of BTC spot samples for velocity
MOM_REVERSAL_MULT = 1.0     # Exit reversal at 1x vol bar
# KXBTC15M oscillates $5-15 around strike every 5-10s. Velocity must
# beat chop: $5 in 5s = 1.0/s is real momentum, not noise.
MOM_MIN_VELOCITY = 1.0      # USD/sec minimum BTC velocity for entry


class MomentumBot:
    """Pure momentum entry/exit. Replaces Clip's defensive trade logic.

    Reuses Engine's Kalshi/BTC/Funding connections but replaces the
    entry/exit strategy entirely. IOC-only: no maker orders, no defense
    rotations, no time-based maker rests.
    """

    def __init__(self, engine: "flow.Engine"):
        self.engine = engine
        self.client = engine.client
        self.btc = engine.btc
        self.funding = engine.funding
        self.clip = engine.clip  # For sizing, fees, cash checks

        # State
        self.inventory: dict[str, float] = {}
        self.entry_cents: dict[str, float] = {}
        self.fees: dict[str, float] = {}
        self.exit_reason: dict[str, str] = {}
        self._cooling: set[str] = set()
        self._cool_ts: dict[str, float] = {}
        self.btc_history: list[tuple[float, float]] = []  # (ts, spot)
        self.tick_count: int = 0

    # ----- Sizing / fees from the existing Clip -----

    def _fee_mult(self) -> float:
        return self.clip.fee_mult

    def _size_for(self, side_px: float, exchange_index: Optional[int]) -> float:
        """Largest clip the shard's cash can cover, using Clip's logic."""
        return self.clip._size_for(side_px, exchange_index)

    def _cool(self, ticker: str, secs: Optional[float] = None):
        """Cooldown to prevent quote churn on rejected fills."""
        self.clip._cool(ticker, secs)
        self._cooling.add(ticker)

    def _cooling_ok(self, ticker: str) -> bool:
        return ticker not in self._cooling and not self.clip._cooling(ticker)

    # ----- BTC momentum tracking -----

    def _update_btc(self, spot: Optional[float]):
        """Track BTC spot history for momentum velocity calc."""
        if spot is None:
            return
        ts = datetime.now(timezone.utc).timestamp()
        self.btc_history.append((ts, spot))
        if len(self.btc_history) > MOM_BTC_HISTORY:
            self.btc_history = self.btc_history[-MOM_BTC_HISTORY:]

    def _btc_velocity(self) -> float:
        """BTC price velocity (USD/sec) over recent history.
        Returns signed velocity: positive = rising, negative = falling.
        """
        hist = self.btc_history
        if len(hist) < 3:
            return 0.0
        dt = hist[-1][0] - hist[0][0]
        if dt < 0.3:
            return 0.0
        dprice = hist[-1][1] - hist[0][1]
        return dprice / dt  # signed: positive = up, negative = down

    def _vol_bar(self, secs: float) -> float:
        """Required BTC movement threshold at this time-to-expiry."""
        bar = self.btc.required_gap(secs)
        if bar is not None:
            return bar
        return cfg.min_btc_gap

    # ----- Momentum signal -----

    def _momentum_side(
        self, strike: float, spot: Optional[float], secs: float
    ) -> Optional[str]:
        """Check if momentum entry conditions are met.

        Returns 'yes' or 'no' if:
          1. BTC is $25+ through the strike (min_btc_gap)
          2. BTC is moving fast (2x vol bar velocity)
          3. ALL BTC sources agree on direction (consensus)

        Returns None otherwise.
        """
        if spot is None:
            return None
        if secs < 10.0:
            return None  # Too late to enter

        vol_bar = self._vol_bar(secs)
        gap = abs(spot - strike)
        if gap < cfg.min_btc_gap:
            return None

        # Momentum: BTC must be moving AWAY from the strike with real
        # directional velocity, not just oscillating. For NO entry, BTC
        # must be falling; for YES, rising. KXBTC15M oscillates $5-15
        # around strike, so >= $1/s directional velocity confirms momentum.
        velocity = self._btc_velocity()
        required_vel = max(
            MOM_MIN_VELOCITY,
            (vol_bar / max(secs, 1.0)) * MOM_ENTRY_VOL_MULT,
        )

        # Consensus across all BTC sources
        if spot > strike + cfg.min_btc_gap:
            # YES entry: BTC above strike AND rising with momentum
            if velocity >= required_vel and self.btc.all_on_side(strike, yes_side=True):
                return "yes"
        elif spot < strike - cfg.min_btc_gap:
            # NO entry: BTC below strike AND falling with momentum
            if velocity <= -required_vel and self.btc.all_on_side(strike, yes_side=False):
                return "no"
        return None

    # ----- Main tick -----

    async def tick(
        self,
        ticker: str,
        book: BookState,
        strike: float,
        spot: Optional[float],
        secs: float,
        exchange_index: Optional[int] = None,
    ):
        """Momentum tick: entry on IOC sweep, exit on reversal or time."""
        self.tick_count += 1
        self._update_btc(spot)

        inv = self.inventory.get(ticker, 0.0)

        if abs(inv) < 0.005:
            await self._try_entry(ticker, book, strike, spot, secs, exchange_index)
        else:
            await self._try_exit(ticker, book, strike, spot, secs, exchange_index)

    # ----- Entry -----

    async def _try_entry(
        self, ticker, book, strike, spot, secs, exchange_index
    ):
        if not self._cooling_ok(ticker):
            return
        if self.clip.has_any_position() and ticker not in self.inventory:
            return

        side = self._momentum_side(strike, spot, secs)
        if side is None:
            return

        take_yes = book.best_ask if side == "yes" else book.best_bid
        if take_yes is None:
            return

        take_px = take_yes if side == "yes" else _complement(take_yes)
        if take_px <= 1.0 or take_px >= 99.0:
            return

        size = self._size_for(take_px, exchange_index)
        if size < 1:
            return

        result = await self.client.place(
            ticker, side, size, take_px,
            tif="immediate_or_cancel",
            post_only=False,
            exchange_index=exchange_index,
        )

        if result is None or _rejected(result):
            self._cool(ticker)
            return

        filled = _order_fill(result) or 0.0
        if filled < 1:
            return  # Offer swept away, try next tick

        signed = filled if side == "yes" else -filled
        taker_fee = _taker_fee_dollars(filled, take_yes / 100.0, self._fee_mult())
        self.inventory[ticker] = self.inventory.get(ticker, 0.0) + signed
        self.entry_cents[ticker] = take_yes
        self.fees[ticker] = taker_fee

        vel = self._btc_velocity()
        log.info(
            f"MOM-ENTRY {side.upper()} {_fmt_qty(filled)} at {take_px:.2f}¢ "
            f"(BTC gap ${abs(spot - strike):.2f} {'above' if spot > strike else 'below'} "
            f"strike ${strike:,.2f}, vel={vel:.1f}/s)"
        )

    # ----- Exit: momentum reversal -----

    async def _exit_reversal(self, ticker, book, exchange_index, reason):
        """IOC sweep out when BTC reverses through the strike."""
        inv = self.inventory.get(ticker, 0.0)
        if abs(inv) < 0.005:
            return
        side = "yes" if inv > 0 else "no"

        yes_px = book.best_bid if side == "yes" else book.best_ask
        if yes_px is None:
            return

        exit_side = "no" if side == "yes" else "yes"
        exit_px = yes_px if exit_side == "yes" else _complement(yes_px)
        qty = round(abs(inv), 2)

        result = await self.client.place(
            ticker, exit_side, qty, exit_px,
            tif="immediate_or_cancel",
            reduce_only=True,
            exchange_index=exchange_index,
        )

        if not _rejected(result):
            filled = _order_fill(result) or 0.0
            self._record_exit(ticker, qty, filled, reason)
            log.info(
                f"EXIT {side.upper()} {_fmt_qty(filled)} @ {exit_px:.2f}¢ "
                f"— {reason}"
            )

    # ----- Exit: time-based final sweep -----

    async def _exit_time(self, ticker, book, exchange_index):
        """IOC sweep toward 0/100 in the last MOM_EXIT_SECS seconds."""
        inv = self.inventory.get(ticker, 0.0)
        if abs(inv) < 0.005:
            return
        side = "yes" if inv > 0 else "no"
        qty = round(abs(inv), 2)

        if side == "yes":
            target = min(99.0, book.best_ask or 50.0)
            await self._ioc_exit(ticker, "no", qty, _complement(target), exchange_index, "time-sweep")
        else:
            target = max(1.0, book.best_bid or 50.0)
            await self._ioc_exit(ticker, "yes", qty, target, exchange_index, "time-sweep")

    async def _ioc_exit(self, ticker, exit_side, qty, price, exchange_index, reason):
        result = await self.client.place(
            ticker, exit_side, qty, price,
            tif="immediate_or_cancel",
            reduce_only=True,
            exchange_index=exchange_index,
        )
        if not _rejected(result):
            filled = _order_fill(result) or 0.0
            self._record_exit(ticker, qty, filled, reason)
            log.info(f"EXIT {exit_side.upper()} {_fmt_qty(filled)} @ {price:.2f}¢ — {reason}")

    def _record_exit(self, ticker, qty, filled, reason):
        if filled >= qty - 0.005:
            self.inventory.pop(ticker, None)
            self.entry_cents.pop(ticker, None)
        self.exit_reason[ticker] = reason

    # ----- Combined exit check -----

    async def _try_exit(self, ticker, book, strike, spot, secs, exchange_index):
        inv = self.inventory.get(ticker, 0.0)
        if abs(inv) < 0.005:
            return

        side = "yes" if inv > 0 else "no"
        vol_bar = self._vol_bar(secs)
        rev_need = max(cfg.min_btc_gap, vol_bar * MOM_REVERSAL_MULT)

        # Exit 1: momentum reversal
        if spot is not None:
            if side == "yes" and spot < strike - rev_need:
                await self._exit_reversal(ticker, book, exchange_index, "momentum-reversal")
                return
            if side == "no" and spot > strike + rev_need:
                await self._exit_reversal(ticker, book, exchange_index, "momentum-reversal")
                return

        # Exit 2: end of cycle
        if secs < MOM_EXIT_SECS:
            await self._exit_time(ticker, book, exchange_index)
            return

    # ----- Position tracking -----

    def has_any_position(self) -> bool:
        return any(abs(v) > 0.005 for v in self.inventory.values())

    def note_realized(self, ticker: Optional[str], pnl: float):
        """Called by engine when a position is closed."""
        if ticker and ticker in self.inventory:
            self._record_exit(ticker, 0, 0, "settled")

    # ----- Bookkeeping -----

    async def ensure_clean(self, ticker: str, exchange_index: Optional[int]):
        """Flush any residual position."""
        if ticker not in self.inventory:
            return
        book = tracker.peek(ticker)
        if book is not None:
            await self._exit_time(ticker, book, exchange_index)
        self.inventory.pop(ticker, None)
        self.entry_cents.pop(ticker, None)

    def reconcile_position(self, ticker: str):
        """Sync local inventory from exchange."""
        inv = self.clip.inventory.get(ticker, 0.0)
        self.inventory[ticker] = inv

    def resting_words(self, ticker: str) -> str:
        return ""  # Momentum bot has no resting orders


if __name__ == "__main__":
    print(f"Config loaded: {cfg.series_ticker}")
    print(f"Momentum bot: entry={MOM_ENTRY_VOL_MULT}x vol bar, exit={MOM_EXIT_SECS}s")
    print(f"  clip_risk_frac: {cfg.clip_risk_frac}, min_btc_gap: {cfg.min_btc_gap}")
    print("Syntax OK — ready to run.")
