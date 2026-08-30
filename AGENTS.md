## Learned User Preferences

- Only live/production Kalshi. No paper, demo, or dry-run path.
- Do not add state files, accounting files, extra docs, or sidecars. `rogue.log` is the only log file allowed.
- Do not add disabled flags or dead strategy branches; delete unused paths instead.
- Focus on the live money path. Skip IDE, linter, and warning-only issues.
- Rejected: 2¢ MM take-profit, half-size rest, daily profit lock, sniper adds, two-sided quotes, sitting out a side after a scratch, and fade-rotating when the BTC lead shrinks but has not crossed. Defense is rotating onto the new BTC favorite only after the strike cross, not chopping a still-correct clip.
- This is a Kalshi prediction clip, not a perp trading bot. Perp funding is a read-only crowded-side veto; do not route orders to Hyperliquid, Binance, Bybit, or Kraken.
- Keep a visible memory-only `WATCH` pulse; a silent fire loop looks idle.
- Do not VPN or proxy to ungeoblock Binance/Bybit trading on this US machine.
- Never start, restart, or supervise live trading. Only the user starts actual runs (`supervise.sh` or the inner `caffeinate -dismu` + `flow.py run`); do not start both.
- Keep structure changes as pre-written fire-loop rules. Do not put an LLM or after-window retune in the live path.

## Learned Workspace Facts

- This repo is a single-file live Clip bot in `flow.py` for Kalshi 15-minute BTC (`KXBTC15M`), not `kbtc_winner`.
- Auth is Kalshi access-key ID + RSA PEM via `KALSHI_ACCESS_KEY` and `KALSHI_PRIVATE_KEY_PATH` (no API secret string).
- Clip: always on the BTC-agreed favorite (62–96¢, book pressure only when the BTC gap is under $25, `|spot−strike| ≥ $25`, vol bar 2.0×σ of 5s moves **not** scaled to time-left; if σ isn't ready yet, the $25 floor still applies). Size up to 10, also capped at 20% of shard cash so a dump cannot take ~30% of a ~$10 stack; leftover entry rests cancel once filled to the cap. IOC-sweep only if the 1-tick take is 80–96¢; otherwise post-only join the touch. Rotate only once BTC is **through the strike by the entry floor** ($25 / 2σ), except a clip already ≤15¢ rides expiry. Do not dump on a hairline poke while WATCH still has BTC on our side. Maker rest at remaining-to-$1 (`P + 0.45*(1-P)`); first-green taker only in the last 25s; never IOC at 1¢. Size is 0 if shard cash is unknown. Do not flatten on a stale BTC hitch. No two-sided quotes. Crowded perp funding is a read-only skip (Hyperliquid BTC hourly, 0.55 ann ≈ 0.05%/8h): veto join YES when longs are packed and NO when shorts are packed; stale feed fails open. Memory-only `WATCH` line every 0.4s; fire loop must not REST-hammer a stale book. Daily-loss cap anchors on cash **plus** open clip value, not available cash alone. Live billed KXBTC15M taker `fee_cost` still matches quadratic M=1 as of 2026-08-28 morning fills; Platinum VIP is not reflected as a lower `fee_multiplier` on this series.
- KXBTC15M fees (live GET `/series/KXBTC15M`): `quadratic`, multiplier 1, maker fills free. API usage tier does **not** discount event-contract fees. Live GET `/account/limits` on 2026-08-27: Advanced, 300/300 tokens/s, buckets 900/900. Create costs 10, cancel 2. `fapi.binance.com` / `api.binance.com` / `api.bybit.com` are geo-blocked here; Hyperliquid + Kraken PF_XBTUSD basis are the live funding sources.
- `supervise.sh` is the one-terminal live wrapper: loads `KALSHI_*` from `~/.zshrc`, `caffeinate -dismu` around `flow.py run`, 15s crash-restart; a clean stop (Ctrl+C, SIGTERM, daily-loss) stays down.
- Do not place orders from `ws.recv()` (buffer overflow).
- Verify Kalshi against live docs.kalshi.com, not local `kbtc_winner` copies.
