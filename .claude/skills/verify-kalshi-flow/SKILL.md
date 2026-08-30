---
name: verify-kalshi-flow
description: Verify the single-file live KXBTC15M bot against current Kalshi API contracts and the project's money-path execution invariants without submitting orders or starting the bot.
---

# Verify Kalshi Flow

Verify `flow.py` as a production-only Kalshi client. Report evidence, not profit guarantees.

## Workflow

1. Read `AGENTS.md`, `flow.py`, and `pyproject.toml`. Confirm the executable path remains a single live strategy and that no paper, demo, disabled strategy, state-file, accounting-file, or lock-file path was introduced.
2. Consult current official `https://docs.kalshi.com/` documentation for authentication, fixed-point prices/counts, order creation, amendment, cancellation, positions, fills, WebSocket subscriptions, order-book snapshots, and order-book deltas. Use primary Kalshi documentation only.
3. Compare every Kalshi REST method and WebSocket parser in `flow.py` with those current contracts. Check route, HTTP verb, request field, response field, side/action semantics, bid-only book interpretation, fixed-point precision, and sequence handling.
4. Trace the live lifecycle end to end: market selection -> book/BTC freshness -> entry qualification -> IOC entry acknowledgement -> fill reconciliation -> maker exit/amend -> partial-fill reconciliation -> taker-green or scratch exit -> rollover/shutdown reconciliation. Confirm no order is submitted from the `ws.recv()` receive loop.
5. Confirm all cancellation is scoped to bot-owned order IDs, entry uncertainty cannot double-submit, one-cent IOC exits are refused, and unresolved exposure is preserved and surfaced rather than declared flat.
6. Query only unauthenticated public production Kalshi endpoints needed to confirm current KXBTC15M market availability and response shapes. Never use credentials, start `flow.py`, submit/amend/cancel an order, or change a running process.
7. Classify findings as CRITICAL, HIGH, MEDIUM, or LOW. Include exact file and line, violated contract/invariant, evidence, and a narrowly scoped repair. PASS only when every lifecycle boundary above is supported by source evidence and current official documentation.

## PASS Criteria

- REST and WebSocket schemas match current official Kalshi contracts.
- Decimal prices and quantities are preserved through books, orders, fills, positions, and PnL.
- Entry, partial fill, exit, rollover, and shutdown paths cannot silently duplicate orders or discard exposure.
- Runtime writes remain limited to `rogue.log`; the verifier itself is development metadata, not bot state.
- Public production API evidence confirms the configured market series exists; no authenticated mutation is performed.

## Exceptions

- `.claude/skills/verify-kalshi-flow/SKILL.md` is explicitly requested verifier metadata and is not a prohibited runtime sidecar.
- Existing IDE metadata, caches, bytecode, and unrelated user-owned workspace files are outside this verifier's scope.
- Build, lint, and unit-test failures belong to the technical verification pass; record them separately rather than duplicating them as pattern findings.
- Profitability is stochastic. Passing verifies implementation integrity and API compatibility, not guaranteed profit.

## Related Files

| File | Purpose |
|------|---------|
| `flow.py` | Entire live trading and execution implementation |
| `pyproject.toml` | Runtime dependency declarations |
| `AGENTS.md` | Project-specific trading and operational invariants |
