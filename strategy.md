# Strategy: Polymarket BTC "Up or Down" 15-minute — Latency Arbitrage

This document describes how the bot decides to trade. It trades a binary option:
*"Will BTC be higher at the end of this 15-minute window than at the window's open?"*
The winning side pays **$1.00** per share, the loser $0.

---

## 0. The idea

A historical study found that the obvious indicator model (Monte Carlo + RSI/MACD/HA)
had **no predictive edge** over a trivial baseline — *"is spot already above the
window's open right now?"* Whoever is winning at the checkpoint usually wins, and that
persistence is fully visible to, and priced by, the market.

The only edge left is **latency**: acting on a Binance spot move *before* Polymarket's
thin book reprices it. The strategy is built around that one idea.

---

## 1. The pipeline

Every second ([`update_loop`](main.py)):

```
Binance spot ─► fair_prob_up ─► EV vs Polymarket price ─► decide_ev ─► size (%/fixed) ─► execute
(fast feed)     (closed-form    (edge = fair − ask;       (EV +         (% or flat $  (FOK,
                GBM)            book stale?)               min-prob)     of balance)   paper/live)
```

A trade fires only when the model's **fast** fair probability beats the market's
(possibly stale) price by a margin.

---

## 2. Fair probability — fast and closed-form
[`fair_prob_up`](bot/indicators.py). The probability that price closes above the
window's open is a closed-form GBM expression:

```
z         = (ln(strike / spot) − n·μ) / (σ·√n)
prob_up   = ½·erfc(z / √2)            # = 1 − Φ(z)
```

where `n = ceil(minutes_left / 5)`, and `μ, σ` are the realized per-5m-candle log-
return drift and vol ([`realized_drift_vol`](bot/indicators.py), 300-candle lookback).
It is equivalent to a Monte-Carlo simulation of the same process but ~1000× cheaper —
which a latency strategy needs. The model is, in essence, *"is spot above the open,
given the volatility still to come?"*

---

## 3. The edge — fair vs the market
[`update_loop`](main.py). Polymarket's prices are normalized to an implied
probability, then:

```
edge_up = fair_up − market_implied_up
```

A positive edge means the book hasn't yet repriced the move our fast feed already
sees. **That gap is the entire thesis.**

---

## 4. Entry decision — the EV gate
[`decide_ev`](bot/engines.py). The side with the higher **expected value**
`EV = fair_side − ask_price_side` is chosen. A trade is taken only if **all** pass:

1. **EV ≥ `EV_THRESHOLD`** (default 0.04) — the book is meaningfully stale.
2. **Fair prob ≥ `MIN_PROB_EV`** (default 0.55) — never bet near-coinflips.

There are no other filters (the RSI / Heiken-Ashi vetoes were removed — they
fought the model's own persistence logic). There is **no flat price cap** — EV
governs reward/risk instead.

---

## 5. Sizing — percent or fixed
[`execute_trade`](main.py). The stake (dollars put at risk) is set by `RISK_TYPE`:
**`percent`** stakes `RISK_VALUE`% of the current balance; **`fixed`** stakes a flat
`RISK_VALUE` dollars. The stake is then trimmed so it never outsizes the ask-side
book liquidity (`MIN_BOOK_LIQUIDITY_USD`), and capped at the available balance.

---

## 6. Execution
[`execute_trade`](main.py). One position at a time. **Paper:** debits the simulated
balance. **Live:** a slippage-capped marketable Fill-Or-Kill BUY on the CLOB
(limit = quote + `CLOB_MAX_SLIPPAGE`). Optional close-and-flip on a strong opposite
signal ([`maybe_flip_position`](main.py), `FLIP_ENABLED`, default off).

---

## 7. Exits & resolution
[`update_trades`](main.py). Positions are held to the 15-minute expiry and resolved
robustly: authoritative Polymarket settlement first, settlement-price-vs-strike
fallback once the window is over, and a 5-minute grace before voiding (paper stake
refunded) so a trade can never stay stuck open.

---

## 8. Logging

[`logs/signals.csv`](logs/) records one row per tick — the fair/market probabilities,
the edge, and the exact decision/blocking reason — so you can see what the bot did and
why. Run in **paper mode** first and confirm EV-positive trades actually appear against
the real book before risking capital; if they don't, the book isn't slow and the
latency edge isn't there.

---

## 9. Tunable parameters

| Setting | Default | Meaning |
|---------|---------|---------|
| `EV_THRESHOLD` | 0.04 | Min expected value (fair − price) to enter. |
| `MIN_PROB_EV` | 0.55 | Min fair probability to enter. |
| `RISK_TYPE` / `RISK_VALUE` | percent / 10 | Stake sizing: % of balance, or flat $. |
| `MIN_BOOK_LIQUIDITY_USD` | 20.0 | Skip if the ask side can't absorb the stake. |
| `FLIP_ENABLED` | false | Close-and-flip on a strong opposite signal. |
| `CLOB_MAX_SLIPPAGE` | 0.02 | Live-order limit buffer. |

---

*This is not financial advice. The edge is unproven — the latency thesis is unverified
until measured live. Use at your own risk; live mode trades real funds.*
