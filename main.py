import asyncio
import time
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from bot.config import settings
import bot.data as data
import bot.ws_data as ws_data
import bot.chainlink as chainlink
import bot.indicators as indicators
import bot.engines as engines
import bot.utils as utils
from bot.clob_trader import clob_trader

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load previous state
    load_state()

    # Initial seeding
    await seed_kline_buffers()

    # Start all background tasks
    tasks = [
        asyncio.create_task(binance_stream.start()),
        asyncio.create_task(binance_kline_1m.start()),
        asyncio.create_task(binance_kline_5m.start()),
        asyncio.create_task(polymarket_ws_stream.start()),
        asyncio.create_task(chainlink_ws_stream.start()),
        asyncio.create_task(update_loop())
    ]

    yield

    # Shutdown cleanup
    for task in tasks:
        task.cancel()

    binance_stream.close()
    binance_kline_1m.close()
    binance_kline_5m.close()
    polymarket_ws_stream.close()
    chainlink_ws_stream.close()

app = FastAPI(title="Polymarket BTC 15m Assistant", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

# Global state to store the latest data
state = {
    "latest_data": {},
    "last_update_ts": 0,
    "trading_mode": settings.MODE,
    "paper_balance": settings.PAPER_BALANCE_USD,
    "active_trades": [],
    "trade_history": [],
    "logs": [],
    "last_trade_side": None,
    "last_balance_refresh": 0,
    # Per-window Chainlink OPEN prices, keyed by window start ms:
    #   {start_ms: {"chainlink": float|None, "binance": float|None, "genuine": bool}}
    # Used to settle trades the way Polymarket does (Chainlink close vs open).
    "market_opens": {},
    "last_window_start": None
}

def save_state():
    try:
        data_to_save = {
            "paper_balance": state["paper_balance"],
            "active_trades": state["active_trades"],
            "trade_history": state["trade_history"],
            "last_trade_side": state["last_trade_side"]
        }
        with open("state_data.json", "w") as f:
            json.dump(data_to_save, f, indent=2)
            
        if os.path.exists("config.json"):
            with open("config.json", "r") as f:
                cfg = json.load(f)
            cfg["paper_balance_usd"] = state["paper_balance"]
            with open("config.json", "w") as f:
                json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"Error saving state: {e}")

def load_state():
    try:
        if os.path.exists("state_data.json"):
            with open("state_data.json", "r") as f:
                loaded = json.load(f)
                state["paper_balance"] = loaded.get("paper_balance", settings.PAPER_BALANCE_USD)
                state["active_trades"] = loaded.get("active_trades", [])
                state["trade_history"] = loaded.get("trade_history", [])
                state["last_trade_side"] = loaded.get("last_trade_side")
                log_message("State loaded from state_data.json")
    except Exception as e:
        print(f"Error loading state: {e}")

def log_message(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted)
    state["logs"].append(formatted)
    if len(state["logs"]) > 100:
        state["logs"].pop(0)

def get_ws_symbol_filter(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("USDT"):
        return s[:-4].lower()
    return s.lower()

# Background task instances
binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL)
binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)

polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
    ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
    symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
)
chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))

def get_candle_window_timing(window_minutes: int) -> Dict[str, float]:
    now_ms = time.time() * 1000
    window_ms = window_minutes * 60_000
    start_ms = (now_ms // window_ms) * window_ms
    end_ms = start_ms + window_ms
    elapsed_ms = now_ms - start_ms
    remaining_ms = end_ms - now_ms
    return {
        "startMs": start_ms,
        "endMs": end_ms,
        "elapsedMs": elapsed_ms,
        "remainingMs": remaining_ms,
        "elapsedMinutes": elapsed_ms / 60_000,
        "remainingMinutes": remaining_ms / 60_000
    }

async def fetch_polymarket_snapshot() -> Dict[str, Any]:
    market = None
    if settings.POLYMARKET_SLUG:
        market = await data.fetch_market_by_slug(settings.POLYMARKET_SLUG)
    elif settings.POLYMARKET_AUTO_SELECT_LATEST:
        events = await data.fetch_live_events_by_series_id(settings.POLYMARKET_SERIES_ID)
        markets = data.flatten_event_markets(events)

        now = time.time() * 1000
        live_markets = [m for m in markets if m.get("endDate") and datetime.fromisoformat(m["endDate"].replace('Z', '+00:00')).timestamp() * 1000 > now]
        if live_markets:
            live_markets.sort(key=lambda x: x["endDate"])
            market = live_markets[0]

    if not market:
        return {"ok": False, "reason": "market_not_found"}

    outcomes = market.get("outcomes", [])
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    clob_token_ids = market.get("clobTokenIds", [])
    if isinstance(clob_token_ids, str):
        clob_token_ids = json.loads(clob_token_ids)

    outcome_prices = market.get("outcomePrices", [])
    if isinstance(outcome_prices, str):
        outcome_prices = json.loads(outcome_prices)

    up_token_id = None
    down_token_id = None

    for i, outcome in enumerate(outcomes):
        token_id = clob_token_ids[i] if i < len(clob_token_ids) else None
        if not token_id: continue
        if outcome.lower() == settings.POLYMARKET_UP_LABEL.lower():
            up_token_id = token_id
        elif outcome.lower() == settings.POLYMARKET_DOWN_LABEL.lower():
            down_token_id = token_id

    up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), -1)
    down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), -1)

    gamma_yes = float(outcome_prices[up_index]) if up_index >= 0 and up_index < len(outcome_prices) else None
    gamma_no = float(outcome_prices[down_index]) if down_index >= 0 and down_index < len(outcome_prices) else None

    if not up_token_id or not down_token_id:
        return {"ok": False, "reason": "missing_token_ids"}

    try:
        up_buy, down_buy, up_book, down_book = await asyncio.gather(
            data.fetch_clob_price(up_token_id, "buy"),
            data.fetch_clob_price(down_token_id, "buy"),
            data.fetch_order_book(up_token_id),
            data.fetch_order_book(down_token_id)
        )
        up_book_summary = data.summarize_order_book(up_book)
        down_book_summary = data.summarize_order_book(down_book)
    except:
        up_buy = None
        down_buy = None
        up_book_summary = {"bestBid": None, "bestAsk": None, "spread": None, "bidLiquidity": None, "askLiquidity": None}
        down_book_summary = {"bestBid": None, "bestAsk": None, "spread": None, "bidLiquidity": None, "askLiquidity": None}

    return {
        "ok": True,
        "market": market,
        "prices": {
            "up": up_buy if up_buy is not None else gamma_yes,
            "down": down_buy if down_buy is not None else gamma_no
        },
        "token_ids": {
            "up": up_token_id,
            "down": down_token_id
        },
        "orderbook": {
            "up": up_book_summary,
            "down": down_book_summary
        }
    }

async def execute_trade(decision: Dict[str, Any], market_prices: Dict[str, Any], market: Dict[str, Any], target_open: float, token_ids: Dict[str, Any], orderbook: Optional[Dict[str, Any]] = None, strike_source: str = "binance_open"):
    # Regular entry from decision engine. Returns a short reason string describing
    # the outcome (entered / which gate vetoed it) for diagnostic logging.
    if decision["action"] != "ENTER":
        return decision.get("reason", "no_trade")

    # CONSTRAINT: Only one position at a time
    if state["active_trades"]:
        return "slot_busy"

    side = decision["side"]

    price = market_prices["up"] if side == "UP" else market_prices["down"]
    if price is None:
        return "no_price"

    # ── Risk per trade ──────────────────────────────────────────────────────────
    # No flat price cap (EV already governs reward/risk). RISK_TYPE selects how the
    # stake (the dollars put at risk) is sized:
    #   "percent" -> RISK_VALUE% of the current balance
    #   "fixed"   -> RISK_VALUE dollars, flat
    balance = state["paper_balance"]
    risk_type = (settings.RISK_TYPE or "percent").lower()
    if risk_type == "fixed":
        amount_to_risk = float(settings.RISK_VALUE)
    else:  # "percent" (default)
        amount_to_risk = (float(settings.RISK_VALUE) / 100.0) * balance

    if amount_to_risk <= 0:
        return "stake_zero"

    # Liquidity: never outsize what the ask side of the book can absorb.
    ob = (orderbook or {}).get("up" if side == "UP" else "down") or {}
    ask_liq_shares = ob.get("askLiquidity")
    if ask_liq_shares is not None and price > 0:
        ask_liq_usd = ask_liq_shares * price
        if ask_liq_usd < settings.MIN_BOOK_LIQUIDITY_USD:
            log_message(f"Skip {side}: thin book (${ask_liq_usd:.2f} ask liquidity)")
            return "thin_book"
        amount_to_risk = min(amount_to_risk, ask_liq_usd)  # don't outsize the book

    if balance < amount_to_risk or amount_to_risk <= 0:
        print(f"Insufficient paper balance ({balance}) or invalid risk amount ({amount_to_risk})")
        return "insufficient_balance"

    end_date_str = market.get("endDate")
    end_ts = 0
    if end_date_str:
        try:
            end_ts = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).timestamp()
        except: pass
    # Fallback so a trade always has a definite expiry even if endDate is missing/unparseable
    if not end_ts:
        end_ts = time.time() + settings.CANDLE_WINDOW_MINUTES * 60

    trade = {
        "market_id": market["id"],
        "market_slug": market.get("slug"),
        "side": side,
        "entry_price": price,
        "amount": amount_to_risk,
        "shares": amount_to_risk / price,
        "entry_time": datetime.now().isoformat(),
        "status": "OPEN",
        "settlement_price": None,
        "profit_loss": None,
        "strike_price": target_open,
        "strike_source": strike_source,   # "chainlink_ws" (matches Polymarket) or "binance_open" (fallback)
        "end_ts": end_ts,
        "mode": state["trading_mode"]
    }

    if state["trading_mode"] == "paper":
        state["paper_balance"] -= amount_to_risk
        state["active_trades"].append(trade)
        state["last_trade_side"] = side
        save_state()

        log_message(f"Executed PAPER trade: {side} @ {price} for {market.get('slug')} (Amount: ${amount_to_risk:.2f})")
        return "entered"
    else:
        # LIVE: place a real Fill-Or-Kill market BUY on the Polymarket CLOB
        token_id = token_ids.get("up") if side == "UP" else token_ids.get("down")
        if not token_id:
            log_message(f"LIVE trade aborted: missing token_id for side {side}")
            return "missing_token_id"

        result = await asyncio.to_thread(clob_trader.place_market_buy, token_id, amount_to_risk, price)
        if result.get("ok"):
            resp = result.get("response") or {}
            order_id = None
            if isinstance(resp, dict):
                order_id = resp.get("orderID") or resp.get("orderId") or resp.get("id")
            trade["order_id"] = order_id
            trade["order_response"] = resp
            state["active_trades"].append(trade)
            state["last_trade_side"] = side
            save_state()
            log_message(f"Executed LIVE trade: {side} ${amount_to_risk:.2f} on {market.get('slug')} (order {order_id})")
            return "entered"
        else:
            log_message(f"LIVE trade FAILED ({side}): {result.get('error')}")
            return "live_order_failed"

async def maybe_flip_position(decision: Dict[str, Any], poly_snapshot: Dict[str, Any], time_left_min: Optional[float]):
    """Close the open position early and flip when a STRONG opposite signal appears.

    Opt-in (FLIP_ENABLED). Guards: the new side must clear FLIP_MIN_CONVICTION and at
    least FLIP_MIN_MINUTES_LEFT must remain, and we only flip within the same market.
    After closing here, execute_trade() opens the new side (slot is now free).
    """
    if not settings.FLIP_ENABLED:
        return
    if decision.get("action") != "ENTER" or not state["active_trades"]:
        return

    new_side = decision["side"]
    new_prob = decision.get("prob", 0) or 0
    if new_prob < settings.FLIP_MIN_CONVICTION:
        return
    if time_left_min is not None and time_left_min < settings.FLIP_MIN_MINUTES_LEFT:
        return

    market = poly_snapshot["market"]
    prices = poly_snapshot["prices"]
    token_ids = poly_snapshot.get("token_ids", {})
    orderbook = poly_snapshot.get("orderbook", {})

    trade = state["active_trades"][0]
    if trade["side"] == new_side:
        return  # already on the signalled side
    if str(trade.get("market_id")) != str(market.get("id")):
        return  # different market — let the old one settle on its own

    held_key = "up" if trade["side"] == "UP" else "down"
    ob = orderbook.get(held_key) or {}
    exit_price = ob.get("bestBid") or prices.get(held_key)
    if not exit_price or exit_price <= 0:
        log_message(f"FLIP aborted: no exit price for {trade['side']}")
        return

    if state["trading_mode"] == "live":
        token_id = token_ids.get(held_key)
        result = await asyncio.to_thread(clob_trader.place_market_sell, token_id, trade["shares"], exit_price)
        if not result.get("ok"):
            log_message(f"FLIP sell FAILED ({trade['side']}): {result.get('error')}")
            return
        # live balance is refreshed from chain elsewhere
    else:
        state["paper_balance"] += trade["shares"] * exit_price  # proceeds from selling out

    trade["status"] = "CLOSED"
    trade["exit_time"] = datetime.now().isoformat()
    trade["exit_reason"] = "flip"
    trade["settlement_price_at_expiry"] = exit_price
    trade["profit_loss"] = (trade["shares"] * exit_price) - trade["amount"]
    state["trade_history"].append(trade)
    state["active_trades"] = [t for t in state["active_trades"] if t is not trade]
    state["last_trade_side"] = None
    save_state()
    log_message(f"FLIP: closed {trade['side']} @ {exit_price:.2f} (P/L ${trade['profit_loss']:.2f}); opening {new_side}")

async def update_trades(current_prices: Dict[str, Any]):
    remaining_active = []
    trades_changed = False
    now_ts = time.time()

    # Freshest price to settle against — the CLOSE price. Polymarket settles on
    # Chainlink, and the strike (open) is now the Chainlink WS value too, so prefer
    # Chainlink here so open and close come from the SAME feed (no cross-feed offset
    # can flip a near-the-money result). Binance spot is only a last-resort fallback.
    cur_price = current_prices.get("chainlink") or current_prices.get("spot")
    SETTLEMENT_GRACE_SECONDS = 300  # if still unresolvable this long past expiry, void it

    for trade in state["active_trades"]:
        # Keep a rolling price snapshot so settlement always has a recent value,
        # even if the feed drops out exactly at expiry.
        if cur_price:
            trade["last_price"] = cur_price

        # Effective window end. If endDate was missing at entry (end_ts == 0), derive
        # it from entry_time + window so a trade can never wait forever.
        end_ts = trade.get("end_ts", 0)
        if not end_ts:
            try:
                end_ts = datetime.fromisoformat(trade["entry_time"]).timestamp() + settings.CANDLE_WINDOW_MINUTES * 60
            except Exception:
                end_ts = now_ts
        expired = now_ts >= end_ts

        # Always poll the market (throttled ~15s) so we can read the AUTHORITATIVE
        # Polymarket resolution even after the local clock says the window expired.
        market = None
        if trade.get("last_api_check", 0) < now_ts - 15:
            try:
                market = await data.fetch_market_by_slug(trade["market_slug"])
            except Exception:
                market = None
            trade["last_api_check"] = now_ts
            if market is not None:
                trade["_market_closed"] = bool(market.get("closed"))
        market_closed = trade.get("_market_closed", False)

        # Still live: window running and market still open → keep waiting.
        if not expired and not market_closed:
            remaining_active.append(trade)
            continue

        # ---- Determine the winning outcome ----
        outcomes = []
        outcome_prices = []
        if market:
            outcomes = market.get("outcomes", [])
            if isinstance(outcomes, str): outcomes = json.loads(outcomes)
            outcome_prices = market.get("outcomePrices", [])
            if isinstance(outcome_prices, str): outcome_prices = json.loads(outcome_prices)
        if not outcomes:
            outcomes = [settings.POLYMARKET_UP_LABEL, settings.POLYMARKET_DOWN_LABEL]

        up_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_UP_LABEL.lower()), 0)
        down_index = next((i for i, x in enumerate(outcomes) if x.lower() == settings.POLYMARKET_DOWN_LABEL.lower()), 1)

        winning_index = -1
        # 1) Authoritative: a settled Polymarket outcome trades at ~$1.
        for i, p in enumerate(outcome_prices):
            try:
                if float(p) > 0.9:
                    winning_index = i
                    break
            except Exception:
                pass

        # 2) Fallback once the window/market is over: settlement price vs strike.
        settlement_price = trade.get("settlement_price_at_expiry") or trade.get("last_price") or cur_price
        if winning_index == -1 and (expired or market_closed):
            strike = trade.get("strike_price")
            if strike and settlement_price:
                trade["settlement_price_at_expiry"] = settlement_price
                winning_index = up_index if settlement_price > strike else down_index

        # ---- Could not resolve yet ----
        if winning_index == -1:
            first_seen = trade.get("unresolved_since")
            if first_seen is None:
                trade["unresolved_since"] = now_ts
                remaining_active.append(trade)
                continue
            if now_ts - first_seen < SETTLEMENT_GRACE_SECONDS:
                remaining_active.append(trade)
                continue
            # Grace exhausted — void so a single bad trade can't block forever.
            trade["status"] = "VOID"
            trade["exit_time"] = datetime.now().isoformat()
            trade["profit_loss"] = 0.0
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += trade["amount"]  # refund the stake
            state["trade_history"].append(trade)
            trades_changed = True
            log_message(f"VOID: Trade for {trade['market_slug']} unresolved past grace; stake refunded (paper).")
            continue

        # ---- Settle WIN / LOSS ----
        won = ((trade["side"] == "UP" and winning_index == up_index) or
               (trade["side"] == "DOWN" and winning_index == down_index))

        if won:
            payout = trade["shares"] * 1.0
            # Paper credits the simulated balance; live balance comes from the
            # on-chain USDC refresh in the main loop, not credited here.
            if trade.get("mode", "paper") == "paper":
                state["paper_balance"] += payout
            trade["profit_loss"] = payout - trade["amount"]
            log_message(f"WIN: Trade for {trade['market_slug']} settled. Profit: ${trade['profit_loss']:.2f}")
        else:
            trade["profit_loss"] = -trade["amount"]
            log_message(f"LOSS: Trade for {trade['market_slug']} settled. Loss: ${trade['profit_loss']:.2f}")

        trade["status"] = "CLOSED"
        trade["exit_time"] = datetime.now().isoformat()
        trade["settlement_price_at_expiry"] = trade.get("settlement_price_at_expiry") or settlement_price
        trade["winning_outcome"] = outcomes[winning_index] if 0 <= winning_index < len(outcomes) else None
        state["trade_history"].append(trade)
        trades_changed = True

    state["active_trades"] = remaining_active
    if trades_changed:
        save_state()

async def seed_kline_buffers():
    try:
        k1m, k5m = await asyncio.gather(
            data.fetch_klines(settings.SYMBOL, "1m", 240),
            data.fetch_klines(settings.SYMBOL, "5m", 200)
        )
        binance_kline_1m.set_candles(k1m)
        binance_kline_5m.set_candles(k5m)
        log_message(f"Seeded Binance kline buffers (1m/5m) for {settings.SYMBOL}")
    except Exception as e:
        log_message(f"Failed to seed kline buffers: {e}")

async def update_loop():
    csv_header = [
        "timestamp", "entry_minute", "time_left_min", "signal",
        "model_up", "model_down", "mkt_up", "mkt_down", "edge_up", "edge_down",
        "recommendation", "reason", "exec_result"
    ]

    while True:
        try:
            timing = get_candle_window_timing(settings.CANDLE_WINDOW_MINUTES)

            binance_ws = binance_stream.get_last()
            if not binance_ws.get("price"):
                poly_ws_last = polymarket_ws_stream.get_last()
                cl_ws_last = chainlink_ws_stream.get_last()
                binance_ws["price"] = poly_ws_last.get("price") or cl_ws_last.get("price")
            poly_ws = polymarket_ws_stream.get_last()
            cl_ws = chainlink_ws_stream.get_last()

            results = await asyncio.gather(
                data.fetch_last_price(settings.SYMBOL),
                chainlink.chainlink_fetcher.fetch_chainlink_btc_usd(),
                fetch_polymarket_snapshot(),
                return_exceptions=True
            )

            last_price = results[0] if not isinstance(results[0], Exception) else None
            chainlink_data = results[1] if not isinstance(results[1], Exception) else {}
            poly_snapshot = results[2] if not isinstance(results[2], Exception) else {"ok": False}

            klines_1m = binance_kline_1m.get_candles()
            klines_5m = binance_kline_5m.get_candles()

            spot_price = binance_ws.get("price") if binance_ws and binance_ws.get("price") else last_price

            mc_steps = max(1, __import__('math').ceil(timing["remainingMinutes"] / 5))

            target_open = spot_price
            if klines_5m:
                start_ms = timing["startMs"]
                for c in reversed(klines_5m):
                    if c["openTime"] <= start_ms:
                        target_open = c["open"]
                        break

            # Fast closed-form fair probability (replaces 1000-sim Monte Carlo —
            # backtest-verified equivalent, ~1000x cheaper, which a latency play needs).
            drift_5m, sigma_5m = indicators.realized_drift_vol(klines_5m, lookback=300)
            fair_up = indicators.fair_prob_up(spot_price or 0, target_open or 0, mc_steps, sigma_5m, drift_per_step=drift_5m or 0.0)
            fair_data = {
                "prob_up": fair_up,
                "prob_down": 1.0 - fair_up,
                "bias": "BULLISH" if fair_up > 0.6 else "BEARISH" if fair_up < 0.4 else "NEUTRAL",
                "steps": mc_steps,
                "sigma_5m": sigma_5m,
            }

            current_price = None
            price_source = None

            # Prefer Polymarket's OWN Chainlink WS feed — it's the exact price stream
            # Polymarket settles on, so marking open/close from it matches the market
            # most faithfully. Fall back to the direct Chainlink RPC WS, then REST.
            if poly_ws.get("price"):
                current_price = poly_ws["price"]
                price_source = "Polymarket WS"
            elif cl_ws.get("price"):
                current_price = cl_ws["price"]
                price_source = "Chainlink RPC WS"
            elif chainlink_data.get("price"):
                current_price = chainlink_data["price"]
                price_source = "Chainlink RPC REST"

            # ── Mark each 15m window's OPEN price from the Chainlink WS feed ──────
            # Polymarket's BTC "Up or Down" markets settle on Chainlink: the close
            # price vs the price at the window's open. We snapshot the Chainlink WS
            # price the instant a window opens, then compare the Chainlink price at
            # expiry against it (see update_trades). `current_price` is already the
            # Chainlink WS value (RPC WS → Polymarket WS → RPC REST), so open and
            # close come from the SAME feed — no Binance/Chainlink offset to flip a
            # near-the-money result. Binance is only a fallback for the open when the
            # bot was NOT running at the window start (so we never saw the real open).
            start_ms = timing["startMs"]
            window_ms = settings.CANDLE_WINDOW_MINUTES * 60_000
            opens = state["market_opens"]
            # "genuine" == we were already running in the immediately-preceding
            # window, so the first price we see in this one really is its open.
            observed_prev = state.get("last_window_start") == start_ms - window_ms
            if start_ms not in opens:
                opens[start_ms] = {"chainlink": None, "binance": None, "genuine": observed_prev}
                for k in list(opens.keys()):           # prune old windows
                    if k < start_ms - 4 * window_ms:
                        del opens[k]
            win = opens[start_ms]
            if win["binance"] is None and target_open:
                win["binance"] = target_open           # Binance 5m open (fallback strike)
            if (win["chainlink"] is None and win["genuine"] and current_price
                    and (time.time() * 1000 - start_ms) < 20_000):
                win["chainlink"] = current_price        # Chainlink open, captured at the boundary
                log_message(f"Window open marked (Chainlink): {current_price:.2f} @ {price_source}")
            state["last_window_start"] = start_ms

            # Strike (open) for a trade entered now: the Chainlink open if we captured
            # it, else the Binance 5m open as a fallback.
            chainlink_open = win["chainlink"]
            strike_open = chainlink_open if chainlink_open is not None else (win["binance"] or target_open)
            strike_source = "chainlink_ws" if chainlink_open is not None else "binance_open"

            settlement_ms = None
            if poly_snapshot["ok"] and poly_snapshot["market"].get("endDate"):
                settlement_ms = datetime.fromisoformat(poly_snapshot["market"]["endDate"].replace('Z', '+00:00')).timestamp() * 1000

            time_left_min = (settlement_ms - time.time() * 1000) / 60_000 if settlement_ms else timing["remainingMinutes"]

            market_up = poly_snapshot["prices"]["up"] if poly_snapshot["ok"] else None
            market_down = poly_snapshot["prices"]["down"] if poly_snapshot["ok"] else None

            # ── LATENCY EDGE ─────────────────────────────────────────────────────
            # Our fast Binance-derived fair prob vs the market's (possibly stale)
            # implied prob. A positive edge = the book hasn't repriced the move yet.
            market_implied_up = None
            if market_up is not None and market_down is not None and (market_up + market_down) > 0:
                market_implied_up = market_up / (market_up + market_down)
            edge = {
                "marketUp": market_implied_up,
                "marketDown": (1 - market_implied_up) if market_implied_up is not None else None,
                "edgeUp": (fair_up - market_implied_up) if market_implied_up is not None else None,
                "edgeDown": ((1 - fair_up) - (1 - market_implied_up)) if market_implied_up is not None else None,
            }
            prob_view = {"adjustedUp": fair_up, "adjustedDown": 1 - fair_up}

            decision = engines.decide_ev({
                "mcProbUp": fair_up,
                "priceUp": market_up,
                "priceDown": market_down,
                "minProb": settings.MIN_PROB_EV,
                "evThreshold": settings.EV_THRESHOLD,
            })

            current_prices_dict = {"spot": spot_price, "chainlink": current_price}

            exec_result = None
            if poly_snapshot["ok"]:
                await maybe_flip_position(decision, poly_snapshot, time_left_min)
                exec_result = await execute_trade(decision, poly_snapshot["prices"], poly_snapshot["market"], strike_open, poly_snapshot.get("token_ids", {}), poly_snapshot.get("orderbook", {}), strike_source)

            await update_trades(current_prices_dict)

            # In live mode, reflect the real on-chain USDC balance in the dashboard
            if state["trading_mode"] == "live":
                now_ts = time.time()
                if now_ts - state.get("last_balance_refresh", 0) > 30:
                    real_bal = await asyncio.to_thread(clob_trader.get_usdc_balance)
                    if real_bal is not None:
                        state["paper_balance"] = real_bal
                    state["last_balance_refresh"] = now_ts

            signal_label = f"BUY {decision['side']}" if decision["action"] == "ENTER" else "NO TRADE"
            utils.append_csv_row("./logs/signals.csv", csv_header, [
                datetime.now().isoformat(), timing["elapsedMinutes"], time_left_min,
                signal_label, fair_up, 1 - fair_up, market_up, market_down,
                edge["edgeUp"], edge["edgeDown"], f"{decision['side']}:{decision['phase']}:{decision['strength']}" if decision["action"] == "ENTER" else "NO_TRADE",
                decision.get("reason", ""), exec_result or ""
            ])

            state["latest_data"] = {
                "timestamp": datetime.now().isoformat(),
                "timing": timing,
                "market": poly_snapshot.get("market") if poly_snapshot["ok"] else None,
                "trading_state": {
                    "mode": state["trading_mode"],
                    "balance": state["paper_balance"],
                    "active_trades": state["active_trades"],
                    "history_count": len(state["trade_history"]),
                    "risk": {"type": settings.RISK_TYPE, "value": settings.RISK_VALUE},
                    "symbol": settings.SYMBOL
                },
                "prices": {
                    "spot": spot_price,
                    "chainlink": current_price,
                    "chainlink_source": price_source,
                    "poly_up": market_up,
                    "poly_down": market_down,
                    "window_open": strike_open,          # this window's marked OPEN (strike)
                    "window_open_source": strike_source  # "chainlink_ws" (Polymarket) or "binance_open" fallback
                },
                "indicators": {
                    "fair": fair_data
                },
                "analysis": {
                    "probability": prob_view, "edge": edge, "decision": decision
                }
            }
            state["last_update_ts"] = time.time()

        except Exception as e:
            print(f"Error in update loop: {e}")

        await asyncio.sleep(settings.POLL_INTERVAL_MS / 1000)


@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/settings", response_class=HTMLResponse)
async def get_settings_page(request: Request):
    return templates.TemplateResponse("settings.html", {"request": request})

@app.get("/api/latest")
async def get_latest():
    return state["latest_data"]

@app.get("/api/logs")
async def get_logs():
    return state["logs"]

@app.get("/api/available-series")
async def get_available_series():
    return await data.fetch_available_15m_series()

@app.get("/api/settings")
async def get_settings():
    pk = settings.PRIVATE_KEY
    masked_pk = pk[:6] + "..." + pk[-4:] if pk and len(pk) > 10 else pk

    return {
        "mode": settings.MODE,
        "paper_balance_usd": settings.PAPER_BALANCE_USD,
        "private_key": masked_pk,
        "live": {
            "signature_type": settings.CLOB_SIGNATURE_TYPE,
            "funder": settings.CLOB_FUNDER
        },
        "polymarket": {
            "series_id": settings.POLYMARKET_SERIES_ID,
            "gamma_base_url": settings.GAMMA_BASE_URL,
            "clob_base_url": settings.CLOB_BASE_URL,
            "live_ws_url": settings.POLYMARKET_LIVE_DATA_WS_URL,
            "up_label": settings.POLYMARKET_UP_LABEL,
            "down_label": settings.POLYMARKET_DOWN_LABEL
        },
        "trading": {
            "symbol": settings.SYMBOL,
            "risk_type": settings.RISK_TYPE,
            "risk_value": settings.RISK_VALUE
        },
        "ev": {
            "ev_threshold": settings.EV_THRESHOLD,
            "min_prob": settings.MIN_PROB_EV,
            "min_book_liquidity_usd": settings.MIN_BOOK_LIQUIDITY_USD
        },
        "flip": {
            "enabled": settings.FLIP_ENABLED,
            "min_conviction": settings.FLIP_MIN_CONVICTION,
            "min_minutes_left": settings.FLIP_MIN_MINUTES_LEFT
        }
    }

@app.post("/api/settings")
async def post_settings(new_settings: Dict[str, Any]):
    global binance_stream, polymarket_ws_stream, chainlink_ws_stream, binance_kline_1m, binance_kline_5m
    old_symbol = settings.SYMBOL

    new_pk = new_settings.get("private_key")
    if new_pk and "..." in new_pk:
        new_settings["private_key"] = settings.PRIVATE_KEY
    elif new_pk:
        settings.PRIVATE_KEY = new_pk

    # Deep-merge into the existing config so keys not present in the settings form
    # (chainlink, binance_base_url, poll_interval_ms, etc.) are preserved.
    existing_cfg = {}
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                existing_cfg = json.load(f)
        except Exception:
            existing_cfg = {}

    def deep_merge(base, override):
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(base.get(k), dict):
                deep_merge(base[k], v)
            else:
                base[k] = v
        return base

    merged_cfg = deep_merge(existing_cfg, new_settings)
    with open("config.json", "w") as f:
        json.dump(merged_cfg, f, indent=2)

    settings.MODE = new_settings.get("mode", settings.MODE)
    settings.PAPER_BALANCE_USD = float(new_settings.get("paper_balance_usd", settings.PAPER_BALANCE_USD))

    if "trading" in new_settings:
        t = new_settings["trading"]
        settings.SYMBOL = t.get("symbol", settings.SYMBOL)
        settings.RISK_TYPE = t.get("risk_type", settings.RISK_TYPE)
        settings.RISK_VALUE = float(t.get("risk_value", settings.RISK_VALUE))

    if "ev" in new_settings:
        e = new_settings["ev"]
        settings.EV_THRESHOLD = float(e.get("ev_threshold", settings.EV_THRESHOLD))
        settings.MIN_PROB_EV = float(e.get("min_prob", settings.MIN_PROB_EV))
        settings.MIN_BOOK_LIQUIDITY_USD = float(e.get("min_book_liquidity_usd", settings.MIN_BOOK_LIQUIDITY_USD))

    if "flip" in new_settings:
        f = new_settings["flip"]
        if "enabled" in f:
            settings.FLIP_ENABLED = bool(f["enabled"])
        settings.FLIP_MIN_CONVICTION = float(f.get("min_conviction", settings.FLIP_MIN_CONVICTION))
        settings.FLIP_MIN_MINUTES_LEFT = float(f.get("min_minutes_left", settings.FLIP_MIN_MINUTES_LEFT))

    if "polymarket" in new_settings:
        p = new_settings["polymarket"]
        settings.POLYMARKET_SERIES_ID = p.get("series_id", settings.POLYMARKET_SERIES_ID)
        settings.POLYMARKET_UP_LABEL = p.get("up_label", settings.POLYMARKET_UP_LABEL)
        settings.POLYMARKET_DOWN_LABEL = p.get("down_label", settings.POLYMARKET_DOWN_LABEL)

    if "live" in new_settings:
        lv = new_settings["live"]
        settings.CLOB_SIGNATURE_TYPE = int(lv.get("signature_type", settings.CLOB_SIGNATURE_TYPE))
        settings.CLOB_FUNDER = lv.get("funder", settings.CLOB_FUNDER)

    # Credentials/signature may have changed — drop the cached CLOB client so the
    # next live order re-initialises with the new key/signature/funder.
    clob_trader.reset()

    state["trading_mode"] = settings.MODE
    state["paper_balance"] = settings.PAPER_BALANCE_USD

    if settings.SYMBOL != old_symbol:
        binance_stream.close()
        binance_stream = ws_data.BinanceTradeStream(symbol=settings.SYMBOL)
        asyncio.create_task(binance_stream.start())

        binance_kline_1m.close()
        binance_kline_1m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="1m", limit=240)
        asyncio.create_task(binance_kline_1m.start())

        binance_kline_5m.close()
        binance_kline_5m = ws_data.BinanceKlineStream(symbol=settings.SYMBOL, interval="5m", limit=200)
        asyncio.create_task(binance_kline_5m.start())

        await seed_kline_buffers()

        polymarket_ws_stream.close()
        polymarket_ws_stream = ws_data.PolymarketChainlinkStream(
            ws_url=settings.POLYMARKET_LIVE_DATA_WS_URL,
            symbol_includes=get_ws_symbol_filter(settings.SYMBOL)
        )
        asyncio.create_task(polymarket_ws_stream.start())

        chainlink_ws_stream.close()
        chainlink_ws_stream = ws_data.ChainlinkPriceStream(aggregator=settings.get_aggregator(settings.SYMBOL))
        asyncio.create_task(chainlink_ws_stream.start())

    return {"status": "ok"}

@app.post("/api/setup-allowances")
async def setup_allowances():
    import bot.allowances as allowances
    try:
        result = await allowances.ensure_allowances()
        if result.get("ok"):
            if result.get("skipped"):
                log_message(f"Allowance setup skipped: {result.get('reason')}")
            elif result.get("already_set"):
                log_message("Allowances already configured for trading wallet")
            else:
                log_message(f"Allowances configured ({len(result.get('actions', []))} tx)")
        else:
            log_message(f"Allowance setup failed: {result.get('error')}")
        return result
    except Exception as e:
        log_message(f"Allowance setup error: {e}")
        return {"ok": False, "error": str(e)}

@app.get("/health")
async def health():
    return {"status": "ok", "last_update": state["last_update_ts"], "mode": state["trading_mode"]}

@app.get("/history")
async def get_history():
    return state["trade_history"]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
