# Trading Bot — Capital.com (Forex, 5m, MACD + EMA)

Day-trading bot that operates on Capital.com via their REST API.

## Strategy
- **Trend filter:** EMA(9) / EMA(21) / EMA(50) stacked.
- **Trigger:** MACD(12,26,9) line crosses signal line in trend direction.
- **Stop:** 1.5 × ATR(14) from entry.
- **Take profit:** 3.0 × ATR(14) (= 2R).
- **Breakeven move:** stop is moved to entry once price runs 1.0 × ATR in profit.
- **Markets:** EURUSD, GBPUSD (configurable in `config.yaml`).
- **Timeframe:** 5-minute candles.
- **Session window:** London/NY overlap, 12:00–16:00 UTC.

## Risk
- 2% risk per trade *(see note below)*.
- 1 concurrent position.
- Daily drawdown kill-switch: -10%.
- Max 3 consecutive losses → halt for the day.
- If broker minimum lot ≥ computed size, the bot **does not enter** (refuses to over-risk). Expect this on a $30 account during the first sessions until equity grows.

## Setup

```
pip install -r apps/trading_bot/requirements.txt
cp apps/trading_bot/.env.example apps/trading_bot/.env
# fill CAPITAL_API_KEY / CAPITAL_IDENTIFIER / CAPITAL_PASSWORD
```

`CAPITAL_ENV=demo` uses `https://demo-api-capital.backend-capital.com`.
Switch to `live` only after watching a full demo session.

## Run

```
python -m apps.trading_bot.bot
```

Logs go to `apps/trading_bot/logs/bot.log`. Trade journal (JSONL) is in `apps/trading_bot/logs/trades.jsonl`.

Stop with `Ctrl-C` (the bot handles SIGINT/SIGTERM cleanly).

## Tests

```
pytest apps/trading_bot/tests -q
```

## Realistic targets

Compounding $30 → $1000 implies a ~33× return. With 2% risk per trade and a strategy with a positive expectancy, the realistic daily P/L band is roughly ±5%. **Plan for weeks-to-months, not 24 hours.** The bot will refuse to trade rather than break its risk rules — that is by design.

## Files

- `bot.py` — main loop
- `capital_client.py` — REST client (session, prices, positions)
- `indicators.py` — EMA, MACD, ATR (pure Python)
- `strategy.py` — entry/exit signal generation
- `risk.py` — position sizing + kill switches
- `config.yaml` — runtime configuration
- `config.py` — config loader
- `tests/` — unit tests
