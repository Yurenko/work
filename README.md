# Hedge Bot — Predict.fun + Polymarket (BTC 5m Up/Down)

Хедж **BTC 5‑хвилинних Up/Down** між **Predict.fun** і **Polymarket**.

Binance Prediction прибрано: orderbook і ордери йдуть напряму в Predict.fun
(саме він був upstream для Binance Prediction).

## Логіка (без змін)

- Якщо **протилежні** сторони на двох біржах у сумі **&lt; ENTRY_SUM_MAX** (0.80) → купуємо **обидві** ноги.
- Ніколи не купуємо **одну й ту саму** сторону на обох.
- Вихід, коли сума **bid** ≥ **EXIT_SUM_MIN** (1.10).
- Спочатку Predict, потім Polymarket на фактично заповнений розмір.
- `MODE=PAPER` — ордери симулюються, ціни реальні.

## Швидкий старт

```bash
cd hedge-bot-predict
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
# заповніть PREDICT_API_KEY (і ключі Polymarket для LIVE)
python main.py
```

Відкрийте **http://localhost:8888** → **Start**.

## Ключі Predict.fun

| Змінна | Опис |
|--------|------|
| `PREDICT_API_KEY` | Обовʼязково на mainnet. [developers.predict.fun](https://developers.predict.fun) |
| `PREDICT_PRIVATE_KEY` | EOA hex key для JWT + підпису ордерів (LIVE) |
| `PREDICT_REST_BASE_URL` | `https://api.predict.fun` |
| `PREDICT_WSS_URL` | `wss://ws.predict.fun/ws` |

### Як працює auth / WS

**REST**
- Header `x-api-key: <key>` на всі mainnet-запити
- JWT: `GET /v1/auth/message` → підпис private key → `POST /v1/auth` → `Authorization: Bearer <jwt>`
- Ордери: `POST /v1/orders` (потрібен JWT)
- Книга: `GET /v1/markets/{id}/orderbook` (Yes-ціни; Down = 1 − Yes)

**WebSocket** `wss://ws.predict.fun/ws`
- API key у handshake: header `x-api-key` або `?apiKey=`
- Subscribe: `{"requestId":1,"method":"subscribe","params":["predictOrderbook/123"]}`
- Push: `{"type":"M","topic":"predictOrderbook/123","data":{bids,asks,...}}`
- Heartbeat кожні 15 с: сервер шле `topic=heartbeat`, клієнт відповідає `{"method":"heartbeat","data": <ts>}`

## LIVE

1. `MODE=LIVE` у `.env`
2. Predict: `PREDICT_API_KEY` + `PREDICT_PRIVATE_KEY`
3. Polymarket: `PRIVATE_KEY`, `FUNDER_ADDRESS`, `SIGNATURE_TYPE`
4. Опційно: `pip install predict-sdk` для EIP-712 підпису ордерів

## Структура

```
hedge-bot-predict/
  main.py
  app/
    predict/       # REST + WSS (замість binance/)
    polymarket/    # CLOB + Gamma + WS
    strategy/      # engine, recovery, fills
    dashboard/     # FastAPI Start/Stop
  static/index.html
```

## Примітки

- На Predict один market = Yes (Up); Down рахується як complement — як раніше на Binance Prediction.
- У PAPER ключ Predict бажаний для market data WS/REST на mainnet (без ключа handshake може дати 401).
- Testnet: `https://api-testnet.predict.fun` — API key не обовʼязковий.\n

## LIVE setup (Predict.fun)

1. Put USDT + a little BNB on the wallet (BNB Chain).
2. Check balances:
   ```bash
   python scripts/check_balances.py
   ```
3. One-time approvals (USDT allowance for exchange):
   ```bash
   python scripts/set_approvals.py
   ```
4. Start bot with `MODE=LIVE`.

Error `create_order_insufficient_collateral_allowance` means step 3 was skipped.
