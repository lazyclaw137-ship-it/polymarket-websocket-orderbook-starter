# Polymarket WebSocket order book starter (read-only)

Starter code for the BotCookery recipe **[polymarket-websocket-starter](https://botcookery.com/recipes/polymarket-websocket-starter/)**. The recipe page has the step-by-step guide, the prompt, the findings and the safety notes; this repo is the code that came out of it.

## What it is

A read-only Python streamer for Polymarket's public CLOB **market** WebSocket. It picks top markets (or a slug you give it), keeps a local order book per outcome from `book` and `price_change` messages, answers PING/PONG, reconnects, checks its book against the server's best bid/ask, and can audit the local book against REST. No keys, no orders.

## What was actually tested

- Mode: **read-only**
- Date: 2026-10-25
- Limits: Public market channel only: three 150-second streams of the top 5 markets (10 outcomes), each with a forced reconnect and REST audits. Order placement is out of scope and the user channel needs your own API keys, so neither was run.

The recipe page has the full findings, dated sample output and the prompt this code came from. The code was written by an AI coding assistant from that prompt, then run as described above. Market tickers and slugs in the config were live when it was tested and will expire; swap in current ones.

## Setup

Python 3.11 or newer.

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

## Run it (no money)

```bash
# stream the top 5 markets for 150 s, print the book every 30 s, audit vs REST every 40 s,
# force one reconnect at 60 s, log raw events
python pmws.py stream --top 5 --duration 150 --print-every 30 --audit-every 40 --drop-after 60 --custom --log events.jsonl

# one market by slug
python pmws.py stream --slug <market-slug>
python pmws.py book --slug <market-slug>
```

The run ends with a stats line (frames, server checks, mismatches, reconnects, audit OK/MOVED/DIFF).

## Safety notes

- Read-only: it never signs anything and never places orders.
- `events.jsonl` can get big; it's git-ignored.
- An empty side arrives as best_bid "0" / best_ask "1"; the code treats those as empty, not as prices.

## Not financial advice

This is example code for learning how these APIs work. It is not financial advice, not a strategy, and not a recommendation to trade anything. The placeholder rules and prices in the config are there to exercise the code, not forecasts. You are responsible for anything you run, especially with real keys or real money.

## License

MIT. See [LICENSE](LICENSE).
