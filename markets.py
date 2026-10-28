import json, time
from datetime import datetime, timezone
import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
_s = requests.Session()

def _get(url, params=None):
    for delay in (1, 2, 4, 8, None):
        r = _s.get(url, params=params, timeout=15)
        if r.status_code == 429 and delay:
            time.sleep(delay); continue
        r.raise_for_status()
        return r.json()

def _map(m, out):
    outcomes = json.loads(m.get("outcomes") or "[]")
    toks = json.loads(m.get("clobTokenIds") or "[]")
    for o, t in zip(outcomes, toks):
        out[t] = {"slug": m["slug"], "outcome": o, "question": m.get("question")}

def resolve(slugs):
    out = {}
    for slug in slugs:
        lst = _get(f"{GAMMA}/markets", {"slug": slug})
        if not lst:
            print(f"skip {slug}: not found"); continue
        m = lst[0]
        if m.get("closed"):
            print(f"skip {slug}: closed"); continue
        if not m.get("enableOrderBook"):
            print(f"skip {slug}: no order book"); continue
        _map(m, out)
    return out

def _decided(m):
    try:
        b, a = m.get("bestBid"), m.get("bestAsk")
        if b is None or a is None: return True
        b, a = float(b), float(a)
    except (TypeError, ValueError):
        return True
    return b <= 0.01 or b >= 0.99 or a <= 0.01 or a >= 0.99

def top_markets(n, include_decided=False):
    lst = _get(f"{GAMMA}/markets", {"closed": "false", "order": "volume24hr",
                                    "ascending": "false", "limit": 3 * n})
    out, kept = {}, 0
    for m in lst:
        if not m.get("enableOrderBook") or not m.get("acceptingOrders"):
            print(f"skip {m.get('slug')}: not accepting orders"); continue
        if not include_decided and _decided(m):
            print(f"skip {m.get('slug')}: decided (bid {m.get('bestBid')} ask {m.get('bestAsk')})"); continue
        _map(m, out); kept += 1
        if kept >= n: break
    return out

def rest_book(token_id):
    b = _get(f"{CLOB}/book", {"token_id": token_id})
    bids = [float(x["price"]) for x in b.get("bids", [])]
    asks = [float(x["price"]) for x in b.get("asks", [])]
    return (max(bids) if bids else None, min(asks) if asks else None, b)
