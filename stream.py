import json, random, threading, time, collections
import websocket
from book import LocalBook, Checker

URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

def default_connect(url):
    ws = websocket.WebSocket()
    ws.connect(url, timeout=10)
    ws.settimeout(1.0)
    return ws

class Streamer:
    def __init__(self, assets, connect=default_connect, clock=time.monotonic, custom=False,
                 stale_after=30, ping_every=10, on_event=None, log=None):
        self.assets = assets  # {token: meta}
        self.connect, self.clock, self.custom = connect, clock, custom
        self.stale_after, self.ping_every = stale_after, ping_every
        self.on_event, self.log = on_event, log
        self.books = {}
        self.checker = Checker()
        self.stats = collections.Counter()
        self.ws = None
        self.resync = False
        self.backoff = 1
        self._reset_books()

    def _reset_books(self):
        self.books = {t: LocalBook(t) for t in self.assets}
        if hasattr(self, "checker"): self.checker.pending.clear()

    def subscribe(self):
        msg = {"assets_ids": list(self.assets), "type": "market"}
        if self.custom:
            msg["custom_feature_enabled"] = True
        self.ws.send(json.dumps(msg))
        self.ws.send("PING"); self.stats["pings"] += 1
        self.last_ping = self.clock()

    def open(self):
        self._reset_books()
        self.ws = self.connect(URL)
        self.subscribe()
        self.last_rx = self.clock()

    def maybe_ping(self):
        if self.clock() - self.last_ping >= self.ping_every:
            self.ws.send("PING"); self.stats["pings"] += 1
            self.last_ping = self.clock()

    def handle_text(self, text):
        self.last_rx = self.clock()
        self.backoff = 1
        self.stats["frames"] += 1
        if text == "PONG":
            self.stats["pongs"] += 1; return
        try:
            data = json.loads(text)
        except ValueError:
            self.stats["junk_text"] += 1; print(f"non-JSON text ignored: {text[:60]!r}"); return
        events = data if isinstance(data, list) else [data]
        for ev in events:
            self.handle_event(ev)

    def handle_event(self, ev):
        et = ev.get("event_type")
        self.stats[f"ev_{et}"] += 1
        if self.log:
            self.log.write(json.dumps(ev) + "\n")
        if et == "book":
            b = self.books.get(ev.get("asset_id"))
            if b:
                self.checker.discard(b.asset_id); b.apply_snapshot(ev); b.updated = time.time()
        elif et == "price_change":
            ts = ev.get("timestamp")
            changes = ev.get("price_changes", [])
            for aid in {ch.get("asset_id") for ch in changes}:
                if aid in self.books and self.checker.due(aid, ts):
                    self._settle(aid)
            for ch in changes:
                b = self.books.get(ch.get("asset_id"))
                if not b: continue
                if b.apply_change(ch, ts):
                    b.updated = time.time()
                    self.checker.note(b.asset_id, ts, ch.get("best_bid"), ch.get("best_ask"))
                else:
                    self.stats["dropped_before_snapshot"] += 1
        elif et == "last_trade_price":
            b = self.books.get(ev.get("asset_id"))
            if b: b.last_trade = ev.get("price")
        elif et in ("tick_size_change", "best_bid_ask", "new_market", "market_resolved"):
            pass
        else:
            self.stats["unknown_events"] += 1
        if self.on_event:
            self.on_event(ev, self)

    def _settle(self, aid):
        if self.checker.settle(self.books[aid]):
            self.resync = True

    def settle_all(self):
        for aid in list(self.checker.pending):
            if aid in self.books:
                self._settle(aid)
        self.checker.pending.clear()

    def close(self, reason):
        try:
            self.ws.close()
        except Exception:
            pass
        print(f"disconnected ({reason})", flush=True)

    def reconnect(self, reason, sleep=time.sleep):
        self.close(reason)
        self.stats["reconnects"] += 1
        delay = min(30, self.backoff) * (0.8 + 0.4 * random.random())
        self.backoff = min(30, self.backoff * 2)
        sleep(delay)
        self.resync = False
        self.open()

    def run(self, duration=None, drop_after=None, tick=None, stop=None):
        start = self.clock()
        self.open()
        dropped = False
        while True:
            now = self.clock()
            if duration is not None and now - start >= duration:
                break
            if stop is not None and stop():
                break
            if tick: tick(self)
            if drop_after is not None and not dropped and now - start >= drop_after:
                dropped = True
                self.reconnect(f"forced drop after {drop_after}s"); continue
            if self.resync:
                self.reconnect("resync: 3 server mismatches"); continue
            if now - self.last_rx > self.stale_after:
                self.reconnect(f"no data for {self.stale_after}s"); continue
            try:
                self.maybe_ping()
                text = self.ws.recv()
            except websocket.WebSocketTimeoutException:
                if self.clock() - self.last_rx >= 1:
                    self.settle_all()
                continue
            except Exception as e:
                self.reconnect(f"error: {type(e).__name__}: {e}"); continue
            if text is None or text == "":
                continue
            self.handle_text(text)
        self.settle_all()
        self.close("done")
