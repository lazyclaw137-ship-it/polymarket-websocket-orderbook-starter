from decimal import Decimal

def norm(p):
    return str(Decimal(p).normalize())

class LocalBook:
    def __init__(self, asset_id):
        self.asset_id = asset_id
        self.bids, self.asks = {}, {}
        self.ready = False
        self.hash = self.ts = None
        self.mismatch_streak = 0
        self.last_trade = None
        self.updated = None

    def apply_snapshot(self, msg):
        self.bids = {norm(l["price"]): Decimal(l["size"]) for l in msg.get("bids", [])}
        self.asks = {norm(l["price"]): Decimal(l["size"]) for l in msg.get("asks", [])}
        self.bids = {k: v for k, v in self.bids.items() if v != 0}
        self.asks = {k: v for k, v in self.asks.items() if v != 0}
        self.hash, self.ts = msg.get("hash"), msg.get("timestamp")
        self.ready = True
        self.mismatch_streak = 0

    def apply_change(self, ch, ts=None):
        if not self.ready:
            return False
        side = self.bids if ch["side"] == "BUY" else self.asks
        p, s = norm(ch["price"]), Decimal(ch["size"])
        if s == 0:
            side.pop(p, None)
        else:
            side[p] = s
        self.hash = ch.get("hash")
        if ts is not None:
            self.ts = ts
        return True

    def best_bid(self):
        return max((Decimal(p) for p in self.bids), default=None)

    def best_ask(self):
        return min((Decimal(p) for p in self.asks), default=None)

    def spread(self):
        b, a = self.best_bid(), self.best_ask()
        return None if b is None or a is None else a - b

    def mid(self):
        b, a = self.best_bid(), self.best_ask()
        return None if b is None or a is None else (a + b) / 2

    def crossed(self):
        b, a = self.best_bid(), self.best_ask()
        return b is not None and a is not None and b >= a

def _server(x, sentinel):
    if x in (None, "") or x == sentinel:
        return None
    return Decimal(x)

class Checker:
    """Server best-price check per (asset, timestamp) batch."""
    def __init__(self):
        self.checks = self.mismatches = self.crossed_skipped = 0
        self.pending = {}  # asset -> (timestamp, server_bid, server_ask)

    def note(self, asset_id, ts, server_bid, server_ask):
        self.pending[asset_id] = (ts, server_bid, server_ask)

    def discard(self, asset_id):
        self.pending.pop(asset_id, None)

    def due(self, asset_id, ts):
        p = self.pending.get(asset_id)
        return p is not None and p[0] != ts

    def settle(self, book):
        """Compare the pending batch for book. Returns True if a resync is needed."""
        p = self.pending.pop(book.asset_id, None)
        if p is None or not book.ready:
            return False
        _, sbid, sask = p
        if sbid in (None, "") and sask in (None, ""):
            return False
        if book.crossed():
            self.crossed_skipped += 1
            return False
        self.checks += 1
        if book.best_bid() != _server(sbid, "0") or book.best_ask() != _server(sask, "1"):
            self.mismatches += 1
            book.mismatch_streak += 1
            if book.mismatch_streak >= 3:
                book.ready = False
                return True
        else:
            book.mismatch_streak = 0
        return False
