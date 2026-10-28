import time

def _f(x):
    return None if x is None else float(x)

def _ts(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None

class Auditor:
    """REST audit that never sleeps: re-checks are scheduled and run from the main loop."""
    def __init__(self, fetch, clock=time.monotonic, recheck_after=2.0, rechecks=2, out=print, label=lambda t: t):
        self.fetch, self.clock, self.recheck_after, self.rechecks = fetch, clock, recheck_after, rechecks
        self.out, self.label = out, label
        self.ok = self.moved = self.diff = 0
        self.due = {}  # token -> (time, rechecks_left)

    def start(self, books):
        for t, b in books.items():
            if b.ready and t not in self.due:
                self._check(t, b, self.rechecks)

    def run_due(self, books):
        now = self.clock()
        for t, (when, left) in list(self.due.items()):
            if now >= when:
                del self.due[t]
                b = books.get(t)
                if b is not None and b.ready:
                    self._check(t, b, left)

    def clear(self):
        self.due.clear()

    def _check(self, t, b, left):
        rb, ra, raw = self.fetch(t)
        lb, la = _f(b.best_bid()), _f(b.best_ask())
        if rb == lb and ra == la:
            self.ok += 1; self.out(f"audit OK {self.label(t)}"); return
        rts, lts = _ts(raw.get("timestamp")), _ts(b.ts)
        if rts is not None and lts is not None and rts > lts:
            if left > 0:
                self.due[t] = (self.clock() + self.recheck_after, left - 1); return
            self.moved += 1
            self.out(f"audit MOVED {self.label(t)} (REST stayed ahead: rest {rb}/{ra} @{rts} local {lb}/{la} @{lts})"); return
        self.diff += 1
        self.out(f"audit DIFF {self.label(t)} rest {rb}/{ra} @{rts} local {lb}/{la} @{lts}")
