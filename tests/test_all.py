import json, os, pathlib
import pytest, websocket
import markets
from book import LocalBook, Checker
from stream import Streamer

def snap(aid, bids, asks):
    return {"event_type": "book", "asset_id": aid, "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks], "hash": "h", "timestamp": "1"}

def test_gamma_decoding(monkeypatch):
    m = {"slug": "s", "question": "q", "closed": False, "enableOrderBook": True, "acceptingOrders": True,
         "outcomes": '["Lakers","Celtics"]', "clobTokenIds": '["1","2"]'}
    monkeypatch.setattr(markets, "_get", lambda url, params=None: [m])
    out = markets.resolve(["s"])
    assert out["1"]["outcome"] == "Lakers" and out["2"]["outcome"] == "Celtics"

def test_top_markets_end_date_and_accepting(monkeypatch):
    seen = {}
    a = {"slug": "a", "enableOrderBook": True, "acceptingOrders": False, "bestBid": "0.3", "bestAsk": "0.4", "outcomes": '["Yes","No"]', "clobTokenIds": '["1","2"]'}
    b = {"slug": "b", "enableOrderBook": True, "acceptingOrders": True, "bestBid": "0.3", "bestAsk": "0.4", "outcomes": '["Yes","No"]', "clobTokenIds": '["3","4"]'}
    def g(url, params=None): seen.update(params); return [a, b]
    monkeypatch.setattr(markets, "_get", g)
    out = markets.top_markets(2)
    assert "end_date_min" not in seen and set(out) == {"3", "4"}

def test_rest_best(monkeypatch):
    monkeypatch.setattr(markets, "_get", lambda url, params=None: {"bids": [{"price": "0.1"}, {"price": "0.4"}], "asks": [{"price": "0.9"}, {"price": "0.5"}]})
    b, a, _ = markets.rest_book("x")
    assert (b, a) == (0.4, 0.5)

def test_snapshot_replaces_and_aggregate():
    b = LocalBook("x"); b.apply_snapshot(snap("x", [("0.4", "10")], [("0.6", "5")]))
    b.apply_change({"price": "0.40", "size": "3", "side": "BUY"})
    assert len(b.bids) == 1 and b.bids["0.4"] == 3
    b.apply_change({"price": "0.4", "size": "0", "side": "BUY"})
    assert b.best_bid() is None
    b.apply_snapshot(snap("x", [("0.3", "1")], []))
    assert str(b.best_bid()) == "0.3" and b.best_ask() is None

def test_drop_before_snapshot():
    b = LocalBook("x"); assert b.apply_change({"price": "0.4", "size": "1", "side": "BUY"}) is False

def ready_book(bids, asks):
    b = LocalBook("x"); b.apply_snapshot(snap("x", bids, asks)); return b

def test_checker_mismatch_and_resync():
    b = ready_book([("0.4", "1")], [("0.6", "1")]); c = Checker()
    c.note("x", "1", "0.4", "0.6"); assert not c.settle(b)
    for ts in "234":
        c.note("x", ts, "0.41", "0.6"); r = c.settle(b)
    assert r and c.mismatches == 3 and c.checks == 4 and not b.ready

def test_sentinels_empty_side():
    b = ready_book([("0.999", "5")], []); c = Checker()
    c.note("x", "1", "0.999", "1"); assert not c.settle(b)
    b2 = ready_book([], [("0.001", "5")]); b2.asset_id = "y"
    c.note("y", "1", "0", "0.001"); assert not c.settle(b2)
    assert c.mismatches == 0 and c.checks == 2

def test_locked_skipped():
    b = ready_book([("0.5", "1")], [("0.5", "1")]); c = Checker()
    c.note("x", "1", "0.4", "0.5"); assert not c.settle(b)
    assert c.crossed_skipped == 1 and c.mismatches == 0

def sweep_frames():
    # taker SELL sweeps bids 0.44, 0.43 down to 0.42; server best already 0.42 on every entry
    pc = lambda price, size: json.dumps({"event_type": "price_change", "timestamp": "100", "price_changes": [
        {"asset_id": "1", "price": price, "size": size, "side": "BUY", "best_bid": "0.42", "best_ask": "0.6"}]})
    return [json.dumps([snap("1", [("0.44", "1"), ("0.43", "1"), ("0.42", "1")], [("0.6", "1")])]),
            pc("0.42", "0.5"), pc("0.44", "0"), pc("0.43", "0")]

def test_sweep_is_one_check():
    ws = FakeWS(sweep_frames()); s = Streamer({"1": {}}, connect=lambda u: ws, clock=Clock())
    s.run(duration=20)
    assert s.checker.checks == 1 and s.checker.mismatches == 0

def test_new_timestamp_settles_previous():
    frames = sweep_frames()[:2] + [json.dumps({"event_type": "price_change", "timestamp": "101", "price_changes": [
        {"asset_id": "1", "price": "0.6", "size": "2", "side": "SELL", "best_bid": "0.44", "best_ask": "0.6"}]})]
    ws = FakeWS(frames); s = Streamer({"1": {}}, connect=lambda u: ws, clock=Clock())
    s.handle_text(frames[0]); s.handle_text(frames[1]); assert s.checker.checks == 0
    s.handle_text(frames[2]); assert s.checker.checks == 1 and s.checker.mismatches == 1  # 0.44 vs 0.42

def test_top_markets_skips_decided(monkeypatch):
    ms = [{"slug": "d", "enableOrderBook": True, "acceptingOrders": True, "bestBid": None, "bestAsk": "0.001", "outcomes": '["A","B"]', "clobTokenIds": '["1","2"]'},
          {"slug": "ok", "enableOrderBook": True, "acceptingOrders": True, "bestBid": "0.3", "bestAsk": "0.31", "outcomes": '["Yes","No"]', "clobTokenIds": '["3","4"]'}]
    seen = {}
    def g(url, params=None): seen.update(params); return ms
    monkeypatch.setattr(markets, "_get", g)
    assert set(markets.top_markets(1)) == {"3", "4"} and seen["limit"] == 3
    assert set(markets.top_markets(2, include_decided=True)) == {"1", "2", "3", "4"}

class FakeWS:
    def __init__(self, frames): self.frames, self.sent, self.closed = list(frames), [], False
    def send(self, m): self.sent.append(m)
    def recv(self):
        if self.frames:
            f = self.frames.pop(0)
            if isinstance(f, Exception): raise f
            return f
        raise websocket.WebSocketTimeoutException()
    def close(self): self.closed = True

class Clock:
    def __init__(self): self.t = 0
    def __call__(self): self.t += 1; return self.t

def test_frames_pong_junk_and_ping():
    frames = [json.dumps([snap("1", [("0.4", "1")], [("0.6", "1")])]),
              json.dumps({"event_type": "price_change", "timestamp": "5", "price_changes": [{"asset_id": "1", "price": "0.45", "size": "2", "side": "BUY", "best_bid": "0.45", "best_ask": "0.6"}]}),
              "PONG", "garbage", json.dumps({"event_type": "weird"})]
    ws = FakeWS(frames); clk = Clock()
    s = Streamer({"1": {}}, connect=lambda u: ws, clock=clk)
    s.run(duration=40)
    assert json.loads(ws.sent[0])["type"] == "market" and ws.sent[1] == "PING"
    assert ws.sent.count("PING") >= 3
    assert s.stats["pongs"] == 1 and s.stats["junk_text"] == 1 and s.stats["unknown_events"] == 1
    assert s.checker.checks == 1 and s.checker.mismatches == 0

def test_reconnect_clears_and_watchdog(monkeypatch):
    import stream
    monkeypatch.setattr(stream.time, "sleep", lambda s: None)
    socks = [FakeWS([json.dumps(snap("1", [("0.4", "1")], [("0.6", "1")])), ConnectionError("x")]), FakeWS([])]
    it = iter(socks); clk = Clock()
    s = Streamer({"1": {}}, connect=lambda u: next(it), clock=clk, stale_after=5)
    seen = []
    orig = s.open
    def op(): orig(); seen.append(s.books["1"].ready)
    s.open = op
    try:
        s.run(duration=30)
    except StopIteration:
        pass
    assert seen[1] is False and len(socks[1].sent) >= 1 and s.stats["reconnects"] >= 2  # error + watchdog

def test_no_trading_strings():
    root = pathlib.Path(__file__).parent.parent
    bad = ["private_key", "eth_account", "ws/user", "post_order", "create_order", "/order", "api_secret"]
    for f in root.glob("*.py"):
        t = f.read_text()
        for b in bad: assert b not in t, (f, b)

from audit import Auditor

class TClock:
    def __init__(self): self.t = 0.0
    def __call__(self): return self.t

def test_audit_rest_newer_rechecks_without_sleep(monkeypatch):
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda s: (_ for _ in ()).throw(AssertionError("slept")))
    b = ready_book([("0.4", "1")], [("0.6", "1")]); b.ts = "100"
    calls = []
    def fetch(t):
        calls.append(t)
        return (0.41, 0.6, {"timestamp": "200"}) if len(calls) == 1 else (0.4, 0.6, {"timestamp": "300"})
    clk = TClock(); out = []
    a = Auditor(fetch, clock=clk, out=out.append)
    a.start({"x": b}); assert a.due and not out
    clk.t = 2.5; a.run_due({"x": b})
    assert a.ok == 1 and a.diff == 0 and len(calls) == 2

def test_audit_moved_after_rechecks():
    b = ready_book([("0.4", "1")], [("0.6", "1")]); b.ts = "100"
    clk = TClock(); a = Auditor(lambda t: (0.41, 0.6, {"timestamp": "200"}), clock=clk, out=lambda m: None)
    a.start({"x": b})
    for k in (1, 2): clk.t = 2.5 * k; a.run_due({"x": b})
    assert a.moved == 1 and a.diff == 0 and a.ok == 0

def test_audit_diff_when_local_has_seen_everything():
    b = ready_book([("0.4", "1")], [("0.6", "1")]); b.ts = "300"
    a = Auditor(lambda t: (0.41, 0.6, {"timestamp": "200"}), out=lambda m: None)
    a.start({"x": b})
    assert a.diff == 1 and not a.due

def test_change_stores_timestamp():
    b = ready_book([("0.4", "1")], [("0.6", "1")])
    b.apply_change({"price": "0.41", "size": "1", "side": "BUY"}, "777"); assert b.ts == "777"
