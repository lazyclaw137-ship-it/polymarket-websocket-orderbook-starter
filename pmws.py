import argparse, json, sys, time
import markets
from audit import Auditor
from stream import Streamer

BANNER = "Public Polymarket market data, read-only. Not advice."

def fmt(x):
    return "-" if x is None else f"{x}"

def cmd_stream(a):
    assets = markets.resolve(a.slug) if a.slug else markets.top_markets(a.top, a.include_decided)
    if not assets:
        print("no markets to stream"); return 1
    print(f"streaming {len(assets)} outcomes", flush=True)
    log = open(a.log, "a") if a.log else None
    st = {"print": time.monotonic(), "audit": time.monotonic(), "rc": 0}
    aud = Auditor(markets.rest_book, label=lambda t: f"{assets[t]['slug']} [{assets[t]['outcome']}]",
                  out=lambda m: print(m, flush=True))

    def on_event(ev, s):
        if ev.get("event_type") == "last_trade_price":
            m = assets.get(ev.get("asset_id"), {})
            print(f"TRADE {m.get('slug')} [{m.get('outcome')}] {ev.get('side')} {ev.get('size')} @ {ev.get('price')}", flush=True)

    def tick(s):
        now = time.monotonic()
        if now - st["print"] >= a.print_every:
            st["print"] = now
            for t, b in s.books.items():
                m = assets[t]
                age = "-" if b.updated is None else f"{time.time()-b.updated:.0f}s"
                print(f"  {m['slug'][:40]:<40} {m['outcome'][:12]:<12} bid {fmt(b.best_bid()):<6} ask {fmt(b.best_ask()):<6} "
                      f"spr {fmt(b.spread()):<6} lv {len(b.bids)}/{len(b.asks)} last {fmt(b.last_trade):<6} upd {age}", flush=True)
        if a.audit_every:
            if s.stats["reconnects"] != st["rc"]:
                st["rc"] = s.stats["reconnects"]; aud.clear()
            aud.run_due(s.books)
            if now - st["audit"] >= a.audit_every:
                st["audit"] = now
                aud.start(s.books)

    s = Streamer(assets, custom=a.custom, stale_after=a.stale_after, on_event=on_event, log=log)
    try:
        s.run(duration=a.duration, drop_after=a.drop_after, tick=tick)
    except KeyboardInterrupt:
        pass
    c = s.stats
    ev = {k[3:]: v for k, v in c.items() if k.startswith("ev_")}
    print(f"stats: frames={c['frames']} events={ev} pings={c['pings']} pongs={c['pongs']} reconnects={c['reconnects']} "
          f"server_checks={s.checker.checks} server_mismatches={s.checker.mismatches} crossed_skipped={s.checker.crossed_skipped} "
          f"audit_ok={aud.ok} audit_moved={aud.moved} audit_diff={aud.diff} dropped_before_snapshot={c['dropped_before_snapshot']} "
          f"unknown={c['unknown_events']}", flush=True)
    return 0

def cmd_book(a):
    toks = markets.resolve([a.slug]) if a.slug else {a.token: {"slug": "?", "outcome": "?"}}
    for t, m in toks.items():
        b, k, _ = markets.rest_book(t)
        print(f"{m['slug']} [{m['outcome']}] best bid {fmt(b)} best ask {fmt(k)}")
    return 0

def main(argv=None):
    print(BANNER)
    ap = argparse.ArgumentParser(); sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stream"); g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--slug", action="append"); g.add_argument("--top", type=int)
    s.add_argument("--duration", type=float); s.add_argument("--print-every", type=float, default=15)
    s.add_argument("--include-decided", action="store_true"); s.add_argument("--custom", action="store_true"); s.add_argument("--log")
    s.add_argument("--drop-after", type=float); s.add_argument("--audit-every", type=float)
    s.add_argument("--stale-after", type=float, default=30); s.set_defaults(f=cmd_stream)
    b = sub.add_parser("book"); g = b.add_mutually_exclusive_group(required=True)
    g.add_argument("--slug"); g.add_argument("--token"); b.set_defaults(f=cmd_book)
    a = ap.parse_args(argv)
    return a.f(a)

if __name__ == "__main__":
    sys.exit(main())
