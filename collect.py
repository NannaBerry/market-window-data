#!/usr/bin/env python3
"""
collect.py — collects trade prints and order-book snapshots for short-horizon (15-minute)
binary up/down markets (BTC and ETH) from public APIs. No keys, stdlib only.

Data sources (all public, unauthenticated):
  - gamma-api.polymarket.com  : market metadata + resolution (outcomePrices)
  - data-api.polymarket.com   : trade prints (paginated)
  - clob.polymarket.com       : order books + minute price history
  - api.binance.com           : 1m / 15m reference klines

Output: data/ledger.jsonl — append-only JSONL, one record per line.
  type=window : one record per resolved 15-min window (prints binned per token per minute,
                price history, reference klines, market metadata, resolution)
  type=book   : one live order-book snapshot (top 3 levels per side, both tokens)

Modes:
  python collect.py                 one pass: backfill resolved windows + one live book snapshot
  python collect.py --resident M    stay alive M minutes; wake at every 15-min boundary,
                                    snapshot both tokens' books in the t+60..150s band for the
                                    new window, then backfill the window(s) that just closed.

Crash-safe: every record is appended the moment it exists; dedup keys are re-read from the
ledger at startup, so killing and re-running loses nothing and double-logs nothing.
"""
import os, sys, json, time, urllib.request, urllib.error
from datetime import datetime, timezone

BASE = os.path.dirname(os.path.abspath(__file__))
LEDGER = os.path.join(BASE, "data", "ledger.jsonl")

COINS = {"btc": "BTCUSDT", "eth": "ETHUSDT"}
W = 900                        # window length seconds
MAX_WINDOWS_PER_COIN = 8       # backfill cap per invocation (~2h of windows)
MAX_PRINT_PAGES = 8            # 8 x 500 = 4,000 prints cap per window
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


def log(m):
    print(m, flush=True)


def gj(url, retries=2, timeout=25):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", errors="replace"))
        except Exception as e:
            if attempt == retries:
                log(f"  fetch fail {url[:90]}: {e}")
                return None
            time.sleep(1.5 ** attempt)


def append(rec):
    os.makedirs(os.path.dirname(LEDGER), exist_ok=True)
    with open(LEDGER, "a") as f:
        f.write(json.dumps(rec) + "\n")


def ledger_window_keys():
    """(coin, window_start) already logged as type=window."""
    keys = set()
    if os.path.exists(LEDGER):
        with open(LEDGER) as f:
            for line in f:
                try:
                    r = json.loads(line)
                    if r.get("type") == "window":
                        keys.add((r["coin"], r["window_start"]))
                except Exception:
                    pass
    return keys


def ledger_book_keys(max_offset=150):
    """(coin, window_start) that already have a book snapshot at offset <= max_offset.
    Used by resident mode for idempotent re-runs."""
    keys = set()
    if os.path.exists(LEDGER):
        with open(LEDGER) as f:
            for line in f:
                try:
                    r = json.loads(line)
                    if r.get("type") == "book" and r.get("offset_s", 9999) <= max_offset:
                        keys.add((r["coin"], r["window_start"]))
                except Exception:
                    pass
    return keys


def fetch_market(coin, w):
    """Market object for a window; open markets via plain slug, resolved need closed=true."""
    slug = f"{coin}-updown-15m-{w}"
    d = gj(f"https://gamma-api.polymarket.com/markets?slug={slug}")
    if not d:
        d = gj(f"https://gamma-api.polymarket.com/markets?slug={slug}&closed=true")
    return (d[0], slug) if d else (None, slug)


def parse_tokens(m):
    toks = m.get("clobTokenIds")
    toks = json.loads(toks) if isinstance(toks, str) else (toks or [])
    return (str(toks[0]), str(toks[1])) if len(toks) >= 2 else (None, None)


def winner_from(m):
    op = m.get("outcomePrices")
    op = json.loads(op) if isinstance(op, str) else op
    if not op or len(op) < 2:
        return None
    try:
        if float(op[0]) >= 0.99:
            return "Up"
        if float(op[1]) >= 0.99:
            return "Down"
    except (ValueError, TypeError):
        pass
    return None


def fetch_prints(condition_id, w):
    """All trade prints for the window's market (paginated, newest-first), clipped to the window."""
    allt, pages = [], 0
    for off in range(0, MAX_PRINT_PAGES * 500, 500):
        tr = gj(f"https://data-api.polymarket.com/trades?market={condition_id}&limit=500&offset={off}")
        pages += 1
        if not tr:
            break
        allt += tr
        if len(tr) < 500 or min(t.get("timestamp", 0) for t in tr) < w - 60:
            break
    inw = [t for t in allt if w <= t.get("timestamp", 0) < w + W]
    truncated = pages >= MAX_PRINT_PAGES and len(allt) >= MAX_PRINT_PAGES * 500
    return inw, len(allt), truncated


def bin_prints(prints, w):
    """Per-token per-minute aggregates.
    rows[token 'Up'/'Down'] = [[minute, n, buy_vol, sell_vol, buy_min, buy_max, buy_vwap,
                                sell_min, sell_max], ...]  (prices None when side absent)"""
    bins = {}
    for t in prints:
        try:
            mn = int((t["timestamp"] - w) // 60)
            side, out, p, sz = t["side"], t["outcome"], float(t["price"]), float(t["size"])
        except (KeyError, ValueError, TypeError):
            continue
        if not (0 <= mn < 15) or out not in ("Up", "Down"):
            continue
        b = bins.setdefault((out, mn), {"n": 0, "bv": 0.0, "sv": 0.0, "bmin": None, "bmax": None,
                                        "bpv": 0.0, "smin": None, "smax": None})
        b["n"] += 1
        if side == "BUY":
            b["bv"] += sz
            b["bpv"] += p * sz
            b["bmin"] = p if b["bmin"] is None else min(b["bmin"], p)
            b["bmax"] = p if b["bmax"] is None else max(b["bmax"], p)
        else:
            b["sv"] += sz
            b["smin"] = p if b["smin"] is None else min(b["smin"], p)
            b["smax"] = p if b["smax"] is None else max(b["smax"], p)
    out = {"Up": [], "Down": []}
    for (tok, mn) in sorted(bins):
        b = bins[(tok, mn)]
        vwap = round(b["bpv"] / b["bv"], 4) if b["bv"] > 0 else None
        out[tok].append([mn, b["n"], round(b["bv"], 1), round(b["sv"], 1),
                         b["bmin"], b["bmax"], vwap, b["smin"], b["smax"]])
    return out


def fetch_price_history(token, w):
    ph = gj(f"https://clob.polymarket.com/prices-history?market={token}&startTs={w}&endTs={w + W}&fidelity=1")
    return [[h["t"] - w, h["p"]] for h in (ph or {}).get("history", [])]


def fetch_binance_1m(symbol, start_s, end_s):
    """1m closes [start_s, end_s] -> {open_ts_s: close}; chunks of <=1000 bars."""
    closes = {}
    s = start_s
    while s < end_s:
        kl = gj(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m"
                f"&startTime={s * 1000}&endTime={end_s * 1000 - 1}&limit=1000")
        if not kl:
            break
        for k in kl:
            closes[k[0] // 1000] = float(k[4])
        last = kl[-1][0] // 1000
        if last + 60 <= s or len(kl) < 1000:
            break
        s = last + 60
    return closes


def fetch_binance_15m(symbol, start_s, end_s):
    """15m bars -> {open_ts_s: (open, close)}."""
    bars = {}
    kl = gj(f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=15m"
            f"&startTime={start_s * 1000}&endTime={end_s * 1000 - 1}&limit=1000")
    for k in (kl or []):
        bars[k[0] // 1000] = (float(k[1]), float(k[4]))
    return bars


def snapshot_book(token):
    bk = gj(f"https://clob.polymarket.com/book?token_id={token}")
    if not bk:
        return None
    asks = sorted(((float(a["price"]), float(a.get("size", 0))) for a in bk.get("asks", [])), key=lambda x: x[0])[:3]
    bids = sorted(((float(b["price"]), float(b.get("size", 0))) for b in bk.get("bids", [])), key=lambda x: -x[0])[:3]
    return {"bids": bids, "asks": asks}


def backfill_coin(coin, symbol, cur, max_windows, run_ts, logged, stats):
    """Backfill up to max_windows resolved windows for one coin, oldest-first, contiguous
    (stops at the first unresolved window so gaps cannot silently form). Appends each
    window record immediately. Returns count backfilled."""
    now = int(time.time())
    prev = [ws for (c, ws) in logged if c == coin]
    start = (max(prev) + W) if prev else cur - max_windows * W
    targets = [ws for ws in range(start, cur, W) if (coin, ws) not in logged][:max_windows]  # oldest-first: contiguity
    log(f"--- {coin.upper()} backfill: {len(targets)} candidate windows ---")
    if targets:
        b1m = fetch_binance_1m(symbol, targets[0] - 3600, targets[-1] + W)
        b15 = fetch_binance_15m(symbol, targets[0] - 2 * W, targets[-1] + W)
    n_coin = 0
    for ws in targets:
        m, slug = fetch_market(coin, ws)
        if m is None:
            if ws + W < now - 600:
                stats["skipped_no_market"] += 1
                log(f"  {slug}: no market (gap), skipping")
                continue
            break
        if not m.get("closed"):
            stats["stopped_unresolved"] += 1
            log(f"  {slug}: not resolved yet — stopping (contiguity)")
            break
        outcome = winner_from(m)
        cid = m.get("conditionId")
        up_tok, down_tok = parse_tokens(m)
        prints, total_prints, truncated = fetch_prints(cid, ws) if cid else ([], 0, False)
        if truncated:
            stats["prints_truncated"] += 1
        pbar = b15.get(ws - W)
        wbar = b15.get(ws)
        prior_dir = ("Up" if pbar[1] >= pbar[0] else "Down") if pbar else None
        binance_dir = ("Up" if wbar[1] >= wbar[0] else "Down") if wbar else None
        rec = {
            "type": "window", "run_ts": run_ts, "coin": coin, "symbol": symbol,
            "window_start": ws, "window_end": ws + W, "slug": slug,
            "question": m.get("question"), "condition_id": cid,
            "up_token": up_tok, "down_token": down_tok,
            "outcome": outcome,  # market resolution (Up/Down/None)
            # prior + current 15m reference bars (exchange klines)
            "prior_bar": {"open": pbar[0], "close": pbar[1],
                          "ret_pct": round((pbar[1] / pbar[0] - 1) * 100, 4)} if pbar else None,
            "prior_dir": prior_dir,
            "window_bar": {"open": wbar[0], "close": wbar[1]} if wbar else None,
            "binance_dir": binance_dir,
            "binance_margin_usd": round(wbar[1] - wbar[0], 2) if wbar else None,
            # resolution source can differ from the exchange's close>=open direction;
            # logged so small-margin disagreements are visible in the data
            "chainlink_vs_binance_diverged": (outcome != binance_dir) if (outcome and binance_dir) else None,
            "n_prints": len(prints), "n_prints_total_pull": total_prints,
            "prints_truncated": truncated,
            "print_bins": bin_prints(prints, ws),
            "up_price_history": fetch_price_history(up_tok, ws) if up_tok else [],
            # 1m closes [start-60m, end], offsets in seconds from window start
            "m1_closes": [[ts - ws, c] for ts, c in sorted(b1m.items())
                          if ws - 3600 <= ts <= ws + W] if targets else [],
            # market fee/tick metadata as published
            "taker_base_fee": m.get("takerBaseFee"), "maker_base_fee": m.get("makerBaseFee"),
            "tick": m.get("orderPriceMinTickSize"), "min_order": m.get("orderMinSize"),
        }
        append(rec)
        logged.add((coin, ws))
        n_coin += 1
        stats["windows_logged"] += 1
        log(f"  {slug}: outcome={outcome} prior={prior_dir} prints={len(prints)}"
            f" diverged={rec['chainlink_vs_binance_diverged']}")
    return n_coin


def capture_books(coin, w, run_ts, stats, extra=None):
    """Snapshot both tokens' live books for window w (must be open). Appends immediately.
    Returns True if a book record was logged."""
    m, slug = fetch_market(coin, w)
    if not m or m.get("closed"):
        return False
    up_tok, down_tok = parse_tokens(m)
    books = {}
    for side, tok in (("Up", up_tok), ("Down", down_tok)):
        if tok:
            bk = snapshot_book(tok)
            if bk:
                books[side] = bk
    if not books:
        return False
    off = int(time.time()) - w
    rec = {"type": "book", "run_ts": run_ts, "coin": coin, "window_start": w,
           "slug": slug, "offset_s": off, "condition_id": m.get("conditionId"),
           "books": books}
    if extra:
        rec.update(extra)
    append(rec)
    stats["books_logged"] += 1
    bb = books.get("Up", {}).get("bids", [])
    ba = books.get("Up", {}).get("asks", [])
    log(f"  {slug}: live book @t+{off}s Up bid={bb[0] if bb else None} ask={ba[0] if ba else None}")
    return True


def resident_loop(minutes, max_windows):
    """Resident mode: wake at every 15-min window boundary; capture the NEW window's live book
    inside the t+60..150s band, then backfill the window(s) that just closed. Crash-tolerant:
    every record appended immediately, dedup keys re-read from the ledger at startup."""
    t0 = time.time()
    deadline = t0 + minutes * 60
    logged = ledger_window_keys()
    early_books = ledger_book_keys(150)  # idempotent re-run: skip windows already book-snapped early
    stats = {"windows_logged": 0, "books_logged": 0, "skipped_no_market": 0,
             "stopped_unresolved": 0, "prints_truncated": 0, "resident_wakes": 0,
             "early_books_logged": 0}
    log(f"=== resident loop: {minutes:.0f}m | deadline {datetime.fromtimestamp(deadline, timezone.utc).isoformat()} | "
        f"{len(logged)} windows / {len(early_books)} early books already in ledger ===")
    while True:
        now = time.time()
        cur = int(now) // W * W
        run_ts = datetime.now(timezone.utc).isoformat()
        stats["resident_wakes"] += 1
        # (a) live book snapshot for the current window, inside the t+60..150s band
        off = now - cur
        if off < 145:  # leave a few seconds of margin for two coins' fetches
            if off < 60:
                time.sleep(60 - off)
            for coin in COINS:
                if (coin, cur) not in early_books and time.time() - cur <= 150:
                    if capture_books(coin, cur, run_ts, stats, extra={"resident": True}):
                        early_books.add((coin, cur))
                        stats["early_books_logged"] += 1
        else:
            log(f"  wake landed at t+{off:.0f}s (>150s) — early-book band missed for window {cur}")
        # (b) backfill the window(s) that just closed (idempotent; a not-yet-resolved prior
        #     window simply rolls to the next wake via the contiguity stop)
        for coin, symbol in COINS.items():
            backfill_coin(coin, symbol, cur, max_windows, run_ts, logged, stats)
        nxt = cur + W
        if nxt >= deadline:
            break
        time.sleep(max(0, nxt - time.time()))
    log(f"=== RESIDENT DONE | wakes={stats['resident_wakes']} windows={stats['windows_logged']} "
        f"early_books={stats['early_books_logged']} books_total={stats['books_logged']} ===")
    log(f"    ledger -> {LEDGER}")


def main():
    max_windows = MAX_WINDOWS_PER_COIN
    if "--max-windows" in sys.argv:
        max_windows = int(sys.argv[sys.argv.index("--max-windows") + 1])
    if "--resident" in sys.argv:
        resident_loop(float(sys.argv[sys.argv.index("--resident") + 1]), max_windows)
        return
    now = int(time.time())
    run_ts = datetime.now(timezone.utc).isoformat()
    cur = now - now % W  # current window start
    log(f"=== collect.py | {run_ts} | current window {cur} | max backfill {max_windows}/coin ===")
    logged = ledger_window_keys()
    log(f"  {len(logged)} windows already in ledger")
    stats = {"windows_logged": 0, "books_logged": 0, "skipped_no_market": 0,
             "stopped_unresolved": 0, "prints_truncated": 0}
    for coin, symbol in COINS.items():
        backfill_coin(coin, symbol, cur, max_windows, run_ts, logged, stats)
        # live book snapshot for the currently-open window (offset = wherever this run lands)
        capture_books(coin, cur, run_ts, stats)
    log(f"=== DONE | windows={stats['windows_logged']} books={stats['books_logged']} "
        f"gaps={stats['skipped_no_market']} unresolved_stop={stats['stopped_unresolved']} ===")
    log(f"    ledger -> {LEDGER}")


if __name__ == "__main__":
    main()
