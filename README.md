# market-window-data

Collects trade prints and order-book snapshots for short-horizon (15-minute) binary markets from public APIs. No keys, Python stdlib only.

Output: `data/ledger.jsonl` (append-only JSONL). Run: `python collect.py --resident 55` (resident mode) or `python collect.py` (one backfill pass).
