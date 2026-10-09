import json
import sys
from collections import defaultdict
from datetime import datetime

# Usage: python3 tools/frame_order_check.py build/ws-probe/frames-XXXX.tsv
# For each channel, counts frames that arrived after a frame with a LATER server time,
# and how late they were (ms). "same channel" = same topic, "any channel" = same symbol.

latest_by_topic = {}
latest_by_symbol = {}
late_same = defaultdict(list)
late_any = defaultdict(list)
counts = defaultdict(int)
last_book = {}
duplicate_books = 0

with open(sys.argv[1], encoding="utf-8") as f:
    for line in f:
        _, _, raw = line.rstrip("\n").split("\t", 2)
        frame = json.loads(raw)
        if frame.get("type") != "message":
            continue

        channel, _, symbol = frame["topic"].split(":")
        server_ms = datetime.fromisoformat(frame["data"]["timestamp"]).timestamp() * 1000
        counts[channel] += 1

        for key, latest, late in ((frame["topic"], latest_by_topic, late_same),
                                  (symbol, latest_by_symbol, late_any)):
            seen = latest.get(key)
            if seen is not None and server_ms < seen:
                late[channel].append(seen - server_ms)
            latest[key] = server_ms if seen is None else max(seen, server_ms)

        if channel == "orderbook":
            book = json.dumps([frame["data"]["asks"], frame["data"]["bids"]])
            if last_book.get(symbol) == book:
                duplicate_books += 1
            last_book[symbol] = book

for channel, total in counts.items():
    for name, late in (("same channel", late_same[channel]), ("any channel", late_any[channel])):
        ordered = sorted(late)
        p99 = ordered[int(len(ordered) * 0.99)] if ordered else 0
        print(f"{channel:9s} vs {name:12s}: late {len(ordered):5d}/{total:5d}"
              f"  p99={p99:.0f}ms  max={max(ordered, default=0):.0f}ms")

print(f"orderbook frames identical to previous: {duplicate_books}/{counts['orderbook']}")