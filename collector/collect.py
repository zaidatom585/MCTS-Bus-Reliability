"""Collect MCTS GTFS-realtime snapshots for a fixed number of minutes.

Saves each response exactly as received (protobuf, gzipped) so nothing is lost before parsing:
    <out>/feed=<vehicles|trips>/date=YYYY-MM-DD/hour=HH/<feed>_<UTC timestamp>.pb.gz
Also writes one small JSON log per run (requests made, failures, bytes) under <out>/_runs/.

Usage:
    python collector/collect.py --minutes 55 --interval 60 --out out
"""
import argparse
import gzip
import json
import os
import time
from datetime import datetime, timezone

import requests

FEEDS = {
    "vehicles": "https://realtime.ridemcts.com/gtfsrt/vehicles",
    "trips": "https://realtime.ridemcts.com/gtfsrt/trips",
}


def utcnow():
    return datetime.now(timezone.utc)


def save(out, feed, content, at):
    folder = os.path.join(out, f"feed={feed}", f"date={at:%Y-%m-%d}", f"hour={at:%H}")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"{feed}_{at:%Y%m%dT%H%M%SZ}.pb.gz")
    with gzip.open(path, "wb") as f:
        f.write(content)
    return path


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--minutes", type=float, default=55)
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--out", default="out")
    args = p.parse_args()

    session = requests.Session()
    session.headers["User-Agent"] = "mcts-bus-reliability (portfolio project; github.com/zaidatom585)"
    started = utcnow()
    deadline = time.monotonic() + args.minutes * 60
    log = {"started_utc": started.isoformat(), "interval_s": args.interval,
           "requests": 0, "saved": 0, "failures": 0, "bytes": 0, "errors": []}

    while True:
        cycle = time.monotonic()
        for feed, url in FEEDS.items():
            log["requests"] += 1
            at = utcnow()
            try:
                r = session.get(url, timeout=20)
                r.raise_for_status()
                if not r.content:
                    raise ValueError("empty response")
                save(args.out, feed, r.content, at)
                log["saved"] += 1
                log["bytes"] += len(r.content)
            except Exception as e:  # keep going: one failed poll must not end the run
                log["failures"] += 1
                if len(log["errors"]) < 20:
                    log["errors"].append(f"{at:%H:%M:%S} {feed}: {type(e).__name__}: {str(e)[:120]}")
        if time.monotonic() + args.interval > deadline:
            break
        time.sleep(max(0, args.interval - (time.monotonic() - cycle)))

    log["finished_utc"] = utcnow().isoformat()
    os.makedirs(os.path.join(args.out, "_runs"), exist_ok=True)
    with open(os.path.join(args.out, "_runs", f"run_{started:%Y%m%dT%H%M%SZ}.json"), "w") as f:
        json.dump(log, f, indent=2)
    print(json.dumps({k: v for k, v in log.items() if k != "errors"}, indent=2))
    for e in log["errors"]:
        print("  error:", e)
    # Fail the workflow only if nothing at all was collected, so a partial run still gets uploaded.
    if log["saved"] == 0:
        raise SystemExit("No snapshots saved in this run.")


if __name__ == "__main__":
    main()
