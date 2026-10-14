# Databricks notebook source
# MAGIC %md
# MAGIC # Profile feeds
# MAGIC Decodes the latest collected snapshots and reports which fields MCTS actually fills in, so the parsing and
# MAGIC arrival-matching logic is built on real data rather than on what the GTFS-realtime spec allows.

# COMMAND ----------

# MAGIC %pip install --quiet gtfs-realtime-bindings

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import glob
import gzip
import os
from collections import Counter
from datetime import datetime, timezone

from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

RAW = "/Volumes/workspace/mcts/raw/realtime"


def latest_files(feed, n):
    files = glob.glob(f"{RAW}/feed={feed}/date=*/hour=*/*.pb.gz")
    return sorted(files, key=os.path.basename)[-n:]


def load(path):
    msg = gtfs_realtime_pb2.FeedMessage()
    with gzip.open(path, "rb") as f:
        msg.ParseFromString(f.read())
    return msg


def flatten(d, prefix=""):
    out = {}
    for k, v in d.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flatten(v, key + "."))
        elif isinstance(v, list):
            out[key] = f"list[{len(v)}]"
        else:
            out[key] = v
    return out


for feed in ["vehicles", "trips"]:
    files = latest_files(feed, 3)
    print(f"\n==================== {feed}: {len(glob.glob(f'{RAW}/feed={feed}/date=*/hour=*/*.pb.gz')):,} files collected so far")
    if not files:
        continue
    msg = load(files[-1])
    ts = datetime.fromtimestamp(msg.header.timestamp, tz=timezone.utc)
    print(f"Latest file: {os.path.basename(files[-1])}   header time: {ts:%Y-%m-%d %H:%M:%S} UTC   entities: {len(msg.entity)}")

    # How often each field is present across all entities in the latest snapshot
    fill = Counter()
    for e in msg.entity:
        fill.update(flatten(MessageToDict(e, preserving_proto_field_name=True)).keys())
    print("\nField fill rate (share of entities that have the field):")
    for field, n in sorted(fill.items()):
        print(f"  {field:<55} {100 * n / max(len(msg.entity), 1):6.1f}%")

    if msg.entity:
        print("\nFirst entity:")
        print(MessageToDict(msg.entity[0], preserving_proto_field_name=True))

# COMMAND ----------

# Trip updates in detail: how many stop updates per trip, and are times or delays given?
files = latest_files("trips", 1)
if files:
    msg = load(files[-1])
    per_trip, has_time, has_delay, rel = [], 0, 0, Counter()
    for e in msg.entity:
        if not e.HasField("trip_update"):
            continue
        stus = e.trip_update.stop_time_update
        per_trip.append(len(stus))
        for s in stus:
            has_time += s.HasField("arrival") and s.arrival.HasField("time")
            has_delay += s.HasField("arrival") and s.arrival.HasField("delay")
            rel[gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(s.schedule_relationship)] += 1
    total = sum(per_trip)
    print(f"Trips: {len(per_trip)}   stop updates: {total:,}   per trip: min {min(per_trip, default=0)}, "
          f"max {max(per_trip, default=0)}, avg {total / max(len(per_trip), 1):.1f}")
    print(f"Stop updates with an arrival TIME:  {100 * has_time / max(total, 1):.1f}%")
    print(f"Stop updates with an arrival DELAY: {100 * has_delay / max(total, 1):.1f}%")
    print("Schedule relationship:", dict(rel))

# COMMAND ----------

# Does the realtime trip_id match the schedule's trip_id? (Essential for computing delays.)
import io
import zipfile

import pandas as pd

sched_dir = "/Volumes/workspace/mcts/raw/schedule"
latest_sched = sorted(os.listdir(sched_dir))[-1]
with zipfile.ZipFile(f"{sched_dir}/{latest_sched}") as z:
    trips = pd.read_csv(z.open("trips.txt"), dtype=str)
    stop_times_head = pd.read_csv(z.open("stop_times.txt"), dtype=str, nrows=5)
    feed_info = pd.read_csv(z.open("feed_info.txt"), dtype=str) if "feed_info.txt" in z.namelist() else None

print("Schedule file:", latest_sched, "  trips:", len(trips))
print(stop_times_head)
if feed_info is not None:
    print(feed_info.T)

files = latest_files("trips", 1)
if files:
    msg = load(files[-1])
    rt_ids = {e.trip_update.trip.trip_id for e in msg.entity if e.HasField("trip_update")}
    matched = rt_ids & set(trips["trip_id"])
    print(f"\nRealtime trips in latest snapshot: {len(rt_ids)}   found in schedule: {len(matched)} "
          f"({100 * len(matched) / max(len(rt_ids), 1):.1f}%)")
    print("Sample realtime trip_ids:", list(rt_ids)[:5])
    print("Sample schedule trip_ids:", trips["trip_id"].head(5).tolist())
