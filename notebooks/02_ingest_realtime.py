# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Ingest realtime feeds
# MAGIC Decodes the collected GTFS-realtime snapshots (protobuf, gzipped) into two Delta tables. Auto Loader tracks
# MAGIC which files are done, so each run only processes snapshots that arrived since the last run.
# MAGIC
# MAGIC | Table | One row per | Kept |
# MAGIC |---|---|---|
# MAGIC | `bronze_trip_updates` | snapshot × trip × stop | only stop predictions due within the next 15 minutes (see below) |
# MAGIC | `bronze_vehicle_positions` | snapshot × bus | all |
# MAGIC
# MAGIC **Why only near-term predictions:** each snapshot predicts every remaining stop of every trip (~3,900 rows a
# MAGIC minute, ~5M a day). The arrival estimate only needs the *last* prediction before a bus reaches a stop, which
# MAGIC is always a near-term one. Keeping predictions due within 15 minutes cuts storage and compute by roughly
# MAGIC 80–90% without losing any arrival.

# COMMAND ----------

# MAGIC %pip install --quiet gtfs-realtime-bindings

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG, SCHEMA = "workspace", "mcts"
T_ = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw/realtime"
CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
HORIZON_S = 15 * 60          # keep predictions due within this many seconds of the snapshot

TRIP_SCHEMA = T.StructType([
    T.StructField("source_file", T.StringType()),
    T.StructField("snapshot_epoch", T.LongType()),
    T.StructField("trip_id", T.StringType()),
    T.StructField("route_id", T.StringType()),
    T.StructField("start_date", T.StringType()),
    T.StructField("start_time", T.StringType()),
    T.StructField("direction_id", T.LongType()),
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("trip_relationship", T.StringType()),
    T.StructField("stop_sequence", T.LongType()),
    T.StructField("stop_id", T.StringType()),
    T.StructField("arrival_epoch", T.LongType()),
    T.StructField("departure_epoch", T.LongType()),
    T.StructField("stop_relationship", T.StringType()),
])

VEHICLE_SCHEMA = T.StructType([
    T.StructField("source_file", T.StringType()),
    T.StructField("snapshot_epoch", T.LongType()),
    T.StructField("vehicle_id", T.StringType()),
    T.StructField("trip_id", T.StringType()),
    T.StructField("route_id", T.StringType()),
    T.StructField("latitude", T.DoubleType()),
    T.StructField("longitude", T.DoubleType()),
    T.StructField("bearing", T.DoubleType()),
    T.StructField("speed_mps", T.DoubleType()),
    T.StructField("vehicle_epoch", T.LongType()),
])


def _frame(rows, schema):
    df = pd.DataFrame(rows, columns=[f.name for f in schema.fields])
    for f in schema.fields:                       # nullable integers stay integers (not floats) for Arrow
        if isinstance(f.dataType, T.LongType):
            df[f.name] = pd.array(df[f.name], dtype="Int64")
    return df


def decode_trip_updates(batches):
    import gzip
    from google.transit import gtfs_realtime_pb2
    rel_trip = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name
    rel_stop = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name
    for pdf in batches:
        rows = []
        for path, content in zip(pdf["path"], pdf["content"]):
            msg = gtfs_realtime_pb2.FeedMessage()
            try:
                msg.ParseFromString(gzip.decompress(content))
            except Exception:
                continue                           # a corrupt file is skipped, not fatal (counted in validation)
            snap = msg.header.timestamp
            for e in msg.entity:
                if not e.HasField("trip_update"):
                    continue
                tu, t = e.trip_update, e.trip_update.trip
                for s in tu.stop_time_update:
                    arr = s.arrival.time if s.HasField("arrival") and s.arrival.HasField("time") else None
                    dep = s.departure.time if s.HasField("departure") and s.departure.HasField("time") else None
                    when = arr or dep
                    if when is None or when - snap > HORIZON_S:
                        continue
                    rows.append((
                        path, snap, t.trip_id or None, t.route_id or None, t.start_date or None,
                        t.start_time or None, t.direction_id if t.HasField("direction_id") else None,
                        tu.vehicle.id or None, rel_trip(t.schedule_relationship),
                        s.stop_sequence if s.HasField("stop_sequence") else None, s.stop_id or None,
                        arr, dep, rel_stop(s.schedule_relationship),
                    ))
        yield _frame(rows, TRIP_SCHEMA)


def decode_vehicles(batches):
    import gzip
    from google.transit import gtfs_realtime_pb2
    for pdf in batches:
        rows = []
        for path, content in zip(pdf["path"], pdf["content"]):
            msg = gtfs_realtime_pb2.FeedMessage()
            try:
                msg.ParseFromString(gzip.decompress(content))
            except Exception:
                continue
            snap = msg.header.timestamp
            for e in msg.entity:
                if not e.HasField("vehicle"):
                    continue
                v = e.vehicle
                rows.append((
                    path, snap, v.vehicle.id or None, v.trip.trip_id or None, v.trip.route_id or None,
                    v.position.latitude, v.position.longitude, v.position.bearing, v.position.speed,
                    v.timestamp or None,
                ))
        yield _frame(rows, VEHICLE_SCHEMA)


def ingest(feed, decoder, schema, table):
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.partitionColumns", "")
        .option("pathGlobFilter", "*.pb.gz")
        .option("cloudFiles.maxFilesPerTrigger", 300)
        .load(f"{RAW}/feed={feed}/")
        .select("path", "content")
        .mapInPandas(decoder, schema)
        .withColumn("snapshot_ts", F.timestamp_seconds("snapshot_epoch"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    q = (
        stream.writeStream
        .option("checkpointLocation", f"{CHECKPOINTS}/{table}")
        .trigger(availableNow=True)
        .toTable(T_(table))
    )
    q.awaitTermination()
    print(f"{table}: {spark.table(T_(table)).count():,} rows in total")

# COMMAND ----------

ingest("trips", decode_trip_updates, TRIP_SCHEMA, "bronze_trip_updates")
ingest("vehicles", decode_vehicles, VEHICLE_SCHEMA, "bronze_vehicle_positions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick look

# COMMAND ----------

display(spark.sql(f"""
    SELECT 'trip updates' AS feed, COUNT(DISTINCT source_file) AS snapshots, COUNT(*) AS rows,
           MIN(snapshot_ts) AS first_snapshot, MAX(snapshot_ts) AS last_snapshot,
           COUNT(DISTINCT trip_id) AS trips, ROUND(AVG(CAST(start_date IS NOT NULL AS INT)) * 100, 1) AS pct_with_start_date
    FROM {T_('bronze_trip_updates')}
    UNION ALL
    SELECT 'vehicle positions', COUNT(DISTINCT source_file), COUNT(*), MIN(snapshot_ts), MAX(snapshot_ts),
           COUNT(DISTINCT trip_id), NULL
    FROM {T_('bronze_vehicle_positions')}
"""))
