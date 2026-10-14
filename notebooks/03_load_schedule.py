# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Load schedule (GTFS)
# MAGIC Loads every downloaded timetable version into Delta tables. Each row carries `feed_version` (the zip file it
# MAGIC came from) and `valid_from` (the day it was downloaded), so each day's buses can be compared with the
# MAGIC timetable that was in force that day. Versions already loaded are skipped.
# MAGIC
# MAGIC Stop times are kept as **seconds after the service day's midnight**, because GTFS times can run past 24:00
# MAGIC (a trip at `25:10:00` leaves at 1:10 AM the next calendar day but belongs to the previous service day).

# COMMAND ----------

import os
import zipfile

import pandas as pd
from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "mcts"
T_ = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
SCHED_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw/schedule"

FILES = {
    "routes": ["route_id", "route_short_name", "route_long_name", "route_type"],
    "stops": ["stop_id", "stop_name", "stop_lat", "stop_lon"],
    "trips": ["trip_id", "route_id", "service_id", "direction_id", "trip_headsign", "shape_id"],
    "stop_times": ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence", "timepoint"],
    "calendar": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday",
                 "start_date", "end_date"],
    "calendar_dates": ["service_id", "date", "exception_type"],
}


def to_seconds(hms):
    """'25:10:00' -> 90600. Blank -> None."""
    if not isinstance(hms, str) or ":" not in hms:
        return None
    h, m, s = (int(x) for x in hms.strip().split(":"))
    return h * 3600 + m * 60 + s


def loaded_versions():
    if not spark.catalog.tableExists(T_("schedule_trips")):
        return set()
    return {r[0] for r in spark.table(T_("schedule_trips")).select("feed_version").distinct().collect()}

# COMMAND ----------

already = loaded_versions()
versions = sorted(f for f in os.listdir(SCHED_DIR) if f.endswith(".zip"))
print("Versions on disk:", versions)
print("Already loaded:  ", sorted(already))

for zip_name in versions:
    if zip_name in already:
        continue
    valid_from = pd.to_datetime(zip_name.split("_")[1], format="%Y%m%d").date()   # gtfs_YYYYMMDD_<hash>.zip
    with zipfile.ZipFile(f"{SCHED_DIR}/{zip_name}") as z:
        for name, cols in FILES.items():
            if f"{name}.txt" not in z.namelist():
                print(f"  {zip_name}: no {name}.txt, skipped")
                continue
            pdf = pd.read_csv(z.open(f"{name}.txt"), dtype=str)
            pdf = pdf[[c for c in cols if c in pdf.columns]].copy()
            pdf = pdf.astype(object).where(pdf.notna(), None)      # blanks become NULL, not NaN
            if name == "stop_times":
                pdf["arrival_s"] = pdf["arrival_time"].map(to_seconds).astype("Int64")
                pdf["departure_s"] = pdf["departure_time"].map(to_seconds).astype("Int64")
                pdf["stop_sequence"] = pd.to_numeric(pdf["stop_sequence"]).astype("Int64")
            if name == "stops":
                pdf["stop_lat"] = pd.to_numeric(pdf["stop_lat"])
                pdf["stop_lon"] = pd.to_numeric(pdf["stop_lon"])
            pdf["feed_version"] = zip_name
            pdf["valid_from"] = valid_from
            (spark.createDataFrame(pdf).write.mode("append").option("mergeSchema", "true")
             .saveAsTable(T_(f"schedule_{name}")))
            print(f"  {zip_name}: schedule_{name} +{len(pdf):,} rows")

# COMMAND ----------

display(spark.sql(f"""
    WITH t AS (SELECT feed_version, valid_from, COUNT(*) AS trips FROM {T_('schedule_trips')} GROUP BY ALL),
         r AS (SELECT feed_version, COUNT(*) AS routes FROM {T_('schedule_routes')} GROUP BY ALL),
         s AS (SELECT feed_version, COUNT(*) AS stop_times FROM {T_('schedule_stop_times')} GROUP BY ALL)
    SELECT t.feed_version, t.valid_from, r.routes, t.trips, s.stop_times
    FROM t LEFT JOIN r USING (feed_version) LEFT JOIN s USING (feed_version)
    ORDER BY t.valid_from
"""))