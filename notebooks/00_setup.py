# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup
# MAGIC Creates the schema and volumes for the MCTS bus reliability project, and downloads the current GTFS
# MAGIC schedule if it has changed. Runs as the first task of the daily job (MCTS changes schedules a few
# MAGIC times a year, and every version is kept so each day's buses are compared against the right timetable).

# COMMAND ----------

import hashlib
import os
from datetime import datetime, timezone

import requests

CATALOG, SCHEMA = "workspace", "mcts"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
for v in ["raw", "checkpoints"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{v}")
RAW = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
for d in ["realtime", "schedule"]:
    os.makedirs(f"{RAW}/{d}", exist_ok=True)
print("Ready:", RAW)

# COMMAND ----------

try:
    r = requests.get("https://kamino.mcts.org/gtfs/google_transit.zip", timeout=120)
    r.raise_for_status()
except requests.RequestException as e:
    # A failed download must not stop the daily job: the timetable already stored stays in use.
    if not any(f.endswith(".zip") for f in os.listdir(f"{RAW}/schedule")):
        raise
    dbutils.notebook.exit(f"SKIPPED: timetable download failed ({type(e).__name__}); using the stored version")
digest = hashlib.sha256(r.content).hexdigest()[:12]
existing = [f for f in os.listdir(f"{RAW}/schedule") if digest in f]
if existing:
    print("Schedule unchanged, already stored:", existing[0])
else:
    name = f"gtfs_{datetime.now(timezone.utc):%Y%m%d}_{digest}.zip"
    with open(f"{RAW}/schedule/{name}", "wb") as f:
        f.write(r.content)
    print(f"Saved new schedule version: {name} ({len(r.content) / 1e6:.1f} MB)")
print(sorted(os.listdir(f"{RAW}/schedule")))
