# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Build arrivals
# MAGIC Turns minute-by-minute predictions into one **estimated arrival per trip per stop**, then compares it with
# MAGIC the timetable.
# MAGIC
# MAGIC **Method.** MCTS's trip-updates feed predicts each upcoming stop, and a stop drops out of the feed once the
# MAGIC bus has passed it. So for every trip × stop, the prediction from the **last snapshot that still listed the
# MAGIC stop** is the best estimate of when the bus actually got there.
# MAGIC
# MAGIC **Quality flags.**
# MAGIC - `lead_s`: how far ahead that last prediction was. With 60-second polling it is normally under 2 minutes;
# MAGIC   a larger value means the collector had a gap (for example between hourly runs), so the arrival is flagged
# MAGIC   `is_reliable = false` and left out of on-time figures.
# MAGIC - `is_final`: a stop still listed in the newest snapshot has not been reached yet, so it is not final.
# MAGIC
# MAGIC **On time** = between 1 minute early and 5 minutes late (a common US transit standard).
# MAGIC
# MAGIC **Incremental.** Only the last `days_back` service days are rebuilt (default 3), since older days can no
# MAGIC longer change. Set `days_back` to 0 to rebuild everything.

# COMMAND ----------

from datetime import date, timedelta

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "mcts"
T_ = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
TZ = "America/Chicago"
MAX_LEAD_S = 120          # last prediction made at most 2 minutes before the arrival
EARLY_S, LATE_S = -60, 300

dbutils.widgets.text("days_back", "3")
DAYS_BACK = int(dbutils.widgets.get("days_back"))
FULL = DAYS_BACK == 0 or not spark.catalog.tableExists(T_("stop_arrivals"))
CUTOFF = None if FULL else date.today() - timedelta(days=DAYS_BACK)
print("Rebuilding:", "all service days" if FULL else f"service days from {CUTOFF}")

# MCTS leaves start_date blank, and the same trip_id runs every day, so each prediction gets a day key:
# start_date when present, otherwise the local date of the arrival minus 4 hours (a bus at 1:30 AM still
# counts as the previous day's service, since MCTS's service day ends in the early morning).
SERVICE_DAY_START_H = 4
tu = (spark.table(T_("raw_trip_updates")).where("trip_id IS NOT NULL AND arrival_epoch IS NOT NULL")
      .withColumn("day_key", F.coalesce(F.col("start_date"), F.date_format(F.from_utc_timestamp(
          F.timestamp_seconds(F.col("arrival_epoch") - SERVICE_DAY_START_H * 3600), TZ), "yyyyMMdd"))))
if not FULL:
    tu = tu.where(F.col("day_key") >= CUTOFF.strftime("%Y%m%d"))
newest_snapshot = tu.agg(F.max("snapshot_epoch")).first()[0]
print("Newest snapshot:", newest_snapshot and spark.sql(f"SELECT timestamp_seconds({newest_snapshot})").first()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Last prediction per trip × stop

# COMMAND ----------

stop_key = F.coalesce(F.col("stop_sequence").cast("string"), F.concat(F.lit("id:"), F.col("stop_id")))
w = Window.partitionBy("trip_id", "day_key", "stop_key").orderBy(F.col("snapshot_epoch").desc())

last_pred = (
    tu.withColumn("stop_key", stop_key)
    .withColumn("rn", F.row_number().over(w))
    .where("rn = 1")
    .select(
        "trip_id", "day_key", "route_id", "start_date", "vehicle_id", "stop_sequence", "stop_id",
        F.col("snapshot_epoch").alias("last_snapshot_epoch"),
        "arrival_epoch",
        (F.col("arrival_epoch") - F.col("snapshot_epoch")).alias("lead_s"),
        (F.col("snapshot_epoch") < F.lit(newest_snapshot) - 90).alias("is_final"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Match to the timetable
# MAGIC The service date comes from the feed's `start_date` when present. Otherwise the arrival's local date and the
# MAGIC day before are both tried, and the one giving the smaller gap to the timetable wins (this handles trips that
# MAGIC run past midnight). Each date uses the newest timetable version downloaded on or before it.

# COMMAND ----------

st = spark.table(T_("schedule_stop_times")).select("feed_version", "valid_from", "trip_id", "stop_id",
                                                     "stop_sequence", "arrival_s")
versions = spark.table(T_("schedule_trips")).select("feed_version", "valid_from").distinct()

arrival_local_date = F.to_date(F.from_utc_timestamp(F.timestamp_seconds("arrival_epoch"), TZ))
candidates = last_pred.withColumn(
    "service_date",
    F.explode(
        F.when(F.col("start_date").isNotNull(), F.array(F.to_date("start_date", "yyyyMMdd")))
        .otherwise(F.array(arrival_local_date, F.date_sub(arrival_local_date, 1)))
    ),
)

# timetable version in force on each service date: newest valid_from <= date (else the oldest version)
ver = (candidates.select("service_date").distinct().crossJoin(versions)
       .withColumn("ok", F.col("valid_from") <= F.col("service_date")))
w_ver = Window.partitionBy("service_date").orderBy(F.col("ok").desc(),
                                                   F.when(F.col("ok"), F.col("valid_from")).desc(),
                                                   F.col("valid_from").asc())
version_for_date = (ver.withColumn("rn", F.row_number().over(w_ver)).where("rn = 1")
                    .select("service_date", "feed_version"))

c = candidates.join(version_for_date, "service_date")
by_seq = c.where("stop_sequence IS NOT NULL").join(
    st.drop("valid_from").withColumnRenamed("stop_id", "sched_stop_id"),
    ["feed_version", "trip_id", "stop_sequence"])
by_id = c.where("stop_sequence IS NULL").drop("stop_sequence").join(
    st.drop("valid_from").withColumn("sched_stop_id", F.col("stop_id")),
    ["feed_version", "trip_id", "stop_id"])
matched = by_seq.unionByName(by_id, allowMissingColumns=True)

scheduled_epoch = F.unix_timestamp(F.to_utc_timestamp(
    F.expr("timestampadd(SECOND, arrival_s, to_timestamp(service_date))"), TZ))
matched = (matched.withColumn("scheduled_epoch", scheduled_epoch)
           .withColumn("delay_s", F.col("arrival_epoch") - F.col("scheduled_epoch")))

w_best = Window.partitionBy("trip_id", "day_key", "stop_sequence", "stop_id").orderBy(F.abs("delay_s"))
# A stop whose predicted time drifts across the 4 AM day-key boundary ends up in two day-key groups that both
# resolve to the same service day. Keep only the group with the most recent prediction for that visit.
w_visit = Window.partitionBy("service_date", "trip_id", "stop_sequence", "stop_id").orderBy(
    F.col("last_snapshot_epoch").desc())
arrivals = (
    matched.withColumn("rn", F.row_number().over(w_best)).where("rn = 1")
    .withColumn("rn", F.row_number().over(w_visit)).where("rn = 1")
    .select(
        "service_date", "feed_version", "trip_id", "route_id", "vehicle_id",
        "stop_sequence", F.coalesce("stop_id", "sched_stop_id").alias("stop_id"),
        F.timestamp_seconds("scheduled_epoch").alias("scheduled_arrival"),
        F.timestamp_seconds("arrival_epoch").alias("estimated_arrival"),
        F.timestamp_seconds("last_snapshot_epoch").alias("last_snapshot"),
        "lead_s", "delay_s", "is_final",
        ((F.col("lead_s") <= MAX_LEAD_S) & F.col("is_final") & (F.abs("delay_s") <= 7200)).alias("is_reliable"),
    )
    .withColumn("is_on_time", F.col("delay_s").between(EARLY_S, LATE_S))
    .withColumn("scheduled_hour_local", F.hour(F.from_utc_timestamp("scheduled_arrival", TZ)))
)

if FULL:
    (arrivals.write.mode("overwrite").option("overwriteSchema", "true")
     .partitionBy("service_date").saveAsTable(T_("stop_arrivals")))
else:
    (arrivals.where(F.col("service_date") >= F.lit(CUTOFF))
     .write.mode("overwrite").option("replaceWhere", f"service_date >= '{CUTOFF}'")
     .saveAsTable(T_("stop_arrivals")))
WINDOW = "TRUE" if FULL else f"service_date >= '{CUTOFF}'"
written = spark.table(T_("stop_arrivals")).where(WINDOW).count()
print(f"stop_arrivals: {written:,} rows rebuilt, {spark.table(T_('stop_arrivals')).count():,} in total")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Checks and a first look

# COMMAND ----------

display(spark.sql(f"""
    SELECT COUNT(*)                                              AS trip_stops,
           SUM(CAST(is_final AS INT))                            AS final,
           SUM(CAST(is_reliable AS INT))                         AS reliable,
           ROUND(100 * AVG(CASE WHEN is_reliable THEN CAST(is_on_time AS INT) END), 1) AS on_time_pct,
           ROUND(AVG(CASE WHEN is_reliable THEN delay_s END) / 60, 2)                  AS avg_delay_min,
           ROUND(PERCENTILE(CASE WHEN is_reliable THEN lead_s END, 0.5), 0)            AS median_lead_s
    FROM {T_('stop_arrivals')} WHERE {WINDOW}
"""))

predicted = last_pred.count()
matched_n = written
print(f"Trip-stops matched to the timetable: {matched_n:,} of {predicted:,} "
      f"({100 * matched_n / max(predicted, 1):.1f}%)")

display(spark.sql(f"""
    SELECT route_id, COUNT(*) AS arrivals,
           ROUND(100 * AVG(CAST(is_on_time AS INT)), 1) AS on_time_pct,
           ROUND(AVG(delay_s) / 60, 2) AS avg_delay_min
    FROM {T_('stop_arrivals')} WHERE is_reliable AND {WINDOW}
    GROUP BY route_id ORDER BY arrivals DESC LIMIT 15
"""))