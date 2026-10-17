# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Build reporting marts
# MAGIC Summary tables for reporting, built from `stop_arrivals`. Only **reliable** arrivals (final, last
# MAGIC prediction made at most 2 minutes ahead) count towards on-time figures. Each table is rebuilt in full on every
# MAGIC run, which takes seconds at this data size and means a fix upstream flows through without extra steps.
# MAGIC
# MAGIC | Table | One row per | Used for |
# MAGIC |---|---|---|
# MAGIC | `dim_route` | route | route names |
# MAGIC | `dim_stop` | stop | stop names and map positions |
# MAGIC | `mart_route_daily` | service day × route | trends, route ranking |
# MAGIC | `mart_route_hourly` | route × day type × scheduled hour | when a route runs late |
# MAGIC | `mart_stop_delays` | stop × route | where delays build up |
# MAGIC | `mart_collection_coverage` | service day | how much of the day the collector captured |
# MAGIC
# MAGIC **Day type**: Weekday, Saturday or Sunday, since MCTS runs a different timetable for each.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "mcts"
T_ = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
LATE_S = 300   # on time = 1 min early to 5 min late (same as notebook 04)

def build(name, sql):
    spark.sql(f"CREATE OR REPLACE TABLE {T_(name)} AS {sql}")
    print(f"{name}: {spark.table(T_(name)).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions (from the newest timetable version)

# COMMAND ----------

build("dim_route", f"""
    SELECT route_id, route_short_name, route_long_name,
           CONCAT_WS(' ', route_short_name, route_long_name) AS route_label
    FROM {T_('schedule_routes')}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY valid_from DESC) = 1
""")

build("dim_stop", f"""
    SELECT stop_id, stop_name, stop_lat AS latitude, stop_lon AS longitude
    FROM {T_('schedule_stops')}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stop_id ORDER BY valid_from DESC) = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

ARR = f"""(
    SELECT *,
           CASE DAYOFWEEK(service_date) WHEN 1 THEN 'Sunday' WHEN 7 THEN 'Saturday' ELSE 'Weekday' END AS day_type
    FROM {T_('stop_arrivals')} WHERE is_reliable
)"""

MEASURES = f"""
    COUNT(*)                                                       AS arrivals,
    SUM(CAST(is_on_time AS INT))                                   AS on_time,
    SUM(CAST(delay_s < -60 AS INT))                                AS early,
    SUM(CAST(delay_s > {LATE_S} AS INT))                           AS late,
    ROUND(100 * AVG(CAST(is_on_time AS INT)), 1)                   AS on_time_pct,
    ROUND(AVG(delay_s) / 60, 2)                                    AS avg_delay_min,
    ROUND(PERCENTILE(delay_s, 0.5) / 60, 2)                        AS median_delay_min,
    ROUND(PERCENTILE(delay_s, 0.9) / 60, 2)                        AS p90_delay_min
"""

build("mart_route_daily", f"""
    SELECT service_date, day_type, route_id, COUNT(DISTINCT trip_id) AS trips, {MEASURES}
    FROM {ARR} GROUP BY service_date, day_type, route_id
""")

build("mart_route_hourly", f"""
    SELECT route_id, day_type, scheduled_hour_local, COUNT(DISTINCT service_date) AS days, {MEASURES}
    FROM {ARR} GROUP BY route_id, day_type, scheduled_hour_local
""")

build("mart_stop_delays", f"""
    SELECT stop_id, route_id, COUNT(DISTINCT service_date) AS days, {MEASURES}
    FROM {ARR} GROUP BY stop_id, route_id
""")

# Coverage: how much of each service day the collector saw, so thin days can be filtered or flagged in the report.
build("mart_collection_coverage", f"""
    WITH snaps AS (
        SELECT TO_DATE(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(snapshot_epoch - 4 * 3600), 'America/Chicago')) AS service_date,
               COUNT(DISTINCT snapshot_epoch) AS snapshots
        FROM (SELECT DISTINCT snapshot_epoch FROM {T_('raw_trip_updates')})
        GROUP BY ALL
    ),
    arr AS (
        SELECT service_date, COUNT(*) AS trip_stops, SUM(CAST(is_reliable AS INT)) AS reliable
        FROM {T_('stop_arrivals')} GROUP BY service_date
    )
    SELECT COALESCE(s.service_date, a.service_date) AS service_date,
           COALESCE(s.snapshots, 0) AS snapshots,
           ROUND(100 * COALESCE(s.snapshots, 0) / 1440, 1) AS pct_of_day_captured,
           a.trip_stops, a.reliable,
           ROUND(100 * a.reliable / NULLIF(a.trip_stops, 0), 1) AS pct_reliable
    FROM snaps s FULL OUTER JOIN arr a ON s.service_date = a.service_date
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## First look

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {T_('mart_collection_coverage')} ORDER BY service_date"))

# COMMAND ----------

display(spark.sql(f"""
    SELECT r.route_label, SUM(d.arrivals) AS arrivals,
           ROUND(100 * SUM(d.on_time) / SUM(d.arrivals), 1) AS on_time_pct,
           ROUND(100 * SUM(d.late) / SUM(d.arrivals), 1)    AS late_pct,
           ROUND(100 * SUM(d.early) / SUM(d.arrivals), 1)   AS early_pct
    FROM {T_('mart_route_daily')} d LEFT JOIN {T_('dim_route')} r USING (route_id)
    GROUP BY r.route_label HAVING SUM(d.arrivals) >= 50
    ORDER BY on_time_pct
"""))
# COMMAND ----------
# compute: serverless (CPU)