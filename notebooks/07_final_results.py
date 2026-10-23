# Databricks notebook source
# MAGIC %md
# MAGIC # Final results
# MAGIC Headline numbers for the README: data collected, timetable match, on-time performance,
# MAGIC and the best and worst routes. Read-only; it only queries the tables built by notebooks 02–05.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "mcts"
T_ = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collection and data volume

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        (SELECT MIN(service_date) FROM {T_('stop_arrivals')})                       AS first_service_day,
        (SELECT MAX(service_date) FROM {T_('stop_arrivals')})                       AS last_service_day,
        (SELECT COUNT(DISTINCT service_date) FROM {T_('stop_arrivals')})            AS service_days,
        (SELECT COUNT(DISTINCT snapshot_epoch) FROM {T_('raw_trip_updates')})       AS feed_snapshots,
        (SELECT COUNT(*) FROM {T_('raw_trip_updates')})                             AS raw_trip_update_rows,
        (SELECT COUNT(*) FROM {T_('raw_vehicle_positions')})                        AS raw_vehicle_position_rows,
        (SELECT COUNT(*) FROM {T_('stop_arrivals')})                                AS trip_stop_arrivals,
        (SELECT ROUND(AVG(pct_of_day_captured), 1) FROM {T_('mart_collection_coverage')}
            WHERE snapshots > 0)                                                    AS avg_pct_of_day_captured
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accuracy of the arrival estimates and overall punctuality

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*)                                                                       AS trip_stops,
        ROUND(100 * AVG(CAST(is_reliable AS INT)), 1)                                  AS pct_reliable,
        ROUND(PERCENTILE(lead_s, 0.5), 0)                                              AS median_lead_s,
        ROUND(100 * AVG(CASE WHEN is_reliable THEN CAST(is_on_time AS INT) END), 1)    AS on_time_pct,
        ROUND(100 * AVG(CASE WHEN is_reliable THEN CAST(delay_s > 300 AS INT) END), 1) AS late_pct,
        ROUND(100 * AVG(CASE WHEN is_reliable THEN CAST(delay_s < -60 AS INT) END), 1) AS early_pct,
        ROUND(AVG(CASE WHEN is_reliable THEN delay_s END) / 60, 2)                     AS avg_delay_min,
        ROUND(PERCENTILE(CASE WHEN is_reliable THEN delay_s END, 0.9) / 60, 2)         AS p90_delay_min
    FROM {T_('stop_arrivals')}
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weekday vs weekend

# COMMAND ----------

display(spark.sql(f"""
    SELECT day_type, SUM(arrivals) AS arrivals,
           ROUND(100 * SUM(on_time) / SUM(arrivals), 1) AS on_time_pct,
           ROUND(100 * SUM(late) / SUM(arrivals), 1)    AS late_pct
    FROM {T_('mart_route_daily')} GROUP BY day_type ORDER BY day_type DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Routes, best and worst (at least 500 arrivals)

# COMMAND ----------

ROUTES = f"""
    SELECT r.route_label, SUM(d.arrivals) AS arrivals,
           ROUND(100 * SUM(d.on_time) / SUM(d.arrivals), 1) AS on_time_pct,
           ROUND(100 * SUM(d.late) / SUM(d.arrivals), 1)    AS late_pct,
           ROUND(100 * SUM(d.early) / SUM(d.arrivals), 1)   AS early_pct
    FROM {T_('mart_route_daily')} d LEFT JOIN {T_('dim_route')} r USING (route_id)
    GROUP BY r.route_label HAVING SUM(d.arrivals) >= 500
"""
display(spark.sql(f"SELECT * FROM ({ROUTES}) ORDER BY on_time_pct DESC LIMIT 5"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM ({ROUTES}) ORDER BY on_time_pct ASC LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time of day (weekdays, all routes)

# COMMAND ----------

display(spark.sql(f"""
    SELECT scheduled_hour_local, SUM(arrivals) AS arrivals,
           ROUND(100 * SUM(on_time) / SUM(arrivals), 1) AS on_time_pct
    FROM {T_('mart_route_hourly')} WHERE day_type = 'Weekday'
    GROUP BY scheduled_hour_local HAVING SUM(arrivals) >= 200
    ORDER BY scheduled_hour_local
"""))