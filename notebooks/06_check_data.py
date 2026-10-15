# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Check data
# MAGIC Runs after the marts are built and **fails the job** if a check marked `fail` does not pass, so a broken
# MAGIC collector or a bad timetable match is noticed the same day rather than weeks later. Checks marked `warn`
# MAGIC are reported but do not fail the run (for example a day with thin coverage because GitHub skipped runs).

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "mcts"
T_ = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
results = []


def check(name, ok, detail, severity="fail"):
    results.append((name, "pass" if ok else severity, detail))


def one(sql):
    return spark.sql(sql).first()

# COMMAND ----------

# 1. The collector is still delivering: newest snapshot within the last 6 hours
r = one(f"""SELECT (UNIX_TIMESTAMP() - MAX(snapshot_epoch)) / 3600 AS hours_old
            FROM {T_('raw_trip_updates')}""")
check("Fresh snapshots", r.hours_old is not None and r.hours_old <= 6,
      f"newest snapshot is {r.hours_old:.1f} hours old" if r.hours_old is not None else "no snapshots")

# 2. Almost every predicted trip-stop finds its timetable entry
r = one(f"""
    WITH pred AS (SELECT COUNT(DISTINCT trip_id, stop_sequence, stop_id,
                         DATE_FORMAT(FROM_UTC_TIMESTAMP(TIMESTAMP_SECONDS(arrival_epoch - 4 * 3600), 'America/Chicago'),
                                     'yyyyMMdd')) AS n
                  FROM {T_('raw_trip_updates')}
                  WHERE trip_id IS NOT NULL AND arrival_epoch IS NOT NULL)
    SELECT (SELECT COUNT(*) FROM {T_('stop_arrivals')}) AS matched, n AS predicted FROM pred""")
pct = 100 * r.matched / max(r.predicted, 1)
check("Timetable match", pct >= 95, f"{pct:.1f}% of predicted trip-stops matched ({r.matched:,} of {r.predicted:,})")

# 3. One arrival per trip, stop and service day
r = one(f"""SELECT COUNT(*) AS dupes FROM (
              SELECT service_date, trip_id, stop_sequence, stop_id FROM {T_('stop_arrivals')}
              GROUP BY ALL HAVING COUNT(*) > 1)""")
check("No duplicate arrivals", r.dupes == 0, f"{r.dupes:,} duplicated trip-stops")

# 4. Delays look physical: few reliable arrivals more than 30 minutes off the timetable
r = one(f"""SELECT AVG(CAST(ABS(delay_s) > 1800 AS INT)) * 100 AS pct_extreme
            FROM {T_('stop_arrivals')} WHERE is_reliable""")
check("Delays in a sensible range", (r.pct_extreme or 0) <= 2,
      f"{r.pct_extreme or 0:.2f}% of reliable arrivals are over 30 min early or late")

# 5. Overall on-time share is plausible (a timezone or matching bug pushes it towards 0% or 100%)
r = one(f"""SELECT 100 * AVG(CAST(is_on_time AS INT)) AS pct FROM {T_('stop_arrivals')} WHERE is_reliable""")
check("On-time share plausible", r.pct is not None and 30 <= r.pct <= 97,
      f"{r.pct:.1f}% on time" if r.pct is not None else "no reliable arrivals")

# 6. Marts add up to the arrivals they summarise
r = one(f"""SELECT (SELECT SUM(arrivals) FROM {T_('mart_route_daily')}) AS mart,
                   (SELECT COUNT(*) FROM {T_('stop_arrivals')} WHERE is_reliable) AS source""")
check("Marts reconcile", r.mart == r.source, f"mart_route_daily {r.mart:,} vs stop_arrivals {r.source:,}")

# 7. Every route in the marts has a name
r = one(f"""SELECT COUNT(DISTINCT d.route_id) AS missing FROM {T_('mart_route_daily')} d
            LEFT ANTI JOIN {T_('dim_route')} r USING (route_id)""")
check("Routes have names", r.missing == 0, f"{r.missing} route(s) missing from dim_route")

# 8. Coverage of the most recent complete service day (warning only)
r = one(f"""SELECT service_date, pct_of_day_captured FROM {T_('mart_collection_coverage')}
            WHERE service_date < CURRENT_DATE() - INTERVAL 1 DAY ORDER BY service_date DESC LIMIT 1""")
if r is None:
    check("Coverage of last full day", False, "no complete service day yet", "warn")
else:
    check("Coverage of last full day", r.pct_of_day_captured >= 80,
          f"{r.service_date}: {r.pct_of_day_captured}% of minutes captured", "warn")

# 9. The timetable is not stale (MCTS publishes a new one a few times a year; warning only)
r = one(f"""SELECT DATEDIFF(CURRENT_DATE(), MAX(valid_from)) AS days FROM {T_('schedule_trips')}""")
check("Timetable recent", r.days <= 120, f"newest timetable version is {r.days} days old", "warn")

# COMMAND ----------

summary = spark.createDataFrame(results, "check STRING, result STRING, detail STRING")
display(summary)

failed = [n for n, res, _ in results if res == "fail"]
if failed:
    raise Exception(f"{len(failed)} check(s) failed: {', '.join(failed)}")
print(f"All {len(results)} checks passed" + (" (with warnings)" if any(r == "warn" for _, r, _ in results) else ""))
