#!/usr/bin/env python

"""
Homework 6 — Yellow Taxi November 2025
Batch processing solution using PySpark.
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

# ── Constants ─────────────────────────────────────────────────
BASE_DIR        = Path(__file__).resolve().parent.parent
DATA_DIR        = BASE_DIR / "data"
RAW_DIR         = DATA_DIR / "raw"
PQ_DIR          = DATA_DIR / "pq" / "yellow" / "2025" / "11"
INPUT_PATH      = RAW_DIR / "yellow_tripdata_2025-11.parquet"
ZONE_LOOKUP_PATH = BASE_DIR / "taxi_zone_lookup.csv"


def init_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(app_name) \
        .getOrCreate()
    print(f"[*] Spark version: {spark.version}")
    return spark


def load_trips(spark: SparkSession) -> pyspark.sql.DataFrame:
    print(f"[*] Loading trips from {INPUT_PATH}...")
    df = spark.read.parquet(str(INPUT_PATH))
    print(f"[✓] Total records (Nov 2025): {df.count():,}")
    return df


def repartition_and_write(df: pyspark.sql.DataFrame) -> None:
    print(f"[*] Repartitioning into 4 files → {PQ_DIR}")
    df.repartition(4).write.parquet(str(PQ_DIR), mode="overwrite")
    print(f"[✓] Written to {PQ_DIR}")


def load_zones(spark: SparkSession) -> pyspark.sql.DataFrame:
    print(f"[*] Loading zone lookup from {ZONE_LOOKUP_PATH}...")
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(str(ZONE_LOOKUP_PATH))
    print(f"[✓] Zones loaded: {df.count()} records")
    return df


def run_queries(spark: SparkSession) -> None:
    # Q3: Total trips on November 15
    print("\n[Q3] Total Trips on November 15, 2025:")
    spark.sql("""
        SELECT COUNT(*) AS trip_count
        FROM yellow_tripdata
        WHERE to_date(tpep_pickup_datetime) = '2025-11-15'
    """).show()

    # Q4: Longest trip duration
    print("\n[Q4] Longest Trip Duration (hours):")
    spark.sql("""
        SELECT ROUND(
            MAX(unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600
        , 2) AS max_duration_hours
        FROM yellow_tripdata
    """).show()

    # Q6: Least frequent pickup zone
    print("\n[Q6] Least Frequent Pickup Zones:")
    spark.sql("""
        SELECT z.Zone, COUNT(*) AS trip_count
        FROM yellow_tripdata t
        JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY z.Zone
        ORDER BY trip_count ASC
        LIMIT 5
    """).show(truncate=False)


def main():
    spark = init_spark("yellow_tripdata_2025-11")

    trips_df = load_trips(spark)
    repartition_and_write(trips_df)

    trips_df.createOrReplaceTempView("yellow_tripdata")
    print("[✓] Temp view 'yellow_tripdata' registered")

    zones_df = load_zones(spark)
    zones_df.createOrReplaceTempView("zones")
    print("[✓] Temp view 'zones' registered")

    run_queries(spark)

    spark.stop()
    print("\n[*] Spark session stopped.")


if __name__ == "__main__":
    main()