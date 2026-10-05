# ============================================================
# A3 - SPARK DATA ENGINEERING PIPELINE
# ============================================================

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, udf
from pyspark.sql.types import DoubleType


# ============================================================
# PATHS
# ============================================================

DATA_DIR = "/home/jagdheesh_ubuntu/assignment-3/data"

TRIP_DATA_PATH = f"{DATA_DIR}/yellow_tripdata_*.parquet"

ZONE_LOOKUP_PATH = f"{DATA_DIR}/taxi_zone_lookup.csv"

OUTPUT_PATH = (
    "/home/jagdheesh_ubuntu/assignment-3/output/"
    "spark_cleaned_data"
)


# ============================================================
# SPARK CONFIGURATION
# ============================================================

spark = (
    SparkSession.builder
    .master("spark://10.45.12.62:7077")
    .appName("A3_Spark_Taxi_Cleaning")

    # --------------------------------------------------------
    # DRIVER NETWORKING
    # --------------------------------------------------------
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.driver.host", "10.45.12.62")
    .config("spark.ui.host", "10.45.12.62")
    .config("spark.ui.port", "4040")

    # --------------------------------------------------------
    # CLUSTER RESOURCES
    #
    # 2 workers
    # 1 executor core per worker
    # 2 total concurrent executor tasks
    # --------------------------------------------------------
    .config("spark.executor.instances", "2")
    .config("spark.executor.cores", "1")
    .config("spark.cores.max", "2")

    # --------------------------------------------------------
    # MEMORY
    # --------------------------------------------------------
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "2g")

    # --------------------------------------------------------
    # SHUFFLE
    # --------------------------------------------------------
    .config("spark.sql.shuffle.partitions", "32")

    # --------------------------------------------------------
    # Python UDF diagnostics
    # --------------------------------------------------------
    .config(
        "spark.sql.execution.pyspark.udf.faulthandler.enabled",
        "true"
    )
    .config(
        "spark.python.worker.faulthandler.enabled",
        "true"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# PIPELINE HEADER
# ============================================================

print()
print("=" * 60)
print("        A3 SPARK DATA ENGINEERING PIPELINE")
print("=" * 60)
print()


# ============================================================
# BENCHMARK START
# ============================================================

start_time = time.time()


# ============================================================
# 1. INGESTION
# ============================================================

print("STEP 1: INGESTION")
print("-" * 60)

trip_data = spark.read.parquet(TRIP_DATA_PATH)

zone_lookup = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(ZONE_LOOKUP_PATH)
)

print("Trip data columns:", len(trip_data.columns))
print("Zone lookup columns:", len(zone_lookup.columns))


# ============================================================
# 2. DATA CLEANSING
# ============================================================

print()
print("STEP 2: DATA CLEANSING")
print("-" * 60)

clean_trips = (
    trip_data
    .dropna()
    .dropDuplicates()
)

clean_trips = (
    clean_trips
    .withColumn(
        "pickup_time",
        col("tpep_pickup_datetime").cast("timestamp")
    )
    .withColumn(
        "dropoff_time",
        col("tpep_dropoff_datetime").cast("timestamp")
    )
)

print("Null removal applied.")
print("Duplicate removal applied.")
print("Timestamp conversion applied.")


# ============================================================
# 3. PICKUP LOCATION JOIN
# ============================================================

print()
print("STEP 3: PICKUP LOCATION JOIN")
print("-" * 60)

joined_data = clean_trips.join(
    broadcast(zone_lookup),
    clean_trips.PULocationID == zone_lookup.LocationID,
    "inner"
)

print("Pickup location broadcast inner join completed.")


# ============================================================
# 4. CUSTOM PYTHON UDF
# ============================================================

print()
print("STEP 4: CUSTOM PYTHON UDF")
print("-" * 60)


def calculate_avg_speed(
    distance,
    pickup_time,
    dropoff_time
):
    """
    Calculate average speed in miles per hour.
    """

    if (
        distance is None
        or pickup_time is None
        or dropoff_time is None
    ):
        return 0.0

    try:
        duration_seconds = (
            dropoff_time.timestamp()
            - pickup_time.timestamp()
        )

        if duration_seconds <= 0:
            return 0.0

        duration_hours = duration_seconds / 3600.0

        return float(distance / duration_hours)

    except Exception:
        return 0.0


avg_speed_udf = udf(
    calculate_avg_speed,
    DoubleType()
)


# ============================================================
# 5. UDF EXECUTION
# ============================================================

print()
print("STEP 5: UDF EXECUTION")
print("-" * 60)

udf_start = time.time()

final_data = joined_data.withColumn(
    "avg_speed_mph",
    avg_speed_udf(
        col("trip_distance"),
        col("pickup_time"),
        col("dropoff_time")
    )
)


# ------------------------------------------------------------
# Force UDF evaluation without a global aggregation
# ------------------------------------------------------------

def consume_partition(rows):
    for _ in rows:
        pass


final_data.select(
    "avg_speed_mph"
).foreachPartition(
    consume_partition
)

udf_time = time.time() - udf_start

print(
    f"Python UDF execution time: "
    f"{udf_time:.2f} seconds"
)


# ============================================================
# 6. EXPORT
# ============================================================

print()
print("STEP 6: EXPORT")
print("-" * 60)

final_data.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print("Output written to:")
print(OUTPUT_PATH)


# ============================================================
# BENCHMARK RESULTS
# ============================================================

total_time = time.time() - start_time

print()
print("=" * 60)
print("        SPARK BENCHMARK RESULTS")
print("=" * 60)

print(
    f"Total Execution Time : "
    f"{total_time:.2f} seconds"
)

print(
    f"Python UDF Stage Time: "
    f"{udf_time:.2f} seconds"
)

print("=" * 60)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()

print()
print("=" * 60)
print("        SPARK PIPELINE COMPLETED")
print("=" * 60)
print()