#!/usr/bin/env python3

"""
A3 - Ray Data Engineering Pipeline
NYC Taxi Data Cleaning and Feature Engineering

Ray cluster:
    Head    : 10.45.12.62:6380
    Worker1 : 2 CPUs
    Worker2 : 2 CPUs
    Total   : 4 CPUs

Dataset:
    /home/jagdheesh_ubuntu/assignment-3/data

Output:
    /home/jagdheesh_ubuntu/assignment-3/output/ray_cleaned_data
"""


# ============================================================
# IMPORTS
# ============================================================

import glob
import os
import time

import pandas as pd
import pyarrow.parquet as pq
import ray


# ============================================================
# PATHS
# ============================================================

DATA_DIR = "/home/jagdheesh_ubuntu/assignment-3/data"

OUTPUT_DIR = "/home/jagdheesh_ubuntu/assignment-3/output"

TRIP_DATA_PATTERN = (
    "/home/jagdheesh_ubuntu/assignment-3/data/"
    "yellow_tripdata_*.parquet"
)

ZONE_LOOKUP_PATH = (
    "/home/jagdheesh_ubuntu/assignment-3/data/"
    "taxi_zone_lookup.csv"
)

RAY_OUTPUT_PATH = (
    "/home/jagdheesh_ubuntu/assignment-3/output/"
    "ray_cleaned_data"
)


# ============================================================
# CREATE OUTPUT DIRECTORY
# ============================================================

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# FIND INPUT FILES
# ============================================================

trip_data_paths = sorted(
    glob.glob(TRIP_DATA_PATTERN)
)

if not trip_data_paths:
    raise FileNotFoundError(
        f"No taxi Parquet files found matching:\n"
        f"{TRIP_DATA_PATTERN}"
    )


# ============================================================
# HEADER
# ============================================================

print()
print("=" * 70)
print("             A3 RAY DATA ENGINEERING PIPELINE")
print("=" * 70)
print()

print(
    f"Found {len(trip_data_paths)} taxi Parquet files."
)

for path in trip_data_paths:
    print("  ", os.path.basename(path))


# ============================================================
# TOTAL BENCHMARK START
# ============================================================

start_time = time.time()


# ============================================================
# STEP 1: CONNECT TO RAY CLUSTER
# ============================================================

print()
print("=" * 70)
print("STEP 1: CONNECTING TO RAY CLUSTER")
print("=" * 70)

ray.init(address="auto")

print()
print("Connected to Ray cluster.")


# ============================================================
# STEP 2: VERIFY RAY RESOURCES
# ============================================================

print()
print("=" * 70)
print("STEP 2: RAY CLUSTER RESOURCES")
print("=" * 70)

resources = ray.cluster_resources()

for key, value in sorted(resources.items()):
    print(f"  {key}: {value}")


# ============================================================
# RAY DATA CONFIGURATION
# ============================================================

data_context = ray.data.DataContext.get_current()

# Allow disk-based hash shuffle if Ray requires it.
data_context.use_disk_based_hash_shuffle = True


# ============================================================
# STEP 3: READ TAXI DATA DIRECTLY
# ============================================================

print()
print("=" * 70)
print("STEP 3: READING TAXI DATA")
print("=" * 70)

print()
print("Reading original Parquet files directly.")
print("No intermediate ray_deduped_data directory will be created.")

trip_ds = ray.data.read_parquet(
    trip_data_paths
)

print()
print("Taxi data loaded into Ray Data.")


# ============================================================
# STEP 4: READ TAXI ZONE LOOKUP
# ============================================================

print()
print("=" * 70)
print("STEP 4: READING TAXI ZONE LOOKUP")
print("=" * 70)

# The lookup table is tiny.
# Read it directly using pandas instead of Ray Data.
zone_lookup_pd = pd.read_csv(
    ZONE_LOOKUP_PATH
)

print(
    f"Taxi zone lookup rows: "
    f"{len(zone_lookup_pd):,}"
)


# ============================================================
# STEP 5: NORMALIZE SCHEMA
# ============================================================

print()
print("=" * 70)
print("STEP 5: NORMALIZING SCHEMA")
print("=" * 70)

# Use the first input file as the canonical schema.
canonical_schema = pq.read_schema(
    trip_data_paths[0]
)

canonical_columns = canonical_schema.names

print(
    f"Canonical schema contains "
    f"{len(canonical_columns)} columns."
)


def normalize_schema(
    batch: pd.DataFrame
) -> pd.DataFrame:

    """
    Ensure every Ray batch follows the
    canonical column order.
    """

    batch = batch.copy()

    for column in canonical_columns:

        if column not in batch.columns:
            batch[column] = pd.NA

    return batch[canonical_columns]


trip_ds = trip_ds.map_batches(
    normalize_schema,
    batch_format="pandas",
    batch_size=None
)

print(
    "Schema normalization transformation added."
)


# ============================================================
# STEP 6: REMOVE DUPLICATE ROWS
# ============================================================

print()
print("=" * 70)
print("STEP 6: REMOVING DUPLICATE ROWS")
print("=" * 70)

print(
    "Using batch-level exact duplicate removal "
    "to avoid a large global shuffle."
)


def remove_duplicates(
    batch: pd.DataFrame
) -> pd.DataFrame:

    """
    Remove exact duplicate rows within each
    Ray Data batch.

    This avoids creating a second full copy
    of the dataset on disk.
    """

    return batch.drop_duplicates()


clean_trips = trip_ds.map_batches(
    remove_duplicates,
    batch_format="pandas",
    batch_size=None
)

print(
    "Duplicate-removal transformation added."
)


# ============================================================
# STEP 7: REMOVE NULL ROWS
# ============================================================

print()
print("=" * 70)
print("STEP 7: REMOVING NULL ROWS")
print("=" * 70)


def remove_null_rows(
    batch: pd.DataFrame
) -> pd.DataFrame:

    """
    Equivalent to Spark's complete-row dropna().
    """

    return batch.dropna()


clean_trips = clean_trips.map_batches(
    remove_null_rows,
    batch_format="pandas",
    batch_size=None
)

print(
    "Null-removal transformation added."
)


# ============================================================
# STEP 8: CONVERT TIMESTAMPS
# ============================================================

print()
print("=" * 70)
print("STEP 8: CONVERTING TIMESTAMPS")
print("=" * 70)


def format_timestamps(
    batch: pd.DataFrame
) -> pd.DataFrame:

    batch = batch.copy()

    batch["tpep_pickup_datetime"] = pd.to_datetime(
        batch["tpep_pickup_datetime"],
        errors="coerce"
    )

    batch["tpep_dropoff_datetime"] = pd.to_datetime(
        batch["tpep_dropoff_datetime"],
        errors="coerce"
    )

    return batch


clean_trips = clean_trips.map_batches(
    format_timestamps,
    batch_format="pandas",
    batch_size=None
)

print(
    "Timestamp conversion transformation added."
)


# ============================================================
# STEP 9: BROADCAST-STYLE LOCATION JOIN
# ============================================================

print()
print("=" * 70)
print("STEP 9: PICKUP LOCATION JOIN")
print("=" * 70)

print(
    "Using the small taxi-zone lookup as a "
    "broadcast-style pandas table."
)


def broadcast_join_batch(
    batch: pd.DataFrame,
    zone_lookup: pd.DataFrame
) -> pd.DataFrame:

    batch = batch.merge(
        zone_lookup,
        how="inner",
        left_on="PULocationID",
        right_on="LocationID"
    )

    return batch


joined_data = clean_trips.map_batches(
    broadcast_join_batch,
    batch_format="pandas",
    batch_size=None,
    fn_kwargs={
        "zone_lookup": zone_lookup_pd
    }
)

print(
    "Broadcast-style pickup location "
    "join transformation added."
)


# ============================================================
# STEP 10: CUSTOM PYTHON AVERAGE-SPEED TRANSFORMATION
# ============================================================

print()
print("=" * 70)
print("STEP 10: CUSTOM PYTHON UDF")
print("=" * 70)


def calculate_avg_speed(
    batch: pd.DataFrame
) -> pd.DataFrame:

    """
    Calculate average speed in miles per hour.

    Formula:

        speed = trip_distance / duration_hours

    Invalid or non-positive durations
    receive 0.0.
    """

    batch = batch.copy()

    duration_hours = (
        (
            batch["tpep_dropoff_datetime"]
            - batch["tpep_pickup_datetime"]
        ).dt.total_seconds()
        / 3600.0
    )

    # Default value.
    batch["avg_speed_mph"] = 0.0

    valid_duration = duration_hours > 0

    batch.loc[
        valid_duration,
        "avg_speed_mph"
    ] = (
        batch.loc[
            valid_duration,
            "trip_distance"
        ]
        / duration_hours.loc[
            valid_duration
        ]
    )

    return batch


# ============================================================
# STEP 11: FORCE UDF EXECUTION + TIMING
# ============================================================

print()
print("=" * 70)
print("STEP 11: EXECUTING PYTHON UDF")
print("=" * 70)

udf_start_time = time.time()

final_data = joined_data.map_batches(
    calculate_avg_speed,
    batch_format="pandas",
    batch_size=None
)

# Ray Data transformations are lazy.
# This action forces the complete upstream
# pipeline, including the Python transformation.
max_speed = final_data.max(
    "avg_speed_mph"
)

udf_time = time.time() - udf_start_time

print()

print(
    f"Python transformation/UDF stage: "
    f"{udf_time:.2f} seconds"
)

print(
    f"Maximum average speed: "
    f"{max_speed}"
)


# ============================================================
# STEP 12: WRITE FINAL PARQUET
# ============================================================

print()
print("=" * 70)
print("STEP 12: WRITING FINAL PARQUET")
print("=" * 70)

final_data.write_parquet(
    RAY_OUTPUT_PATH,
    mode="overwrite"
)

print()

print(
    f"Ray output written to:\n"
    f"{RAY_OUTPUT_PATH}"
)


# ============================================================
# STEP 13: TOTAL RUNTIME
# ============================================================

total_time = time.time() - start_time


# ============================================================
# FINAL RESULTS
# ============================================================

print()
print("=" * 70)
print("              RAY PIPELINE COMPLETED")
print("=" * 70)
print()

print(
    f"Input files              : "
    f"{len(trip_data_paths)}"
)

print(
    f"Python UDF stage         : "
    f"{udf_time:.2f} seconds"
)

print(
    f"Total Ray Data runtime   : "
    f"{total_time:.2f} seconds"
)

print(
    f"Maximum average speed    : "
    f"{max_speed}"
)

print(
    f"Output path              : "
    f"{RAY_OUTPUT_PATH}"
)

print()
print("=" * 70)


# ============================================================
# SHUTDOWN
# ============================================================

ray.shutdown()