# Assignment 3 — Spark vs Ray Data

## Overview

This project implements the same data preprocessing pipeline using:

- Apache Spark (PySpark)
- Ray Data

The objective is to compare both frameworks on the NYC Taxi dataset using the same preprocessing operations and a 2-worker cluster setup.

## Dataset

The dataset used is the NYC Taxi trip dataset along with the taxi zone lookup table.

The preprocessing pipeline includes:

1. Reading multiple input files.
2. Cleaning null and invalid values.
3. Removing duplicate records.
4. Converting timestamp columns.
5. Joining trip data with taxi zone information.
6. Computing derived features.
7. Computing average speed using a Python UDF.
8. Writing the cleaned data in Parquet format.

## Project Structure

```text
AI_ops_assignment_3/
│
├── spark_clean.py
├── ray_clean.py
│
├── spark_benchmark.jpeg
├── spark_evidence_at_4040.jpeg
├── ray_benchmark.jpeg
├── ray_evidence.jpeg
│
├── AI_ops_assignment_3.pdf
├── README.md
└── .gitignore
```

## Requirements

The experiments were performed in a Linux/Ubuntu environment with:

- Python
- Java
- Apache Spark
- PySpark
- Ray Data
- Conda

The exact environment used for the final experiments included Spark 4.0.1 and Ray 2.59.0.

---

# How to Run

## 1. Activate the Python Environment

```bash
conda activate myenv
```

Check the environment:

```bash
python --version
java -version
spark-submit --version
python -c "import ray; print(ray.__version__)"
```

---

# 2. Run Apache Spark

The Spark setup uses one master and two workers.

### Start the Spark Master

```bash
$SPARK_HOME/sbin/start-master.sh
```

The Spark Master uses:

```text
spark://10.45.12.62:7077
```

The Spark Master UI is available at:

```text
http://10.45.12.62:8080
```

### Start Worker 1

```bash
$SPARK_HOME/sbin/start-worker.sh \
  --memory 3G \
  --cores 2 \
  spark://10.45.12.62:7077
```

### Start Worker 2

Run the same command again:

```bash
$SPARK_HOME/sbin/start-worker.sh \
  --memory 3G \
  --cores 2 \
  spark://10.45.12.62:7077
```

The Spark Master UI should show two active workers.

### Run the Spark Pipeline

```bash
spark-submit \
  --master spark://10.45.12.62:7077 \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 2g \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=32 \
  spark_clean.py
```

During execution, the Spark application UI is available at:

```text
http://10.45.12.62:4040
```

The Spark implementation performs the same cleaning, duplicate removal, joins, feature generation, Python UDF processing, and Parquet export as the Ray implementation.

---

# 3. Run Ray Data

The Ray setup uses one head node and two worker nodes on the same machine.

### Stop Previous Ray Processes

```bash
ray stop --force
```

### Start the Ray Head Node

```bash
ray start --head \
  --node-ip-address=10.45.12.62 \
  --port=6379 \
  --dashboard-host=0.0.0.0 \
  --dashboard-port=8265 \
  --num-cpus=0
```

### Start Worker 1

```bash
ray start \
  --address=10.45.12.62:6379 \
  --node-ip-address=10.45.12.62 \
  --num-cpus=2 \
  --memory=3000000000 \
  --object-store-memory=1000000000
```

### Start Worker 2

Run the same command again:

```bash
ray start \
  --address=10.45.12.62:6379 \
  --node-ip-address=10.45.12.62 \
  --num-cpus=2 \
  --memory=3000000000 \
  --object-store-memory=1000000000
```

The Ray Dashboard is available at:

```text
http://10.45.12.62:8265
```

### Run the Ray Pipeline

```bash
python ray_clean.py
```

The Ray implementation uses `ray.data` and performs the same preprocessing pipeline as the Spark implementation.

---

# 4. Benchmark Results

The final benchmark was performed using actual executions of both pipelines.

| Framework | Total Time | Python UDF Time | UDF Share |
|---|---:|---:|---:|
| Spark | 923.69 s | 467.50 s | 50.6% |
| Ray | 298.91 s | 182.94 s | 61.2% |

Ray was approximately **3.09× faster** than Spark for the complete pipeline.

For the Python UDF stage, Ray was approximately **2.56× faster** than Spark.

The detailed benchmark analysis is available in:

```text
AI_ops_assignment_3.pdf
```

---

# 5. Python UDF

The pipeline contains a custom Python UDF for calculating average speed from trip distance and trip duration.

Conceptually:

```text
average speed = trip distance / trip duration
```

The UDF was measured separately to understand its contribution to the overall execution time.

The measured UDF contribution was:

- Spark: approximately 50.6% of total execution time.
- Ray: approximately 61.2% of total execution time.

This shows that Python-level processing is an important part of the runtime for both implementations.

---

# 6. Evidence

The repository contains screenshots showing the cluster and benchmark results:

- `spark_benchmark.jpeg`
- `spark_evidence_at_4040.jpeg`
- `ray_benchmark.jpeg`
- `ray_evidence.jpeg`

The Spark screenshots provide evidence from the Spark UI, including the application monitoring interface.

The Ray screenshots provide evidence from the Ray Dashboard and benchmark execution.

---

# 7. Performance Comparison

The final measurements show that Ray completed the complete preprocessing pipeline faster than Spark under the tested configuration.

The main measured results are:

```text
Spark total time : 923.69 seconds
Ray total time   : 298.91 seconds
```

Therefore:

```text
Spark / Ray ≈ 3.09×
```

For the Python UDF stage:

```text
Spark UDF time : 467.50 seconds
Ray UDF time   : 182.94 seconds
```

Therefore:

```text
Spark UDF / Ray UDF ≈ 2.56×
```

These results are specific to the tested hardware, cluster configuration, dataset, and execution environment.

---
# 8. AI Index

I used AI mainly when I was stuck with some parts of the code or needed help understanding the Spark and Ray implementation.

It helped me with:
- understanding and converting parts of the preprocessing code between Spark and Ray,
- fixing small coding issues,
- checking whether both pipelines were doing the same preprocessing steps,
- and organizing the benchmark and report.

I did the cluster setup and testing myself. This includes starting the Spark Master and workers, starting the Ray head and workers, running both pipelines, checking the dashboards, and recording the execution times.

The results shown in this repository are from my actual runs on the dataset. I did not use any simulated or synthetic benchmark results.

# 9. Report

The complete analysis is provided in:

```text
AI_ops_assignment_3.pdf
```

The report contains:

- Pipeline description
- Spark and Ray cluster configuration
- Benchmark results
- Performance comparison
- Python UDF analysis
- Resource utilization discussion
- Evidence screenshots
- Performance tuning discussion

---

# 10. Stopping the Clusters

After completing the Spark experiment:

```bash
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh
```

After completing the Ray experiment:

```bash
ray stop --force
```

---


