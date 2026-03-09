from __future__ import annotations

import argparse
import time
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, count, rand


def create_spark(app_name: str = "EfficiencyPerformanceTuning") -> SparkSession:
    # Mantém partições de shuffle explícitas para comportamento reprodutível no benchmark.
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "200")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def generate_synthetic_data(spark: SparkSession, rows: int):
    # Modelo sintético de alto volume para emular cargas transacionais de produção.
    transactions = (
        spark.range(rows)
        .withColumnRenamed("id", "transaction_id")
        .withColumn("customer_id", (col("transaction_id") % 50000).cast("long"))
        .withColumn("region_id", (col("transaction_id") % 25).cast("int"))
        .withColumn("amount", (rand(seed=42) * 1000).cast("double"))
    )

    customers = (
        spark.range(50000)
        .withColumnRenamed("id", "customer_id")
        .withColumn("customer_segment", (col("customer_id") % 10).cast("int"))
    )

    regions = (
        spark.range(25)
        .withColumnRenamed("id", "region_id")
        .withColumn("region_name", col("region_id").cast("string"))
    )

    return transactions, customers, regions


def baseline_job(transactions, customers, regions):
    # Caminho baseline: sem planejamento de partição e sem hints de broadcast.
    joined = (
        transactions.join(customers, on="customer_id", how="inner")
        .join(regions, on="region_id", how="inner")
    )
    result = joined.groupBy("region_name", "customer_segment").agg(count("*").alias("total_transactions"))
    result.count()


def optimized_job(spark: SparkSession, transactions, customers, regions, output_path: str):
    partitioned_path = str(Path(output_path) / "transactions_delta")

    (
        transactions.repartition("region_id")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("region_id")
        .save(partitioned_path)
    )

    optimized_transactions = spark.read.format("delta").load(partitioned_path)

    # OPTIMIZE + ZORDER está disponível em runtimes compatíveis com Databricks.
    # Se não estiver disponível localmente, o benchmark segue com particionamento + broadcast.
    try:
        spark.sql(f"OPTIMIZE delta.`{partitioned_path}` ZORDER BY (customer_id)")
    except Exception as exc:
        print(f"Z-Order step skipped in this environment: {exc}")

    joined = (
        optimized_transactions.join(broadcast(customers), on="customer_id", how="inner")
        .join(broadcast(regions), on="region_id", how="inner")
    )

    result = joined.groupBy("region_name", "customer_segment").agg(count("*").alias("total_transactions"))
    result.count()


def timed_execution(label: str, fn, *args) -> float:
    start = time.perf_counter()
    fn(*args)
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.2f}s")
    return elapsed


def run_benchmark(args: argparse.Namespace) -> None:
    spark = create_spark()
    transactions, customers, regions = generate_synthetic_data(spark, args.rows)

    baseline_seconds = timed_execution("Baseline processing", baseline_job, transactions, customers, regions)
    optimized_seconds = timed_execution(
        "Optimized processing",
        optimized_job,
        spark,
        transactions,
        customers,
        regions,
        args.output_path,
    )

    gain = (baseline_seconds - optimized_seconds) / baseline_seconds * 100 if baseline_seconds else 0.0
    print(f"Performance improvement: {gain:.2f}%")

    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        "\n".join(
            [
                "run_mode,seconds",
                f"baseline,{baseline_seconds:.2f}",
                f"optimized,{optimized_seconds:.2f}",
                f"improvement_pct,{gain:.2f}",
            ]
        ),
        encoding="utf-8",
    )

    spark.stop()


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Spark performance tuning benchmark framework.")
    parser.add_argument("--rows", type=int, default=5_000_000, help="Synthetic transaction row count.")
    parser.add_argument("--output-path", default=str(root / "data"), help="Output path for Delta assets.")
    parser.add_argument(
        "--report-path",
        default=str(root / "reports" / "benchmark_results.csv"),
        help="Where benchmark metrics will be exported.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_benchmark(parse_args())
