from __future__ import annotations

import argparse
import time
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count,
    expr,
    from_unixtime,
    lit,
    rand,
    sum as sum_,
    to_timestamp,
    when,
)


def create_spark(app_name: str = "EfficiencyPerformanceTuning") -> SparkSession:
    # I keep shuffle partitions explicit so I can compare runs with fewer moving parts.
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "200")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def generate_synthetic_data(spark: SparkSession, rows: int):
    # I intentionally generate imperfect data because perfectly clean synthetic datasets hide real Spark behavior.
    transactions = (
        spark.range(rows)
        .withColumnRenamed("id", "transaction_id")
        .withColumn("customer_id", (col("transaction_id") % 50000).cast("long"))
        .withColumn(
            "region_id",
            when(rand(seed=7) < 0.80, lit(1)).otherwise(((col("transaction_id") % 24) + 2).cast("int")),
        )
        .withColumn("amount", (rand(seed=42) * 1000).cast("double"))
        .withColumn(
            "event_ts",
            to_timestamp(from_unixtime(lit(1_700_000_000) - (col("transaction_id") % 86_400))),
        )
        .withColumn(
            "event_ts",
            when(rand(seed=17) < 0.01, col("event_ts") + expr("INTERVAL 2 DAYS")).otherwise(col("event_ts")),
        )
        .withColumn("status", when(rand(seed=99) < 0.03, lit(None)).otherwise(lit("ok")))
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


def _write_explain(plan_target, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plan_text = plan_target._sc._jvm.PythonSQLUtils.explainString(plan_target._jdf.queryExecution(), "formatted")
    output_path.write_text(plan_text, encoding="utf-8")


def _count_exchange_nodes(plan_target) -> int:
    executed_plan = plan_target._jdf.queryExecution().executedPlan().toString()
    return executed_plan.count("Exchange")


def _safe_optimize_with_zorder(spark: SparkSession, partitioned_path: str) -> tuple[bool, str]:
    try:
        spark.sql(f"OPTIMIZE delta.`{partitioned_path}` ZORDER BY (customer_id)")
        return True, "OPTIMIZE with ZORDER executed"
    except Exception as exc:
        return False, f"ZORDER skipped in this environment: {exc}"


def baseline_job(transactions, customers, regions, explain_path: Path) -> tuple[float, int]:
    # Baseline on purpose: no partition strategy and no broadcast hints.
    joined = (
        transactions.join(customers, on="customer_id", how="inner")
        .join(regions, on="region_id", how="inner")
    )
    result = joined.groupBy("region_name", "customer_segment").agg(count("*").alias("total_transactions"))
    _write_explain(result, explain_path)

    start = time.perf_counter()
    result.count()
    elapsed = time.perf_counter() - start
    exchanges = _count_exchange_nodes(result)
    return elapsed, exchanges


def high_partition_attempt_job(transactions, customers, regions) -> float:
    # I tried aggressive repartitioning here to test if more partitions helps with skew; in my tests it often increases shuffle cost.
    stressed_transactions = transactions.repartition(600, "region_id")
    joined = (
        stressed_transactions.join(customers, on="customer_id", how="inner")
        .join(regions, on="region_id", how="inner")
    )
    result = joined.groupBy("region_name", "customer_segment").agg(count("*").alias("total_transactions"))

    start = time.perf_counter()
    result.count()
    elapsed = time.perf_counter() - start
    return elapsed


def run_customer_filter_experiment(
    spark: SparkSession,
    partitioned_path: str,
    probe_customer_id: int,
    explain_before: Path,
    explain_after: Path,
) -> dict[str, float | str]:
    metrics: dict[str, float | str] = {}

    pre_df = spark.read.format("delta").load(partitioned_path)
    pre_query = (
        pre_df.filter(col("customer_id") == lit(probe_customer_id))
        .groupBy("region_id")
        .agg(count("*").alias("txn_count"), sum_("amount").alias("amount_total"))
    )
    _write_explain(pre_query, explain_before)

    start = time.perf_counter()
    pre_query.count()
    pre_elapsed = time.perf_counter() - start
    metrics["customer_filter_before_seconds"] = pre_elapsed

    zorder_ok, zorder_msg = _safe_optimize_with_zorder(spark, partitioned_path)
    metrics["zorder_status"] = zorder_msg

    post_df = spark.read.format("delta").load(partitioned_path)
    post_query = (
        post_df.filter(col("customer_id") == lit(probe_customer_id))
        .groupBy("region_id")
        .agg(count("*").alias("txn_count"), sum_("amount").alias("amount_total"))
    )
    _write_explain(post_query, explain_after)

    start = time.perf_counter()
    post_query.count()
    post_elapsed = time.perf_counter() - start
    metrics["customer_filter_after_seconds"] = post_elapsed
    metrics["customer_filter_exchange_nodes_before"] = float(_count_exchange_nodes(pre_query))
    metrics["customer_filter_exchange_nodes_after"] = float(_count_exchange_nodes(post_query))

    gain = (pre_elapsed - post_elapsed) / pre_elapsed * 100 if pre_elapsed else 0.0
    metrics["customer_filter_improvement_pct"] = gain
    metrics["zorder_applied_flag"] = 1.0 if zorder_ok else 0.0
    return metrics


def optimized_job(
    spark: SparkSession,
    transactions,
    customers,
    regions,
    output_path: str,
    explain_path: Path,
    probe_customer_id: int,
    reports_dir: Path,
) -> tuple[float, int, dict[str, float | str]]:
    partitioned_path = str(Path(output_path) / "transactions_delta")

    (
        transactions.repartition("region_id")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("region_id")
        .save(partitioned_path)
    )

    optimized_transactions = spark.read.format("delta").load(partitioned_path)

    joined = (
        optimized_transactions.join(broadcast(customers), on="customer_id", how="inner")
        .join(broadcast(regions), on="region_id", how="inner")
    )

    result = joined.groupBy("region_name", "customer_segment").agg(count("*").alias("total_transactions"))
    _write_explain(result, explain_path)

    start = time.perf_counter()
    result.count()
    elapsed = time.perf_counter() - start
    exchanges = _count_exchange_nodes(result)

    filter_metrics = run_customer_filter_experiment(
        spark=spark,
        partitioned_path=partitioned_path,
        probe_customer_id=probe_customer_id,
        explain_before=reports_dir / "explain_customer_filter_before_zorder.txt",
        explain_after=reports_dir / "explain_customer_filter_after_zorder.txt",
    )
    return elapsed, exchanges, filter_metrics


def timed_execution(label: str, fn, *args) -> float:
    start = time.perf_counter()
    fn(*args)
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.2f}s")
    return elapsed


def _estimate_delta_size_bytes(spark: SparkSession, path: str) -> int:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    content_summary = fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(path))
    return int(content_summary.getLength())


def run_benchmark(args: argparse.Namespace) -> None:
    spark = create_spark()
    transactions, customers, regions = generate_synthetic_data(spark, args.rows)

    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    baseline_seconds, baseline_exchanges = baseline_job(
        transactions,
        customers,
        regions,
        reports_dir / "explain_baseline_groupby.txt",
    )
    print(f"Baseline processing: {baseline_seconds:.2f}s")

    optimized_seconds, optimized_exchanges, filter_metrics = optimized_job(
        spark,
        transactions,
        customers,
        regions,
        args.output_path,
        reports_dir / "explain_optimized_groupby.txt",
        args.probe_customer_id,
        reports_dir,
    )
    print(f"Optimized processing: {optimized_seconds:.2f}s")

    high_partition_seconds = timed_execution(
        "High-partition attempt (often worse)",
        high_partition_attempt_job,
        transactions,
        customers,
        regions,
    )

    gain = (baseline_seconds - optimized_seconds) / baseline_seconds * 100 if baseline_seconds else 0.0
    print(f"Performance improvement: {gain:.2f}%")
    if isinstance(filter_metrics.get("zorder_status"), str):
        print(filter_metrics["zorder_status"])

    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    delta_path = str(Path(args.output_path) / "transactions_delta")
    delta_size = _estimate_delta_size_bytes(spark, delta_path)
    data_files = len(spark.read.format("delta").load(delta_path).inputFiles())

    customer_filter_before = float(filter_metrics["customer_filter_before_seconds"])
    customer_filter_after = float(filter_metrics["customer_filter_after_seconds"])
    customer_filter_improvement = float(filter_metrics["customer_filter_improvement_pct"])
    customer_exchange_before = int(filter_metrics["customer_filter_exchange_nodes_before"])
    customer_exchange_after = int(filter_metrics["customer_filter_exchange_nodes_after"])
    zorder_applied = int(filter_metrics["zorder_applied_flag"])
    zorder_status = str(filter_metrics["zorder_status"]).replace("\n", " ").replace(",", ";")

    report_path.write_text(
        "\n".join(
            [
                "metric,value",
                f"rows,{args.rows}",
                f"baseline_seconds,{baseline_seconds:.2f}",
                f"optimized_seconds,{optimized_seconds:.2f}",
                f"overall_improvement_pct,{gain:.2f}",
                f"high_partition_attempt_seconds,{high_partition_seconds:.2f}",
                f"high_partition_vs_baseline_pct,{((high_partition_seconds - baseline_seconds) / baseline_seconds * 100) if baseline_seconds else 0.0:.2f}",
                f"baseline_exchange_nodes,{baseline_exchanges}",
                f"optimized_exchange_nodes,{optimized_exchanges}",
                f"customer_probe_id,{args.probe_customer_id}",
                f"customer_filter_before_seconds,{customer_filter_before:.2f}",
                f"customer_filter_after_seconds,{customer_filter_after:.2f}",
                f"customer_filter_improvement_pct,{customer_filter_improvement:.2f}",
                f"customer_filter_exchange_nodes_before,{customer_exchange_before}",
                f"customer_filter_exchange_nodes_after,{customer_exchange_after}",
                f"delta_data_file_count,{data_files}",
                f"delta_data_size_bytes,{delta_size}",
                f"zorder_applied,{zorder_applied}",
                f"zorder_status,{zorder_status}",
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
        "--reports-dir",
        default=str(root / "reports"),
        help="Directory where explain plans and notes are exported.",
    )
    parser.add_argument(
        "--probe-customer-id",
        type=int,
        default=42,
        help="Customer id used for the ZORDER hypothesis filter benchmark.",
    )
    parser.add_argument(
        "--report-path",
        default=str(root / "reports" / "benchmark_results.csv"),
        help="Where benchmark metrics will be exported.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_benchmark(parse_args())
