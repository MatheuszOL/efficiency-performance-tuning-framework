# Efficiency Project: Spark Performance Tuning Framework

Practical Spark benchmark focused on testing performance decisions with measurable evidence.

## Hypothesis-Driven Context

Main hypothesis tested in this project:

> **Hypothesis**: applying `ZORDER BY (customer_id)` should improve customer-level filtering queries after data is written in Delta format.

I also compare:

- baseline joins (no partition strategy / no broadcast)
- optimized path (partitioned write + broadcast joins)
- an intentional "bad" attempt (aggressive repartition) to document a real trade-off

## Project Structure

```
efficiency-performance-tuning-framework/
├─ data/
├─ reports/
│  └─ performance_notes.md
├─ src/
│  └─ tuning_framework.py
├─ requirements.txt
└─ README.md
```

## Setup

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

## Run Benchmark

```bash
python src/tuning_framework.py --rows 5000000
```

Main outputs:

- `reports/benchmark_results.csv`
- `reports/explain_baseline_groupby.txt`
- `reports/explain_optimized_groupby.txt`
- `reports/explain_customer_filter_before_zorder.txt`
- `reports/explain_customer_filter_after_zorder.txt`

Optional arguments:

```bash
python src/tuning_framework.py --rows 5000000 --probe-customer-id 42 --reports-dir reports
```

## What Is Being Measured

The benchmark exports practical metrics, such as:

- baseline vs optimized runtime
- customer filter runtime before/after `ZORDER`
- exchange node count (proxy for shuffle pressure)
- Delta data file count and data size
- result of the high-partition attempt vs baseline

## Realistic Data Imperfections Included

To avoid a tutorial-style perfect dataset, the synthetic data intentionally includes:

- skewed `region_id` distribution (`region_id=1` concentrated)
- random nulls in `status`
- out-of-order timestamps and some late-like timestamp behavior

## Optimization Techniques Demonstrated

1. **Partitioning by `region_id`**
   - Reduces broad scans for regional workloads and gives predictable write layout.

2. **Z-Order (Delta Lake)**
   - Tested specifically for customer-filter query behavior.
   - Automatically skipped if the runtime does not support `OPTIMIZE`.

3. **Broadcast Joins**
   - Broadcasts small dimensions (`customers`, `regions`) to reduce join shuffle.

4. **Failed attempt documented**
   - Aggressive repartition (`600`) is kept as an explicit experiment because it often increases shuffle overhead.

## Operational Constraints

- Runtime can vary by cluster size, file compaction state, and concurrent jobs.
- `OPTIMIZE ... ZORDER` depends on Delta-compatible runtime support.
- On Windows local runs, Spark may require `HADOOP_HOME`/`winutils.exe` to initialize correctly.
- Benchmark is interpreted by trend and relative gain, not absolute wall-clock only.

Study notes and lessons are documented in `LESSONS_LEARNED.md`.
