# Efficiency Project: Spark Performance Tuning Framework

Spark performance tuning framework focused on reproducible benchmark scenarios.

## Study Context

In this project, I compared two Spark execution paths:

- **Baseline processing** (simple joins and default execution behavior)
- **Optimized processing** using:
  - partitioning strategy
  - Delta Lake `OPTIMIZE` with `ZORDER`
  - broadcast joins for small dimension tables

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

This execution writes results to:

- `reports/benchmark_results.csv`
- `reports/performance_notes.md`

## Optimization Techniques Demonstrated

1. **Partitioning**
   - Writes transaction data partitioned by `region_id` to reduce scanned data.

2. **Z-Order (Delta Lake)**
   - Applies `OPTIMIZE ... ZORDER BY (customer_id)` to improve file skipping and locality.

3. **Broadcast Joins**
   - Broadcasts dimension tables (`customers`, `regions`) to avoid costly shuffle joins.

## Example Theoretical Comparison

| Approach | Execution Time |
|---|---:|
| Baseline | 24h |
| Tuned Framework | 20min |

Estimated gain: **98.61%**.

## Operational Constraints

- Runtime can vary by cluster size, file compaction state, and concurrent jobs.
- `OPTIMIZE ... ZORDER` depends on Delta-compatible runtime support.
- Benchmark should be interpreted by trend and relative gain, not absolute time only.

## Local Run Notes

- Study date: `2026-03-09` (Windows local environment)
- Local logs: `reports/run_2026-03-09_17-50-37.log` and `reports/run_2026-03-09_17-50-54.log`
- Environment snapshot: `reports/pip_freeze_2026-03-09_17-50-37.txt`

Main local blocker: Spark startup failed on Windows due to missing `HADOOP_HOME/winutils`.

## Lessons Learned

- Installing Python packages alone is not enough for local Spark; Java and Hadoop layers must be aligned.
- Setting `JAVA_HOME` solved the first error (`JAVA_GATEWAY_EXITED`), but `winutils` was still missing on Windows.
- `OPTIMIZE ... ZORDER` depends on runtime support; outside compatible environments, fallback behavior is expected.
- For benchmarking, trend and relative gain matter more than isolated absolute runtime.
