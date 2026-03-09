# Efficiency Project: Spark Performance Tuning Framework

Spark performance tuning framework focused on reproducible benchmark scenarios.

## Scope

Reproducible benchmark comparing:

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

## Execution Evidence (Local Run)

- Run date: `2026-03-09` (Windows local environment)
- Transcript attempt 1 (missing Java): `reports/run_2026-03-09_17-50-37.log`
- Transcript attempt 2 (Java configured): `reports/run_2026-03-09_17-50-54.log`
- Environment snapshot: `reports/pip_freeze_2026-03-09_17-50-37.txt`

Observed blocker in this machine: Spark startup fails on Windows due to missing `HADOOP_HOME/winutils`.
The logs were intentionally kept to provide transparent execution evidence and environment diagnostics.
