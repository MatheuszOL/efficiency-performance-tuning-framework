# Efficiency Project: Spark Performance Tuning Framework

Performance tuning framework to demonstrate how a long Spark workload can be reduced from hours to minutes using the right execution strategy.

## What this project proves

Reproducible benchmark comparing:

- **Baseline processing** (simple joins and default execution behavior)
- **Optimized processing** using:
  - partitioning strategy
  - Delta Lake `OPTIMIZE` with `ZORDER`
  - broadcast joins for small dimension tables

Core mindset: optimize for runtime and cost without compromising data reliability.

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

## Why this project is portfolio-relevant

- Gives concrete evidence for performance tuning discussions in interviews
- Combines engineering tactics (partitioning/broadcast/Z-Order) with measurable impact
- Communicates technical decisions in a way that business stakeholders can understand

## Operational Constraints

- Runtime can vary by cluster size, file compaction state, and concurrent jobs.
- `OPTIMIZE ... ZORDER` depends on Delta-compatible runtime support.
- Benchmark should be interpreted by trend and relative gain, not absolute time only.
