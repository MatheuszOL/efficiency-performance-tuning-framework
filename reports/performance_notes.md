# Performance Notes (Simulation Example)

## Scenario

- Dataset volume: 5M synthetic transactions
- Baseline path: default join strategy + no partition planning
- Optimized path: partitioning by `region_id`, `OPTIMIZE ... ZORDER BY (customer_id)`, and broadcast joins for small dimensions

## Simulated Results

- Baseline processing: 1,440 minutes (24h)
- Optimized processing: 20 minutes
- Improvement: 98.61%

These values are intentionally representative of high-impact tuning in distributed Spark environments.

In real projects, absolute runtime varies by cluster size, file layout, and concurrency. The point is to document the tuning method and the gain trend.
