# Performance Notes

## Current Experiment Setup

- Dataset volume target: 5M synthetic transactions
- Baseline path: no broadcast hints and no partition strategy
- Optimized path: partitioned Delta write + broadcast joins
- Hypothesis path: customer filter query measured before/after `ZORDER BY (customer_id)`

## Why this looks closer to real engineering work

- Data now includes skew (`region_id=1` concentration), nulls, and out-of-order timestamps.
- Explain plans are exported to files for later inspection instead of relying only on elapsed time.
- A deliberately worse attempt (high repartition) is kept for trade-off documentation.

## Trade-offs observed during development

- Increasing partitions aggressively can increase shuffle overhead and total runtime.
- Broadcast joins help for small dimensions, but they are not a blanket rule for all join patterns.
- `OPTIMIZE ... ZORDER` is environment-dependent; if unsupported, experiment still runs and logs this explicitly.

## Next Practical Checks

- Compare `Exchange` nodes in baseline vs optimized explain plans.
- Track customer-filter query latency trend across reruns.
- Keep an eye on Delta file count growth to avoid over-fragmentation.

## Environment note

- On this Windows setup, Spark initialization can fail without `HADOOP_HOME` and `winutils.exe`.
