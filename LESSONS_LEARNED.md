# Lessons Learned

## Local Run Notes

- Study date: `2026-03-09` (Windows local environment)
- Local logs: `reports/run_2026-03-09_17-50-37.log` and `reports/run_2026-03-09_17-50-54.log`
- Environment snapshot: `reports/pip_freeze_2026-03-09_17-50-37.txt`

Main local blocker: Spark startup failed on Windows due to missing `HADOOP_HOME/winutils`.

## Lessons

- Installing Python packages alone is not enough for local Spark; Java and Hadoop layers must be aligned.
- Setting `JAVA_HOME` solved the first error (`JAVA_GATEWAY_EXITED`), but `winutils` was still missing on Windows.
- `OPTIMIZE ... ZORDER` depends on runtime support; outside compatible environments, fallback behavior is expected.
- For benchmarking, trend and relative gain matter more than isolated absolute runtime.
