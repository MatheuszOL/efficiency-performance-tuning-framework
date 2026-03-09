# Trial and Error Notes

Run date: `2026-03-09` on Windows.

First test I ran:
`python src/tuning_framework.py --rows 500000 --report-path reports/benchmark_results_2026-03-09_17-50-37.csv`

What happened:
- Spark did not start.
- Error raised: `JAVA_GATEWAY_EXITED`.
- Log: `reports/run_2026-03-09_17-50-37.log`

What I changed:
- Set `JAVA_HOME` to local JDK 17 and retried.

Second test I ran:
same command, with Java configured.

What happened:
- `JAVA_GATEWAY_EXITED` disappeared.
- New blocker appeared: `HADOOP_HOME and hadoop.home.dir are unset` / missing `winutils`.
- Log: `reports/run_2026-03-09_17-50-54.log`

What this taught me in practice:
- In local Spark, fixing Python dependencies is only one piece; Java + Hadoop runtime is the real startup gate.
- Errors often come in layers: solving one environment issue usually reveals the next one.
- Benchmark code can be fine while environment blocks execution.
- For this kind of project, the useful signal is still in repeatable setup and relative performance trend, not one absolute runtime number.

Environment snapshot used in the run:
`reports/pip_freeze_2026-03-09_17-50-37.txt`
