from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


def time_result_collection(result: Any, clock: Callable[[], float] = time.perf_counter) -> tuple[float, int]:
    """Measure one complete result action and return elapsed seconds and row count.

    Collecting the aggregate rows materializes every output column. In contrast,
    ``count()`` can let Spark prune aggregate expressions that are not needed to
    count rows, making it a weaker measurement of the benchmark query.
    """
    started_at = clock()
    rows = result.collect()
    elapsed_seconds = clock() - started_at
    return elapsed_seconds, len(rows)
