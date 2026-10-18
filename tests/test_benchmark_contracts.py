import unittest

from src.benchmark_contracts import time_result_collection


class FakeResult:
    def __init__(self):
        self.collected = False

    def collect(self):
        self.collected = True
        return [("north", 2, 3)]


class TimeResultCollectionTests(unittest.TestCase):
    def test_times_full_collection_and_reports_materialized_row_count(self):
        result = FakeResult()
        timestamps = iter([10.0, 10.25])

        elapsed, row_count = time_result_collection(result, clock=lambda: next(timestamps))

        self.assertTrue(result.collected)
        self.assertEqual(elapsed, 0.25)
        self.assertEqual(row_count, 1)


if __name__ == "__main__":
    unittest.main()
