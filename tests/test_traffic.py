import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from generate_data import generate_data
from traffic import FIELDS, aggregate_logs, clean_logs, compare_daily, read_logs


DAY = "2025-01-30"


def log(**changes):
    row = dict(zip(FIELDS, [
        "request-1", f"{DAY} 12:00:00", "192.0.2.1", "GET", "/catalog", "200", "100", "Demo/1.0",
    ]))
    return {**row, **changes}


class CleaningTests(unittest.TestCase):
    def test_only_exact_duplicates_are_removed(self):
        first = log()
        changed = log(response_ms="200")
        rows, stats = clean_logs([first, first.copy(), changed], DAY)
        self.assertEqual([row["response_ms"] for row in rows], [100.0, 200.0])
        self.assertEqual(stats, {"source_rows": 3, "duplicates": 1, "invalid_rows": 0, "valid_rows": 2})

    def test_invalid_rows_and_other_dates_are_rejected(self):
        defects = [
            {"request_id": " "}, {"ip": ""}, {"method": ""}, {"user_agent": ""},
            {"timestamp": "bad-date"}, {"timestamp": "2025-02-30 12:00:00"},
            {"timestamp": "2025-01-31 12:00:00"}, {"timestamp": "2025-1-30 12:00:00"},
            {"status_code": "99"}, {"status_code": "600"}, {"status_code": "200.0"},
            {"response_ms": "NaN"}, {"response_ms": "inf"}, {"response_ms": "-1"},
            {"response_ms": "1e2"}, {"path": "catalog"}, {"path": "?page=2"},
        ]
        for changes in defects:
            with self.subTest(changes=changes):
                rows, stats = clean_logs([log(**changes)], DAY)
                self.assertEqual(rows, [])
                self.assertEqual(stats["invalid_rows"], 1)

    def test_query_parameters_and_surrounding_spaces_are_removed(self):
        rows, _ = clean_logs([log(path=" /catalog?page=2&x=3 ", status_code=" 200 ")], DAY)
        self.assertEqual(rows, [{
            "event_date": date(2025, 1, 30), "path": "/catalog", "ip": "192.0.2.1",
            "status_code": 200, "response_ms": 100.0,
        }])

    def test_metrics_have_hand_calculated_values(self):
        rows, _ = clean_logs([
            log(request_id="a", path="/catalog?page=1", response_ms="100"),
            log(request_id="b", status_code="404", response_ms="200"),
            log(request_id="c", ip="192.0.2.2", status_code="503", response_ms="300"),
            log(request_id="d", status_code="301", response_ms="400"),
            log(request_id="e", path="/", response_ms="50"),
        ], DAY)
        self.assertEqual(aggregate_logs(rows), [
            [date(2025, 1, 30), "/", 1, 1, 0, 0, 0.0, 50.0],
            [date(2025, 1, 30), "/catalog", 4, 2, 1, 1, 0.5, 250.0],
        ])

    def test_malformed_csv_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "logs.csv"
            source.write_text("wrong,header\na,b\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Unexpected CSV header"):
                read_logs(source)
            with source.open("w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output)
                writer.writerow(FIELDS)
                writer.writerow(["too", "few"])
            with self.assertRaisesRegex(ValueError, "Wrong number of CSV fields"):
                read_logs(source)


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self.metric = [date(2025, 1, 30), "/catalog", 4, 2, 1, 1, 0.5, 250.0]

    def compare(self, etl, elt):
        client = MagicMock()
        client.query.side_effect = [MagicMock(result_set=etl), MagicMock(result_set=elt)]
        with patch("traffic.get_client") as get_client:
            get_client.return_value.__enter__.return_value = client
            return compare_daily(DAY)

    def test_empty_aggregates_fail(self):
        for etl, elt in [([], []), ([self.metric], []), ([], [self.metric])]:
            with self.subTest(etl=etl, elt=elt):
                with self.assertRaisesRegex(ValueError, "Empty daily aggregate for 2025-01-30"):
                    self.compare(etl, elt)

    def test_different_group_counts_fail(self):
        with self.assertRaisesRegex(ValueError, "different group counts"):
            self.compare([self.metric], [self.metric, self.metric])

    def test_different_keys_and_counters_fail(self):
        for index, value in enumerate([date(2025, 1, 31), "/other", 5, 3, 0, 0]):
            with self.subTest(field=index):
                changed = self.metric.copy()
                changed[index] = value
                with self.assertRaisesRegex(ValueError, "Different keys or counters"):
                    self.compare([self.metric], [changed])

    def test_different_fractional_metrics_fail(self):
        for index in (6, 7):
            with self.subTest(field=index):
                changed = self.metric.copy()
                changed[index] += 0.01
                with self.assertRaisesRegex(ValueError, "Different"):
                    self.compare([self.metric], [changed])

    def test_small_float_rounding_is_allowed(self):
        changed = self.metric.copy()
        changed[6] += 1e-12
        changed[7] += 1e-8
        self.assertEqual(self.compare([self.metric], [changed]), {
            "process_date": DAY, "groups": 1, "result": "ETL = ELT",
        })


class GeneratorTests(unittest.TestCase):
    def test_dataset_is_reproducible(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            generate_data(output)
            first = {source.name: source.read_bytes() for source in output.glob("*.csv")}
            generate_data(output)
            self.assertEqual(first, {source.name: source.read_bytes() for source in output.glob("*.csv")})
            self.assertEqual(sorted(first), [
                "2025-01-30.csv", "2025-01-31.csv", "2025-02-01.csv", "2025-02-02.csv",
            ])


if __name__ == "__main__":
    unittest.main()
