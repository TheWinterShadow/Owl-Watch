import unittest
from unittest.mock import patch

from execution.utils import extract_partitions


class TestExtractPartitions(unittest.TestCase):

    @patch("execution.utils.logger")
    def test_extract_partitions_with_single_partition(self, mock_logger):
        s3_key = "data/year=2024/file.json"

        result = extract_partitions(s3_key)

        self.assertEqual(result, {"year": "2024"})

    @patch("execution.utils.logger")
    def test_extract_partitions_with_multiple_partitions(self, mock_logger):
        s3_key = "data/year=2024/month=01/day=15/file.json"

        result = extract_partitions(s3_key)

        expected = {"year": "2024", "month": "01", "day": "15"}
        self.assertEqual(result, expected)

    @patch("execution.utils.logger")
    def test_extract_partitions_with_no_partitions(self, mock_logger):
        s3_key = "data/file.json"

        result = extract_partitions(s3_key)

        self.assertEqual(result, {})

    @patch("execution.utils.logger")
    def test_extract_partitions_with_mixed_content(self, mock_logger):
        s3_key = "bucket/raw/data/year=2024/month=12/processed/hour=14/file.parquet"

        result = extract_partitions(s3_key)

        expected = {"year": "2024", "month": "12", "hour": "14"}
        self.assertEqual(result, expected)

    @patch("execution.utils.logger")
    def test_extract_partitions_with_special_characters(self, mock_logger):
        s3_key = "data/region=us-east-1/env=prod_v2/file.json"

        result = extract_partitions(s3_key)

        expected = {"region": "us-east-1", "env": "prod_v2"}
        self.assertEqual(result, expected)

    @patch("execution.utils.logger")
    def test_extract_partitions_with_numeric_values(self, mock_logger):
        s3_key = "data/year=2024/month=01/day=05/hour=09/file.json"

        result = extract_partitions(s3_key)

        expected = {"year": "2024", "month": "01", "day": "05", "hour": "09"}
        self.assertEqual(result, expected)

    @patch("execution.utils.logger")
    def test_extract_partitions_edge_case_empty_string(self, mock_logger):
        s3_key = ""

        result = extract_partitions(s3_key)

        self.assertEqual(result, {})

    @patch("execution.utils.logger")
    def test_extract_partitions_edge_case_malformed_partition(self, mock_logger):
        s3_key = "data/year2024/month=01/invalid=partition=value/file.json"

        result = extract_partitions(s3_key)

        expected = {"month": "01", "invalid": "partition=value"}
        self.assertEqual(result, expected)

    @patch("execution.utils.logger")
    def test_extract_partitions_logs_input_and_output(self, mock_logger):
        s3_key = "data/year=2024/month=01/file.json"

        extract_partitions(s3_key)

        self.assertEqual(mock_logger.secure_info.call_count, 2)

        first_call_args = mock_logger.secure_info.call_args_list[0]
        self.assertIn(s3_key, first_call_args[0][0])

        second_call_args = mock_logger.secure_info.call_args_list[1]
        self.assertIn("Extracted partitions", second_call_args[0][0])

    @patch("execution.utils.logger")
    def test_extract_partitions_with_unicode_characters(self, mock_logger):
        s3_key = "data/location=café/type=résumé/file.json"

        result = extract_partitions(s3_key)

        expected = {"location": "café", "type": "résumé"}
        self.assertEqual(result, expected)

    @patch("execution.utils.logger")
    def test_extract_partitions_case_sensitivity(self, mock_logger):
        s3_key = "data/Year=2024/MONTH=01/file.json"

        result = extract_partitions(s3_key)

        expected = {"Year": "2024", "MONTH": "01"}
        self.assertEqual(result, expected)

    @patch("execution.utils.logger")
    def test_extract_partitions_repeated_keys(self, mock_logger):
        s3_key = "data/year=2023/nested/year=2024/file.json"

        result = extract_partitions(s3_key)

        self.assertIn("year", result)

    @patch("execution.utils.logger")
    def test_extract_partitions_with_equals_in_value(self, mock_logger):
        s3_key = "data/config=key=value/other=normal/file.json"

        result = extract_partitions(s3_key)

        expected = {"config": "key=value", "other": "normal"}
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
