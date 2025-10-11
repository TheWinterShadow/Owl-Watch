import sys
import unittest
from unittest.mock import patch

from execution.core.config_manager import JobConfigManager
from execution.models.job_metadata import JobConfiguration, JobType


class TestJobConfigManager(unittest.TestCase):

    def setUp(self):
        self.config_manager = JobConfigManager()

    @patch("execution.core.config_manager.getResolvedOptions")
    @patch("execution.core.config_manager.os.environ")
    def test_parse_job_arguments_success(self, mock_environ, mock_get_resolved_options):
        expected_args = {
            "JOB_NAME": "test-job",
            "s3_prefix": "test-prefix",
            "POWERTOOLS_LOG_LEVEL": "INFO",
        }
        mock_get_resolved_options.return_value = expected_args

        result = self.config_manager.parse_job_arguments()

        self.assertEqual(result, expected_args)
        mock_get_resolved_options.assert_called_once_with(
            sys.argv, self.config_manager.REQUIRED_ARGS
        )
        mock_environ.__setitem__.assert_called_once_with("POWERTOOLS_LOG_LEVEL", "INFO")

    @patch("execution.core.config_manager.getResolvedOptions")
    def test_parse_job_arguments_failure(self, mock_get_resolved_options):
        mock_get_resolved_options.side_effect = Exception("Missing required argument")

        with self.assertRaises(ValueError) as context:
            self.config_manager.parse_job_arguments()

        self.assertIn("Failed to parse job arguments", str(context.exception))

    def test_determine_job_type_etl(self):
        file_path = "raw/data/file.json"

        result = self.config_manager.determine_job_type(file_path)

        self.assertEqual(result, JobType.ETL)

    def test_determine_job_type_cleaning(self):
        file_path = "etl/processed/file.json"

        result = self.config_manager.determine_job_type(file_path)

        self.assertEqual(result, JobType.CLEANING)

    def test_determine_job_type_sentiment(self):
        file_path = "cleaned/data/file.json"

        result = self.config_manager.determine_job_type(file_path)

        self.assertEqual(result, JobType.SENTIMENT)

    def test_determine_job_type_nlp(self):
        file_path = "sentiment/analysis/file.json"

        result = self.config_manager.determine_job_type(file_path)

        self.assertEqual(result, JobType.NLP)

    def test_determine_job_type_analytics(self):
        file_path = "nlp/results/file.json"

        result = self.config_manager.determine_job_type(file_path)

        self.assertEqual(result, JobType.ANALYSTICS)

    def test_determine_job_type_invalid(self):
        file_path = "invalid/path/file.json"

        with self.assertRaises(ValueError) as context:
            self.config_manager.determine_job_type(file_path)

        self.assertIn("Cannot determine job type", str(context.exception))

    @patch("execution.core.config_manager.extract_partitions")
    def test_create_job_config_success(self, mock_extract_partitions):
        args = {
            "JOB_NAME": "test-job",
            "s3_prefix": "test-prefix",
            "POWERTOOLS_LOG_LEVEL": "INFO",
            "source_bucket": "test-bucket",
            "source_key": "raw/data/test.json",
        }
        mock_extract_partitions.return_value = {"year": "2024", "month": "01"}

        result = self.config_manager.create_job_config(args)

        self.assertIsInstance(result, JobConfiguration)
        self.assertEqual(result.job_name, "test-job")
        self.assertEqual(result.job_type, JobType.ETL)
        self.assertEqual(result.incoming_file, "raw/data/test.json")

    def test_create_job_config_missing_params(self):
        args = {"JOB_NAME": "test-job"}

        with self.assertRaises(Exception):
            self.config_manager.create_job_config(args)

    def test_create_s3_job_config_missing_s3_params(self):
        args = {
            "JOB_NAME": "test-job",
            "s3_prefix": "test-prefix",
            "POWERTOOLS_LOG_LEVEL": "INFO",
        }

        with self.assertRaises(ValueError) as context:
            self.config_manager._create_job_config(args)

        self.assertIn(
            "S3 jobs require source-bucket and source-key parameters",
            str(context.exception),
        )

    @patch("execution.core.config_manager.extract_partitions")
    def test_create_s3_job_config_success(self, mock_extract_partitions):
        args = {
            "JOB_NAME": "test-job",
            "s3_prefix": "test-prefix",
            "POWERTOOLS_LOG_LEVEL": "INFO",
            "source_bucket": "test-bucket",
            "source_key": "raw/data/test.json",
        }
        mock_extract_partitions.return_value = {"year": "2024"}

        result = self.config_manager._create_job_config(args)

        self.assertIsInstance(result, JobConfiguration)
        self.assertEqual(result.job_name, "test-job")
        self.assertEqual(result.incoming_file, "raw/data/test.json")
        mock_extract_partitions.assert_called_once_with("raw/data/test.json")


if __name__ == "__main__":
    unittest.main()
