import sys
import unittest
from unittest.mock import Mock, patch

from execution.core.job_runner import JobRunner
from execution.models.job_metadata import JobConfiguration, JobType


class TestJobRunner(unittest.TestCase):

    def setUp(self):
        self.job_config = JobConfiguration(
            job_name="test-job", job_type=JobType.ETL, incoming_file="test/file.json"
        )
        self.runner = JobRunner(self.job_config)

    def test_init(self):
        self.assertEqual(self.runner.job_config, self.job_config)
        self.assertIsNotNone(self.runner.factory)

    @patch("execution.core.job_runner.JobFactory")
    def test_run_with_job_type_parameter(self, mock_factory_class):
        job_type = "email_communication"
        mock_factory = Mock()
        mock_factory_class.return_value = mock_factory

        mock_etl_job = Mock()
        mock_etl_job.run.return_value = "test_result"
        mock_factory.create_job.return_value = mock_etl_job

        runner = JobRunner(self.job_config)

        result = runner.run(job_type)

        mock_factory.create_job.assert_called_once_with(job_type)
        mock_etl_job.run.assert_called_once()
        mock_etl_job.commit.assert_called_once()
        mock_etl_job.cleanup.assert_called_once()
        self.assertEqual(result, "test_result")

    @patch("execution.core.job_runner.JobFactory")
    @patch("sys.argv", ["script.py", "--JOB_TYPE", "data_cleaning"])
    def test_run_extract_job_type_from_args(self, mock_factory_class):
        mock_factory = Mock()
        mock_factory_class.return_value = mock_factory

        mock_etl_job = Mock()
        mock_etl_job.run.return_value = "test_result"
        mock_factory.create_job.return_value = mock_etl_job

        runner = JobRunner(self.job_config)

        result = runner.run()

        mock_factory.create_job.assert_called_once_with("data_cleaning")
        mock_etl_job.run.assert_called_once()
        mock_etl_job.commit.assert_called_once()
        mock_etl_job.cleanup.assert_called_once()
        self.assertEqual(result, "test_result")

    @patch("execution.core.job_runner.JobFactory")
    @patch("sys.argv", ["script.py"])
    def test_run_no_job_type_provided(self, mock_factory_class):
        mock_factory = Mock()
        mock_factory_class.return_value = mock_factory
        runner = JobRunner(self.job_config)

        with self.assertRaises(ValueError) as context:
            runner.run()

        self.assertIn("Job type must be provided", str(context.exception))

    @patch("execution.core.job_runner.JobFactory")
    def test_run_job_execution_failure(self, mock_factory_class):
        job_type = "email_communication"
        mock_factory = Mock()
        mock_factory_class.return_value = mock_factory

        mock_etl_job = Mock()
        mock_etl_job.run.side_effect = Exception("Job execution failed")
        mock_factory.create_job.return_value = mock_etl_job

        runner = JobRunner(self.job_config)

        with self.assertRaises(Exception) as context:
            runner.run(job_type)

        self.assertEqual(str(context.exception), "Job execution failed")
        mock_etl_job.run.assert_called_once()
        mock_etl_job.commit.assert_not_called()
        mock_etl_job.cleanup.assert_not_called()

    def test_extract_job_type_with_job_type_flag(self):
        test_argv = ["script.py", "--JOB_TYPE", "sentiment_analysis"]

        with patch.object(sys, "argv", test_argv):
            result = self.runner._extract_job_type()

            self.assertEqual(result, "sentiment_analysis")

    def test_extract_job_type_with_job_type_lowercase_flag(self):
        test_argv = ["script.py", "--job_type", "data_cleaning"]

        with patch.object(sys, "argv", test_argv):
            result = self.runner._extract_job_type()

            self.assertEqual(result, "data_cleaning")

    def test_extract_job_type_with_job_type_hyphen_flag(self):
        test_argv = ["script.py", "--job-type", "nlp_analysis"]

        with patch.object(sys, "argv", test_argv):
            result = self.runner._extract_job_type()

            self.assertEqual(result, "nlp_analysis")

    def test_extract_job_type_no_flag(self):
        test_argv = ["script.py", "other", "args"]

        with patch.object(sys, "argv", test_argv):
            result = self.runner._extract_job_type()

            self.assertIsNone(result)

    def test_extract_job_type_flag_without_value(self):
        test_argv = ["script.py", "--JOB_TYPE"]

        with patch.object(sys, "argv", test_argv):
            result = self.runner._extract_job_type()

            self.assertIsNone(result)

    def test_handle_result_output_no_result(self):
        mock_etl_job = Mock()
        mock_etl_job.args = {}

        self.runner._handle_result_output(mock_etl_job, None)

    def test_handle_result_output_no_valid_bucket(self):
        mock_etl_job = Mock()
        mock_etl_job.args = {}
        mock_etl_job.__class__.__name__ = "TestETL"

        mock_result = Mock()
        mock_result.write = Mock()

        self.runner._handle_result_output(mock_etl_job, mock_result)

    def test_handle_result_output_with_bucket(self):
        mock_etl_job = Mock()
        mock_etl_job.args = {"data_lake_bucket": "test-bucket"}
        mock_etl_job.__class__.__name__ = "TestETL"

        mock_result = Mock()
        mock_write = Mock()
        mock_write.mode.return_value = mock_write
        mock_result.write = mock_write

        self.runner._handle_result_output(mock_etl_job, mock_result)

        mock_write.mode.assert_called_once_with("overwrite")
        mock_write.parquet.assert_called_once()

    def test_handle_result_output_s3_write_failure(self):
        mock_etl_job = Mock()
        mock_etl_job.args = {"data_lake_bucket": "test-bucket"}
        mock_etl_job.__class__.__name__ = "TestETL"

        mock_result = Mock()
        mock_write = Mock()
        mock_write.mode.return_value = mock_write
        mock_write.parquet.side_effect = Exception("S3 write failed")
        mock_result.write = mock_write

        self.runner._handle_result_output(mock_etl_job, mock_result)

    def test_list_jobs(self):
        expected_jobs = {"test_job": "Test job description"}
        self.runner.factory.list_available_jobs = Mock(return_value=expected_jobs)

        result = self.runner.list_jobs()

        self.assertEqual(result, expected_jobs)
        self.runner.factory.list_available_jobs.assert_called_once()


if __name__ == "__main__":
    unittest.main()
