import unittest
from unittest.mock import Mock, patch

from execution.core.local_runner import LocalRunner


class TestLocalRunner(unittest.TestCase):

    def setUp(self):
        self.runner = LocalRunner()

    def test_init(self):
        self.assertIsNone(self.runner.spark)
        self.assertIsNone(self.runner.sc)

    @patch("execution.core.local_runner.PYSPARK_AVAILABLE", True)
    @patch("execution.core.local_runner.SparkSession")
    def test_setup_spark_success(self, mock_spark_session):
        mock_builder = Mock()
        mock_spark = Mock()
        mock_sc = Mock()

        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        mock_spark.sparkContext = mock_sc

        self.runner.setup_spark()

        self.assertEqual(self.runner.spark, mock_spark)
        self.assertEqual(self.runner.sc, mock_sc)
        mock_builder.appName.assert_called_once_with("LocalETLRunner")
        mock_builder.master.assert_called_once_with("local[*]")
        mock_sc.setLogLevel.assert_called_once_with("WARN")

    @patch("execution.core.local_runner.PYSPARK_AVAILABLE", False)
    def test_setup_spark_pyspark_not_available(self):
        with self.assertRaises(ImportError) as context:
            self.runner.setup_spark()

        self.assertIn("PySpark not available", str(context.exception))

    def test_cleanup_spark_with_spark_session(self):
        mock_spark = Mock()
        self.runner.spark = mock_spark

        self.runner.cleanup_spark()

        mock_spark.stop.assert_called_once()

    def test_cleanup_spark_without_spark_session(self):
        self.runner.spark = None

        self.runner.cleanup_spark()

    @patch.object(LocalRunner, "setup_spark")
    @patch.object(LocalRunner, "cleanup_spark")
    @patch.object(LocalRunner, "_run_email_job")
    def test_run_job_email_success(self, mock_run_email, mock_cleanup, mock_setup):
        job_type = "email_communication"
        job_args = {"input-bucket": "test-input"}
        expected_result = "email_result"
        mock_run_email.return_value = expected_result

        result = self.runner.run_job(job_type, job_args)

        mock_setup.assert_called_once()
        mock_run_email.assert_called_once_with(job_args)
        mock_cleanup.assert_called_once()
        self.assertEqual(result, expected_result)

    @patch.object(LocalRunner, "setup_spark")
    @patch.object(LocalRunner, "cleanup_spark")
    @patch.object(LocalRunner, "_run_slack_job")
    def test_run_job_slack_success(self, mock_run_slack, mock_cleanup, mock_setup):
        job_type = "slack_communication"
        job_args = {"input-bucket": "test-input"}
        expected_result = "slack_result"
        mock_run_slack.return_value = expected_result

        result = self.runner.run_job(job_type, job_args)

        mock_setup.assert_called_once()
        mock_run_slack.assert_called_once_with(job_args)
        mock_cleanup.assert_called_once()
        self.assertEqual(result, expected_result)

    @patch.object(LocalRunner, "setup_spark")
    @patch.object(LocalRunner, "cleanup_spark")
    @patch.object(LocalRunner, "_run_cleaning_job")
    def test_run_job_cleaning_success(
        self, mock_run_cleaning, mock_cleanup, mock_setup
    ):
        job_type = "data_cleaning"
        job_args = {"input-bucket": "test-input"}
        expected_result = "cleaning_result"
        mock_run_cleaning.return_value = expected_result

        result = self.runner.run_job(job_type, job_args)

        mock_setup.assert_called_once()
        mock_run_cleaning.assert_called_once_with(job_args)
        mock_cleanup.assert_called_once()
        self.assertEqual(result, expected_result)

    @patch.object(LocalRunner, "setup_spark")
    @patch.object(LocalRunner, "cleanup_spark")
    @patch.object(LocalRunner, "_run_sentiment_job")
    def test_run_job_sentiment_success(
        self, mock_run_sentiment, mock_cleanup, mock_setup
    ):
        job_type = "sentiment_analysis"
        job_args = {"input-bucket": "test-input"}
        expected_result = "sentiment_result"
        mock_run_sentiment.return_value = expected_result

        result = self.runner.run_job(job_type, job_args)

        mock_setup.assert_called_once()
        mock_run_sentiment.assert_called_once_with(job_args)
        mock_cleanup.assert_called_once()
        self.assertEqual(result, expected_result)

    @patch.object(LocalRunner, "setup_spark")
    @patch.object(LocalRunner, "cleanup_spark")
    def test_run_job_unknown_type(self, mock_cleanup, mock_setup):
        job_type = "unknown_job"
        job_args = {"input-bucket": "test-input"}

        with self.assertRaises(ValueError) as context:
            self.runner.run_job(job_type, job_args)

        self.assertIn("Unknown job type for local execution", str(context.exception))
        mock_setup.assert_called_once()
        mock_cleanup.assert_called_once()

    @patch.object(LocalRunner, "setup_spark")
    @patch.object(LocalRunner, "cleanup_spark")
    @patch.object(LocalRunner, "_run_email_job")
    def test_run_job_execution_failure(self, mock_run_email, mock_cleanup, mock_setup):
        job_type = "email_communication"
        job_args = {"input-bucket": "test-input"}
        mock_run_email.side_effect = Exception("Job execution failed")

        with self.assertRaises(Exception) as context:
            self.runner.run_job(job_type, job_args)

        self.assertEqual(str(context.exception), "Job execution failed")
        mock_setup.assert_called_once()
        mock_cleanup.assert_called_once()

    @patch("execution.core.local_runner.os.makedirs")
    def test_save_local_output_parquet_success(self, mock_makedirs):
        mock_df = Mock()
        mock_coalesce = Mock()
        mock_write = Mock()
        mock_mode = Mock()

        mock_df.coalesce.return_value = mock_coalesce
        mock_coalesce.write = mock_write
        mock_write.mode.return_value = mock_mode

        job_args = {"output-path": "/test/output"}
        job_suffix = "test"

        self.runner._save_local_output(mock_df, job_args, job_suffix)

        mock_makedirs.assert_called_once_with("/test/output", exist_ok=True)
        mock_df.coalesce.assert_called_once_with(1)
        mock_write.mode.assert_called_once_with("overwrite")
        mock_mode.parquet.assert_called_once_with("/test/output")

    @patch("execution.core.local_runner.os.makedirs")
    def test_save_local_output_parquet_failure_json_fallback(self, mock_makedirs):
        mock_df = Mock()
        mock_coalesce = Mock()
        mock_write = Mock()
        mock_mode = Mock()

        mock_df.coalesce.return_value = mock_coalesce
        mock_coalesce.write = mock_write
        mock_write.mode.return_value = mock_mode
        mock_mode.parquet.side_effect = Exception("Parquet write failed")

        job_args = {"output-path": "/test/output"}
        job_suffix = "test"

        self.runner._save_local_output(mock_df, job_args, job_suffix)

        mock_makedirs.assert_called_once_with("/test/output", exist_ok=True)
        mock_mode.parquet.assert_called_once()
        mock_mode.json.assert_called_once_with("/test/output")

    def test_save_local_output_default_path(self):
        mock_df = Mock()
        mock_coalesce = Mock()
        mock_write = Mock()
        mock_mode = Mock()

        mock_df.coalesce.return_value = mock_coalesce
        mock_coalesce.write = mock_write
        mock_write.mode.return_value = mock_mode

        job_args = {}
        job_suffix = "test"

        with patch("execution.core.local_runner.os.makedirs") as mock_makedirs:
            self.runner._save_local_output(mock_df, job_args, job_suffix)

            mock_makedirs.assert_called_once_with("./output/test", exist_ok=True)

    @patch.object(LocalRunner, "_save_local_output")
    def test_run_email_job(self, mock_save_output):
        mock_spark = Mock()
        mock_df = Mock()
        mock_transformed_df = Mock()

        mock_spark.createDataFrame.return_value = mock_df
        mock_df.selectExpr.return_value = mock_transformed_df
        mock_transformed_df.show = Mock()

        self.runner.spark = mock_spark
        job_args = {"output-path": "/test/output"}

        result = self.runner._run_email_job(job_args)

        mock_spark.createDataFrame.assert_called_once()
        mock_df.selectExpr.assert_called_once()
        mock_save_output.assert_called_once_with(
            mock_transformed_df, job_args, "emails"
        )
        self.assertEqual(result, mock_transformed_df)

    @patch.object(LocalRunner, "_save_local_output")
    def test_run_slack_job(self, mock_save_output):
        mock_spark = Mock()
        mock_df = Mock()
        mock_transformed_df = Mock()

        mock_spark.createDataFrame.return_value = mock_df
        mock_df.selectExpr.return_value = mock_transformed_df
        mock_transformed_df.show = Mock()

        self.runner.spark = mock_spark
        job_args = {"output-path": "/test/output"}

        result = self.runner._run_slack_job(job_args)

        mock_spark.createDataFrame.assert_called_once()
        mock_df.selectExpr.assert_called_once()
        mock_save_output.assert_called_once_with(mock_transformed_df, job_args, "slack")
        self.assertEqual(result, mock_transformed_df)


if __name__ == "__main__":
    unittest.main()
