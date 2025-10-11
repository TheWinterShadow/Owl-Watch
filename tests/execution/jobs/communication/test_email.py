import unittest
from unittest.mock import Mock, patch

from execution.jobs.communication.email import EmailETL
from execution.models.job_metadata import DataSource


class TestEmailETL(unittest.TestCase):

    def setUp(self):
        self.args_to_extract = ["raw_data", "cleaned_data"]

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    @patch("execution.jobs.communication.email.logger")
    def test_init(self, mock_logger, mock_super_init):
        mock_super_init.return_value = None

        EmailETL(self.args_to_extract)

        mock_super_init.assert_called_once_with(self.args_to_extract)
        mock_logger.secure_debug.assert_called()

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    @patch("execution.jobs.communication.email.COMMUNICATION_SCHEMA")
    def test_get_expected_schema(self, mock_schema, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)

        result = job.get_expected_schema()

        self.assertEqual(result, mock_schema)

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    def test_run_missing_buckets(self, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)
        job.args = {}

        with self.assertRaises(ValueError) as context:
            job.run()

        self.assertIn(
            "Both raw_data and cleaned_data must be specified", str(context.exception)
        )

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    @patch("execution.jobs.communication.email.logger")
    def test_run_success(self, mock_logger, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)
        job.args = {
            "raw_data": "test-raw-bucket",
            "cleaned_data": "test-cleaned-bucket",
        }

        mock_spark = Mock()
        mock_df = Mock()
        mock_result_df = Mock()

        mock_spark.read.parquet.return_value = mock_df
        mock_df.count.return_value = 1000
        mock_result_df.count.return_value = 950

        job.spark = mock_spark

        job.add_base_fields = Mock(return_value=mock_df)
        job.generate_record_id = Mock(return_value=mock_df)
        job._apply_source_mappings = Mock(return_value=mock_df)
        job.add_quality_flags = Mock(return_value=mock_df)
        job.validate_schema = Mock(return_value={"is_valid": True})
        job.select_expected_fields = Mock(return_value=mock_result_df)
        job.get_expected_schema = Mock(return_value=Mock())

        result = job.run()

        self.assertEqual(result, mock_result_df)
        mock_spark.read.parquet.assert_called_once_with("s3://test-raw-bucket/emails/")
        job.add_base_fields.assert_called_once_with(mock_df, DataSource.ENRON, "emails")
        job.generate_record_id.assert_called_once_with(mock_df, "EMAIL")

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    def test_apply_source_mappings_enron(self, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()
        job._map_enron_fields = Mock(return_value=mock_result_df)

        result = job._apply_source_mappings(mock_df, DataSource.ENRON)

        self.assertEqual(result, mock_result_df)
        job._map_enron_fields.assert_called_once_with(mock_df)

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    def test_apply_source_mappings_clinton(self, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()
        job._map_clinton_fields = Mock(return_value=mock_result_df)

        result = job._apply_source_mappings(mock_df, DataSource.CLINTON_EMAILS)

        self.assertEqual(result, mock_result_df)
        job._map_clinton_fields.assert_called_once_with(mock_df)

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    def test_apply_source_mappings_biden(self, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()
        job._map_biden_fields = Mock(return_value=mock_result_df)

        result = job._apply_source_mappings(mock_df, DataSource.BIDEN_EMAILS)

        self.assertEqual(result, mock_result_df)
        job._map_biden_fields.assert_called_once_with(mock_df)

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    def test_apply_source_mappings_unknown_source(self, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)

        mock_df = Mock()

        with self.assertRaises(ValueError) as context:
            job._apply_source_mappings(mock_df, "unknown_source")

        self.assertIn("Unknown source", str(context.exception))

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    @patch("execution.jobs.communication.email.udf")
    @patch("execution.jobs.communication.email.lit")
    @patch("execution.jobs.communication.email.col")
    def test_map_enron_fields(self, mock_col, mock_lit, mock_udf, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)

        mock_df = Mock()
        mock_parsed_df = Mock()
        mock_result_df = Mock()

        mock_df.__getitem__ = Mock(return_value=Mock())

        mock_udf_instance = Mock()
        mock_udf.return_value = mock_udf_instance

        mock_df.withColumn.return_value = mock_parsed_df
        mock_parsed_df.select.return_value = mock_result_df

        mock_result_df.withColumnRenamed.return_value = mock_result_df
        mock_result_df.withColumn.return_value = mock_result_df

        result = job._map_enron_fields(mock_df)

        self.assertEqual(result, mock_result_df)
        mock_udf.assert_called_once()
        mock_df.withColumn.assert_called_once()

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    @patch("execution.jobs.communication.email.logger")
    def test_map_clinton_fields(self, mock_logger, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["col1", "col2"]

        result = job._map_clinton_fields(mock_df)

        self.assertEqual(result, mock_df)
        mock_logger.secure_debug.assert_called()

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    @patch("execution.jobs.communication.email.logger")
    def test_map_biden_fields(self, mock_logger, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["col1", "col2"]

        result = job._map_biden_fields(mock_df)

        self.assertEqual(result, mock_df)
        mock_logger.secure_debug.assert_called()

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    @patch("execution.jobs.communication.email.logger")
    def test_run_with_exception_handling(self, mock_logger, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)
        job.args = {
            "raw_data": "test-raw-bucket",
            "cleaned_data": "test-cleaned-bucket",
        }

        mock_spark = Mock()
        mock_spark.read.parquet.side_effect = Exception("Processing error")
        job.spark = mock_spark

        with self.assertRaises(Exception):
            job.run()

        mock_logger.secure_error.assert_called()

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    @patch("execution.jobs.communication.email.logger")
    def test_run_with_schema_validation_failure(self, mock_logger, mock_super_init):
        mock_super_init.return_value = None
        job = EmailETL(self.args_to_extract)
        job.args = {
            "raw_data": "test-raw-bucket",
            "cleaned_data": "test-cleaned-bucket",
        }

        mock_spark = Mock()
        mock_df = Mock()

        mock_spark.read.parquet.return_value = mock_df
        job.spark = mock_spark

        job.add_base_fields = Mock(return_value=mock_df)
        job.generate_record_id = Mock(return_value=mock_df)
        job._apply_source_mappings = Mock(return_value=mock_df)
        job.add_quality_flags = Mock(return_value=mock_df)
        job.validate_schema = Mock(
            return_value={"is_valid": False, "errors": ["Schema error"]}
        )
        job.get_expected_schema = Mock(return_value=Mock())

        with self.assertRaises(ValueError) as context:
            job.run()

        self.assertIn("Schema validation failed", str(context.exception))


if __name__ == "__main__":
    unittest.main()
