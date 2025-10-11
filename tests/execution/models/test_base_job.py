import os
import sys
import unittest
from unittest.mock import Mock, patch

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from execution.models.base_job import BaseGlueETLJob
from execution.models.job_metadata import DataSource


class ConcreteGlueETLJob(BaseGlueETLJob):

    def get_expected_schema(self) -> StructType:
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("timestamp", TimestampType(), True),
            ]
        )

    def run(self):
        return Mock()


class TestBaseGlueETLJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        python_exe = sys.executable
        os.environ["PYSPARK_PYTHON"] = python_exe
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
        cls.spark = (
            SparkSession.builder.appName("test_base_job")
            .master("local[1]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.args_to_extract = ["raw_data", "cleaned_data"]

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    def test_init(
        self,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        mock_sc = Mock()
        mock_spark_context_class.return_value = mock_sc

        mock_glue_context = Mock()
        mock_glue_context.spark_session = Mock()
        mock_glue_context_class.return_value = mock_glue_context

        mock_job = Mock()
        mock_job_class.return_value = mock_job

        job = ConcreteGlueETLJob(self.args_to_extract)

        expected_args = self.args_to_extract + ["JOB_NAME"]
        mock_get_resolved_options.assert_called_once_with(
            unittest.mock.ANY, expected_args
        )
        self.assertEqual(job.args, mock_args)
        mock_spark_context_class.assert_called_once()
        mock_glue_context_class.assert_called_once_with(mock_sc)
        mock_job_class.assert_called_once_with(mock_glue_context)

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    def test_commit(
        self,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        mock_job = Mock()
        mock_job_class.return_value = mock_job

        job = ConcreteGlueETLJob(self.args_to_extract)

        job.commit()

        mock_job.commit.assert_called_once()

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    def test_cleanup(
        self,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        mock_sc = Mock()
        mock_spark_context_class.return_value = mock_sc

        job = ConcreteGlueETLJob(self.args_to_extract)

        job.cleanup()

        mock_sc.stop.assert_called_once()

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    @patch("execution.models.base_job.current_timestamp")
    @patch("execution.models.base_job.lit")
    def test_add_base_fields(
        self,
        mock_lit,
        mock_current_timestamp,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        job = ConcreteGlueETLJob(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()
        mock_df.withColumns.return_value = mock_result_df

        data_source = DataSource.ENRON
        dataset_name = "test_dataset"

        with patch("execution.models.base_job.datetime") as mock_datetime:
            mock_date = Mock()
            mock_date.date.return_value = "2024-01-01"
            mock_datetime.now.return_value = mock_date

            result = job.add_base_fields(mock_df, data_source, dataset_name)

        self.assertEqual(result, mock_result_df)
        mock_df.withColumns.assert_called_once()

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    def test_generate_record_id(
        self,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        job = ConcreteGlueETLJob(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()
        mock_df.withColumn.return_value = mock_result_df

        with (
            patch("pyspark.sql.functions.concat") as mock_concat,
            patch("pyspark.sql.functions.monotonically_increasing_id") as mock_mono_id,
            patch("pyspark.sql.functions.lit") as mock_lit,
        ):
            mock_column = Mock()
            mock_mono_id.return_value.cast.return_value = mock_column
            mock_lit.return_value = mock_column
            mock_concat.return_value = mock_column

            result = job.generate_record_id(mock_df, "TEST")

            self.assertEqual(result, mock_result_df)
            mock_df.withColumn.assert_called_once_with("record_id", unittest.mock.ANY)

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    def test_validate_schema(
        self,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        job = ConcreteGlueETLJob(self.args_to_extract)

        mock_df = Mock()
        expected_schema = StructType([StructField("id", StringType(), True)])

        mock_validation_result = {"is_valid": True, "errors": [], "warnings": []}
        job.schema_validator.validate_schema = Mock(return_value=mock_validation_result)

        result = job.validate_schema(mock_df, expected_schema)

        self.assertEqual(result, mock_validation_result)
        job.schema_validator.validate_schema.assert_called_once_with(
            mock_df, expected_schema
        )

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    @patch("execution.models.base_job.lit")
    def test_add_quality_flags(
        self,
        mock_lit,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        job = ConcreteGlueETLJob(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()
        mock_df.withColumns.return_value = mock_result_df

        result = job.add_quality_flags(mock_df)

        self.assertEqual(result, mock_result_df)
        mock_df.withColumns.assert_called_once()

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    @patch("execution.models.base_job.lit")
    def test_add_partitions_to_dataframe(
        self,
        mock_lit,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        job = ConcreteGlueETLJob(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()
        mock_df.withColumns.return_value = mock_result_df

        partitions = {"year": "2024", "month": "01"}
        source_key = "test/source/key"

        result = job.add_partitions_to_dataframe(mock_df, partitions, source_key)

        self.assertEqual(result, mock_result_df)
        mock_df.withColumns.assert_called_once()

    @patch("execution.models.base_job.GlueContext")
    @patch("execution.models.base_job.SparkContext")
    @patch("execution.models.base_job.Job")
    @patch("awsglue.utils.getResolvedOptions")
    def test_select_expected_fields(
        self,
        mock_get_resolved_options,
        mock_job_class,
        mock_spark_context_class,
        mock_glue_context_class,
    ):
        mock_args = {
            "JOB_NAME": "test-job",
            "raw_data": "bucket1",
            "cleaned_data": "bucket2",
        }
        mock_get_resolved_options.return_value = mock_args

        job = ConcreteGlueETLJob(self.args_to_extract)

        mock_df = Mock()
        mock_result_df = Mock()
        job.schema_validator.select_expected_fields = Mock(return_value=mock_result_df)

        result = job.select_expected_fields(mock_df)

        self.assertEqual(result, mock_result_df)
        expected_schema = job.get_expected_schema()
        job.schema_validator.select_expected_fields.assert_called_once_with(
            mock_df, expected_schema
        )

    def test_get_expected_schema_abstract_implementation(self):
        with (
            patch("execution.models.base_job.GlueContext"),
            patch("execution.models.base_job.SparkContext"),
            patch("execution.models.base_job.Job"),
            patch("awsglue.utils.getResolvedOptions") as mock_get_resolved_options,
        ):

            mock_args = {
                "JOB_NAME": "test-job",
                "raw_data": "bucket1",
                "cleaned_data": "bucket2",
            }
            mock_get_resolved_options.return_value = mock_args

            job = ConcreteGlueETLJob(self.args_to_extract)
            schema = job.get_expected_schema()

        self.assertIsInstance(schema, StructType)
        self.assertEqual(len(schema.fields), 3)
        self.assertEqual(schema.fields[0].name, "id")
        self.assertEqual(schema.fields[1].name, "name")
        self.assertEqual(schema.fields[2].name, "timestamp")


if __name__ == "__main__":
    unittest.main()
