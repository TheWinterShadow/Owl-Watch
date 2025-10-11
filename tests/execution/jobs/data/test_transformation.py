import os
import sys
import unittest
from unittest.mock import Mock, patch

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from execution.jobs.data.transformation import DataTransformationETL


class TestableDataTransformationETL(DataTransformationETL):

    def __init__(self, args_to_extract, call_super=False):
        if call_super:
            super().__init__(args_to_extract)
        else:
            self.args_to_extract = args_to_extract
            self.args = {}

    def get_expected_schema(self) -> StructType:
        return StructType(
            [
                StructField("record_id", StringType(), False),
                StructField("data", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )

    def run(self) -> DataFrame:
        return super().run()


class TestDataTransformationETL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        python_exe = sys.executable
        os.environ["PYSPARK_PYTHON"] = python_exe
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
        cls.spark = (
            SparkSession.builder.appName("test_data_transformation")
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
        self.args_to_extract = ["input-bucket", "output-bucket", "transformation-type"]

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_init(self, mock_super_init):
        mock_super_init.return_value = None

        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            TestableDataTransformationETL(self.args_to_extract, call_super=True)

        mock_super_init.assert_called_once_with(self.args_to_extract)

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_run_missing_buckets(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)
            job.args = {}

            with self.assertRaises(ValueError) as context:
                job.run()

            self.assertIn(
                "Both input-bucket and output-bucket must be specified",
                str(context.exception),
            )

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_run_communication_transformation(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)
            job.args = {
                "input-bucket": "test-input",
                "output-bucket": "test-output",
                "transformation-type": "communication",
            }

            mock_df = Mock()
            mock_transformed_df = Mock()
            mock_df.count.return_value = 100
            mock_transformed_df.count.return_value = 95
            mock_transformed_df.write.mode.return_value.parquet.return_value = None

            with patch.object(job, "_read_input_data", return_value=mock_df):
                with patch.object(
                    job,
                    "_transform_communication_data",
                    return_value=mock_transformed_df,
                ):
                    result = job.run()

                    self.assertIsNotNone(result)

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_run_user_profile_transformation(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)
            job.args = {
                "input-bucket": "test-input",
                "output-bucket": "test-output",
                "transformation-type": "user_profile",
            }

            mock_df = Mock()
            mock_transformed_df = Mock()
            mock_df.count.return_value = 50
            mock_transformed_df.count.return_value = 50

            with patch.object(job, "_read_input_data", return_value=mock_df):
                with patch.object(
                    job,
                    "_transform_user_profile_data",
                    return_value=mock_transformed_df,
                ):
                    result = job.run()

                    self.assertIsNotNone(result)

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_run_temporal_transformation(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)
            job.args = {
                "input-bucket": "test-input",
                "output-bucket": "test-output",
                "transformation-type": "temporal",
            }

            mock_df = Mock()
            mock_transformed_df = Mock()
            mock_df.count.return_value = 200
            mock_transformed_df.count.return_value = 200

            with patch.object(job, "_read_input_data", return_value=mock_df):
                with patch.object(
                    job, "_transform_temporal_data", return_value=mock_transformed_df
                ):
                    result = job.run()

                    self.assertIsNotNone(result)

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_run_standard_transformation(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)
            job.args = {
                "input-bucket": "test-input",
                "output-bucket": "test-output",
            }

            mock_df = Mock()
            mock_transformed_df = Mock()
            mock_df.count.return_value = 150
            mock_transformed_df.count.return_value = 145

            with patch.object(job, "_read_input_data", return_value=mock_df):
                with patch.object(
                    job,
                    "_apply_standard_transformation",
                    return_value=mock_transformed_df,
                ):
                    result = job.run()

                    self.assertIsNotNone(result)

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_read_input_data_parquet_success(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)

            mock_spark = Mock()
            mock_df = Mock()
            mock_spark.read.parquet.return_value = mock_df
            job.spark = mock_spark

            result = job._read_input_data("test-bucket")

            self.assertEqual(result, mock_df)
            mock_spark.read.parquet.assert_called_once_with("s3://test-bucket/")

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_read_input_data_json_fallback(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)

            mock_spark = Mock()
            mock_df = Mock()
            mock_spark.read.parquet.side_effect = Exception("Parquet read failed")
            mock_spark.read.json.return_value = mock_df
            job.spark = mock_spark

            result = job._read_input_data("test-bucket")

            self.assertEqual(result, mock_df)
            mock_spark.read.json.assert_called_once_with("s3://test-bucket/")

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_read_input_data_csv_fallback(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)

            mock_spark = Mock()
            mock_df = Mock()
            mock_spark.read.parquet.side_effect = Exception("Parquet read failed")
            mock_spark.read.json.side_effect = Exception("JSON read failed")
            mock_spark.read.option.return_value.csv.return_value = mock_df
            job.spark = mock_spark

            result = job._read_input_data("test-bucket")

            self.assertEqual(result, mock_df)
            mock_spark.read.option.assert_called_once_with("header", "true")

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_transform_communication_data(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)

            mock_df = Mock()
            mock_result_df = Mock()

            with patch.object(
                job, "_transform_communication_data", return_value=mock_result_df
            ) as mock_transform:
                result = job._transform_communication_data(mock_df)

                self.assertEqual(result, mock_result_df)
                mock_transform.assert_called_once_with(mock_df)

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_transform_user_profile_data(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)

            mock_df = Mock()
            mock_result_df = Mock()

            with patch.object(
                job, "_transform_user_profile_data", return_value=mock_result_df
            ) as mock_transform:
                result = job._transform_user_profile_data(mock_df)

                self.assertEqual(result, mock_result_df)
                mock_transform.assert_called_once_with(mock_df)

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_transform_temporal_data_missing_timestamp(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)

            mock_df = Mock()
            mock_df.columns = ["id", "name", "data"]

            with self.assertRaises(ValueError) as context:
                job._transform_temporal_data(mock_df)

            self.assertIn(
                "Temporal transformation requires 'timestamp' column",
                str(context.exception),
            )

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    def test_transform_temporal_data_success(self, mock_super_init):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)

            mock_df = Mock()
            mock_result_df = Mock()

            with patch.object(
                job, "_transform_temporal_data", return_value=mock_result_df
            ) as mock_transform:
                result = job._transform_temporal_data(mock_df)

                self.assertEqual(result, mock_result_df)
                mock_transform.assert_called_once_with(mock_df)

    @patch("execution.jobs.data.transformation.BaseGlueETLJob.__init__")
    @patch("execution.jobs.data.transformation.col")
    @patch("execution.jobs.data.transformation.lit")
    @patch("execution.jobs.data.transformation.hash")
    def test_apply_standard_transformation(
        self, mock_hash, mock_lit, mock_col, mock_super_init
    ):
        mock_super_init.return_value = None
        with patch.multiple(DataTransformationETL, __abstractmethods__=set()):
            job = TestableDataTransformationETL(self.args_to_extract)

            mock_df = Mock()
            mock_df.columns = ["id", "name", "data"]
            mock_result_df = Mock()
            mock_df.withColumn.return_value = mock_result_df
            mock_result_df.withColumn.return_value = mock_result_df
            mock_result_df.dropDuplicates.return_value = mock_result_df

            job.spark = Mock()
            mock_timestamp = Mock()
            mock_row = Mock()
            mock_row.__getitem__ = Mock(return_value=mock_timestamp)
            job.spark.sql.return_value.collect.return_value = [mock_row]

            result = job._apply_standard_transformation(mock_df)

            self.assertEqual(result, mock_result_df)
            mock_result_df.dropDuplicates.assert_called_once_with(["record_hash"])


if __name__ == "__main__":
    unittest.main()
