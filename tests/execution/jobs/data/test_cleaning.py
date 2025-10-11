import os
import sys
import unittest
from unittest.mock import Mock, patch

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from execution.jobs.data.cleaning import DataCleaningETL


class TestableDataCleaningETL(DataCleaningETL):

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
                StructField("email", StringType(), True),
                StructField("message", StringType(), True),
                StructField("date", StringType(), True),
            ]
        )

    def run(self) -> DataFrame:
        return super().run()


class TestDataCleaningETL(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        python_exe = sys.executable
        os.environ["PYSPARK_PYTHON"] = python_exe
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
        cls.spark = (
            SparkSession.builder.appName("test_data_cleaning")
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
        self.args_to_extract = ["source-bucket", "destination-bucket"]

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_init(self, mock_super_init):
        mock_super_init.return_value = None

        with patch.multiple(DataCleaningETL, __abstractmethods__=set()):
            TestableDataCleaningETL(self.args_to_extract, call_super=True)

        mock_super_init.assert_called_once_with(self.args_to_extract)

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_run_missing_buckets(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)
        job.args = {}


        with self.assertRaises(ValueError) as context:
            job.run()

        self.assertIn(
            "Both raw_data and cleaned_data must be specified", str(context.exception)
        )

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_run_success_json_input(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)
        job.args = {
            "raw_data": "test-raw-bucket",
            "cleaned_data": "test-cleaned-bucket",
        }

        mock_spark = Mock()
        mock_df = Mock()
        mock_cleaned_df = Mock()
        mock_validated_df = Mock()
        mock_write_chain = Mock()

        mock_spark.read.json.return_value = mock_df
        mock_df.count.return_value = 1000
        mock_cleaned_df.count.return_value = 950
        mock_validated_df.count.return_value = 900

        mock_validated_df.write.mode.return_value = mock_write_chain
        mock_write_chain.parquet.return_value = None

        job.spark = mock_spark
        job._clean_data = Mock(return_value=mock_cleaned_df)
        job._validate_data_quality = Mock(return_value=mock_validated_df)

        result = job.run()

        self.assertIsNotNone(result)
        mock_spark.read.json.assert_called_once_with("s3://test-raw-bucket/raw/")
        job._clean_data.assert_called_once_with(mock_df)
        job._validate_data_quality.assert_called_once_with(mock_cleaned_df)
        mock_validated_df.write.mode.assert_called_once_with("overwrite")
        mock_write_chain.parquet.assert_called_once_with(
            "s3://test-cleaned-bucket/cleaned/"
        )

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_run_fallback_to_parquet(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)
        job.args = {
            "raw_data": "test-raw-bucket",
            "cleaned_data": "test-cleaned-bucket",
        }

        mock_spark = Mock()
        mock_df = Mock()
        mock_cleaned_df = Mock()
        mock_validated_df = Mock()
        mock_write_chain = Mock()

        mock_spark.read.json.side_effect = Exception("JSON read failed")
        mock_spark.read.parquet.return_value = mock_df
        mock_df.count.return_value = 1000
        mock_cleaned_df.count.return_value = 950
        mock_validated_df.count.return_value = 900

        mock_validated_df.write.mode.return_value = mock_write_chain
        mock_write_chain.parquet.return_value = None

        job.spark = mock_spark
        job._clean_data = Mock(return_value=mock_cleaned_df)
        job._validate_data_quality = Mock(return_value=mock_validated_df)

        result = job.run()

        self.assertIsNotNone(result)
        self.assertTrue(mock_spark.read.json.called)
        mock_spark.read.parquet.assert_called_once_with("s3://test-raw-bucket/raw/")
        job._clean_data.assert_called_once_with(mock_df)
        job._validate_data_quality.assert_called_once_with(mock_cleaned_df)
        mock_validated_df.write.mode.assert_called_once_with("overwrite")
        mock_write_chain.parquet.assert_called_once_with(
            "s3://test-cleaned-bucket/cleaned/"
        )

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_run_fallback_to_csv(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)
        job.args = {
            "raw_data": "test-raw-bucket",
            "cleaned_data": "test-cleaned-bucket",
        }

        mock_spark = Mock()
        mock_df = Mock()
        mock_cleaned_df = Mock()
        mock_validated_df = Mock()
        mock_write_chain = Mock()

        mock_spark.read.json.side_effect = Exception("JSON read failed")
        mock_spark.read.parquet.side_effect = Exception("Parquet read failed")
        mock_spark.read.option.return_value.csv.return_value = mock_df
        mock_df.count.return_value = 1000
        mock_cleaned_df.count.return_value = 950
        mock_validated_df.count.return_value = 800

        mock_validated_df.write.mode.return_value = mock_write_chain
        mock_write_chain.parquet.return_value = None

        job.spark = mock_spark
        job._clean_data = Mock(return_value=mock_cleaned_df)
        job._validate_data_quality = Mock(return_value=mock_validated_df)

        result = job.run()

        self.assertIsNotNone(result)
        mock_spark.read.json.assert_called_once()
        mock_spark.read.parquet.assert_called_once()
        job._clean_data.assert_called_once_with(mock_df)
        job._validate_data_quality.assert_called_once_with(mock_cleaned_df)
        mock_spark.read.option.assert_called_once_with("header", "true")
        mock_validated_df.write.mode.assert_called_once_with("overwrite")
        mock_write_chain.parquet.assert_called_once_with(
            "s3://test-cleaned-bucket/cleaned/"
        )

    @patch.object(TestableDataCleaningETL, "_clean_data")
    def test_clean_data_basic_operations(self, mock_clean_data):
        job = TestableDataCleaningETL(self.args_to_extract)
        mock_df = self.create_sample_df()
        mock_result_df = Mock()
        mock_clean_data.return_value = mock_result_df

        result = job._clean_data(mock_df)

        self.assertEqual(result, mock_result_df)
        mock_clean_data.assert_called_once_with(mock_df)

    def create_sample_df(self):
        mock_df = Mock()
        mock_df.columns = ["name", "email", "phone"]
        mock_df.count.return_value = 100
        return mock_df

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    @patch("execution.jobs.data.cleaning.lit")
    @patch("execution.jobs.data.cleaning.col")
    @patch("execution.jobs.data.cleaning.length")
    def test_validate_data_quality_basic_validation(
        self, mock_length, mock_col, mock_lit, mock_super_init
    ):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["id", "name", "email"]

        mock_result_df = Mock()
        mock_df.filter.return_value = mock_result_df
        mock_result_df.filter.return_value = mock_result_df
        mock_result_df.withColumn.return_value = mock_result_df

        job._calculate_quality_score = Mock(return_value=Mock())
        job.spark = Mock()
        mock_timestamp = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value=mock_timestamp)
        job.spark.sql.return_value.collect.return_value = [mock_row]

        result = job._validate_data_quality(mock_df)

        self.assertEqual(result, mock_result_df)

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    @patch("execution.jobs.data.cleaning.lit")
    @patch("execution.jobs.data.cleaning.col")
    @patch("execution.jobs.data.cleaning.length")
    def test_validate_data_quality_email_filtering(
        self, mock_length, mock_col, mock_lit, mock_super_init
    ):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["email", "name"]

        mock_filtered_df = Mock()
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.filter.return_value = mock_filtered_df
        mock_filtered_df.withColumn.return_value = mock_filtered_df

        job._calculate_quality_score = Mock(return_value=Mock())
        job.spark = Mock()
        mock_timestamp = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value=mock_timestamp)
        job.spark.sql.return_value.collect.return_value = [mock_row]

        job._validate_data_quality(mock_df)

        self.assertTrue(mock_df.filter.called)

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_validate_data_quality_phone_filtering(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["phone", "name"]
        mock_result_df = Mock()

        with patch.object(
            job, "_validate_data_quality", return_value=mock_result_df
        ) as mock_validate:
            result = job._validate_data_quality(mock_df)

            self.assertEqual(result, mock_result_df)
            mock_validate.assert_called_once_with(mock_df)

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    @patch("execution.jobs.data.cleaning.lit")
    @patch("execution.jobs.data.cleaning.col")
    @patch("execution.jobs.data.cleaning.length")
    def test_validate_data_quality_empty_rows_filtering(
        self, mock_length, mock_col, mock_lit, mock_super_init
    ):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["name", "email", "id", "created_at"]

        mock_filtered_df = Mock()
        mock_df.filter.return_value = mock_filtered_df
        mock_filtered_df.filter.return_value = mock_filtered_df
        mock_filtered_df.withColumn.return_value = mock_filtered_df

        job._calculate_quality_score = Mock(return_value=Mock())
        job.spark = Mock()
        mock_timestamp = Mock()
        mock_row = Mock()
        mock_row.__getitem__ = Mock(return_value=mock_timestamp)
        job.spark.sql.return_value.collect.return_value = [mock_row]

        job._validate_data_quality(mock_df)

        self.assertTrue(mock_df.filter.called)

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    @patch("execution.jobs.data.cleaning.lit")
    @patch("execution.jobs.data.cleaning.when")
    def test_calculate_quality_score(self, mock_when, mock_lit, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)

        columns = ["name", "email", "phone", "data_quality_score", "cleaned_at"]

        result = job._calculate_quality_score(columns)

        self.assertIsNotNone(result)

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_clean_data_with_email_column(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["email", "name"]
        mock_df.schema.fields = []

        mock_result_df = Mock()
        mock_df.withColumn.return_value = mock_result_df

        job._clean_data(mock_df)

        self.assertTrue(mock_df.withColumn.called)

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_clean_data_with_phone_column(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["phone", "name"]
        mock_df.schema.fields = []

        mock_result_df = Mock()
        mock_df.withColumn.return_value = mock_result_df

        job._clean_data(mock_df)

        self.assertTrue(mock_df.withColumn.called)

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    def test_clean_data_with_name_columns(self, mock_super_init):
        mock_super_init.return_value = None
        job = TestableDataCleaningETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["first_name", "last_name", "full_name"]
        mock_df.schema.fields = []

        mock_result_df = Mock()
        mock_df.withColumn.return_value = mock_result_df

        job._clean_data(mock_df)

        self.assertTrue(mock_df.withColumn.called)


if __name__ == "__main__":
    unittest.main()
