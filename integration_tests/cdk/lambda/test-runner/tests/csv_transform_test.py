"""
CSV Transform Integration Test

This test validates the CSV to JSON/Parquet transformation pipeline.
"""

import json
import time
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class CSVTransformTest:
    """Test for CSV to JSON/Parquet transformation pipeline."""

    def __init__(self, runner, test_id: str, timeout_seconds: int = 900):
        """
        Initialize the CSV transform test.

        Args:
            runner: IntegrationTestRunner instance
            test_id: Unique identifier for this test run
            timeout_seconds: Maximum time to wait for pipeline completion
        """
        self.runner = runner
        self.test_id = test_id
        self.timeout_seconds = timeout_seconds

        # Test-specific configuration
        self.input_key = f"test-data/{test_id}/input_data.csv"
        self.expected_json_key = f"test-data/{test_id}/expected_output.json"
        self.expected_parquet_key = f"test-data/{test_id}/expected_output.parquet"

        # Expected output locations (TODO: Adjust based on your Glue job output structure)
        # TODO: Update with actual output prefix
        self.output_json_prefix = f"processed/{test_id}/"
        # TODO: Update with actual output prefix
        self.output_parquet_prefix = f"processed/{test_id}/"

        # Test data and results
        self.test_details = {
            'test_id': test_id,
            'input_key': self.input_key,
            'glue_job_name': runner.glue_job_name_csv,
            'timeout_seconds': timeout_seconds
        }

        logger.info(f"Initialized CSV transform test: {test_id}")

    def run(self) -> None:
        """
        Execute the CSV transform test.

        This test:
        1. Uploads test CSV data to S3
        2. Waits for Glue job to process the data
        3. Validates the JSON and Parquet outputs
        4. Performs data quality checks
        """
        try:
            # Step 1: Upload test data
            self._upload_test_data()

            # Step 2: Wait for Glue job to process the data
            self._wait_for_pipeline_completion()

            # Step 3: Validate outputs
            self._validate_json_output()
            self._validate_parquet_output()

            # Step 4: Perform data quality checks
            self._validate_data_quality()

            logger.info(
                f"CSV transform test {self.test_id} completed successfully")

        except Exception as e:
            logger.error(f"CSV transform test {self.test_id} failed: {str(e)}")
            raise

    def _upload_test_data(self) -> None:
        """Upload test CSV data to S3 input bucket."""
        # TODO: Customize this CSV structure based on your data requirements
        test_csv_data = self._generate_test_csv_data()

        logger.info(
            f"Uploading test CSV data to s3://{self.runner.input_bucket}/{self.input_key}")

        self.runner.s3_client.put_object(
            Bucket=self.runner.input_bucket,
            Key=self.input_key,
            Body=test_csv_data,
            ContentType='text/csv'
        )

        self.test_details['input_uploaded_at'] = time.time()
        self.test_details['input_size_bytes'] = len(
            test_csv_data.encode('utf-8'))

        logger.info(
            f"Uploaded {self.test_details['input_size_bytes']} bytes of test data")

    def _generate_test_csv_data(self) -> str:
        """
        Generate test CSV data.

        TODO: Customize this method to generate CSV data that matches your pipeline's expected input format.

        Returns:
            CSV data as string
        """
        # Sample CSV data - TODO: Replace with your actual data structure
        csv_data = """id,timestamp,sensor_type,value,location,quality_score
1,2025-10-12T10:00:00Z,temperature,23.5,sensor_001,0.95
2,2025-10-12T10:01:00Z,humidity,45.2,sensor_002,0.92
3,2025-10-12T10:02:00Z,pressure,1013.25,sensor_003,0.98
4,2025-10-12T10:03:00Z,temperature,24.1,sensor_004,0.89
5,2025-10-12T10:04:00Z,humidity,43.8,sensor_005,0.96
6,2025-10-12T10:05:00Z,pressure,1012.85,sensor_006,0.91
7,2025-10-12T10:06:00Z,temperature,22.8,sensor_007,0.87
8,2025-10-12T10:07:00Z,humidity,48.1,sensor_008,0.94
9,2025-10-12T10:08:00Z,pressure,1014.02,sensor_009,0.93
10,2025-10-12T10:09:00Z,temperature,25.3,sensor_010,0.97"""

        return csv_data

    def _wait_for_pipeline_completion(self) -> None:
        """Wait for the ETL pipeline to complete processing."""
        logger.info(
            f"Waiting for Glue job {self.runner.glue_job_name_csv} to complete...")

        start_time = time.time()

        # Wait a moment for the S3 trigger to start the Glue job
        time.sleep(10)

        try:
            # Wait for Glue job completion
            job_run = self.runner.wait_for_glue_job_completion(
                job_name=self.runner.glue_job_name_csv,
                timeout_seconds=self.timeout_seconds
            )

            self.test_details['glue_job_run_id'] = job_run['Id']
            self.test_details['glue_job_status'] = job_run['JobRunState']
            self.test_details['glue_job_duration'] = time.time() - start_time

            if job_run['JobRunState'] != 'SUCCEEDED':
                error_message = job_run.get('ErrorMessage', 'Unknown error')
                raise Exception(
                    f"Glue job failed with status {job_run['JobRunState']}: {error_message}")

            logger.info(
                f"Glue job completed successfully in {self.test_details['glue_job_duration']:.2f} seconds")

        except Exception as e:
            logger.error(f"Pipeline completion check failed: {str(e)}")
            raise

    def _validate_json_output(self) -> None:
        """Validate the JSON output from the ETL pipeline."""
        logger.info("Validating JSON output...")

        # TODO: Adjust the output key pattern based on your Glue job's output structure
        json_objects = self._list_output_objects(
            self.output_json_prefix, '.json')

        if not json_objects:
            raise AssertionError(
                f"No JSON output files found with prefix {self.output_json_prefix}")

        # Validate the first JSON file (adjust if multiple files expected)
        json_key = json_objects[0]
        json_content = self._download_output_file(json_key)

        try:
            json_data = json.loads(json_content)
        except json.JSONDecodeError as e:
            raise AssertionError(f"Invalid JSON output: {str(e)}")

        # TODO: Add specific validation logic for your JSON structure
        self._validate_json_structure(json_data)

        self.test_details['json_output_key'] = json_key
        self.test_details['json_record_count'] = len(
            json_data) if isinstance(json_data, list) else 1

        logger.info(
            f"JSON validation passed: {self.test_details['json_record_count']} records")

    def _validate_parquet_output(self) -> None:
        """Validate the Parquet output from the ETL pipeline."""
        logger.info("Validating Parquet output...")

        # TODO: Adjust the output key pattern based on your Glue job's output structure
        parquet_objects = self._list_output_objects(
            self.output_parquet_prefix, '.parquet')

        if not parquet_objects:
            raise AssertionError(
                f"No Parquet output files found with prefix {self.output_parquet_prefix}")

        # Validate the first Parquet file (adjust if multiple files expected)
        parquet_key = parquet_objects[0]

        # For Parquet validation, we'll check if the file exists and has content
        # TODO: Add more sophisticated Parquet validation if needed
        try:
            response = self.runner.s3_client.head_object(
                Bucket=self.runner.output_bucket,
                Key=parquet_key
            )
            file_size = response['ContentLength']

            if file_size <= 0:
                raise AssertionError(f"Parquet file {parquet_key} is empty")

        except Exception as e:
            raise AssertionError(f"Parquet validation failed: {str(e)}")

        self.test_details['parquet_output_key'] = parquet_key
        self.test_details['parquet_file_size'] = file_size

        logger.info(f"Parquet validation passed: {file_size} bytes")

    def _validate_data_quality(self) -> None:
        """Perform data quality checks on the output."""
        logger.info("Performing data quality checks...")

        # TODO: Customize these quality checks based on your requirements
        quality_checks = {
            'output_files_exist': True,
            'json_valid': True,
            'parquet_valid': True,
            'record_count_acceptable': self.test_details.get('json_record_count', 0) > 0
        }

        # TODO: Add more sophisticated quality checks:
        # - Schema validation
        # - Data type validation
        # - Business rule validation
        # - Completeness checks

        failed_checks = [check for check,
                         passed in quality_checks.items() if not passed]

        if failed_checks:
            raise AssertionError(
                f"Data quality checks failed: {failed_checks}")

        self.test_details['quality_checks'] = quality_checks
        logger.info("Data quality validation passed")

    def _validate_json_structure(self, json_data: Any) -> None:
        """
        Validate the structure of JSON output.

        TODO: Customize this method based on your expected JSON structure.

        Args:
            json_data: Parsed JSON data
        """
        # Example validation - TODO: Replace with your actual validation logic
        if isinstance(json_data, list):
            if len(json_data) == 0:
                raise AssertionError("JSON output is empty")

            # Validate first record structure
            first_record = json_data[0]
            # TODO: Add your required fields
            required_fields = ['id', 'timestamp']

            missing_fields = [
                field for field in required_fields if field not in first_record]
            if missing_fields:
                raise AssertionError(
                    f"Missing required fields in JSON: {missing_fields}")

        elif isinstance(json_data, dict):
            # Handle single record case
            # TODO: Add your required fields
            required_fields = ['id', 'timestamp']
            missing_fields = [
                field for field in required_fields if field not in json_data]
            if missing_fields:
                raise AssertionError(
                    f"Missing required fields in JSON: {missing_fields}")

        else:
            raise AssertionError(
                f"Unexpected JSON structure type: {type(json_data)}")

    def _list_output_objects(self, prefix: str, suffix: str = "") -> list:
        """List objects in the output bucket with given prefix and suffix."""
        try:
            response = self.runner.s3_client.list_objects_v2(
                Bucket=self.runner.output_bucket,
                Prefix=prefix
            )

            objects = response.get('Contents', [])
            if suffix:
                filtered_objects = [obj['Key']
                                    for obj in objects if obj['Key'].endswith(suffix)]
            else:
                filtered_objects = [obj['Key'] for obj in objects]

            return filtered_objects

        except Exception as e:
            logger.error(
                f"Failed to list objects with prefix {prefix}: {str(e)}")
            return []

    def _download_output_file(self, key: str) -> str:
        """Download and return content of an output file."""
        try:
            response = self.runner.s3_client.get_object(
                Bucket=self.runner.output_bucket,
                Key=key
            )
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            raise Exception(f"Failed to download output file {key}: {str(e)}")

    def get_test_details(self) -> Dict[str, Any]:
        """Return detailed information about the test execution."""
        return self.test_details

    def cleanup(self) -> None:
        """Clean up test resources."""
        logger.info(f"Cleaning up test resources for {self.test_id}")

        # Clean up input data
        try:
            self.runner.s3_client.delete_object(
                Bucket=self.runner.input_bucket,
                Key=self.input_key
            )
            logger.info(f"Deleted input file: {self.input_key}")
        except Exception as e:
            logger.warning(f"Failed to delete input file: {str(e)}")

        # Clean up output data (optional - lifecycle rules will handle this)
        try:
            # Delete JSON outputs
            json_objects = self._list_output_objects(self.output_json_prefix)
            for key in json_objects:
                self.runner.s3_client.delete_object(
                    Bucket=self.runner.output_bucket,
                    Key=key
                )

            # Delete Parquet outputs
            parquet_objects = self._list_output_objects(
                self.output_parquet_prefix)
            for key in parquet_objects:
                self.runner.s3_client.delete_object(
                    Bucket=self.runner.output_bucket,
                    Key=key
                )

            logger.info(
                f"Deleted {len(json_objects + parquet_objects)} output files")

        except Exception as e:
            logger.warning(f"Failed to delete output files: {str(e)}")

        logger.info(f"Cleanup completed for test {self.test_id}")
