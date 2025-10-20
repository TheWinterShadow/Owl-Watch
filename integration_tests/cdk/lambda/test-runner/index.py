"""
Integration Test Runner Lambda Function

This Lambda function orchestrates integration tests for the Owl-Watch ETL pipeline.
It runs tests in parallel and returns GitHub Actions compatible results.
"""

import json
import os
import time
import boto3
import uuid
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Optional
from dataclasses import dataclass

# Import test modules
from tests.csv_transform_test import CSVTransformTest

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


@dataclass
class TestResult:
    """Data class for test results."""
    test_name: str
    status: str  # 'PASSED', 'FAILED', 'ERROR'
    duration_seconds: float
    message: str
    details: Dict[str, Any]
    test_id: str


class IntegrationTestRunner:
    """Main test runner class."""

    def __init__(self):
        """Initialize the test runner with AWS clients and configuration."""
        self.s3_client = boto3.client('s3')
        self.glue_client = boto3.client('glue')

        # Environment variables
        self.environment = os.environ.get('ENVIRONMENT', 'dev')
        self.input_bucket = os.environ['INPUT_BUCKET']
        self.output_bucket = os.environ['OUTPUT_BUCKET']
        self.results_bucket = os.environ['RESULTS_BUCKET']
        self.glue_job_name_csv = os.environ.get(
            'GLUE_JOB_NAME_CSV_TRANSFORM', 'placeholder-job')

        # Test configuration
        self.test_timeout_seconds = 900  # 15 minutes
        self.test_id = str(uuid.uuid4())[:8]
        self.timestamp = datetime.utcnow().isoformat()

        logger.info(
            f"Initialized test runner - Environment: {self.environment}, Test ID: {self.test_id}")

    def run_tests(self, test_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run all integration tests in parallel.

        Args:
            test_config: Configuration for tests to run

        Returns:
            Test results summary compatible with GitHub Actions
        """
        start_time = time.time()
        test_results = []

        # Define available tests
        available_tests = {
            'csv-etl-transform-test': CSVTransformTest(
                runner=self,
                test_id=f"csv-transform-{self.test_id}",
                timeout_seconds=self.test_timeout_seconds
            )
        }

        # Get tests to run from config (default to all)
        tests_to_run = test_config.get('tests', list(available_tests.keys()))

        logger.info(f"Running {len(tests_to_run)} tests: {tests_to_run}")

        # Execute tests in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_test = {
                executor.submit(self._run_single_test, test_name, available_tests[test_name]): test_name
                for test_name in tests_to_run
                if test_name in available_tests
            }

            for future in as_completed(future_to_test):
                test_name = future_to_test[future]
                try:
                    result = future.result()
                    test_results.append(result)
                    logger.info(f"Test {test_name} completed: {result.status}")
                except Exception as e:
                    logger.error(
                        f"Test {test_name} failed with exception: {str(e)}")
                    test_results.append(TestResult(
                        test_name=test_name,
                        status='ERROR',
                        duration_seconds=0,
                        message=f"Test execution failed: {str(e)}",
                        details={'exception': str(e)},
                        test_id=self.test_id
                    ))

        total_duration = time.time() - start_time

        # Generate summary
        passed_tests = [r for r in test_results if r.status == 'PASSED']
        failed_tests = [r for r in test_results if r.status == 'FAILED']
        error_tests = [r for r in test_results if r.status == 'ERROR']

        summary = {
            'test_run_id': self.test_id,
            'timestamp': self.timestamp,
            'environment': self.environment,
            'duration_seconds': round(total_duration, 2),
            'total_tests': len(test_results),
            'passed': len(passed_tests),
            'failed': len(failed_tests),
            'errors': len(error_tests),
            'success': len(failed_tests) + len(error_tests) == 0,
            'tests': [
                {
                    'name': r.test_name,
                    'status': r.status,
                    'duration': r.duration_seconds,
                    'message': r.message,
                    'details': r.details
                }
                for r in test_results
            ]
        }

        # Store results in S3 for later analysis
        self._store_test_results(summary)

        # Log summary
        logger.info(
            f"Test run completed: {summary['passed']}/{summary['total_tests']} passed")

        return summary

    def _run_single_test(self, test_name: str, test_instance) -> TestResult:
        """
        Run a single test and return its result.

        Args:
            test_name: Name of the test
            test_instance: Test instance to run

        Returns:
            TestResult object
        """
        start_time = time.time()

        try:
            logger.info(f"Starting test: {test_name}")

            # Run the test
            test_instance.run()

            duration = time.time() - start_time

            return TestResult(
                test_name=test_name,
                status='PASSED',
                duration_seconds=round(duration, 2),
                message='Test completed successfully',
                details=test_instance.get_test_details(),
                test_id=self.test_id
            )

        except AssertionError as e:
            duration = time.time() - start_time
            return TestResult(
                test_name=test_name,
                status='FAILED',
                duration_seconds=round(duration, 2),
                message=f"Test assertion failed: {str(e)}",
                details=test_instance.get_test_details() if hasattr(
                    test_instance, 'get_test_details') else {},
                test_id=self.test_id
            )

        except Exception as e:
            duration = time.time() - start_time
            return TestResult(
                test_name=test_name,
                status='ERROR',
                duration_seconds=round(duration, 2),
                message=f"Test error: {str(e)}",
                details=test_instance.get_test_details() if hasattr(
                    test_instance, 'get_test_details') else {},
                test_id=self.test_id
            )
        finally:
            # Cleanup test resources
            try:
                if hasattr(test_instance, 'cleanup'):
                    test_instance.cleanup()
            except Exception as e:
                logger.warning(f"Cleanup failed for {test_name}: {str(e)}")

    def _store_test_results(self, results: Dict[str, Any]) -> None:
        """Store test results in S3 for later analysis."""
        try:
            key = f"test-results/{self.environment}/{self.timestamp}/results-{self.test_id}.json"
            self.s3_client.put_object(
                Bucket=self.results_bucket,
                Key=key,
                Body=json.dumps(results, indent=2),
                ContentType='application/json'
            )
            logger.info(
                f"Test results stored at s3://{self.results_bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to store test results: {str(e)}")

    def wait_for_glue_job_completion(
        self,
        job_name: str,
        job_run_id: Optional[str] = None,
        timeout_seconds: int = 900
    ) -> Dict[str, Any]:
        """
        Wait for a Glue job to complete.

        Args:
            job_name: Name of the Glue job
            job_run_id: Specific job run ID (if None, gets the latest)
            timeout_seconds: Maximum time to wait

        Returns:
            Job run details
        """
        start_time = time.time()

        # If no job_run_id provided, get the latest job run
        if job_run_id is None:
            try:
                response = self.glue_client.get_job_runs(
                    JobName=job_name, MaxResults=1)
                if not response['JobRuns']:
                    raise Exception(f"No job runs found for {job_name}")
                job_run_id = response['JobRuns'][0]['Id']
                logger.info(f"Monitoring latest job run: {job_run_id}")
            except Exception as e:
                raise Exception(
                    f"Failed to get latest job run for {job_name}: {str(e)}")

        logger.info(
            f"Waiting for Glue job {job_name} (run {job_run_id}) to complete...")

        while time.time() - start_time < timeout_seconds:
            try:
                response = self.glue_client.get_job_run(
                    JobName=job_name, RunId=job_run_id)
                job_run = response['JobRun']
                status = job_run['JobRunState']

                logger.info(f"Job {job_name} status: {status}")

                if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'ERROR', 'TIMEOUT']:
                    return job_run

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                logger.error(f"Error checking job status: {str(e)}")
                raise

        raise Exception(
            f"Job {job_name} did not complete within {timeout_seconds} seconds")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda handler for integration tests.

    Args:
        event: Lambda event containing test configuration
        context: Lambda context

    Returns:
        Test results compatible with GitHub Actions
    """
    try:
        logger.info(
            f"Starting integration tests with event: {json.dumps(event, default=str)}")

        # Initialize test runner
        runner = IntegrationTestRunner()

        # Extract test configuration from event
        test_config = event.get('test_config', {})

        # Run tests
        results = runner.run_tests(test_config)

        # Return results
        return {
            'statusCode': 200 if results['success'] else 1,
            'body': json.dumps(results, default=str),
            'success': results['success'],
            'summary': f"{results['passed']}/{results['total_tests']} tests passed"
        }

    except Exception as e:
        logger.error(f"Integration test execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'success': False,
                'timestamp': datetime.utcnow().isoformat()
            }),
            'success': False,
            'summary': 'Test execution failed'
        }
