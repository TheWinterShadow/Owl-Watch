"""
Test wrapper for invoking Lambda-based integration tests

This module provides a pytest interface for running the Lambda-based integration tests
locally for development and debugging.
"""

import json
import boto3
import pytest


class TestIntegrationTestRunner:
    """Test class for running integration tests via Lambda invocation."""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_csv_transform_pipeline(self):
        """
        Test CSV transformation pipeline via Lambda invocation.

        This test invokes the Lambda function that runs the actual integration test.
        """
        # TODO: Set these environment variables or configure for your AWS account
        # TODO: Update based on deployment
        lambda_function_name = "owl-watch-integration-test-runner-dev"

        lambda_client = boto3.client('lambda')

        # Test configuration
        test_config = {
            "tests": ["csv-etl-transform-test"],
            "timeout_seconds": 900
        }

        # Invoke the Lambda function
        response = lambda_client.invoke(
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "test_config": test_config
            })
        )

        # Parse response
        result = json.loads(response['Payload'].read().decode('utf-8'))

        # Assert test results
        assert result['statusCode'] in [
            200, 1], f"Unexpected status code: {result['statusCode']}"
        assert 'body' in result

        body = json.loads(result['body'])
        assert 'success' in body
        assert 'tests' in body

        # Check if tests passed
        if not body['success']:
            failed_tests = [test for test in body['tests']
                            if test['status'] != 'PASSED']
            pytest.fail(f"Integration tests failed: {failed_tests}")

        # Log test results
        print("\nIntegration Test Results:")
        print(f"Total Tests: {body['total_tests']}")
        print(f"Passed: {body['passed']}")
        print(f"Failed: {body['failed']}")
        print(f"Errors: {body['errors']}")
        print(f"Duration: {body['duration_seconds']}s")

        for test in body['tests']:
            print(f"  {test['name']}: {test['status']} ({test['duration']}s)")

    @pytest.mark.integration
    def test_lambda_function_exists(self):
        """
        Verify that the Lambda function is deployed and accessible.
        """
        # TODO: Update function name based on your deployment
        lambda_function_name = "owl-watch-integration-test-runner-dev"

        lambda_client = boto3.client('lambda')

        try:
            response = lambda_client.get_function(
                FunctionName=lambda_function_name)
            assert response['Configuration']['FunctionName'] == lambda_function_name
            print(f"Lambda function found: {lambda_function_name}")
        except lambda_client.exceptions.ResourceNotFoundException:
            pytest.skip(
                f"Lambda function {lambda_function_name} not found. Deploy infrastructure first.")

    @pytest.mark.integration
    def test_s3_buckets_exist(self):
        """
        Verify that the required S3 buckets exist.
        """
        s3_client = boto3.client('s3')

        # TODO: Update bucket names based on your deployment
        required_buckets = [
            "owl-watch-integration-tests-input-dev-us-east-1",  # TODO: Update
            "owl-watch-integration-tests-output-dev-us-east-1",  # TODO: Update
            "owl-watch-integration-tests-results-dev-us-east-1",  # TODO: Update
        ]

        for bucket_name in required_buckets:
            try:
                s3_client.head_bucket(Bucket=bucket_name)
                print(f"S3 bucket found: {bucket_name}")
            except s3_client.exceptions.NoSuchBucket:
                pytest.skip(
                    f"S3 bucket {bucket_name} not found. Deploy infrastructure first.")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
