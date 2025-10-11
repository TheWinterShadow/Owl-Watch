"""
Test Configuration and Fixtures

This module provides shared test fixtures and configuration for the execution test suite.
It includes AWS service mocks, sample data, and common test utilities.
"""

from typing import Any, Dict

import boto3
import pytest
from moto import mock_glue, mock_s3


@pytest.fixture
def lambda_event():
    """
    Fixture providing a sample AWS Lambda event structure.

    Returns:
        Dictionary containing a mock S3 event record
    """
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-cleaned_data"},
                    "object": {"key": "test-key.json"},
                }
            }
        ]
    }


@pytest.fixture
def monitor():
    """
    Fixture providing a dummy monitor for testing.

    Returns:
        DummyMonitor instance with a run method that always returns True
    """

    class DummyMonitor:
        def run(self, *args, **kwargs):
            return True

    return DummyMonitor()


@pytest.fixture
def sample_event_data():
    """
    Fixture providing sample S3 event data for testing.

    Returns:
        Dictionary containing mock S3 event records
    """
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-cleaned_data"},
                    "object": {"key": "test-key.json"},
                }
            }
        ]
    }


@pytest.fixture
def aws_credentials():
    """
    Fixture that sets up mock AWS credentials for testing.

    Sets environment variables with dummy AWS credentials to prevent
    tests from attempting to use real AWS credentials.
    """
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def glue_client(aws_credentials):
    with mock_glue():
        yield boto3.client("glue", region_name="us-east-1")


@pytest.fixture
def sample_data() -> Dict[str, Any]:
    return {
        "id": "test-001",
        "text": "This is a positive message about our product",
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "customer_feedback",
        "metadata": {"customer_id": "cust-123", "product": "widget-a"},
    }


@pytest.fixture
def test_buckets(s3_client):
    buckets = {
        "raw": "test-owl-watch-raw",
        "cleaned": "test-owl-watch-cleaned",
        "curated": "test-owl-watch-curated",
    }
    for bucket_name in buckets.values():
        s3_client.create_bucket(Bucket=bucket_name)
    return buckets
