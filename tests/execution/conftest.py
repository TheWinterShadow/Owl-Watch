import json
from typing import Any, Dict
import boto3
import pytest
from moto import mock_glue, mock_s3


@pytest.fixture
def lambda_event():
    """Mocked Lambda event for testing."""
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-cleaned-bucket"},
                    "object": {"key": "test-key.json"},
                }
            }
        ]
    }


@pytest.fixture
def monitor():
    """Mocked monitor object for testing."""

    class DummyMonitor:
        def run(self, *args, **kwargs):
            return True

    return DummyMonitor()


@pytest.fixture
def sample_event_data():
    """Mocked sample event data for end-to-end monitoring test."""
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": "test-cleaned-bucket"},
                    "object": {"key": "test-key.json"},
                }
            }
        ]
    }


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os

    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def s3_client(aws_credentials):
    """Create mocked S3 client."""
    with mock_s3():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture
def glue_client(aws_credentials):
    """Create mocked Glue client."""
    with mock_glue():
        yield boto3.client("glue", region_name="us-east-1")


@pytest.fixture
def sample_data() -> Dict[str, Any]:
    """Sample data for testing."""
    return {
        "id": "test-001",
        "text": "This is a positive message about our product",
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "customer_feedback",
        "metadata": {"customer_id": "cust-123", "product": "widget-a"},
    }


@pytest.fixture
def test_buckets(s3_client):
    """Create test S3 buckets."""
    buckets = {
        "raw": "test-owl-watch-raw",
        "cleaned": "test-owl-watch-cleaned",
        "curated": "test-owl-watch-curated",
    }
    for bucket_name in buckets.values():
        s3_client.create_bucket(Bucket=bucket_name)
    return buckets
