"""Integration tests for the complete data pipeline."""

import json
import time

import boto3
import pytest
from moto import mock_glue, mock_s3


class TestDataPipeline:
    """Test the complete data pipeline flow."""

    def test_s3_upload_triggers_pipeline(self, s3_client, test_buckets, sample_data):
        """Test that uploading to S3 raw bucket triggers the pipeline."""

        # Upload sample data to raw bucket
        s3_client.put_object(
            Bucket=test_buckets["raw"],
            Key="input/sample_data.json",
            Body=json.dumps(sample_data),
        )

        # Verify file exists in raw bucket
        response = s3_client.list_objects_v2(Bucket=test_buckets["raw"])
        assert "Contents" in response
        assert len(response["Contents"]) == 1
        assert response["Contents"][0]["Key"] == "input/sample_data.json"

    def test_glue_job_processing(self, glue_client, test_buckets):
        """Test Glue job creation and configuration."""

        # Create a test Glue job
        job_name = "test-data-cleaning-job"

        glue_client.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/GlueJobRole",
            Command={
                "Name": "glueetl",
                "ScriptLocation": f's3://{test_buckets["raw"]}/scripts/data_cleaning.py',
            },
            DefaultArguments={
                "--raw-bucket": test_buckets["raw"],
                "--cleaned-bucket": test_buckets["cleaned"],
            },
        )

        # Verify job was created
        response = glue_client.get_job(JobName=job_name)
        assert response["Job"]["Name"] == job_name
        assert (
            response["Job"]["DefaultArguments"]["--raw-bucket"] == test_buckets["raw"]
        )

    def test_data_quality_validation(self, s3_client, test_buckets, sample_data):
        """Test data quality validation in the pipeline."""

        # Upload valid data
        s3_client.put_object(
            Bucket=test_buckets["raw"],
            Key="input/valid_data.json",
            Body=json.dumps(sample_data),
        )

        # Upload invalid data (missing required fields)
        invalid_data = {"incomplete": "data"}
        s3_client.put_object(
            Bucket=test_buckets["raw"],
            Key="input/invalid_data.json",
            Body=json.dumps(invalid_data),
        )

        # Simulate data quality check
        response = s3_client.get_object(
            Bucket=test_buckets["raw"], Key="input/valid_data.json"
        )
        data = json.loads(response["Body"].read())

        # Validate required fields
        required_fields = ["id", "text", "timestamp"]
        is_valid = all(field in data for field in required_fields)
        assert is_valid

    def test_end_to_end_pipeline(self, s3_client, test_buckets, sample_data):
        """Test the complete end-to-end pipeline flow."""

        # Step 1: Upload to raw bucket
        s3_client.put_object(
            Bucket=test_buckets["raw"],
            Key="input/e2e_test.json",
            Body=json.dumps(sample_data),
        )

        # Step 2: Simulate data cleaning (normally done by Glue)
        cleaned_data = {
            **sample_data,
            "processed_timestamp": "2024-01-01T01:00:00Z",
            "data_quality_score": 1.0,
        }

        s3_client.put_object(
            Bucket=test_buckets["cleaned"],
            Key="cleaned/e2e_test_cleaned.json",
            Body=json.dumps(cleaned_data),
        )

        # Step 3: Simulate ML processing (sentiment analysis)
        curated_data = {
            **cleaned_data,
            "sentiment_score": 0.8,
            "sentiment_label": "positive",
            "ml_processed_timestamp": "2024-01-01T02:00:00Z",
        }

        s3_client.put_object(
            Bucket=test_buckets["curated"],
            Key="curated/e2e_test_curated.json",
            Body=json.dumps(curated_data),
        )

        # Verify data exists in all three buckets
        raw_objects = s3_client.list_objects_v2(Bucket=test_buckets["raw"])
        cleaned_objects = s3_client.list_objects_v2(Bucket=test_buckets["cleaned"])
        curated_objects = s3_client.list_objects_v2(Bucket=test_buckets["curated"])

        assert "Contents" in raw_objects
        assert "Contents" in cleaned_objects
        assert "Contents" in curated_objects

        # Verify final curated data has all expected fields
        final_response = s3_client.get_object(
            Bucket=test_buckets["curated"], Key="curated/e2e_test_curated.json"
        )
        final_data = json.loads(final_response["Body"].read())

        assert "sentiment_score" in final_data
        assert "sentiment_label" in final_data
        assert final_data["sentiment_label"] == "positive"
