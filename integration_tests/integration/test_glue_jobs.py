"""Integration tests for Glue ETL jobs."""

import json


class TestGlueJobs:
    """Test Glue ETL job functionality."""

    def test_glue_job_creation(self, glue_client):
        """Test creating Glue ETL jobs."""

        job_name = "test-data-cleaning"

        glue_client.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/GlueJobRole",
            Command={
                "Name": "glueetl",
                "ScriptLocation": "s3://test-bucket/scripts/data_cleaning.py",
                "PythonVersion": "3",
            },
            DefaultArguments={
                "--job-language": "python",
                "--raw-bucket": "test-raw-bucket",
                "--cleaned-bucket": "test-cleaned-bucket",
            },
            GlueVersion="4.0",
            MaxRetries=1,
            Timeout=60,
        )

        # Verify job creation
        response = glue_client.get_job(JobName=job_name)
        job = response["Job"]

        assert job["Name"] == job_name  # nosec
        assert job["Command"]["Name"] == "glueetl"  # nosec
        assert job["GlueVersion"] == "4.0"  # nosec
        assert job["DefaultArguments"]["--raw-bucket"] == "test-raw-bucket"  # nosec

    def test_glue_job_run_simulation(self, glue_client, s3_client, test_buckets):
        """Test simulating a Glue job run."""

        # Create job
        job_name = "test-sentiment-analysis"

        glue_client.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/GlueJobRole",
            Command={
                "Name": "glueetl",
                "ScriptLocation": f's3://{test_buckets["raw"]}/scripts/sentiment_analysis.py',
            },
            DefaultArguments={
                "--cleaned-bucket": test_buckets["cleaned"],
                "--curated-bucket": test_buckets["curated"],
            },
        )

        # Start job run
        run_response = glue_client.start_job_run(JobName=job_name)
        job_run_id = run_response["JobRunId"]

        # Verify job run was started
        assert job_run_id is not None  # nosec

        # Get job run details
        run_details = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
        assert run_details["JobRun"]["JobName"] == job_name  # nosec
        assert run_details["JobRun"]["Id"] == job_run_id  # nosec

    def test_glue_database_creation(self, glue_client):
        """Test creating Glue database."""

        database_name = "test_owl_watch_db"

        glue_client.create_database(
            DatabaseInput={
                "Name": database_name,
                "Description": "Test database for Owl Watch pipeline",
            }
        )

        # Verify database creation
        response = glue_client.get_database(Name=database_name)
        db = response["Database"]

        assert db["Name"] == database_name  # nosec
        assert "Test database" in db["Description"]  # nosec

    def test_data_processing_simulation(self, s3_client, test_buckets, sample_data):
        """Test simulating the data processing that would happen in Glue."""

        # Upload sample data
        s3_client.put_object(
            Bucket=test_buckets["raw"],
            Key="input/test_processing.json",
            Body=json.dumps(sample_data),
        )

        # Simulate data cleaning transformation
        cleaned_data = {
            **sample_data,
            "processed_timestamp": "2024-01-01T01:00:00Z",
            "data_quality_score": 1.0,
            "is_duplicate": False,
        }

        # Write cleaned data
        s3_client.put_object(
            Bucket=test_buckets["cleaned"],
            Key="cleaned/test_processing_cleaned.parquet",
            Body=json.dumps(cleaned_data),
        )

        # Simulate sentiment analysis
        if "text" in sample_data:
            sentiment_score = 0.8 if "positive" in sample_data["text"].lower() else 0.2
            sentiment_label = "positive" if sentiment_score > 0.5 else "negative"
        else:
            sentiment_score = 0.5
            sentiment_label = "neutral"

        curated_data = {
            **cleaned_data,
            "sentiment_score": sentiment_score,
            "sentiment_label": sentiment_label,
            "ml_processed_timestamp": "2024-01-01T02:00:00Z",
            "model_version": "1.0",
        }

        # Write curated data
        s3_client.put_object(
            Bucket=test_buckets["curated"],
            Key=f"curated/sentiment_{sentiment_label}/test_processing_curated.parquet",
            Body=json.dumps(curated_data),
        )

        # Verify processing results
        curated_response = s3_client.get_object(
            Bucket=test_buckets["curated"],
            Key=f"curated/sentiment_{sentiment_label}/test_processing_curated.parquet",
        )

        result_data = json.loads(curated_response["Body"].read())

        assert "sentiment_score" in result_data  # nosec
        assert "sentiment_label" in result_data  # nosec
        assert result_data["sentiment_label"] == sentiment_label  # nosec
        assert "ml_processed_timestamp" in result_data  # nosec
