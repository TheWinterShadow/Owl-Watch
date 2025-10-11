import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock

from execution.models.job_metadata import (
    DataSource,
    JobConfiguration,
    JobMetadata,
    JobResult,
    JobStatus,
    JobType,
    ProcessingContext,
    ResourceUsage,
)


class TestResourceUsage(unittest.TestCase):

    def test_init_with_defaults(self):
        resource_usage = ResourceUsage()

        self.assertIsNone(resource_usage.cpu_usage_percent)
        self.assertIsNone(resource_usage.memory_usage_mb)
        self.assertIsNone(resource_usage.disk_usage_mb)
        self.assertIsNone(resource_usage.network_io_mb)
        self.assertIsNone(resource_usage.execution_time_seconds)

    def test_init_with_values(self):
        resource_usage = ResourceUsage(
            cpu_usage_percent=75.5,
            memory_usage_mb=1024.0,
            disk_usage_mb=512.0,
            network_io_mb=256.0,
            execution_time_seconds=120.5,
        )

        self.assertEqual(resource_usage.cpu_usage_percent, 75.5)
        self.assertEqual(resource_usage.memory_usage_mb, 1024.0)
        self.assertEqual(resource_usage.disk_usage_mb, 512.0)
        self.assertEqual(resource_usage.network_io_mb, 256.0)
        self.assertEqual(resource_usage.execution_time_seconds, 120.5)


class TestJobConfiguration(unittest.TestCase):

    def test_init_required_fields(self):
        config = JobConfiguration(job_type=JobType.ETL, job_name="test-job")

        self.assertEqual(config.job_type, JobType.ETL)
        self.assertEqual(config.job_name, "test-job")
        self.assertIsNone(config.incoming_file)

    def test_init_with_incoming_file(self):
        config = JobConfiguration(
            job_type=JobType.CLEANING,
            job_name="cleaning-job",
            incoming_file="s3://bucket/data.json",
        )

        self.assertEqual(config.job_type, JobType.CLEANING)
        self.assertEqual(config.job_name, "cleaning-job")
        self.assertEqual(config.incoming_file, "s3://bucket/data.json")


class TestProcessingContext(unittest.TestCase):

    def test_init_with_defaults(self):
        context = ProcessingContext(
            execution_id="test-exec-123", job_type=JobType.SENTIMENT
        )

        self.assertEqual(context.execution_id, "test-exec-123")
        self.assertEqual(context.job_type, JobType.SENTIMENT)
        self.assertEqual(context.environment, "production")
        self.assertEqual(context.trigger_type, "manual")
        self.assertIsInstance(context.created_at, datetime)
        self.assertIsNone(context.started_at)
        self.assertIsNone(context.completed_at)

    def test_start_processing(self):
        context = ProcessingContext(
            execution_id="test-exec-123", job_type=JobType.SENTIMENT
        )

        context.start_processing()

        self.assertIsInstance(context.started_at, datetime)
        self.assertIsNone(context.completed_at)

    def test_complete_processing(self):
        context = ProcessingContext(
            execution_id="test-exec-123", job_type=JobType.SENTIMENT
        )
        context.start_processing()

        context.complete_processing()

        self.assertIsInstance(context.started_at, datetime)
        self.assertIsInstance(context.completed_at, datetime)

    def test_processing_duration(self):
        context = ProcessingContext(
            execution_id="test-exec-123", job_type=JobType.SENTIMENT
        )
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(minutes=5)

        context.started_at = start_time
        context.completed_at = end_time

        duration = context.processing_duration

        self.assertEqual(duration, timedelta(minutes=5))

    def test_processing_duration_none_when_not_completed(self):
        context = ProcessingContext(
            execution_id="test-exec-123", job_type=JobType.SENTIMENT
        )
        context.start_processing()

        duration = context.processing_duration

        self.assertIsNone(duration)

    def test_is_running_true(self):
        context = ProcessingContext(
            execution_id="test-exec-123", job_type=JobType.SENTIMENT
        )
        context.start_processing()

        self.assertTrue(context.is_running)

    def test_is_running_false_when_not_started(self):
        context = ProcessingContext(
            execution_id="test-exec-123", job_type=JobType.SENTIMENT
        )

        self.assertFalse(context.is_running)

    def test_is_running_false_when_completed(self):
        context = ProcessingContext(
            execution_id="test-exec-123", job_type=JobType.SENTIMENT
        )
        context.start_processing()
        context.complete_processing()

        self.assertFalse(context.is_running)


class TestJobMetadata(unittest.TestCase):

    def _create_metadata(self, **kwargs):
        from execution.models.base import RecordType

        defaults = {"id": "test-record-123", "record_type": RecordType.METADATA}
        defaults.update(kwargs)
        return JobMetadata(**defaults)

    def test_init_with_defaults(self):
        from execution.models.base import RecordType

        metadata = JobMetadata(id="test-record-123", record_type=RecordType.METADATA)

        self.assertEqual(metadata.job_type, JobType.ETL)
        self.assertEqual(metadata.status, JobStatus.PENDING)
        self.assertEqual(metadata.attempt_number, 1)
        self.assertEqual(metadata.max_attempts, 3)

    def test_validate_success(self):
        metadata = self._create_metadata(
            job_id="test-job-123", job_name="test-job", attempt_number=1, max_attempts=3
        )

        errors = metadata.validate()

        self.assertEqual(len(errors), 0)

    def test_validate_missing_job_id(self):
        metadata = self._create_metadata(job_name="test-job")

        errors = metadata.validate()

        self.assertIn("job_id is required", errors)

    def test_validate_missing_job_name(self):
        metadata = self._create_metadata(job_id="test-job-123")

        errors = metadata.validate()

        self.assertIn("job_name is required", errors)

    def test_validate_invalid_attempt_number(self):
        metadata = self._create_metadata(
            job_id="test-job-123", job_name="test-job", attempt_number=0
        )

        errors = metadata.validate()

        self.assertIn("attempt_number must be positive", errors)

    def test_validate_attempt_exceeds_max(self):
        metadata = self._create_metadata(
            job_id="test-job-123", job_name="test-job", attempt_number=5, max_attempts=3
        )

        errors = metadata.validate()

        self.assertIn("attempt_number cannot exceed max_attempts", errors)

    def test_start_job(self):
        metadata = self._create_metadata()
        execution_id = "exec-123"

        metadata.start_job(execution_id)

        self.assertEqual(metadata.status, JobStatus.RUNNING)
        self.assertEqual(metadata.execution_id, execution_id)
        self.assertIsInstance(metadata.started_at, datetime)

    def test_complete_job(self):
        metadata = self._create_metadata()
        mock_stats = Mock()
        output_locations = ["s3://bucket/output1", "s3://bucket/output2"]

        metadata.complete_job(mock_stats, output_locations)

        self.assertEqual(metadata.status, JobStatus.COMPLETED)
        self.assertEqual(metadata.processing_stats, mock_stats)
        self.assertEqual(metadata.output_locations, output_locations)
        self.assertIsInstance(metadata.completed_at, datetime)

    def test_fail_job(self):
        metadata = self._create_metadata()
        mock_error = Mock()
        mock_error.error_message = "Test error message"

        metadata.fail_job(mock_error)

        self.assertEqual(metadata.status, JobStatus.FAILED)
        self.assertIn(mock_error, metadata.errors)
        self.assertEqual(metadata.last_error, "Test error message")
        self.assertIsInstance(metadata.completed_at, datetime)

    def test_retry_job_within_limits(self):
        metadata = self._create_metadata(attempt_number=2, max_attempts=3)

        result = metadata.retry_job()

        self.assertTrue(result)
        self.assertEqual(metadata.attempt_number, 3)
        self.assertEqual(metadata.status, JobStatus.RETRYING)
        self.assertIsNone(metadata.started_at)
        self.assertIsNone(metadata.completed_at)
        self.assertIsNone(metadata.last_error)

    def test_retry_job_exceeds_limits(self):
        metadata = self._create_metadata(attempt_number=3, max_attempts=3)

        result = metadata.retry_job()

        self.assertFalse(result)
        self.assertEqual(metadata.attempt_number, 3)

    def test_execution_duration(self):
        metadata = self._create_metadata()
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(minutes=10)

        metadata.started_at = start_time
        metadata.completed_at = end_time

        duration = metadata.execution_duration

        self.assertEqual(duration, timedelta(minutes=10))

    def test_is_final_state_completed(self):
        metadata = self._create_metadata(status=JobStatus.COMPLETED)

        self.assertTrue(metadata.is_final_state)

    def test_is_final_state_failed(self):
        metadata = self._create_metadata(status=JobStatus.FAILED)

        self.assertTrue(metadata.is_final_state)

    def test_is_final_state_running(self):
        metadata = self._create_metadata(status=JobStatus.RUNNING)

        self.assertFalse(metadata.is_final_state)


class TestJobResult(unittest.TestCase):

    def test_init_with_defaults(self):
        result = JobResult(job_type=JobType.ETL)

        self.assertEqual(result.job_type, JobType.ETL)
        self.assertEqual(result.status, JobStatus.PENDING)
        self.assertFalse(result.success)
        self.assertEqual(len(result.errors), 0)
        self.assertEqual(len(result.warnings), 0)

    def test_add_output_file_data(self):
        result = JobResult(job_type=JobType.ETL)
        file_path = "s3://bucket/output.parquet"

        result.add_output_file(file_path, "data")

        self.assertIn(file_path, result.output_files)
        self.assertEqual(len(result.reports), 0)

    def test_add_output_file_report(self):
        result = JobResult(job_type=JobType.ETL)
        file_path = "s3://bucket/report.html"

        result.add_output_file(file_path, "report")

        self.assertIn(file_path, result.output_files)
        self.assertIn("report.html", result.reports)
        self.assertEqual(result.reports["report.html"], file_path)

    def test_add_error_non_critical(self):
        result = JobResult(job_type=JobType.ETL, success=True)
        mock_error = Mock()
        mock_error.error_type = "warning"

        result.add_error(mock_error)

        self.assertIn(mock_error, result.errors)
        self.assertTrue(result.success)

    def test_add_error_critical(self):
        result = JobResult(job_type=JobType.ETL, success=True)
        mock_error = Mock()
        mock_error.error_type = "critical"

        result.add_error(mock_error)

        self.assertIn(mock_error, result.errors)
        self.assertFalse(result.success)
        self.assertEqual(result.status, JobStatus.FAILED)

    def test_add_warning(self):
        result = JobResult(job_type=JobType.ETL)
        warning_message = "This is a warning"

        result.add_warning(warning_message)

        self.assertIn(warning_message, result.warnings)

    def test_execution_duration(self):
        result = JobResult(job_type=JobType.ETL)
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(hours=2)

        result.started_at = start_time
        result.completed_at = end_time

        duration = result.execution_duration

        self.assertEqual(duration, timedelta(hours=2))

    def test_to_summary_dict(self):
        result = JobResult(
            job_type=JobType.SENTIMENT,
            job_id="job-123",
            execution_id="exec-456",
            success=True,
            message="Job completed successfully",
        )
        result.add_output_file("s3://bucket/output.parquet")
        result.add_warning("Minor issue detected")

        mock_stats = Mock()
        mock_stats.records_processed = 1000
        result.processing_stats = mock_stats
        result.quality_score = 95.5

        summary = result.to_summary_dict()

        expected_keys = [
            "job_id",
            "execution_id",
            "job_type",
            "status",
            "success",
            "message",
            "records_processed",
            "output_files_count",
            "error_count",
            "warning_count",
            "quality_score",
        ]

        for key in expected_keys:
            self.assertIn(key, summary)

        self.assertEqual(summary["job_id"], "job-123")
        self.assertEqual(summary["execution_id"], "exec-456")
        self.assertEqual(summary["job_type"], JobType.SENTIMENT.value)
        self.assertTrue(summary["success"])
        self.assertEqual(summary["records_processed"], 1000)
        self.assertEqual(summary["output_files_count"], 1)
        self.assertEqual(summary["error_count"], 0)
        self.assertEqual(summary["warning_count"], 1)
        self.assertEqual(summary["quality_score"], 95.5)


class TestEnums(unittest.TestCase):

    def test_job_status_values(self):
        self.assertEqual(JobStatus.PENDING.value, "pending")
        self.assertEqual(JobStatus.RUNNING.value, "running")
        self.assertEqual(JobStatus.COMPLETED.value, "completed")
        self.assertEqual(JobStatus.FAILED.value, "failed")
        self.assertEqual(JobStatus.CANCELLED.value, "cancelled")
        self.assertEqual(JobStatus.RETRYING.value, "retrying")

    def test_job_type_values(self):
        self.assertEqual(JobType.ETL.value, "etl")
        self.assertEqual(JobType.CLEANING.value, "cleaned")
        self.assertEqual(JobType.SENTIMENT.value, "sentiment")
        self.assertEqual(JobType.NLP.value, "nlp")
        self.assertEqual(JobType.ANALYSTICS.value, "analytics")
        self.assertEqual(JobType.COMMUNICATION_ETL.value, "communication_etl")

    def test_data_source_values(self):
        self.assertEqual(DataSource.ENRON.value, "enron")
        self.assertEqual(DataSource.WHATSAPP.value, "whatsapp")
        self.assertEqual(DataSource.CLINTON_EMAILS.value, "clinton_emails")
        self.assertEqual(DataSource.BIDEN_EMAILS.value, "biden_emails")


if __name__ == "__main__":
    unittest.main()
