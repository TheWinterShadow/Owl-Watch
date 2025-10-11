"""
Job Metadata and Configuration Models

This module defines the data structures and enumerations used for job metadata,
configuration, status tracking, and resource monitoring throughout the ETL pipeline.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from .base import BaseRecord, ErrorInfo, ProcessingStats, RecordType


@dataclass
class ResourceUsage:
    """
    Container for tracking job resource consumption metrics.

    Used for monitoring and optimizing job performance across
    different execution environments (local, Glue, etc.).
    """

    cpu_usage_percent: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    disk_usage_mb: Optional[float] = None
    network_io_mb: Optional[float] = None
    execution_time_seconds: Optional[float] = None


class JobStatus(Enum):
    """
    Enumeration of possible job execution states.

    Used for tracking job lifecycle and enabling monitoring,
    alerting, and recovery mechanisms.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class JobType(Enum):
    """
    Enumeration of supported ETL job types.

    Defines the different categories of processing jobs
    available in the Owl-Watch pipeline.
    """

    ETL = "etl"
    CLEANING = "cleaned"
    SENTIMENT = "sentiment"
    NLP = "nlp"
    ANALYTICS = "analytics"
    COMMUNICATION_ETL = "communication_etl"


class DataSource(Enum):
    """
    Enumeration of supported data sources.

    Identifies the origin systems and datasets that
    can be processed by the ETL pipeline.
    """

    ENRON = "enron"
    WHATSAPP = "whatsapp"
    CLINTON_EMAILS = "clinton_emails"
    BIDEN_EMAILS = "biden_emails"


@dataclass
class JobConfiguration:

    job_type: JobType
    job_name: str
    incoming_file: Optional[str] = None


@dataclass
class ProcessingContext:

    execution_id: str
    job_type: JobType
    environment: str = "production"

    aws_region: Optional[str] = None
    glue_job_name: Optional[str] = None
    glue_job_run_id: Optional[str] = None
    spark_application_id: Optional[str] = None

    input_paths: List[str] = field(default_factory=list)
    output_paths: List[str] = field(default_factory=list)
    schema_version: Optional[str] = None

    batch_size: Optional[int] = None
    parallelism_level: Optional[int] = None
    memory_allocation: Optional[str] = None

    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    triggered_by: Optional[str] = None
    trigger_type: str = "manual"

    def start_processing(self):
        self.started_at = datetime.utcnow()

    def complete_processing(self):
        self.completed_at = datetime.utcnow()

    @property
    def processing_duration(self) -> Optional[timedelta]:
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None

    @property
    def is_running(self) -> bool:
        return self.started_at is not None and self.completed_at is None


@dataclass
class JobMetadata(BaseRecord):

    job_type: JobType = JobType.ETL
    job_id: str = ""
    job_name: str = ""
    job_version: str = "1.0"

    status: JobStatus = JobStatus.PENDING
    execution_id: Optional[str] = None
    attempt_number: int = 1
    max_attempts: int = 3

    configuration: Optional[JobConfiguration] = None
    processing_context: Optional[ProcessingContext] = None

    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    processing_stats: Optional[ProcessingStats] = None
    output_locations: List[str] = field(default_factory=list)

    errors: List[ErrorInfo] = field(default_factory=list)
    last_error: Optional[str] = None

    def __post_init__(self):
        self.record_type = RecordType.METADATA

    def validate(self) -> List[str]:
        errors: List[str] = []

        if not self.job_id:
            errors.append("job_id is required")

        if not self.job_name:
            errors.append("job_name is required")

        if self.attempt_number < 1:
            errors.append("attempt_number must be positive")

        if self.max_attempts < 1:
            errors.append("max_attempts must be positive")

        if self.attempt_number > self.max_attempts:
            errors.append("attempt_number cannot exceed max_attempts")

        return errors

    def start_job(self, execution_id: str):
        self.status = JobStatus.RUNNING
        self.execution_id = execution_id
        self.started_at = datetime.utcnow()

        if self.processing_context:
            self.processing_context.start_processing()

    def complete_job(
        self, stats: ProcessingStats, output_locations: Optional[List[str]] = None
    ):
        self.status = JobStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        self.processing_stats = stats

        if output_locations:
            self.output_locations = output_locations

        if self.processing_context:
            self.processing_context.complete_processing()

    def fail_job(self, error: ErrorInfo):
        self.status = JobStatus.FAILED
        self.completed_at = datetime.utcnow()
        self.errors.append(error)
        self.last_error = error.error_message

        if self.processing_context:
            self.processing_context.complete_processing()

    def retry_job(self):
        if self.attempt_number < self.max_attempts:
            self.attempt_number += 1
            self.status = JobStatus.RETRYING
            self.started_at = None
            self.completed_at = None
            self.last_error = None

            return True

        return False

    @property
    def execution_duration(self) -> Optional[timedelta]:
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None

    @property
    def is_final_state(self) -> bool:
        return self.status in [
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        ]

    @property
    def success_rate(self) -> float:
        if not self.processing_stats or self.processing_stats.records_processed == 0:
            return 0.0

        return (
            self.processing_stats.records_successful
            / self.processing_stats.records_processed
        ) * 100


@dataclass
class JobResult:

    job_type: JobType
    job_id: str = ""
    execution_id: str = ""

    status: JobStatus = JobStatus.PENDING
    success: bool = False
    message: str = ""

    processing_stats: Optional[ProcessingStats] = None

    output_files: List[str] = field(default_factory=list)
    output_tables: List[str] = field(default_factory=list)
    reports: Dict[str, str] = field(default_factory=dict)

    quality_score: Optional[float] = None
    validation_results: List[Dict[str, Any]] = field(default_factory=list)

    errors: List[ErrorInfo] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def execution_duration(self) -> Optional[timedelta]:
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None

    def add_output_file(self, file_path: str, file_type: str = "data"):
        self.output_files.append(file_path)

        if file_type == "report":
            filename = file_path.split("/")[-1]
            self.reports[filename] = file_path

    def add_error(self, error: ErrorInfo):
        self.errors.append(error)
        if self.success and error.error_type == "critical":
            self.success = False
            self.status = JobStatus.FAILED

    def add_warning(self, warning: str):
        self.warnings.append(warning)

    def to_summary_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "execution_id": self.execution_id,
            "job_type": self.job_type.value,
            "status": self.status.value,
            "success": self.success,
            "message": self.message,
            "records_processed": (
                self.processing_stats.records_processed if self.processing_stats else 0
            ),
            "output_files_count": len(self.output_files),
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "quality_score": self.quality_score,
        }
