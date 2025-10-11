from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class RecordType(Enum):

    EMAIL = "email"
    COMMUNICATION = "communication"
    USER_PROFILE = "user_profile"
    ANALYTICS = "analytics"
    DATA_QUALITY = "data_quality"
    METADATA = "metadata"


@dataclass
class ErrorInfo:

    error_type: str
    error_message: str
    error_code: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    stacktrace: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessingStats:

    records_processed: int = 0
    records_successful: int = 0
    records_failed: int = 0
    processing_time_seconds: float = 0.0
    memory_usage_mb: Optional[float] = None
    cpu_usage_percent: Optional[float] = None

    @property
    def success_rate(self) -> float:
        if self.records_processed == 0:
            return 0.0
        return (self.records_successful / self.records_processed) * 100.0

    @property
    def failure_rate(self) -> float:
        return 100.0 - self.success_rate


@dataclass
class BaseRecord:

    id: str
    record_type: RecordType
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    processing_stats: Optional[ProcessingStats] = None
    errors: List[ErrorInfo] = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.record_type, str):
            self.record_type = RecordType(self.record_type)

    def add_error(
        self,
        error_type: str,
        error_message: str,
        error_code: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        error = ErrorInfo(
            error_type=error_type,
            error_message=error_message,
            error_code=error_code,
            context=context or {},
        )
        self.errors.append(error)
        self.updated_at = datetime.now()

    def has_errors(self) -> bool:
        return len(self.errors) > 0

    def update_metadata(self, key: str, value: Any) -> None:
        self.metadata[key] = value
        self.updated_at = datetime.now()
