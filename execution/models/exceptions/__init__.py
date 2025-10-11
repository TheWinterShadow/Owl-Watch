from datetime import datetime
from typing import Any, Dict, List, Optional


class OwlWatchExecutionException(Exception):
    def __init__(
        self,
        message: str,
        context: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
    ):
        super().__init__(message)
        self.message = message
        self.context = context or {}
        self.timestamp = timestamp or datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "exception_type": self.__class__.__name__,
            "message": self.message,
            "context": self.context,
            "timestamp": self.timestamp.isoformat(),
        }


class ConfigurationException(OwlWatchExecutionException):
    pass


class MissingConfigurationError(ConfigurationException):
    def __init__(
        self,
        parameters: List[str],
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.parameters = parameters

        if message is None:
            if len(parameters) == 1:
                message = f"Missing required configuration parameter: {parameters[0]}"
            else:
                message = f"Missing required configuration parameters: {', '.join(parameters)}"

        super().__init__(message, context)


class InvalidConfigurationError(ConfigurationException):
    def __init__(
        self,
        parameter: str,
        value: Any,
        expected: str,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.parameter = parameter
        self.value = value
        self.expected = expected

        if message is None:
            message = f"Invalid configuration for '{parameter}': got '{value}', expected {expected}"

        param_context = {
            "parameter": parameter,
            "value": str(value),
            "expected": expected,
        }
        if context:
            param_context.update(context)

        super().__init__(message, param_context)


class ConfigurationValidationError(ConfigurationException):
    def __init__(
        self,
        validation_errors: List[str],
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.validation_errors = validation_errors

        if message is None:
            message = f"Configuration validation failed: {'; '.join(validation_errors)}"

        validation_context = {"validation_errors": validation_errors}
        if context:
            validation_context.update(context)

        super().__init__(message, validation_context)


class SchemaException(OwlWatchExecutionException):
    pass


class SchemaValidationError(SchemaException):
    def __init__(
        self,
        schema_name: str,
        validation_errors: List[str],
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.schema_name = schema_name
        self.validation_errors = validation_errors

        if message is None:
            message = f"Schema validation failed for '{schema_name}': {'; '.join(validation_errors)}"

        schema_context = {
            "schema_name": schema_name,
            "validation_errors": validation_errors,
        }
        if context:
            schema_context.update(context)

        super().__init__(message, schema_context)


class InvalidSchemaError(SchemaException):
    def __init__(
        self,
        schema_name: str,
        reason: str,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.schema_name = schema_name
        self.reason = reason

        if message is None:
            message = f"Invalid schema '{schema_name}': {reason}"

        super().__init__(message, context)


class DataProcessingException(OwlWatchExecutionException):
    pass


class DataProcessingError(DataProcessingException):
    def __init__(
        self,
        operation: str,
        reason: str,
        record_count: Optional[int] = None,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        self.reason = reason
        self.record_count = record_count

        if message is None:
            count_info = f" (processing {record_count} records)" if record_count else ""
            message = (
                f"Data processing failed during '{operation}'{count_info}: {reason}"
            )

        processing_context = {
            "operation": operation,
            "reason": reason,
            "record_count": record_count,
        }
        if context:
            processing_context.update(context)

        super().__init__(message, processing_context)


class DataQualityError(DataProcessingException):
    def __init__(
        self,
        quality_check: str,
        failed_records: int,
        total_records: int,
        threshold: Optional[float] = None,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.quality_check = quality_check
        self.failed_records = failed_records
        self.total_records = total_records
        self.threshold = threshold
        self.failure_rate = failed_records / total_records if total_records > 0 else 0.0

        if message is None:
            failure_pct = self.failure_rate * 100
            threshold_info = (
                f" (threshold: {threshold * 100:.1f}%)" if threshold else ""
            )
            message = (
                f"Data quality check '{quality_check}' failed: "
                f"{failed_records}/{total_records} records ({failure_pct:.1f}%){threshold_info}"
            )

        quality_context = {
            "quality_check": quality_check,
            "failed_records": failed_records,
            "total_records": total_records,
            "failure_rate": self.failure_rate,
            "threshold": threshold,
        }
        if context:
            quality_context.update(context)

        super().__init__(message, quality_context)


class JobExecutionException(OwlWatchExecutionException):
    pass


class TypeMismatchError(OwlWatchExecutionException):
    def __init__(
        self,
        field_name: str,
        expected_type: str,
        actual_type: str,
        record_id: Optional[str] = None,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.field_name = field_name
        self.expected_type = expected_type
        self.actual_type = actual_type
        self.record_id = record_id

        if message is None:
            record_info = f" in record '{record_id}'" if record_id else ""
            message = (
                f"Type mismatch for field '{field_name}'{record_info}: "
                f"expected '{expected_type}', got '{actual_type}'"
            )

        type_context = {
            "field_name": field_name,
            "expected_type": expected_type,
            "actual_type": actual_type,
            "record_id": record_id,
        }
        if context:
            type_context.update(context)

        super().__init__(message, type_context)


class MissingFieldError(OwlWatchExecutionException):
    def __init__(
        self,
        missing_fields: List[str],
        record_id: Optional[str] = None,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.missing_fields = missing_fields
        self.record_id = record_id

        if message is None:
            record_info = f" in record '{record_id}'" if record_id else ""
            message = f"Missing required fields {missing_fields}{record_info}"

        field_context = {"missing_fields": missing_fields, "record_id": record_id}
        if context:
            field_context.update(context)

        super().__init__(message, field_context)


class JobExecutionError(JobExecutionException):
    def __init__(
        self,
        job_name: str,
        stage: str,
        reason: str,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.job_name = job_name
        self.stage = stage
        self.reason = reason

        if message is None:
            message = f"Job '{job_name}' failed at stage '{stage}': {reason}"

        job_context = {"job_name": job_name, "stage": stage, "reason": reason}
        if context:
            job_context.update(context)

        super().__init__(message, job_context)


class JobTimeoutError(JobExecutionException):
    def __init__(
        self,
        job_name: str,
        timeout_seconds: int,
        elapsed_seconds: int,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.job_name = job_name
        self.timeout_seconds = timeout_seconds
        self.elapsed_seconds = elapsed_seconds

        if message is None:
            message = f"Job '{job_name}' timed out after {elapsed_seconds}s (limit: {timeout_seconds}s)"

        super().__init__(message, context)


class AWSException(OwlWatchExecutionException):
    pass


class S3Error(AWSException):
    def __init__(
        self,
        operation: str,
        bucket: str,
        key: Optional[str] = None,
        reason: str = "Unknown error",
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        self.bucket = bucket
        self.key = key
        self.reason = reason

        if message is None:
            location = f"s3://{bucket}/{key}" if key else f"s3://{bucket}"
            message = f"S3 {operation} failed for {location}: {reason}"

        super().__init__(message, context)


class AthenaError(AWSException):
    def __init__(
        self,
        operation: str,
        query_id: Optional[str] = None,
        reason: str = "Unknown error",
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        self.query_id = query_id
        self.reason = reason

        if message is None:
            query_info = f" (query: {query_id})" if query_id else ""
            message = f"Athena {operation} failed{query_info}: {reason}"

        super().__init__(message, context)


class GlueError(AWSException):
    def __init__(
        self,
        operation: str,
        job_name: Optional[str] = None,
        reason: str = "Unknown error",
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.operation = operation
        self.job_name = job_name
        self.reason = reason

        if message is None:
            job_info = f" for job '{job_name}'" if job_name else ""
            message = f"Glue {operation} failed{job_info}: {reason}"

        super().__init__(message, context)


class AIProcessingException(OwlWatchExecutionException):
    pass


class BedrockError(AIProcessingException):

    def __init__(
        self,
        model_id: str,
        operation: str,
        reason: str = "Unknown error",
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.model_id = model_id
        self.operation = operation
        self.reason = reason

        if message is None:
            message = f"Bedrock {operation} failed for model '{model_id}': {reason}"

        super().__init__(message, context)


class SentimentAnalysisError(AIProcessingException):

    def __init__(
        self,
        text_sample: str,
        reason: str = "Unknown error",
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        self.text_sample = (
            text_sample[:100] + "..." if len(text_sample) > 100 else text_sample
        )
        self.reason = reason

        if message is None:
            message = f"Sentiment analysis failed: {reason}"

        super().__init__(message, context)


def create_error_context(
    job_name: Optional[str] = None,
    stage: Optional[str] = None,
    record_id: Optional[str] = None,
    **additional_context,
) -> Dict[str, Any]:
    context = {}

    if job_name:
        context["job_name"] = job_name
    if stage:
        context["stage"] = stage
    if record_id:
        context["record_id"] = record_id

    context.update(additional_context)
    return context


def handle_exception_with_context(
    exception: Exception,
    job_name: Optional[str] = None,
    stage: Optional[str] = None,
    record_id: Optional[str] = None,
    **additional_context,
) -> OwlWatchExecutionException:
    context = create_error_context(
        job_name=job_name,
        stage=stage,
        record_id=record_id,
        original_exception=str(exception),
        **additional_context,
    )

    if isinstance(exception, ValueError):
        if "configuration" in str(exception).lower():
            return InvalidConfigurationError(
                parameter="unknown",
                value="unknown",
                expected="valid value",
                message=str(exception),
                context=context,
            )
        else:
            return DataProcessingError(
                operation="validation", reason=str(exception), context=context
            )
    elif isinstance(exception, KeyError):
        return MissingConfigurationError(
            parameters=[str(exception).strip("'\"")], context=context
        )
    elif isinstance(exception, FileNotFoundError):
        return S3Error(
            operation="read", bucket="unknown", reason=str(exception), context=context
        )
    else:
        return OwlWatchExecutionException(
            message=f"{exception.__class__.__name__}: {str(exception)}", context=context
        )


__all__ = [
    "OwlWatchExecutionException",
    "ConfigurationException",
    "MissingConfigurationError",
    "InvalidConfigurationError",
    "ConfigurationValidationError",
    "SchemaException",
    "SchemaValidationError",
    "InvalidSchemaError",
    "DataProcessingException",
    "DataProcessingError",
    "DataQualityError",
    "JobExecutionException",
    "JobExecutionError",
    "JobTimeoutError",
    "AWSException",
    "S3Error",
    "AthenaError",
    "GlueError",
    "AIProcessingException",
    "BedrockError",
    "SentimentAnalysisError",
    "create_error_context",
    "handle_exception_with_context",
]
