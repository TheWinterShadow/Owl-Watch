"""
Exception Handling Documentation
===============================

This document provides comprehensive guidance on using the exception system
in the execution package.

## Exception Hierarchy

The execution package uses a hierarchical exception system based on 
`OwlWatchExecutionException`. All custom exceptions inherit from this base
class and provide structured error information.

```
OwlWatchExecutionException (base)
├── ConfigurationException
│   ├── MissingConfigurationError
│   ├── InvalidConfigurationError
│   └── ConfigurationValidationError
├── SchemaException
│   ├── SchemaValidationError
│   └── InvalidSchemaError
├── DataProcessingException
│   ├── DataProcessingError
│   └── DataQualityError
├── JobExecutionException
│   ├── JobExecutionError
│   └── JobTimeoutError
├── AWSException
│   ├── S3Error
│   ├── AthenaError
│   └── GlueError
└── AIProcessingException
    ├── BedrockError
    └── SentimentAnalysisError
```

## Usage Examples

### Configuration Exceptions

#### MissingConfigurationError
Used when required configuration parameters are missing:

```python
from execution.schemas.exceptions import MissingConfigurationError

# Single parameter
raise MissingConfigurationError(parameters=["source_bucket"])

# Multiple parameters  
raise MissingConfigurationError(parameters=["source_bucket", "source_key"])

# With custom message and context
raise MissingConfigurationError(
    parameters=["database_url"],
    message="Database connection configuration is incomplete",
    context={"stage": "initialization", "job_id": "job_123"}
)
```

#### InvalidConfigurationError
Used when configuration values are invalid:

```python
from execution.schemas.exceptions import InvalidConfigurationError

# Basic usage
raise InvalidConfigurationError(
    parameter="batch_size",
    value=0,
    expected="positive integer greater than 0"
)

# With context
raise InvalidConfigurationError(
    parameter="schema_type",
    value="invalid_type",
    expected="one of: communication, document, user_profile",
    context={"available_types": ["communication", "document", "user_profile"]}
)
```

### Schema Exceptions

#### SchemaValidationError
Used when data doesn't conform to expected schema:

```python
from execution.schemas.exceptions import SchemaValidationError

# DataFrame schema validation
validation_errors = [
    "Missing required column: 'timestamp'",
    "Invalid data type for 'user_id': expected string, got int"
]

raise SchemaValidationError(
    schema_name="communication_schema",
    validation_errors=validation_errors,
    context={"record_count": 1000, "failed_records": 25}
)
```

### Data Processing Exceptions

#### DataProcessingError
Used for general data processing failures:

```python
from execution.schemas.exceptions import DataProcessingError

# Processing operation failure
raise DataProcessingError(
    operation="sentiment_analysis",
    reason="Text preprocessing failed due to encoding issues",
    record_count=500,
    context={"input_format": "json", "encoding": "utf-8"}
)
```

#### DataQualityError
Used when data quality checks fail:

```python
from execution.schemas.exceptions import DataQualityError

# Quality threshold not met
raise DataQualityError(
    quality_check="null_value_check",
    failed_records=150,
    total_records=1000,
    threshold=0.05,  # 5% threshold
    context={"column": "email_address", "job_id": "cleaning_job_456"}
)
```

### Job Execution Exceptions

#### JobExecutionError
Used for job execution failures:

```python
from execution.schemas.exceptions import JobExecutionError

# Stage-specific failure
raise JobExecutionError(
    job_name="biden_data_etl",
    stage="data_transformation",
    reason="Memory limit exceeded during DataFrame join operation",
    context={"memory_usage": "8GB", "limit": "6GB"}
)
```

### AWS Exceptions

#### S3Error
Used for S3 operation failures:

```python
from execution.schemas.exceptions import S3Error

# Read operation failure
raise S3Error(
    operation="read",
    bucket="my-data-bucket",
    key="input/data.parquet",
    reason="Access denied - insufficient permissions",
    context={"aws_account": "123456789012"}
)

# Write operation failure
raise S3Error(
    operation="write", 
    bucket="output-bucket",
    key="results/processed_data.parquet",
    reason="Bucket does not exist"
)
```

#### AthenaError
Used for Athena operation failures:

```python
from execution.schemas.exceptions import AthenaError

raise AthenaError(
    operation="query_execution",
    query_id="12345678-1234-1234-1234-123456789012",
    reason="Query timeout after 30 minutes",
    context={"query": "SELECT * FROM large_table", "timeout_minutes": 30}
)
```

### AI Processing Exceptions

#### BedrockError
Used for AWS Bedrock operation failures:

```python
from execution.schemas.exceptions import BedrockError

raise BedrockError(
    model_id="anthropic.claude-v2",
    operation="text_generation",
    reason="Model quota exceeded",
    context={"requests_made": 1000, "quota_limit": 1000}
)
```

## Exception Context and Logging

All exceptions provide structured context that can be used for debugging
and monitoring. The context is automatically serialized for logging:

```python
try:
    # Some operation that might fail
    process_data()
except Exception as e:
    # Convert to our exception with context
    execution_error = handle_exception_with_context(
        e,
        job_name="my_etl_job",
        stage="data_validation",
        record_id="rec_12345",
        additional_info="Custom debugging info"
    )
    
    # Log the structured exception
    logger.error("Job failed", extra=execution_error.to_dict())
    raise execution_error
```

## Utility Functions

### create_error_context
Creates standardized error context:

```python
from execution.schemas.exceptions import create_error_context

context = create_error_context(
    job_name="sentiment_analysis",
    stage="preprocessing", 
    record_id="msg_789",
    batch_size=100,
    processing_time=45.2
)
```

### handle_exception_with_context
Converts standard Python exceptions to execution exceptions:

```python
from execution.schemas.exceptions import handle_exception_with_context

try:
    # Operation that might raise ValueError, KeyError, etc.
    validate_config(config)
except Exception as e:
    # Automatically map to appropriate execution exception
    execution_error = handle_exception_with_context(
        e, 
        job_name="config_validation",
        stage="startup"
    )
    raise execution_error
```

## Integration with Existing Code

### Updating config_manager.py

Replace existing exception usage:

```python
# Before
raise ValueError("Missing required parameter")

# After  
from execution.schemas.exceptions import MissingConfigurationError
raise MissingConfigurationError(parameters=["required_param"])
```

### Updating Job Files

Replace generic exceptions with specific types:

```python
# Before
raise ValueError("Both input-bucket and output-bucket must be specified")

# After
from execution.schemas.exceptions import InvalidConfigurationError
raise InvalidConfigurationError(
    parameter="s3_configuration",
    value="incomplete",
    expected="both input-bucket and output-bucket parameters"
)
```

## Best Practices

### 1. Use Specific Exception Types
Choose the most specific exception type for your error condition:

```python
# Good - specific exception
raise SchemaValidationError(schema_name="communication", validation_errors=[...])

# Avoid - generic exception
raise Exception("Schema validation failed")
```

### 2. Provide Rich Context
Include relevant context information for debugging:

```python
# Good - rich context
raise DataProcessingError(
    operation="sentiment_analysis",
    reason="Invalid text encoding",
    record_count=batch_size,
    context={
        "encoding_detected": detected_encoding,
        "expected_encoding": "utf-8",
        "file_path": input_file
    }
)
```

### 3. Use Exception Chaining
Preserve the original exception when wrapping:

```python
try:
    risky_operation()
except ValueError as e:
    raise InvalidConfigurationError(
        parameter="value",
        value="invalid",
        expected="valid format",
        context={"original_error": str(e)}
    ) from e
```

### 4. Log Exceptions Consistently
Use the structured context for consistent logging:

```python
try:
    process_data()
except OwlWatchExecutionException as e:
    # Log with structured context
    logger.error(
        "Processing failed",
        extra=e.to_dict()
    )
    raise
```

### 5. Handle Exceptions at Appropriate Levels
Catch and handle exceptions at the right abstraction level:

```python
# Job level - catch and convert to job execution error
try:
    run_etl_pipeline()
except (DataProcessingError, SchemaValidationError) as e:
    raise JobExecutionError(
        job_name=self.job_name,
        stage="pipeline_execution", 
        reason=str(e),
        context=e.context
    ) from e
```

## Migration Guide

### Step 1: Update Imports
Add exception imports to existing files:

```python
from execution.schemas.exceptions import (
    MissingConfigurationError,
    InvalidConfigurationError,
    DataProcessingError,
    S3Error,
    # ... other exceptions as needed
)
```

### Step 2: Replace Exception Usage
Update existing raise statements:

```python
# Old
raise ValueError(f"Missing parameter: {param}")

# New  
raise MissingConfigurationError(parameters=[param])
```

### Step 3: Update Exception Handling
Update catch blocks to handle specific exception types:

```python
# Old
except ValueError as e:
    logger.error(f"Configuration error: {e}")

# New
except (MissingConfigurationError, InvalidConfigurationError) as e:
    logger.error("Configuration error", extra=e.to_dict())
```

### Step 4: Add Context to Existing Exceptions
Enhance exception information with context:

```python
# Enhanced exception with context
raise DataProcessingError(
    operation="data_cleaning",
    reason="Null values exceed threshold", 
    record_count=len(df),
    context={
        "null_percentage": null_pct,
        "threshold": 0.1,
        "columns_affected": null_columns
    }
)
```

## Testing Exception Handling

### Unit Test Examples

```python
import pytest
from execution.schemas.exceptions import MissingConfigurationError, InvalidConfigurationError

def test_missing_configuration_error():
    """Test missing configuration error handling."""
    with pytest.raises(MissingConfigurationError) as exc_info:
        validate_required_params({})  # Missing required params
    
    error = exc_info.value
    assert "source_bucket" in error.parameters
    assert "Missing required configuration parameter" in error.message

def test_exception_context():
    """Test exception context structure."""
    error = InvalidConfigurationError(
        parameter="batch_size",
        value=-1,
        expected="positive integer"
    )
    
    context = error.to_dict()
    assert context["exception_type"] == "InvalidConfigurationError"
    assert context["context"]["parameter"] == "batch_size"
    assert "timestamp" in context
```

### Integration Test Examples

```python
def test_job_exception_handling():
    """Test job-level exception handling."""
    job = MyETLJob(invalid_config)
    
    with pytest.raises(JobExecutionError) as exc_info:
        job.run()
    
    error = exc_info.value
    assert error.job_name == "my_etl_job"
    assert "configuration" in error.stage
    assert error.context is not None
```

This exception system provides comprehensive error handling capabilities
while maintaining consistency and debuggability across the execution package.