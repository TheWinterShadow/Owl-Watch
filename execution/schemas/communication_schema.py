"""
Communication Data Schema Definitions

This module defines the standardized schemas for communication data across
different sources (email, Slack, WhatsApp). Provides both PySpark StructType
and JSON representations for consistency and interoperability.
"""

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Standardized schema for all communication data types
COMMUNICATION_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("sender", StringType(), True),
        StructField("recipient", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("body", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("attachments", ArrayType(StringType()), True),
        StructField("sentiment", StringType(), True),
        StructField("language", StringType(), True),
    ]
)

# JSON representation of the communication schema for API documentation
# and external system integration
COMMUNICATION_SCHEMA_JSON = {
    "id": "string",
    "sender": "string",
    "recipient": "string",
    "timestamp": "timestamp",
    "body": "string",
    "subject": "string",
    "channel": "string",
    "attachments": "array<string>",
    "sentiment": "string",
    "language": "string",
}
