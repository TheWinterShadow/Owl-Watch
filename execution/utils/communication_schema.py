"""
Standardized schema for communication data for sentiment and NLP analysis.
"""
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType

# PySpark StructType schema
COMMUNICATION_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("sender", StringType(), True),
    StructField("recipient", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("body", StringType(), True),
    # For emails, optional for chat
    StructField("subject", StringType(), True),
    # For chat/slack, optional for emails
    StructField("channel", StringType(), True),
    StructField("attachments", ArrayType(StringType()), True),
    StructField("sentiment", StringType(), True),  # To be filled by analysis
    StructField("language", StringType(), True),   # Detected language
])

# JSON schema for AWS CDK Glue Table
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
    "language": "string"
}
