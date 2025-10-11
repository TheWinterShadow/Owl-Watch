"""
Email ETL Job

This module provides ETL functionality for processing email communication data.
It standardizes email data formats and applies consistent schemas for downstream processing.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from execution.models.base_job import BaseGlueETLJob
from execution.models.job_metadata import DataSource
from execution.schemas import COMMUNICATION_SCHEMA
from execution.utils.logger import logger


class EmailETL(BaseGlueETLJob):
    """
    ETL job for processing email communication data.

    This job standardizes email data from various sources (e.g., Enron dataset)
    into a consistent schema for downstream analytics and processing.
    """

    def __init__(self, args_to_extract):
        """Initialize the Email ETL job."""
        logger.secure_debug("Initializing EmailETL")
        super().__init__(args_to_extract)
        logger.secure_debug("EmailETL initialized")

    def get_expected_schema(self) -> StructType:
        """Return the expected schema for email data."""
        logger.secure_debug("Building expected schema for emails")
        return COMMUNICATION_SCHEMA

    def run(self) -> DataFrame:
        """
        Execute email data processing pipeline.

        Returns:
            DataFrame containing standardized email data
        """
        raw_bucket = self.args.get("raw_data")
        cleaned_bucket = self.args.get("cleaned_data")

        if not raw_bucket or not cleaned_bucket:
            raise ValueError("Both raw_data and cleaned_data must be specified")

        logger.secure_info(
            f"Processing email data from s3://{raw_bucket}/emails/",
            raw_bucket=raw_bucket,
            cleaned_bucket=cleaned_bucket,
        )

        try:
            df = self.spark.read.parquet(f"s3://{raw_bucket}/emails/")
            logger.secure_debug(f"Read {df.count()} email records")

            logger.secure_debug("Adding base fields")
            df = self.add_base_fields(df, DataSource.ENRON, "emails")

            logger.secure_debug("Generating record IDs with prefix 'EMAIL'")
            df = self.generate_record_id(df, "EMAIL")

            logger.secure_debug("Applying email-specific field mappings")
            df = self._apply_source_mappings(df, DataSource.ENRON)

            try:
                logger.secure_debug(
                    "Completed mapping - record count", record_count=df.count()
                )
            except Exception as e:
                logger.secure_debug(
                    "Completed mapping - unable to log sample record", error=str(e)
                )

            logger.secure_debug("Adding quality flags")
            df = self.add_quality_flags(df)

            logger.secure_debug("Validating output schema")
            validation_result = self.validate_schema(df, self.get_expected_schema())
            if not validation_result["is_valid"]:
                logger.secure_error(
                    "Schema validation failed",
                    validation_result=validation_result,
                )
                raise ValueError(f"Schema validation failed: {validation_result}")

            logger.secure_debug("Selecting expected fields for output")
            df = self.select_expected_fields(df)

            output_count = df.count()
            logger.secure_info(
                "Attributes transformation completed successfully",
                output_record_count=output_count,
            )
            return df

            output_path = f"s3://{cleaned_bucket}/communications/emails/"
            logger.secure_debug(f"Writing results to {output_path}")
            df.write.mode("overwrite").parquet(output_path)

            output_count = df.count()
            logger.secure_info(
                "Email ETL transformation completed",
                output_record_count=output_count,
            )
            return df

        except Exception as e:
            logger.secure_error(
                "Error during email transformation",
                error=str(e),
            )
            raise

    def _apply_source_mappings(self, df: DataFrame, source: DataSource) -> DataFrame:
        if hasattr(source, "value"):
            source_str = source.value
        else:
            source_str = str(source)

        logger.secure_debug("Applying source mappings", source=source_str)

        try:
            if source == DataSource.ENRON:
                logger.secure_debug("Using Enron field mapping")
                return self._map_enron_fields(df)
            elif source == DataSource.CLINTON_EMAILS:
                logger.secure_debug("Using Clinton Emails field mapping")
                return self._map_clinton_fields(df)
            elif source == DataSource.BIDEN_EMAILS:
                logger.secure_debug("Using Biden Emails field mapping")
                return self._map_biden_fields(df)
            else:
                logger.secure_warning(f"Unknown source {source_str}")
                raise ValueError(f"Unknown source {source_str}")
        except Exception as e:
            logger.secure_error(
                "Error applying source mappings", source=source_str, error=str(e)
            )
            raise

    def _map_enron_fields(self, df: DataFrame) -> DataFrame:
        logger.secure_debug(
            "Mapping Enron fields to attributes schema", input_columns=str(df.columns)
        )

        enron_schema = StructType(
            [
                StructField("Message-ID", StringType(), True),
                StructField("Date", StringType(), True),
                StructField("From", StringType(), True),
                StructField("To", StringType(), True),
                StructField("Subject", StringType(), True),
                StructField("Mime-Version", StringType(), True),
                StructField("Content-Type", StringType(), True),
                StructField("Content-Transfer-Encoding", StringType(), True),
                StructField("X-From", StringType(), True),
                StructField("X-To", StringType(), True),
                StructField("X-cc", StringType(), True),
                StructField("X-bcc", StringType(), True),
                StructField("X-Folder", StringType(), True),
                StructField("X-Origin", StringType(), True),
                StructField("X-FileName", StringType(), True),
                StructField("Body", StringType(), True),
            ]
        )

        def parse_email(email_text):
            if not email_text:
                return None

            parts = email_text.split("\n\n", 1)
            headers_text = parts[0]
            body = parts[1].strip() if len(parts) > 1 else ""

            headers = {}
            for line in headers_text.split("\n"):
                colon_idx = line.find(":")
                if colon_idx > 0:
                    key = line[:colon_idx].strip()
                    value = line[colon_idx + 1 :].strip()  # noqa: E203
                    headers[key] = value

            return (
                headers.get("Message-ID"),
                headers.get("Date"),
                headers.get("From"),
                headers.get("To"),
                headers.get("Subject"),
                headers.get("Mime-Version"),
                headers.get("Content-Type"),
                headers.get("Content-Transfer-Encoding"),
                headers.get("X-From"),
                headers.get("X-To"),
                headers.get("X-cc"),
                headers.get("X-bcc"),
                headers.get("X-Folder"),
                headers.get("X-Origin"),
                headers.get("X-FileName"),
                body,
            )

        parse_email_udf = udf(parse_email, enron_schema)
        result_df = df.withColumn("parsed", parse_email_udf(df["message"])).select(
            "parsed.*"
        )

        return (
            result_df.withColumnRenamed("Message-ID", "id")
            .withColumnRenamed("Date", "timestamp")
            .withColumn("timestamp", col("timestamp").cast(TimestampType()))
            .withColumnRenamed("From", "sender")
            .withColumnRenamed("To", "recipient")
            .withColumnRenamed("Subject", "subject")
            .withColumnRenamed("Body", "body")
            .withColumn("channel", lit("email"))
            .withColumn("attachments", lit([]))
            .withColumn("sentiment", lit(None).cast(StringType()))
            .withColumn("language", lit(None).cast(StringType()))
        )

    def _map_clinton_fields(self, df: DataFrame) -> DataFrame:

        logger.secure_debug(
            "Mapping Clinton Emails fields to attributes schema",
            input_columns=str(df.columns),
        )

        return df

    def _map_biden_fields(self, df: DataFrame) -> DataFrame:

        logger.secure_debug(
            "Mapping Biden Emails fields to attributes schema",
            input_columns=str(df.columns),
        )

        return df
