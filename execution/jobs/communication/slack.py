"""
Slack Communication ETL Job

This module provides ETL functionality for processing Slack communication data.
It standardizes Slack message formats, extracts metadata, and prepares data
for downstream analytics and sentiment analysis.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, concat, lit, regexp_replace, trim, when

from execution.models.base_job import BaseGlueETLJob


class SlackCommunicationETL(BaseGlueETLJob):
    """
    ETL job for processing Slack communication data.

    This job standardizes Slack message data from various sources,
    extracts relevant metadata (channels, users, timestamps),
    and transforms it into a consistent schema for analytics.

    Key transformations:
    - Standardize message formats
    - Extract user and channel information
    - Parse timestamps and metadata
    - Clean and normalize message content
    """

    def __init__(self, args_to_extract: List[str]):
        """Initialize the Slack communication ETL job."""
        super().__init__(args_to_extract)

    def run(self) -> DataFrame:
        """
        Execute the Slack data processing pipeline.

        Reads Slack data from various formats, standardizes it,
        and writes the processed data to the communications path.

        Returns:
            DataFrame containing standardized Slack communication data

        Raises:
            ValueError: If required bucket parameters are missing
        """
        input_bucket = self.args.get("input-bucket")
        output_bucket = self.args.get("output-bucket")

        if not input_bucket or not output_bucket:
            raise ValueError("Both input-bucket and output-bucket must be specified")

        print(f"Processing Slack data from s3://{input_bucket}/slack/")

        try:
            df = self.spark.read.json(f"s3://{input_bucket}/slack/")
        except Exception:
            try:
                df = self.spark.read.parquet(f"s3://{input_bucket}/slack/")
            except Exception:
                df = self.spark.read.option("header", "true").csv(
                    f"s3://{input_bucket}/slack/"
                )

        print(f"Read {df.count()} Slack records")

        standardized_df = self._standardize_slack_data(df)

        output_path = f"s3://{output_bucket}/communications/slack/"
        print(f"Writing standardized data to {output_path}")

        standardized_df.write.mode("overwrite").parquet(output_path)

        print(f"Successfully processed {standardized_df.count()} Slack records")
        return standardized_df

    def _standardize_slack_data(self, df: DataFrame) -> DataFrame:
        standardized_df = df.select(
            coalesce(col("user"), col("sender"), col("username"), col("user_id")).alias(
                "sender"
            ),
            coalesce(col("channel"), col("channel_id"), col("channel_name")).alias(
                "recipient"
            ),
            when(col("thread_ts").isNotNull(), lit("Thread Reply"))
            .otherwise(lit("Slack Message"))
            .alias("subject"),
            coalesce(col("text"), col("message"), col("content"), col("body")).alias(
                "body"
            ),
            coalesce(
                col("ts"), col("timestamp"), col("created_at"), col("sent_at")
            ).alias("timestamp"),
            col("thread_ts").alias("thread_timestamp"),
            coalesce(col("subtype"), lit("message")).alias("message_subtype"),
            col("reactions"),
            col("reply_count"),
        )

        standardized_df = self._clean_slack_data(standardized_df)

        standardized_df = standardized_df.withColumn("communication_type", lit("slack"))

        return standardized_df

    def _clean_slack_data(self, df: DataFrame) -> DataFrame:
        cleaned_df = (
            df.withColumn(
                "body",
                regexp_replace(col("body"), r"<@U[A-Z0-9]+>", "@user"),
            )
            .withColumn(
                "body",
                regexp_replace(col("body"), r"<#C[A-Z0-9]+\|([^>]+)>", "#$1"),
            )
            .withColumn(
                "body",
                regexp_replace(col("body"), r"<(https?://[^|>]+)\|([^>]+)>", "$2 ($1)"),
            )
            .withColumn(
                "body",
                regexp_replace(col("body"), r"<([^>]+)>", "$1"),
            )
            .withColumn(
                "body",
                trim(col("body")),
            )
        )

        cleaned_df = cleaned_df.withColumn(
            "recipient",
            when(col("recipient").startswith("#"), col("recipient")).otherwise(
                concat(lit("#"), col("recipient"))
            ),
        )

        cleaned_df = cleaned_df.filter(
            col("sender").isNotNull()
            & col("recipient").isNotNull()
            & col("body").isNotNull()
            & (col("body") != "")
            & (col("message_subtype") != "channel_join")
            & (col("message_subtype") != "channel_leave")
        )

        return cleaned_df
