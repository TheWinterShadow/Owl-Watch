"""
WhatsApp Communication ETL Job

This module provides ETL functionality for processing WhatsApp communication data.
It handles various WhatsApp export formats, parses chat messages, and standardizes
them for downstream analytics and sentiment analysis.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, lit, regexp_extract, trim, when

from execution.models.base_job import BaseGlueETLJob


class WhatsAppCommunicationETL(BaseGlueETLJob):
    """
    ETL job for processing WhatsApp communication data.

    This job handles WhatsApp chat exports in various formats (JSON, text, CSV),
    parses message content and metadata, and transforms it into a standardized
    schema for analytics and sentiment analysis.

    Key features:
    - Parse WhatsApp text export format
    - Extract timestamps, senders, and messages
    - Handle multimedia message indicators
    - Standardize contact information
    - Clean and normalize message content
    """

    def __init__(self, args_to_extract: List[str]):
        super().__init__(args_to_extract)

    def run(self) -> DataFrame:
        input_bucket = self.args.get("input-bucket")
        output_bucket = self.args.get("output-bucket")

        if not input_bucket or not output_bucket:
            raise ValueError(
                "Both input-bucket and output-bucket must be specified")

        print(f"Processing WhatsApp data from s3://{input_bucket}/whatsapp/")

        try:
            df = self.spark.read.json(f"s3://{input_bucket}/whatsapp/")
        except Exception:
            try:
                df = self.spark.read.text(f"s3://{input_bucket}/whatsapp/")
                df = self._parse_whatsapp_text(df)
            except Exception:
                df = self.spark.read.option("header", "true").csv(
                    f"s3://{input_bucket}/whatsapp/"
                )

        print(f"Read {df.count()} WhatsApp records")

        standardized_df = self._standardize_whatsapp_data(df)

        output_path = f"s3://{output_bucket}/communications/whatsapp/"
        print(f"Writing standardized data to {output_path}")

        standardized_df.write.mode("overwrite").parquet(output_path)

        print(
            f"Successfully processed {standardized_df.count()} WhatsApp records")
        return standardized_df

    def _parse_whatsapp_text(self, df: DataFrame) -> DataFrame:
        parsed_df = df.select(
            regexp_extract(
                col(
                    "value"), r"(\d{1,2}/\d{1,2}/\d{2,4}, \d{1,2}:\d{2} [AP]M)", 1
            ).alias("timestamp"),
            regexp_extract(
                col("value"),
                r"\d{1,2}/\d{1,2}/\d{2,4}, \d{1,2}:\d{2} [AP]M - ([^:]+):",
                1,
            ).alias("sender"),
            regexp_extract(
                col("value"),
                r"\d{1,2}/\d{1,2}/\d{2,4}, \d{1,2}:\d{2} [AP]M - [^:]+: (.+)",
                1,
            ).alias("message"),
        ).filter(
            (col("timestamp") != "") & (
                col("sender") != "") & (col("message") != "")
        )

        return parsed_df

    def _standardize_whatsapp_data(self, df: DataFrame) -> DataFrame:
        standardized_df = df.select(
            coalesce(col("sender"), col("from"), col("username"), col("contact")).alias(
                "sender"
            ),
            coalesce(col("recipient"), col("to"), col(
                "chat_name")).alias("recipient"),
            lit("WhatsApp Message").alias("subject"),
            coalesce(col("message"), col("text"), col("content"), col("body")).alias(
                "body"
            ),
            coalesce(col("timestamp"), col("date"),
                     col("sent_at")).alias("timestamp"),
            col("message_type"),
            col("media_type"),
        )

        standardized_df = standardized_df.withColumn(
            "sender", trim(col("sender"))
        ).withColumn(
            "recipient",
            when(
                col("recipient").isNull() | (col("recipient") == ""),
                lit("Direct Message"),
            ).otherwise(col("recipient")),
        )

        standardized_df = standardized_df.filter(
            (col("sender").isNotNull())
            & (col("body").isNotNull())
            & (col("body") != "")
            & (not col("body").rlike(r".*left|joined|added|removed.*"))
        )

        standardized_df = standardized_df.withColumn(
            "communication_type", lit("whatsapp")
        )

        return standardized_df
