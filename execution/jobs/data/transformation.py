"""
Data Transformation ETL Job

This module provides data transformation capabilities for converting cleaned data
into analytics-ready formats. It applies business logic, creates derived fields,
and structures data for downstream analytics and reporting.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    hash,
    lit,
    regexp_extract,
    size,
    split,
    to_timestamp,
    when,
)

from execution.models.base_job import BaseGlueETLJob


class DataTransformationETL(BaseGlueETLJob):
    """
    ETL job for transforming cleaned data into analytics-ready formats.

    This job applies business logic transformations, creates derived fields,
    aggregates data, and structures it for specific analytical use cases.

    Supported transformation types:
    - communication: Transform communication data for analytics
    - standard: Apply standard business transformations
    - aggregation: Create summary and aggregated views
    """

    def __init__(self, args_to_extract: List[str]):
        """Initialize the data transformation ETL job."""
        super().__init__(args_to_extract)

    def run(self) -> DataFrame:
        """
        Execute the data transformation pipeline.

        Applies business logic transformations based on the specified
        transformation type (communication, standard, aggregation).

        Returns:
            DataFrame containing transformed data ready for analytics

        Raises:
            ValueError: If required bucket parameters are missing
        """
        input_bucket = self.args.get("input-bucket")
        output_bucket = self.args.get("output-bucket")
        transformation_type = self.args.get("transformation-type", "standard")

        if not input_bucket or not output_bucket:
            raise ValueError("Both input-bucket and output-bucket must be specified")

        print(f"Processing data from s3://{input_bucket}/")
        self.logger.secure_info(
            "Processing data",
            bucket=input_bucket,
            transformation_type=transformation_type
        )

        df = self._read_input_data(input_bucket)

        print(f"Read {df.count()} records for transformation")

        if transformation_type == "communication":
            transformed_df = self._transform_communication_data(df)
        elif transformation_type == "user_profile":
            transformed_df = self._transform_user_profile_data(df)
        elif transformation_type == "temporal":
            transformed_df = self._transform_temporal_data(df)
        else:
            transformed_df = self._apply_standard_transformation(df)

        output_path = f"s3://{output_bucket}/transformed/"
        print(f"Writing transformed data to {output_path}")

        transformed_df.write.mode("overwrite").parquet(output_path)

        print(f"Successfully transformed {transformed_df.count()} records")
        return transformed_df

    def _read_input_data(self, input_bucket: str) -> DataFrame:
        try:
            return self.spark.read.parquet(f"s3://{input_bucket}/")
        except Exception:
            try:
                return self.spark.read.json(f"s3://{input_bucket}/")
            except Exception:
                return self.spark.read.option("header", "true").csv(
                    f"s3://{input_bucket}/"
                )

    def _transform_communication_data(self, df: DataFrame) -> DataFrame:
        transformed_df = df.select(
            coalesce(col("sender"), col("from"), col("user")).alias("sender"),
            coalesce(col("recipient"), col("to"), col("channel")).alias("recipient"),
            coalesce(col("subject"), lit("No Subject")).alias("subject"),
            coalesce(col("body"), col("message"), col("text")).alias("body"),
            coalesce(col("timestamp"), col("date"), col("sent_at")).alias("timestamp"),
            coalesce(col("communication_type"), lit("unknown")).alias("type"),
        )

        transformed_df = transformed_df.withColumn(
            "timestamp", to_timestamp(col("timestamp"))
        )

        transformed_df = (
            transformed_df.withColumn(
                "word_count",
                when(
                    col("body").isNotNull(), size(split(col("body"), r"\s+"))
                ).otherwise(0),
            )
            .withColumn(
                "has_attachments",
                when(col("body").rlike(r".*\[attachment\].*"), True).otherwise(False),
            )
            .withColumn(
                "is_reply",
                when(col("subject").rlike(r"^(Re:|RE:).*"), True).otherwise(False),
            )
        )

        return transformed_df

    def _transform_user_profile_data(self, df: DataFrame) -> DataFrame:
        transformed_df = df.select(
            coalesce(col("user_id"), col("id")).alias("user_id"),
            coalesce(col("username"), col("handle")).alias("username"),
            coalesce(col("email"), col("email_address")).alias("email"),
            coalesce(
                col("full_name"), concat_ws(" ", col("first_name"), col("last_name"))
            ).alias("full_name"),
            coalesce(col("department"), col("team")).alias("department"),
            coalesce(col("role"), col("position"), col("title")).alias("role"),
        )

        transformed_df = transformed_df.withColumn(
            "email_domain",
            when(
                col("email").isNotNull(), regexp_extract(col("email"), r"@(.+)", 1)
            ).otherwise(None),
        )

        transformed_df = transformed_df.withColumn(
            "user_category",
            when(
                col("email_domain").rlike(r".*(gmail|yahoo|hotmail|outlook).*"),
                "external",
            ).otherwise("internal"),
        )

        return transformed_df

    def _transform_temporal_data(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import (
            date_format,
            dayofweek,
            hour,
            month,
            weekofyear,
            year,
        )

        if "timestamp" not in df.columns:
            raise ValueError("Temporal transformation requires 'timestamp' column")

        transformed_df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

        transformed_df = (
            transformed_df.withColumn("year", year(col("timestamp")))
            .withColumn("month", month(col("timestamp")))
            .withColumn("day_of_week", dayofweek(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .withColumn("week_of_year", weekofyear(col("timestamp")))
            .withColumn("day_name", date_format(col("timestamp"), "EEEE"))
            .withColumn("month_name", date_format(col("timestamp"), "MMMM"))
        )

        transformed_df = (
            transformed_df.withColumn(
                "time_of_day",
                when(col("hour").between(6, 11), "morning")
                .when(col("hour").between(12, 17), "afternoon")
                .when(col("hour").between(18, 22), "evening")
                .otherwise("night"),
            )
            .withColumn(
                "is_weekend",
                when(col("day_of_week").isin([1, 7]), True).otherwise(False),
            )
            .withColumn(
                "is_business_hours",
                when(col("hour").between(9, 17) & ~col("is_weekend"), True).otherwise(
                    False
                ),
            )
        )

        return transformed_df

    def _apply_standard_transformation(self, df: DataFrame) -> DataFrame:
        transformed_df = df.withColumn(
            "processed_at",
            lit(self.spark.sql("SELECT current_timestamp()").collect()[0][0]),
        ).withColumn(
            "record_hash",
            hash(*[col(c) for c in df.columns]),
        )

        transformed_df = transformed_df.dropDuplicates(["record_hash"])

        return transformed_df
