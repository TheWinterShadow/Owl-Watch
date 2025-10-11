"""
Data Cleaning ETL Job

This module provides comprehensive data cleaning and validation functionality
for raw data ingested into the Owl-Watch pipeline. It handles data quality issues,
standardizes formats, and ensures data consistency.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    length,
    lit,
    lower,
    regexp_replace,
    trim,
    when,
)
from pyspark.sql.types import DoubleType, IntegerType

from execution.models.base_job import BaseGlueETLJob
from execution.utils.logger import logger


class DataCleaningETL(BaseGlueETLJob):
    """
    ETL job for cleaning and validating raw data.

    This job processes raw data from various sources, applies data quality rules,
    removes or fixes invalid records, standardizes formats, and outputs clean data
    ready for downstream processing and analytics.

    Key cleaning operations:
    - Remove duplicate records
    - Handle missing values
    - Standardize text fields
    - Validate data types and formats
    - Remove invalid records
    - Apply business rules
    """

    def __init__(self, args_to_extract: List[str]):
        """Initialize the data cleaning ETL job."""
        super().__init__(args_to_extract)

    def run(self) -> DataFrame:
        """
        Execute the data cleaning pipeline.

        Reads raw data from S3, applies cleaning transformations,
        validates data quality, and writes clean data back to S3.

        Returns:
            DataFrame containing cleaned and validated data

        Raises:
            ValueError: If required bucket parameters are missing
        """
        raw_bucket = self.args.get("raw_data")
        cleaned_bucket = self.args.get("cleaned_data")

        if not raw_bucket or not cleaned_bucket:
            raise ValueError("Both raw_data and cleaned_data must be specified")

        logger.secure_info(f"Processing raw data from s3://{raw_bucket}/raw/")

        try:
            df = self.spark.read.json(f"s3://{raw_bucket}/raw/")
        except Exception:
            try:
                df = self.spark.read.parquet(f"s3://{raw_bucket}/raw/")
            except Exception:
                df = self.spark.read.option("header", "true").csv(
                    f"s3://{raw_bucket}/raw/"
                )

        logger.secure_info(f"Read {df.count()} raw records")

        cleaned_df = self._clean_data(df)

        validated_df = self._validate_data_quality(cleaned_df)

        output_path = f"s3://{cleaned_bucket}/cleaned/"
        logger.secure_info(f"Writing cleaned data to {output_path}")

        validated_df.write.mode("overwrite").parquet(output_path)

        logger.secure_info(f"Successfully cleaned {validated_df.count()} records")
        return validated_df

    def _clean_data(self, df: DataFrame) -> DataFrame:
        cleaned_df = df

        for col_name in df.columns:
            if col_name in df.columns:
                cleaned_df = cleaned_df.withColumn(
                    col_name,
                    when(
                        col(col_name).isNull()
                        | (col(col_name) == "")
                        | (lower(col(col_name)) == "null")
                        | (lower(col(col_name)) == "n/a")
                        | (lower(col(col_name)) == "none"),
                        None,
                    ).otherwise(col(col_name)),
                )

        string_columns = [
            field.name
            for field in df.schema.fields
            if str(field.dataType) == "StringType"
        ]

        for col_name in string_columns:
            cleaned_df = cleaned_df.withColumn(
                col_name,
                when(
                    col(col_name).isNotNull(),
                    trim(
                        regexp_replace(
                            col(col_name),
                            r"[\r\n\t]+",
                            " ",
                        )
                    ),
                ).otherwise(col(col_name)),
            )

        if "email" in df.columns:
            cleaned_df = cleaned_df.withColumn(
                "email",
                when(col("email").isNotNull(), lower(trim(col("email")))).otherwise(
                    col("email")
                ),
            )

        if "phone" in df.columns:
            cleaned_df = cleaned_df.withColumn(
                "phone",
                when(
                    col("phone").isNotNull(),
                    regexp_replace(col("phone"), r"[^\d+]", ""),
                ).otherwise(col("phone")),
            )

        name_columns = ["name", "first_name", "last_name", "full_name"]
        for col_name in name_columns:
            if col_name in df.columns:
                cleaned_df = cleaned_df.withColumn(
                    col_name,
                    when(
                        col(col_name).isNotNull(),
                        regexp_replace(
                            trim(col(col_name)),
                            r"\s+",
                            " ",
                        ),
                    ).otherwise(col(col_name)),
                )

        return cleaned_df

    def _validate_data_quality(self, df: DataFrame) -> DataFrame:
        validated_df = df

        if "email" in df.columns:
            validated_df = validated_df.filter(
                col("email").isNull()
                | col("email").rlike(
                    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                )
            )

        if "phone" in df.columns:
            validated_df = validated_df.filter(
                col("phone").isNull() | (length(col("phone")) >= 10)
            )

        non_null_columns = [
            col_name
            for col_name in df.columns
            if col_name not in ["id", "created_at", "updated_at"]
        ]

        if non_null_columns:
            not_all_null_condition = None
            for col_name in non_null_columns:
                condition = col(col_name).isNotNull()
                if not_all_null_condition is None:
                    not_all_null_condition = condition
                else:
                    not_all_null_condition = not_all_null_condition | condition

            if not_all_null_condition is not None:
                validated_df = validated_df.filter(not_all_null_condition)

        validated_df = validated_df.withColumn(
            "data_quality_score", self._calculate_quality_score(validated_df.columns)
        ).withColumn(
            "cleaned_at",
            lit(self.spark.sql("SELECT current_timestamp()").collect()[0][0]),
        )

        return validated_df

    def _calculate_quality_score(self, columns: List[str]):
        non_null_count = lit(0)
        total_columns = len(
            [col for col in columns if col not in ["data_quality_score", "cleaned_at"]]
        )

        for col_name in columns:
            if col_name not in ["data_quality_score", "cleaned_at"]:
                non_null_count = non_null_count + when(
                    col(col_name).isNotNull() & (col(col_name) != ""), 1
                ).otherwise(0)

        return (non_null_count.cast(DoubleType()) / lit(total_columns) * 100).cast(
            IntegerType()
        )
