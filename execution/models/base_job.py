"""
Base ETL Job Model

This module provides the abstract base class for all ETL jobs in the Owl-Watch pipeline.
It standardizes job initialization, Spark/Glue context management, and common operations.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType

from execution.models.job_metadata import DataSource
from execution.utils.logger import logger
from execution.utils.validation import SchemaValidator, ValidationResult


class BaseGlueETLJob(ABC):
    """
    Abstract base class for all ETL jobs in the Owl-Watch pipeline.

    This class provides common functionality for AWS Glue ETL jobs including:
    - Spark/Glue context initialization
    - Argument parsing and validation
    - Schema validation
    - Common data operations (ID generation, field addition)
    - Job lifecycle management (commit, cleanup)

    Attributes:
        args_to_extract: List of command line arguments to extract
        args: Dictionary of parsed job arguments
        sc: SparkContext for Spark operations
        glue_context: GlueContext for AWS Glue operations
        spark: SparkSession for DataFrame operations
        job: Glue Job instance for job management
        schema_validator: Validator for DataFrame schemas
    """

    def __init__(self, args_to_extract: List[str]):
        """
        Initialize the base ETL job with argument parsing and context setup.

        Args:
            args_to_extract: List of command line arguments to extract and parse
        """
        self.args_to_extract = args_to_extract + ["JOB_NAME"]
        self.args = self._get_args()
        self._initialize_glue_context()
        self.schema_validator = SchemaValidator(check_nullable=False)
        logger.secure_info(
            "BaseGlueETLJob initialized", job_class=self.__class__.__name__
        )

    def _get_args(self) -> Dict[str, Any]:
        """Parse command line arguments using AWS Glue utilities."""
        import sys

        from awsglue.utils import getResolvedOptions

        return getResolvedOptions(sys.argv, self.args_to_extract)

    def _initialize_glue_context(self):
        """Initialize Spark and Glue contexts for job execution."""
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(self.args["JOB_NAME"], self.args)

    def commit(self):
        """Commit the Glue job (save state, bookmarks, etc.)."""
        self.job.commit()

    def cleanup(self):
        """Clean up resources by stopping the Spark context."""
        self.sc.stop()

    @abstractmethod
    def get_expected_schema(self) -> StructType:
        """
        Return the expected schema for this job's data.

        Must be implemented by subclasses to define their expected data structure.

        Returns:
            StructType defining the expected DataFrame schema
        """
        pass

    @abstractmethod
    def run(self) -> DataFrame:
        pass

    def add_base_fields(
        self, df: DataFrame, data_source: DataSource, dataset_name: str
    ) -> DataFrame:
        if hasattr(data_source, "value"):
            data_source_str = data_source.value
        else:
            data_source_str = str(data_source)

        logger.secure_info(
            "Adding base fields to DataFrame",
            data_source=data_source_str,
            dataset_name=dataset_name,
        )
        current_date = datetime.now()

        return df.withColumns(
            {
                "data_source": lit(data_source_str),
                "dataset_name": lit(dataset_name),
                "ingestion_timestamp": current_timestamp(),
                "processing_date": lit(current_date.date()),
                "schema_version": lit("2.0"),
            }
        )

    def generate_record_id(self, df: DataFrame, prefix: str = "REC") -> DataFrame:
        try:
            from pyspark.sql.functions import concat, monotonically_increasing_id

            logger.secure_debug("Generating record IDs", prefix=prefix)
            result_df = df.withColumn(
                "record_id",
                concat(lit(f"{prefix}-"),
                       monotonically_increasing_id().cast("string")),
            )
            logger.secure_debug("Record IDs generated successfully")
            return result_df
        except Exception as e:
            logger.secure_error(
                "Error generating record IDs", prefix=prefix, error=str(e)
            )
            raise

    def validate_schema(
        self, df: DataFrame, expected_schema: StructType
    ) -> ValidationResult:
        logger.secure_debug("Starting schema validation")

        validation_result = self.schema_validator.validate_schema(
            df, expected_schema)

        if validation_result["is_valid"]:
            logger.secure_info("Schema validation passed")
        else:
            logger.secure_warning(
                "Schema validation failed",
                errors=validation_result["errors"],
                warnings=validation_result["warnings"],
            )

        return validation_result

    def add_quality_flags(self, df: DataFrame) -> DataFrame:
        try:
            logger.secure_debug("Adding quality flags to DataFrame")
            result_df = df.withColumns(
                {"data_quality_score": lit(100), "quality_flags": lit("VALID")}
            )

            logger.secure_debug("Quality flags added successfully")
            return result_df
        except Exception as e:
            logger.secure_error("Error adding quality flags", error=str(e))
            raise

    def add_partitions_to_dataframe(
        self, df: DataFrame, partitions: Dict[str, str], source_key: str
    ) -> DataFrame:
        try:
            logger.secure_info(
                "Adding partition columns to DataFrame",
                partition_keys=list(partitions.keys()),
                source_key=source_key,
            )

            result_df = df
            result_df = df.withColumns(
                {key: lit(value) for key, value in partitions.items()}
            )
            logger.secure_debug(f"Added partition columns: {partitions}")

            logger.secure_debug("Partition columns added successfully")
            return result_df
        except Exception as e:
            logger.secure_error(
                "Error adding partition columns", source_key=source_key, error=str(e)
            )
            raise

    def select_expected_fields(self, df: DataFrame) -> DataFrame:
        try:
            expected_schema = self.get_expected_schema()
            logger.secure_debug(
                "Selecting expected fields", field_count=len(expected_schema.fields)
            )

            result_df = self.schema_validator.select_expected_fields(
                df, expected_schema
            )

            logger.secure_info("Expected fields selected successfully")
            return result_df
        except Exception as e:
            logger.secure_error(
                "Error selecting expected fields", error=str(e))
            raise
