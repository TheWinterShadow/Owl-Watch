"""
Job Configuration Manager Module

This module provides centralized configuration management for ETL jobs.
It handles parsing command line arguments, creating job configurations,
and determining job types based on file paths.
"""

import os
import sys
from typing import Any, Dict, List

from awsglue.utils import getResolvedOptions

from execution.models.exceptions import MissingConfigurationError
from execution.models.job_metadata import JobConfiguration, JobType
from execution.utils import extract_partitions
from execution.utils.logger import logger


class JobConfigManager:
    """
    Manages job configuration and argument parsing for ETL jobs.

    This class handles the parsing of command line arguments using AWS Glue's
    getResolvedOptions utility and creates JobConfiguration objects with
    proper validation and type detection.

    Attributes:
        REQUIRED_ARGS: List of required command line arguments
        args: Dictionary containing parsed job arguments
    """

    # List of required command line arguments for all jobs
    REQUIRED_ARGS = [
        "JOB_NAME",
        "s3_prefix",
        "POWERTOOLS_LOG_LEVEL",
    ]

    def __init__(self):
        """
        Initialize the JobConfigManager.

        Sets up logging and initializes the args dictionary for storing
        parsed command line arguments.
        """
        logger.secure_debug("JobConfigManager initialized")
        self.args: Dict[str, Any]

    def parse_job_arguments(self) -> Dict:
        """
        Parse required job arguments from command line using AWS Glue utilities.

        Returns:
            Dictionary containing parsed job arguments

        Raises:
            ValueError: If required arguments cannot be parsed
        """
        logger.secure_debug("Starting job argument parsing")

        try:
            # Parse required arguments using AWS Glue's getResolvedOptions
            self.args = getResolvedOptions(sys.argv, self.REQUIRED_ARGS)
            logger.secure_debug(
                "Required arguments parsed successfully", required_args=self.args
            )

            logger.secure_debug(
                "Optional arguments parsed successfully", optional_args=self.args
            )

            # Set logging level from parsed arguments
            os.environ["POWERTOOLS_LOG_LEVEL"] = self.args["POWERTOOLS_LOG_LEVEL"]

            logger.secure_debug(
                "Job arguments parsed successfully", job_name=self.args["JOB_NAME"]
            )
            return self.args

        except Exception as e:
            logger.secure_error("Failed to parse job arguments", error=str(e))
            raise ValueError(f"Failed to parse job arguments: {str(e)}")

    def _add_optional_arguments(self, optional_args: List[str]) -> None:
        """
        Add optional arguments to the parsed arguments dictionary.

        Args:
            optional_args: List of optional argument names to parse
        """
        for arg in optional_args:
            # Parse each optional argument and add to args dictionary
            optional_arg = getResolvedOptions(sys.argv, [arg])
            self.args.update(optional_arg)

    def create_job_config(self, args: Dict) -> JobConfiguration:
        """
        Create a JobConfiguration object from parsed arguments.

        Args:
            args: Dictionary containing parsed job arguments

        Returns:
            JobConfiguration object with validated parameters

        Raises:
            MissingConfigurationError: If required parameters are missing
        """
        logger.secure_debug("Creating job configuration")

        # Validate that all required parameters are present
        required_params = [
            "JOB_NAME",
            "s3_prefix",
            "POWERTOOLS_LOG_LEVEL",
        ]
        for param in required_params:
            if not args.get(param):
                raise MissingConfigurationError(parameters=[param])

        return self._create_job_config(args)

    def determine_job_type(self, file_path: str) -> JobType:
        """
        Determine the job type based on the file path prefix.

        Args:
            file_path: S3 file path to analyze

        Returns:
            JobType enum value corresponding to the file path

        Raises:
            ValueError: If the file path doesn't match any known job type patterns
        """
        # Mapping of S3 path prefixes to job types
        prefix_map = {
            "raw": JobType.ETL,
            "etl": JobType.CLEANING,
            "cleaned": JobType.SENTIMENT,
            "sentiment": JobType.NLP,
            "nlp": JobType.ANALYSTICS,
        }

        # Extract the top-level prefix from the file path
        file_parts = file_path.lower().split("/")
        top_prefix = file_parts[0]
        job_type = prefix_map.get(top_prefix)

        if job_type is None:
            raise ValueError(
                f"Cannot determine job type from file path '{file_path}'. "
                f"Expected prefixes: {list(prefix_map.keys())}"
            )

        return job_type

    def _create_job_config(self, args: Dict) -> JobConfiguration:
        """
        Create a JobConfiguration object for S3-based jobs.

        Args:
            args: Dictionary containing job arguments

        Returns:
            JobConfiguration object with S3 source information

        Raises:
            ValueError: If required S3 parameters are missing
        """
        logger.secure_debug("Configuring S3 job")

        # Validate S3-specific required parameters
        if not args.get("source_bucket") or not args.get("source_key"):
            raise ValueError("S3 jobs require source-bucket and source-key parameters")

        # Extract partition information from the S3 key
        partitions = extract_partitions(args["source_key"])
        logger.secure_debug("Partitions extracted", partitions=str(partitions))

        # Determine job type based on file path
        job_type = self.determine_job_type(args["source_key"])

        # Create the configuration object
        config = JobConfiguration(
            job_name=args["JOB_NAME"],
            job_type=job_type,
            incoming_file=args["source_key"],
        )

        logger.secure_debug("S3 job configuration created")
        return config
