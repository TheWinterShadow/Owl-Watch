"""
Job Factory Module

This module provides a factory pattern implementation for creating ETL job instances
based on job type strings. It centralizes job registration and creation logic.
"""

from typing import Dict, List, Optional, Type

from execution.jobs.analytics.bedrock_ai import BedrockAIProcessingETL
from execution.jobs.analytics.sentiment import SentimentAnalysisETL
from execution.jobs.communication.email import EmailETL
from execution.jobs.data.cleaning import DataCleaningETL
from execution.models.base_job import BaseGlueETLJob


class JobFactory:
    """
    Factory class for creating ETL job instances.

    This factory implements a registry pattern to manage available job types
    and provides methods to create job instances with appropriate configuration.

    Attributes:
        _job_registry: Dictionary mapping job type strings to job classes
    """

    def __init__(self):
        """
        Initialize the JobFactory with an empty registry and register all available jobs.
        """
        self._job_registry: Dict[str, Type[BaseGlueETLJob]] = {}
        self._register_jobs()

    def _register_jobs(self):
        """
        Register all available job types with their corresponding classes.

        This method populates the job registry with mappings from job type strings
        to their respective ETL job classes.
        """
        self._job_registry = {
            "email_communication": EmailETL,
            "data_cleaning": DataCleaningETL,
            "sentiment_analysis": SentimentAnalysisETL,
            "nlp_analysis": BedrockAIProcessingETL,
        }

    def get_job_class(self, job_type: str) -> Type[BaseGlueETLJob]:
        """
        Retrieve the job class for a given job type.

        Args:
            job_type: String identifier for the job type

        Returns:
            The job class corresponding to the specified job type

        Raises:
            ValueError: If the job type is not registered
        """
        job_class: Optional[Type[BaseGlueETLJob]] = self._job_registry.get(job_type)
        if job_class is None:
            available_types = list(self._job_registry.keys())
            raise ValueError(
                f"Unknown job type '{job_type}'. " f"Available types: {available_types}"
            )
        return job_class

    def create_job(
        self, job_type: str, args_to_extract: Optional[List[str]] = None
    ) -> BaseGlueETLJob:
        """
        Create and return an instance of the specified job type.

        Args:
            job_type: String identifier for the job type to create
            args_to_extract: Optional list of arguments to extract from command line.
                           If None, uses default arguments for the job type.

        Returns:
            An initialized instance of the requested job type

        Raises:
            ValueError: If the job type is not registered
        """
        job_class = self.get_job_class(job_type)

        # Use default arguments if none provided
        if args_to_extract is None:
            args_to_extract = self._get_default_args(job_type)

        return job_class(args_to_extract)

    def _get_default_args(self, job_type: str) -> list:
        """
        Get default arguments for a specific job type.

        Args:
            job_type: String identifier for the job type

        Returns:
            List of default argument names for the job type
        """
        # Define default arguments for each job type
        # These typically represent input and output data sources
        default_args_map = {
            "email_communication": ["raw_data", "cleaned_data"],
            "data_cleaning": ["cleaned_data", "cleaned_data"],
            "sentiment_analysis": ["cleaned_data", "cleaned_data"],
            "nlp_analysis": ["cleaned_data", "cleaned_data"],
        }

        return default_args_map.get(job_type, ["raw_data", "raw_data"])

    def list_available_jobs(self) -> Dict[str, str]:
        """
        Get a dictionary of all available job types with their descriptions.

        Returns:
            Dictionary mapping job type strings to their descriptions
        """
        # Human-readable descriptions for each job type
        descriptions = {
            "email_communication": "Process and standardize email communication data",
            "data_cleaning": "Clean and validate raw data",
            "sentiment_analysis": "Analyze sentiment in communication data",
            "nlp_analysis": "Perform NLP analysis on text data",
        }

        return {
            job_type: descriptions.get(job_type, "No description available")
            for job_type in self._job_registry.keys()
        }
