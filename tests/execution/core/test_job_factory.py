"""
Test suite for JobFactory functionality.

This module contains comprehensive tests for the JobFactory class,
validating job registration, creation, argument handling, and error scenarios.
"""

import unittest
from unittest.mock import patch

from execution.core.job_factory import JobFactory
from execution.jobs.analytics.bedrock_ai import BedrockAIProcessingETL
from execution.jobs.analytics.sentiment import SentimentAnalysisETL
from execution.jobs.communication.email import EmailETL
from execution.jobs.data.cleaning import DataCleaningETL
from execution.models.base_job import BaseGlueETLJob


class TestJobFactory(unittest.TestCase):
    """
    Test suite for JobFactory class.

    Tests job registration, creation, argument handling, and validation
    of the factory pattern implementation for ETL job management.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.factory = JobFactory()

    def test_init_registers_jobs(self):
        """Test that JobFactory properly registers all expected job types."""
        expected_jobs = {
            "email_communication": EmailETL,
            "data_cleaning": DataCleaningETL,
            "sentiment_analysis": SentimentAnalysisETL,
            "nlp_analysis": BedrockAIProcessingETL,
        }

        self.assertEqual(self.factory._job_registry, expected_jobs)

    def test_get_job_class_valid_types(self):
        """Test that get_job_class returns correct job classes for valid types."""
        test_cases = [
            ("email_communication", EmailETL),
            ("data_cleaning", DataCleaningETL),
            ("sentiment_analysis", SentimentAnalysisETL),
            ("nlp_analysis", BedrockAIProcessingETL),
        ]

        for job_type, expected_class in test_cases:
            with self.subTest(job_type=job_type):
                result = self.factory.get_job_class(job_type)
                self.assertEqual(result, expected_class)

    def test_get_job_class_invalid_type(self):
        """Test that get_job_class raises ValueError for invalid job types."""
        invalid_job_type = "invalid_job_type"

        with self.assertRaises(ValueError) as context:
            self.factory.get_job_class(invalid_job_type)

        error_message = str(context.exception)
        self.assertIn("Unknown job type 'invalid_job_type'", error_message)
        self.assertIn("Available types:", error_message)

    @patch("execution.jobs.communication.email.BaseGlueETLJob.__init__")
    def test_create_job_with_default_args(self, mock_super_init):
        mock_super_init.return_value = None
        job_type = "email_communication"

        result = self.factory.create_job(job_type)

        self.assertIsInstance(result, EmailETL)
        mock_super_init.assert_called_once_with(["raw_data", "cleaned_data"])

    @patch("execution.jobs.data.cleaning.BaseGlueETLJob.__init__")
    @patch.multiple(DataCleaningETL, __abstractmethods__=set())
    def test_create_job_with_custom_args(self, mock_super_init):
        mock_super_init.return_value = None
        job_type = "data_cleaning"
        custom_args = ["custom_bucket", "output_bucket"]

        result = self.factory.create_job(job_type, custom_args)

        self.assertIsInstance(result, DataCleaningETL)
        mock_super_init.assert_called_once_with(custom_args)

    def test_create_job_invalid_type(self):
        invalid_job_type = "invalid_job"

        with self.assertRaises(ValueError):
            self.factory.create_job(invalid_job_type)

    def test_get_default_args(self):
        test_cases = [
            ("email_communication", ["raw_data", "cleaned_data"]),
            ("data_cleaning", ["cleaned_data", "cleaned_data"]),
            ("sentiment_analysis", ["cleaned_data", "cleaned_data"]),
            ("nlp_analysis", ["cleaned_data", "cleaned_data"]),
            ("unknown_job", ["raw_data", "raw_data"]),
        ]

        for job_type, expected_args in test_cases:
            with self.subTest(job_type=job_type):
                result = self.factory._get_default_args(job_type)

                self.assertEqual(result, expected_args)

    def test_list_available_jobs(self):
        result = self.factory.list_available_jobs()

        expected_jobs = {
            "email_communication": "Process and standardize email communication data",
            "data_cleaning": "Clean and validate raw data",
            "sentiment_analysis": "Analyze sentiment in communication data",
            "nlp_analysis": "Perform NLP analysis on text data",
        }

        self.assertEqual(result, expected_jobs)
        self.assertEqual(len(result), 4)

    def test_job_registry_types(self):
        for job_type, job_class in self.factory._job_registry.items():
            with self.subTest(job_type=job_type):
                self.assertTrue(
                    issubclass(job_class, BaseGlueETLJob),
                    f"{job_class} should inherit from BaseGlueETLJob",
                )


if __name__ == "__main__":
    unittest.main()
