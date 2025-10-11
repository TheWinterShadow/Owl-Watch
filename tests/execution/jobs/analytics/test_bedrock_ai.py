"""
Test suite for Bedrock AI Processing ETL Job.

This module contains comprehensive tests for the BedrockAIProcessingETL class,
validating AI processing functionality, error handling, and integration scenarios.
"""

import json
import unittest
from unittest.mock import Mock, patch

from execution.jobs.analytics.bedrock_ai import BedrockAIProcessingETL


class TestableBedrockAIProcessingETL(BedrockAIProcessingETL):
    """
    Testable version of BedrockAIProcessingETL that bypasses initialization issues.

    This subclass allows for easier testing by avoiding Glue context initialization
    and providing mock implementations of required methods.
    """

    def __init__(self, args_to_extract):
        self.args = {}
        super().__init__(args_to_extract)

    def get_expected_schema(self):
        mock_schema = Mock()
        mock_schema.fields = []
        return mock_schema


class TestBedrockAIProcessingETL(unittest.TestCase):
    """
    Test suite for BedrockAIProcessingETL functionality.

    Tests the AI processing pipeline including Bedrock API integration,
    data processing workflows, error handling, and mock scenarios.
    """

    def setUp(self):
        """Set up test fixtures and patches before each test method."""
        self.args_to_extract = [
            "input-bucket",
            "output-bucket",
            "model-id",
            "aws-region",
        ]

        # Patch schema validation to avoid Glue context issues
        self.schema_patcher = patch.object(
            BedrockAIProcessingETL, "get_expected_schema", return_value={}
        )
        self.schema_patcher.start()

    def tearDown(self):
        """Clean up patches after each test method."""
        self.schema_patcher.stop()

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_init(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_bedrock_client = Mock()
        mock_setup_bedrock.return_value = mock_bedrock_client

        TestableBedrockAIProcessingETL(self.args_to_extract)

        mock_super_init.assert_called_once_with(self.args_to_extract)
        mock_setup_bedrock.assert_called_once()

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_run_missing_buckets(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)
        job.args = {}

        with self.assertRaises(ValueError) as context:
            job.run()

        self.assertIn(
            "Both input-bucket and output-bucket must be specified",
            str(context.exception),
        )

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_run_success(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_bedrock_client = Mock()
        mock_setup_bedrock.return_value = mock_bedrock_client

        job = TestableBedrockAIProcessingETL(self.args_to_extract)
        job.args = {
            "input-bucket": "test-input",
            "output-bucket": "test-output",
            "model-id": "anthropic.claude-v2",
        }

        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_processed_df = Mock()
        mock_processed_df.count.return_value = 100
        mock_processed_df.write.mode.return_value.parquet.return_value = None

        job._read_input_data = Mock(return_value=mock_df)
        job._process_with_bedrock = Mock(return_value=mock_processed_df)

        result = job.run()

        self.assertEqual(result, mock_processed_df)
        job._read_input_data.assert_called_once_with("test-input")
        job._process_with_bedrock.assert_called_once_with(
            mock_df, "anthropic.claude-v2"
        )

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_read_input_data_communications(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.read.parquet.return_value = mock_df
        job.spark = mock_spark

        result = job._read_input_data("test-bucket")

        self.assertEqual(result, mock_df)
        mock_spark.read.parquet.assert_called_with(
            "s3://test-bucket/communications/")

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_read_input_data_cleaned_fallback(
        self, mock_setup_bedrock, mock_super_init
    ):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        mock_spark = Mock()
        mock_df = Mock()

        mock_spark.read.parquet.side_effect = [
            Exception("No communications"), mock_df]
        job.spark = mock_spark

        result = job._read_input_data("test-bucket")

        self.assertEqual(result, mock_df)
        self.assertEqual(mock_spark.read.parquet.call_count, 2)

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_setup_bedrock_client_success(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_bedrock_client = Mock()
        mock_setup_bedrock.return_value = mock_bedrock_client

        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        self.assertEqual(job._bedrock_client, mock_bedrock_client)
        mock_setup_bedrock.assert_called_once()

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_setup_bedrock_client_failure(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.side_effect = Exception("AWS credentials not found")

        with self.assertRaises(Exception):
            TestableBedrockAIProcessingETL(self.args_to_extract)

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_identify_text_column_body(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["id", "body", "timestamp"]

        result = job._identify_text_column(mock_df)

        self.assertEqual(result, "body")

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_identify_text_column_fallback(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)
        mock_df = Mock()
        mock_df.columns = ["id", "notes", "timestamp"]
        id_field = Mock()
        id_field.name = "id"
        id_field.dataType.__str__ = lambda x: "IntegerType"

        notes_field = Mock()
        notes_field.name = "notes"
        notes_field.dataType.__str__ = lambda x: "StringType"

        timestamp_field = Mock()
        timestamp_field.name = "timestamp"
        timestamp_field.dataType.__str__ = lambda x: "TimestampType"

        mock_df.schema.fields = [id_field, notes_field, timestamp_field]

        result = job._identify_text_column(mock_df)

        self.assertEqual(result, "notes")

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_identify_text_column_none(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        mock_df = Mock()
        mock_df.columns = ["id", "count"]
        mock_df.schema.fields = [
            Mock(name="id", dataType=Mock(__str__=lambda x: "IntegerType")),
            Mock(name="count", dataType=Mock(__str__=lambda x: "IntegerType")),
        ]

        result = job._identify_text_column(mock_df)

        self.assertIsNone(result)

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_call_bedrock_api_empty_text(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_bedrock_client = Mock()
        mock_setup_bedrock.return_value = mock_bedrock_client
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        result = job._call_bedrock_api("", "anthropic.claude-v2")

        expected = job._empty_analysis()
        self.assertEqual(result, expected)

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_call_bedrock_api_claude_success(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_bedrock_client = Mock()
        mock_setup_bedrock.return_value = mock_bedrock_client
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        mock_response = {
            "body": Mock(
                read=Mock(
                    return_value=json.dumps(
                        {
                            "completion": '{"analysis": "Test analysis", "sentiment": "positive"}'
                        }
                    )
                )
            )
        }
        mock_bedrock_client.invoke_model.return_value = mock_response

        result = job._call_bedrock_api("Test text", "anthropic.claude-v2")

        self.assertIn("analysis", result)
        self.assertIn("sentiment", result)

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_call_bedrock_api_exception(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_bedrock_client = Mock()
        mock_bedrock_client.invoke_model.side_effect = Exception("API Error")
        mock_setup_bedrock.return_value = mock_bedrock_client
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        result = job._call_bedrock_api("Test text", "anthropic.claude-v2")

        self.assertIn("analysis", result)
        self.assertIn("Mock analysis", result["analysis"])

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_parse_ai_response_json(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        json_response = json.dumps(
            {
                "analysis": "Test analysis",
                "key_themes": "theme1, theme2",
                "sentiment": "positive",
                "entities": "entity1",
                "confidence": "high",
            }
        )

        result = job._parse_ai_response(json_response)

        self.assertEqual(result["analysis"], "Test analysis")
        self.assertEqual(result["sentiment"], "positive")
        self.assertEqual(result["confidence"], "high")

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_parse_ai_response_fallback(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        text_response = "This is a free-form response"

        result = job._parse_ai_response(text_response)

        self.assertIn("This is a free-form response", result["analysis"])
        self.assertEqual(result["sentiment"], "neutral")
        self.assertEqual(result["confidence"], "medium")

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_mock_ai_analysis_positive(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        result = job._mock_ai_analysis("This is excellent work great job")

        self.assertEqual(result["sentiment"], "positive")
        self.assertIn("Mock analysis", result["analysis"])

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_mock_ai_analysis_negative(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        result = job._mock_ai_analysis("This is terrible and bad")

        self.assertEqual(result["sentiment"], "negative")
        self.assertIn("Mock analysis", result["analysis"])

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_mock_ai_analysis_neutral(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        result = job._mock_ai_analysis("This is a normal message")

        self.assertEqual(result["sentiment"], "neutral")
        self.assertIn("Mock analysis", result["analysis"])

    @patch("execution.jobs.analytics.bedrock_ai.BaseGlueETLJob.__init__")
    @patch(
        "execution.jobs.analytics.bedrock_ai.BedrockAIProcessingETL._setup_bedrock_client"
    )
    def test_empty_analysis(self, mock_setup_bedrock, mock_super_init):
        mock_super_init.return_value = None
        mock_setup_bedrock.return_value = Mock()
        job = TestableBedrockAIProcessingETL(self.args_to_extract)

        result = job._empty_analysis()

        self.assertEqual(result["analysis"], "")
        self.assertEqual(result["sentiment"], "neutral")
        self.assertEqual(result["confidence"], "low")
        self.assertIn("key_themes", result)
        self.assertIn("entities", result)


if __name__ == "__main__":
    unittest.main()
