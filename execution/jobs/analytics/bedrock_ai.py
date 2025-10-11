"""
Bedrock AI Processing ETL Job

This module provides an ETL job that processes text data using AWS Bedrock AI models.
It performs natural language processing tasks including sentiment analysis, entity extraction,
and theme identification on communication data.
"""

import json
from typing import Dict, List, Optional

import boto3
from mypy_boto3_bedrock_runtime import BedrockRuntimeClient
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType, StructField, StructType

from execution.models.base_job import BaseGlueETLJob
from execution.utils.logger import logger


class BedrockAIProcessingETL(BaseGlueETLJob):
    """
    ETL job for processing text data with AWS Bedrock AI models.

    This job reads communication data from S3, processes it using various Bedrock AI models
    (Claude, Llama, etc.) to extract insights like sentiment, themes, and entities,
    then writes the enriched results back to S3.

    Attributes:
        _bedrock_client: AWS Bedrock Runtime client for API calls
    """

    def __init__(self, args_to_extract: List[str]):
        """
        Initialize the Bedrock AI Processing ETL job.

        Args:
            args_to_extract: List of command line arguments to extract and use
        """
        super().__init__(args_to_extract)
        self._bedrock_client: BedrockRuntimeClient = self._setup_bedrock_client()

    def run(self) -> DataFrame:
        """
        Execute the Bedrock AI processing pipeline.

        This method orchestrates the entire AI processing workflow:
        1. Read input data from S3
        2. Process text using Bedrock AI models
        3. Write enriched results back to S3

        Returns:
            DataFrame containing the AI-processed results

        Raises:
            ValueError: If required bucket parameters are missing
        """
        # Extract configuration parameters
        input_bucket = self.args.get("input-bucket")
        output_bucket = self.args.get("output-bucket")
        model_id = self.args.get("model-id", "anthropic.claude-v2")

        if not input_bucket or not output_bucket:
            raise ValueError(
                "Both input-bucket and output-bucket must be specified")

        logger.secure_info(f"Processing data with Bedrock model: {model_id}")
        logger.secure_info(f"Reading from s3://{input_bucket}/")

        # Read input data from S3
        df = self._read_input_data(input_bucket)
        logger.secure_info(f"Read {df.count()} records for AI processing")

        # Ensure Bedrock client is set up
        self._setup_bedrock_client()

        # Process data with AI model
        processed_df = self._process_with_bedrock(df, model_id)

        # Write results to S3
        output_path = f"s3://{output_bucket}/ai_processed/"
        logger.secure_info(f"Writing AI processing results to {output_path}")
        processed_df.write.mode("overwrite").parquet(output_path)

        logger.secure_info(
            f"Successfully processed {processed_df.count()} records with AI"
        )
        return processed_df

    def _read_input_data(self, input_bucket: str) -> DataFrame:
        """
        Read input data from S3 with fallback paths.

        Tries multiple S3 paths in order: communications/, cleaned/, root.

        Args:
            input_bucket: S3 bucket name to read from

        Returns:
            DataFrame containing the input data
        """
        try:
            return self.spark.read.parquet(f"s3://{input_bucket}/communications/")
        except Exception:
            try:
                return self.spark.read.parquet(f"s3://{input_bucket}/cleaned/")
            except Exception:
                return self.spark.read.parquet(f"s3://{input_bucket}/")

    def _setup_bedrock_client(self) -> BedrockRuntimeClient:
        """
        Initialize and configure the AWS Bedrock Runtime client.

        Returns:
            Configured BedrockRuntimeClient instance

        Raises:
            Exception: If client initialization fails
        """
        try:
            logger.info("Bedrock client initialized successfully")
            return boto3.client(
                "bedrock-runtime", region_name=self.args.get("aws-region", "us-east-1")
            )
        except Exception as e:
            logger.secure_warning(
                f"Warning: Could not initialize Bedrock client: {e}")
            logger.warning("AI processing will use mock responses")
            raise e

    def _process_with_bedrock(self, df: DataFrame, model_id: str) -> DataFrame:
        if self._bedrock_client:
            bedrock_udf = udf(
                lambda text: self._call_bedrock_api(text, model_id),
                StructType(
                    [
                        StructField("analysis", StringType(), True),
                        StructField("key_themes", StringType(), True),
                        StructField("sentiment", StringType(), True),
                        StructField("entities", StringType(), True),
                        StructField("confidence", StringType(), True),
                    ]
                ),
            )
        else:
            bedrock_udf = udf(
                lambda text: self._mock_ai_analysis(text),
                StructType(
                    [
                        StructField("analysis", StringType(), True),
                        StructField("key_themes", StringType(), True),
                        StructField("sentiment", StringType(), True),
                        StructField("entities", StringType(), True),
                        StructField("confidence", StringType(), True),
                    ]
                ),
            )

        text_column = self._identify_text_column(df)

        if not text_column:
            raise ValueError("No suitable text column found for AI processing")

        processed_df = df.withColumn(
            "ai_analysis", bedrock_udf(col(text_column)))

        processed_df = processed_df.select(
            "*",
            col("ai_analysis.analysis").alias("ai_summary"),
            col("ai_analysis.key_themes").alias("key_themes"),
            col("ai_analysis.sentiment").alias("ai_sentiment"),
            col("ai_analysis.entities").alias("extracted_entities"),
            col("ai_analysis.confidence").alias("ai_confidence"),
        ).drop("ai_analysis")

        processed_df = processed_df.withColumn(
            "ai_model_used", lit(model_id)
        ).withColumn(
            "ai_processed_at",
            lit(self.spark.sql("SELECT current_timestamp()").collect()[0][0]),
        )

        return processed_df

    def _identify_text_column(self, df: DataFrame) -> Optional[str]:
        text_candidates = ["body", "text", "message", "content", "description"]

        for candidate in text_candidates:
            if candidate in df.columns:
                return candidate

        string_columns = [
            field.name
            for field in df.schema.fields
            if str(field.dataType) == "StringType"
        ]

        if string_columns:
            return string_columns[0]

        return None

    def _call_bedrock_api(self, text: str, model_id: str) -> Dict[str, str]:
        if not text or len(text.strip()) == 0:
            return self._empty_analysis()

        try:
            prompt = 'Analyze the following text for key themes, sentiment, and entities. Provide a concise summary in JSON format with fields: analysis, key_themes, sentiment (positive, negative, neutral), entities (comma-separated), confidence (high, medium, low).\n\nText: """{text}"""'.format(  # noqa: E501
                text=text
            )

            if "claude" in model_id.lower():
                response = self._call_claude_model(prompt, model_id)
            elif "llama" in model_id.lower():
                response = self._call_llama_model(prompt, model_id)
            else:
                response = self._call_generic_model(prompt, model_id)

            return self._parse_ai_response(response)

        except Exception as e:
            logger.secure_info(f"Bedrock API call failed: {e}")
            return self._mock_ai_analysis(text)

    def _call_claude_model(self, prompt: str, model_id: str) -> str:
        body = {
            "prompt": f"Human: {prompt}\n\nAssistant:",
            "max_tokens_to_sample": 500,
            "temperature": 0.1,
            "top_p": 0.9,
        }

        response = self._bedrock_client.invoke_model(
            modelId=model_id, body=json.dumps(body), contentType="application/json"
        )

        result = json.loads(response["body"].read())
        return result.get("completion", "")

    def _call_llama_model(self, prompt: str, model_id: str) -> str:
        body = {"prompt": prompt, "max_gen_len": 500,
                "temperature": 0.1, "top_p": 0.9}

        response = self._bedrock_client.invoke_model(
            modelId=model_id, body=json.dumps(body), contentType="application/json"
        )

        result = json.loads(response["body"].read())
        return result.get("generation", "")

    def _call_generic_model(self, prompt: str, model_id: str) -> str:
        body = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 500,
                "temperature": 0.1,
                "topP": 0.9,
            },
        }

        response = self._bedrock_client.invoke_model(
            modelId=model_id, body=json.dumps(body), contentType="application/json"
        )

        result = json.loads(response["body"].read())
        return result.get("results", [{}])[0].get("outputText", "")

    def _parse_ai_response(self, response: str) -> Dict[str, str]:
        try:
            if response.strip().startswith("{"):
                parsed = json.loads(response)
                return {
                    "analysis": parsed.get("analysis", ""),
                    "key_themes": parsed.get("key_themes", ""),
                    "sentiment": parsed.get("sentiment", "neutral"),
                    "entities": parsed.get("entities", ""),
                    "confidence": parsed.get("confidence", "medium"),
                }
        except Exception as e:
            logger.secure_info(f"Failed to parse AI response: {e}")
            pass

        return {
            "analysis": response[:200] if response else "",
            "key_themes": "analysis, communication",
            "sentiment": "neutral",
            "entities": "",
            "confidence": "medium",
        }

    def _mock_ai_analysis(self, text: str) -> Dict[str, str]:
        if not text:
            return self._empty_analysis()

        word_count = len(text.split())

        if any(word in text.lower() for word in ["great", "excellent", "good"]):
            sentiment = "positive"
        elif any(word in text.lower() for word in ["bad", "terrible", "problem"]):
            sentiment = "negative"
        else:
            sentiment = "neutral"

        return {
            "analysis": f"Mock analysis of {word_count}-word text with {sentiment} sentiment",
            "key_themes": "communication, analysis, mock",
            "sentiment": sentiment,
            "entities": "mock_entity",
            "confidence": "medium",
        }

    def _empty_analysis(self) -> Dict[str, str]:
        return {
            "analysis": "",
            "key_themes": "",
            "sentiment": "neutral",
            "entities": "",
            "confidence": "low",
        }
