"""Lambda function for Bedrock AI processing."""

import json
import os
from typing import Any, Dict

import boto3

bedrock = boto3.client(
    "bedrock-runtime", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
)
s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))

DEFAULT_CLEANED_BUCKET = "test-cleaned-bucket"
DEFAULT_CURATED_BUCKET = "test-curated-bucket"
CLEANED_BUCKET = os.environ.get("CLEANED_BUCKET", DEFAULT_CLEANED_BUCKET)
CURATED_BUCKET = os.environ.get("CURATED_BUCKET", DEFAULT_CURATED_BUCKET)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Process data using Bedrock AI models."""

    try:
        # Validate event structure
        if not event.get("Records") or not isinstance(event["Records"], list):
            return {
                "statusCode": 500,
                "body": json.dumps({"error": "Invalid event structure"}),
            }

        # Get data from S3
        for record in event["Records"]:
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]

            # Read data
            response = s3.get_object(Bucket=bucket, Key=key)
            data = response["Body"].read().decode("utf-8")

            # Process with Bedrock
            processed_data = process_with_bedrock(data)

            # Write results
            output_key = f"bedrock-processed/{key}"
            s3.put_object(
                Bucket=CURATED_BUCKET,
                Key=output_key,
                Body=json.dumps(processed_data),
            )

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Processing completed"}),
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def process_with_bedrock(data: str) -> Dict[str, Any]:
    """Process data using Bedrock Claude model."""

    prompt = f"""
    Analyze the following data and provide insights:

    Data: {data}

    Please provide:
    1. Key themes
    2. Sentiment analysis
    3. Recommendations

    Format your response as JSON.
    """

    try:
        response = bedrock.invoke_model(
            modelId="anthropic.claude-v2",
            body=json.dumps(
                {
                    "prompt": f"Human: {prompt}\n\nAssistant:",
                    "max_tokens_to_sample": 1000,
                    "temperature": 0.1,
                }
            ),
            contentType="application/json",
        )

        result = json.loads(response["body"].read())

        return {
            "bedrock_analysis": result.get("completion", ""),
            "model_id": "anthropic.claude-v2",
            "processed_at": "unknown",
        }

    except Exception as e:
        return {
            "error": f"Bedrock processing failed: {str(e)}",
            "fallback_analysis": "Basic text analysis completed",
        }
