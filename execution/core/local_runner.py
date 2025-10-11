import argparse
import os
import sys
from typing import Any, Dict

execution_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, execution_dir)

try:
    from pyspark.sql import SparkSession

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class LocalRunner:

    def __init__(self):
        self.spark = None
        self.sc = None

    def setup_spark(self):
        if not PYSPARK_AVAILABLE:
            raise ImportError(
                "PySpark not available. Install with: pip install pyspark"
            )

        self.spark = (
            SparkSession.builder.appName("LocalETLRunner")
            .master("local[*]")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel("WARN")

    def cleanup_spark(self):
        if self.spark:
            self.spark.stop()

    def run_job(self, job_type: str, job_args: Dict[str, Any]) -> Any:
        try:
            self.setup_spark()

            if job_type == "email_communication":
                return self._run_email_job(job_args)
            elif job_type == "slack_communication":
                return self._run_slack_job(job_args)
            elif job_type == "data_cleaning":
                return self._run_cleaning_job(job_args)
            elif job_type == "sentiment_analysis":
                return self._run_sentiment_job(job_args)
            else:
                raise ValueError(f"Unknown job type for local execution: {job_type}")

        except Exception as e:
            print(f"Local ETL job failed: {e}")
            raise
        finally:
            self.cleanup_spark()

    def _run_email_job(self, job_args: Dict[str, Any]) -> Any:
        print("Running email communication ETL locally...")

        sample_data = [
            {
                "from": "john.doe@company.com",
                "to": "jane.smith@company.com",
                "subject": "Project Update",
                "body": "The project is progressing well. We should meet next week.",
                "timestamp": "2023-01-01T10:30:00Z",
            },
            {
                "from": "alice@startup.com",
                "to": "bob@startup.com",
                "subject": "Budget Review",
                "body": "Can we review the Q4 budget tomorrow?",
                "timestamp": "2023-01-02T14:15:00Z",
            },
        ]

        df = self.spark.createDataFrame(sample_data)
        print("Sample email data:")
        df.show(truncate=False)

        standardized_df = df.selectExpr(
            "from as sender", "to as recipient", "subject", "body", "timestamp"
        )

        print("Transformed email data:")
        standardized_df.show(truncate=False)

        self._save_local_output(standardized_df, job_args, "emails")

        return standardized_df

    def _run_slack_job(self, job_args: Dict[str, Any]) -> Any:
        print("Running Slack communication ETL locally...")

        sample_data = [
            {
                "user": "john_doe",
                "channel": "#general",
                "message": "Good morning everyone! Ready for the standup?",
                "timestamp": "2023-01-01T09:00:00Z",
            },
            {
                "user": "jane_smith",
                "channel": "#development",
                "message": "The new feature is ready for testing",
                "timestamp": "2023-01-02T11:30:00Z",
            },
        ]

        df = self.spark.createDataFrame(sample_data)
        print("Sample Slack data:")
        df.show(truncate=False)

        standardized_df = df.selectExpr(
            "user as sender",
            "channel as recipient",
            "'slack_message' as subject",
            "message as body",
            "timestamp",
        )

        print("Transformed Slack data:")
        standardized_df.show(truncate=False)

        self._save_local_output(standardized_df, job_args, "slack")

        return standardized_df

    def _run_cleaning_job(self, job_args: Dict[str, Any]) -> Any:
        print("Running data cleaning ETL locally...")

        # Check if CSV file path is provided
        csv_path = job_args.get("csv-file")
        if csv_path and os.path.exists(csv_path):
            print(f"Reading CSV file: {csv_path}")
            df = self.spark.read.option("header", "true").csv(csv_path)
        else:
            print("Using sample data (no CSV file provided)")
            sample_data = [
                {
                    "id": "1",
                    "name": "John Doe",
                    "email": "john@example.com",
                    "age": "25",
                },
                {"id": "2", "name": "", "email": "invalid-email", "age": "abc"},
                {
                    "id": "3",
                    "name": "Jane Smith",
                    "email": "jane@example.com",
                    "age": "30",
                },
                {
                    "id": None,
                    "name": "Bob Wilson",
                    "email": "bob@example.com",
                    "age": "35",
                },
            ]
            df = self.spark.createDataFrame(sample_data)
        print("Raw data with quality issues:")
        df.show(truncate=False)

        from pyspark.sql.functions import col, when

        cleaned_df = df.filter(
            col("id").isNotNull()
            & (col("name") != "")
            & col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
        ).withColumn(
            "age_numeric",
            when(col("age").rlike(r"^\d+$"), col("age").cast("int")).otherwise(None),
        )

        print("Cleaned data:")
        cleaned_df.show(truncate=False)

        self._save_local_output(cleaned_df, job_args, "cleaned")

        return cleaned_df

    def _run_sentiment_job(self, job_args: Dict[str, Any]) -> Any:
        print("Running sentiment analysis ETL locally...")

        sample_data = [
            {
                "sender": "john@example.com",
                "recipient": "jane@example.com",
                "subject": "Great job on the project!",
                "body": "I'm really happy with the progress. Excellent work!",
                "timestamp": "2023-01-01T10:00:00Z",
            },
            {
                "sender": "alice@example.com",
                "recipient": "bob@example.com",
                "subject": "Issues with deployment",
                "body": "We're having serious problems with the deployment. This is frustrating.",
                "timestamp": "2023-01-02T14:00:00Z",
            },
        ]

        df = self.spark.createDataFrame(sample_data)
        print("Communication data for sentiment analysis:")
        df.show(truncate=False)

        from pyspark.sql.functions import col, lower, when

        sentiment_df = df.withColumn(
            "sentiment",
            when(
                lower(col("body")).rlike(r".*(happy|excellent|great|good|wonderful).*"),
                "positive",
            )
            .when(
                lower(col("body")).rlike(
                    r".*(problem|issue|frustrating|bad|terrible).*"
                ),
                "negative",
            )
            .otherwise("neutral"),
        ).withColumn(
            "sentiment_score",
            when(col("sentiment") == "positive", 0.8)
            .when(col("sentiment") == "negative", -0.6)
            .otherwise(0.0),
        )

        print("Data with sentiment analysis:")
        sentiment_df.show(truncate=False)

        self._save_local_output(sentiment_df, job_args, "sentiment")

        return sentiment_df

    def _save_local_output(self, df, job_args: Dict[str, Any], job_suffix: str):
        output_path = job_args.get("output-path", f"./output/{job_suffix}")
        os.makedirs(output_path, exist_ok=True)

        try:
            df.coalesce(1).write.mode("overwrite").parquet(output_path)
            print(f"Results written to {output_path}")
        except Exception as e:
            print(f"Warning: Could not write parquet, trying JSON: {e}")
            df.coalesce(1).write.mode("overwrite").json(output_path)
            print(f"Results written as JSON to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Run ETL jobs locally for development")
    parser.add_argument(
        "--job-type",
        required=True,
        choices=[
            "email_communication",
            "slack_communication",
            "data_cleaning",
            "sentiment_analysis",
        ],
        help="Type of ETL job to run",
    )
    parser.add_argument(
        "--input-bucket", default="local-input", help="Input bucket (simulated locally)"
    )
    parser.add_argument(
        "--output-bucket",
        default="local-output",
        help="Output bucket (simulated locally)",
    )
    parser.add_argument(
        "--output-path", default="./output", help="Local output path for results"
    )
    parser.add_argument(
        "--csv-file",
        help="Path to CSV file to process (optional, uses sample data if not provided)",
    )

    args = parser.parse_args()

    job_args = {
        "input-bucket": args.input_bucket,
        "output-bucket": args.output_bucket,
        "output-path": args.output_path,
        "csv-file": args.csv_file,
    }

    print(f"Starting local ETL job: {args.job_type}")
    print(f"Job arguments: {job_args}")

    runner = LocalRunner()
    try:
        result = runner.run_job(args.job_type, job_args)
        print(f"Job completed successfully! Processed {result.count()} records")
    except Exception as e:
        print(f"Job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
