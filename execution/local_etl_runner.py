#!/usr/bin/env python3
"""
Local ETL runner for development and testing.
This script runs ETL jobs locally using local Spark instead of AWS Glue.
"""

import argparse
import os
import sys
from typing import Dict, Any

# Add the current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from pyspark.sql import SparkSession
    from pyspark import SparkContext
except ImportError:
    print("PySpark not available. Install with: pip install pyspark")
    sys.exit(1)


class LocalETLRunner:
    """Local ETL runner that simulates Glue job execution."""
    
    def __init__(self):
        self.spark = None
        self.sc = None
        
    def setup_spark(self):
        """Initialize local Spark session."""
        self.spark = SparkSession.builder \
            .appName("LocalETLRunner") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel("WARN")  # Reduce log noise
        
    def cleanup_spark(self):
        """Clean up Spark resources."""
        if self.spark:
            self.spark.stop()
            
    def run_etl_job(self, job_type: str, job_args: Dict[str, Any]):
        """Run an ETL job locally."""
        try:
            # Setup Spark
            self.setup_spark()
            
            # Import and create ETL job
            if job_type == "email_communication":
                from etl.jobs.email_communication import EmailCommunicationETL
                
                # Create mock job args that work locally
                args_to_extract = ["input-bucket", "output-bucket"]
                
                # Create a mock ETL job that works locally
                class LocalEmailETL:
                    def __init__(self, spark_session):
                        self.spark = spark_session
                        self.args = job_args
                        
                    def run(self):
                        print(f"Running email communication ETL with args: {self.args}")
                        
                        # For local testing, create sample data
                        sample_data = [
                            {"from": "user1@example.com", "to": "user2@example.com", 
                             "subject": "Test Email", "body": "This is a test", "timestamp": "2023-01-01T00:00:00Z"},
                            {"from": "user3@example.com", "to": "user4@example.com", 
                             "subject": "Another Test", "body": "Another test email", "timestamp": "2023-01-02T00:00:00Z"}
                        ]
                        
                        df = self.spark.createDataFrame(sample_data)
                        print("Sample input data:")
                        df.show(truncate=False)
                        
                        # Transform to standardized schema
                        standardized_df = df.selectExpr(
                            "from as sender", 
                            "to as recipient", 
                            "subject", 
                            "body", 
                            "timestamp"
                        )
                        
                        print("Transformed data:")
                        standardized_df.show(truncate=False)
                        
                        # In local mode, save to local filesystem instead of S3
                        output_path = job_args.get("output-path", "./output/emails")
                        os.makedirs(output_path, exist_ok=True)
                        
                        standardized_df.coalesce(1).write.mode("overwrite").parquet(output_path)
                        print(f"Results written to {output_path}")
                        
                        return standardized_df
                        
                    def commit(self):
                        print("ETL job committed successfully")
                        
                    def cleanup(self):
                        print("ETL job cleanup completed")
                
                etl_job = LocalEmailETL(self.spark)
                
            elif job_type == "slack_communication":
                print(f"Running slack communication ETL with args: {job_args}")
                
                # Create sample Slack data
                sample_data = [
                    {"user": "user1", "channel": "#general", "message": "Hello world!", 
                     "timestamp": "2023-01-01T00:00:00Z"},
                    {"user": "user2", "channel": "#random", "message": "How's everyone doing?", 
                     "timestamp": "2023-01-02T00:00:00Z"}
                ]
                
                df = self.spark.createDataFrame(sample_data)
                print("Sample Slack data:")
                df.show(truncate=False)
                
                # Transform data
                standardized_df = df.selectExpr(
                    "user as sender",
                    "channel as recipient", 
                    "'slack_message' as subject",
                    "message as body",
                    "timestamp"
                )
                
                print("Transformed data:")
                standardized_df.show(truncate=False)
                
                # Save locally
                output_path = job_args.get("output-path", "./output/slack")
                os.makedirs(output_path, exist_ok=True)
                standardized_df.coalesce(1).write.mode("overwrite").parquet(output_path)
                print(f"Results written to {output_path}")
                
                class MockJob:
                    def commit(self): pass
                    def cleanup(self): pass
                    
                etl_job = MockJob()
                
            else:
                raise ValueError(f"Unknown job type: {job_type}")
                
            # Run the ETL job
            result = etl_job.run() if hasattr(etl_job, 'run') else None
            etl_job.commit()
            etl_job.cleanup()
            
            print(f"ETL job '{job_type}' completed successfully!")
            return result
            
        except Exception as e:
            print(f"ETL job failed: {e}")
            raise
        finally:
            self.cleanup_spark()


def main():
    """Main entry point for local ETL runner."""
    parser = argparse.ArgumentParser(description="Run ETL jobs locally for development")
    parser.add_argument("--job-type", required=True, 
                       choices=["email_communication", "slack_communication"],
                       help="Type of ETL job to run")
    parser.add_argument("--input-bucket", default="local-input",
                       help="Input bucket (simulated locally)")
    parser.add_argument("--output-bucket", default="local-output", 
                       help="Output bucket (simulated locally)")
    parser.add_argument("--output-path", default="./output",
                       help="Local output path for results")
    
    args = parser.parse_args()
    
    # Convert args to job arguments
    job_args = {
        "input-bucket": args.input_bucket,
        "output-bucket": args.output_bucket,
        "output-path": args.output_path
    }
    
    print(f"Starting local ETL job: {args.job_type}")
    print(f"Job arguments: {job_args}")
    
    runner = LocalETLRunner()
    try:
        runner.run_etl_job(args.job_type, job_args)
    except Exception as e:
        print(f"Job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()