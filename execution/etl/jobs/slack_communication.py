"""
ETL job for transforming Slack data into a standardized communication schema.
"""
from utils.base_glue_job import BaseGlueETLJob
from typing import List


class SlackCommunicationETL(BaseGlueETLJob):
    def __init__(self, args_to_extract: List[str]):
        super().__init__(args_to_extract)

    def run(self):
        # Example: expects 'input-bucket' and 'output-bucket' in args
        input_bucket = self.args.get('input-bucket')
        output_bucket = self.args.get('output-bucket')
        # Read raw Slack data
        df = self.spark.read.json(f"s3://{input_bucket}/slack/")
        # Transform to standardized schema
        standardized_df = df.selectExpr(
            "user as sender",
            "channel as recipient",
            "text as body",
            "ts as timestamp"
        )
        # Write to output S3 bucket
        standardized_df.write.mode('overwrite').parquet(
            f"s3://{output_bucket}/communications/slack/")
        return standardized_df
