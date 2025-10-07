"""
ETL job for transforming email data into a standardized communication schema.
"""
from utils.base_glue_job import BaseGlueETLJob
from typing import List


class EmailCommunicationETL(BaseGlueETLJob):
    def __init__(self, args_to_extract: List[str]):
        super().__init__(args_to_extract)

    def run(self):
        # Example: expects 'input-bucket' and 'output-bucket' in args
        input_bucket = self.args.get('input-bucket')
        output_bucket = self.args.get('output-bucket')
        # Read raw email data
        df = self.spark.read.json(f"s3://{input_bucket}/emails/")
        # Transform to standardized schema
        standardized_df = df.selectExpr(
            "from as sender",
            "to as recipient",
            "subject",
            "body",
            "timestamp"
        )
        # Write to output S3 bucket
        standardized_df.write.mode('overwrite').parquet(
            f"s3://{output_bucket}/communications/emails/")
        return standardized_df
