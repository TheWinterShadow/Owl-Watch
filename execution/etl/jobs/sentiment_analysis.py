"""Glue ETL job for sentiment analysis using ML techniques."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME", "cleaned-bucket", "curated-bucket"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

cleaned_bucket = args["cleaned_bucket"]
curated_bucket = args["curated_bucket"]

# Read cleaned data
df = spark.read.parquet(f"s3://{cleaned_bucket}/cleaned/")

job.commit()
