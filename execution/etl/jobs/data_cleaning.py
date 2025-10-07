"""Glue ETL job for data cleaning and transformation."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "raw-bucket", "cleaned-bucket"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

job.commit()
