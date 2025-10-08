"""
Main runner for Glue ETL jobs. Intakes parameters, selects ETL class, runs job, and saves results to S3.
"""

import sys

from utils.base_glue_job import BaseGlueETLJob


def main():
    # Extract job_type from command line args
    job_type = None
    for i, arg in enumerate(sys.argv):
        if arg in ("--JOB_TYPE", "--job_type") and i + 1 < len(sys.argv):
            job_type = sys.argv[i + 1]
            break
    if not job_type:
        raise ValueError("Missing required parameter: --JOB_TYPE")

    # Get the ETL class from the factory
    etl_class = BaseGlueETLJob(["job_type"])

    # Define required args for each job type
    job_args_map = {
        "data_cleaning": ["raw-bucket", "cleaned-bucket"],
        "sentiment_analysis": ["cleaned-bucket", "curated-bucket"],
        # Add more job types and their required args here
    }
    args_to_extract = job_args_map.get(job_type, [])

    # Instantiate and run the ETL job
    etl_job: BaseGlueETLJob = etl_class(args_to_extract)
    result = etl_job.run()
    etl_job.commit()
    etl_job.cleanup()

    # Save result to S3 (if applicable)
    # This assumes result is a DataFrame or similar object
    s3_bucket = etl_job.args.get("cleaned-bucket") or etl_job.args.get("curated-bucket")
    if result is not None and s3_bucket:
        output_path = f"s3://{s3_bucket}/results/"
        try:
            result.write.mode("overwrite").parquet(output_path)
            print(f"Results written to {output_path}")
        except Exception as e:
            print(f"Failed to write results to S3: {e}")


if __name__ == "__main__":
    main()
