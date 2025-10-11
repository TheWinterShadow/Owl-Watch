"""
Job Runner Module

This module provides the main job execution engine for ETL jobs.
It handles job instantiation, execution, and result management.
"""

import sys
from typing import Any, Dict, Optional

from execution.core.job_factory import JobFactory
from execution.models.job_metadata import JobConfiguration


class JobRunner:
    """
    Main job execution engine for ETL jobs.

    This class coordinates the execution of ETL jobs by using the JobFactory
    to create job instances and managing their lifecycle (run, commit, cleanup).

    Attributes:
        job_config: Configuration object containing job parameters
        factory: JobFactory instance for creating job objects
    """

    def __init__(self, job_config: JobConfiguration):
        """
        Initialize the JobRunner with a job configuration.

        Args:
            job_config: Configuration object containing job parameters and settings
        """
        self.job_config = job_config
        self.factory = JobFactory()

    def run(self, job_type: Optional[str] = None) -> Any:
        """
        Execute an ETL job with the specified type.

        Args:
            job_type: Optional job type string. If None, extracts from command line arguments

        Returns:
            The result returned by the job's run method

        Raises:
            ValueError: If no job type is provided or found in command line arguments
            Exception: Re-raises any exception encountered during job execution
        """
        # Extract job type from command line if not provided
        if job_type is None:
            job_type = self._extract_job_type()

        if not job_type:
            raise ValueError(
                "Job type must be provided either as parameter or via "
                "--JOB_TYPE/--job_type command line argument"
            )

        print(f"Running ETL job: {job_type}")

        try:
            # Create the job instance using the factory
            etl_job = self.factory.create_job(job_type)
            print(f"Job created successfully: {etl_job.__class__.__name__}")

            # Execute the job's main logic
            result = etl_job.run()

            # Commit any pending changes (e.g., database transactions)
            etl_job.commit()

            # Clean up resources (e.g., close connections, temp files)
            etl_job.cleanup()

            print(f"Job '{job_type}' completed successfully")

            # Handle result output (e.g., save to S3)
            self._handle_result_output(etl_job, result)

            return result

        except Exception as e:
            print(f"Job '{job_type}' failed: {str(e)}")
            raise

    def _extract_job_type(self) -> Optional[str]:
        """
        Extract job type from command line arguments.

        Looks for --JOB_TYPE, --job_type, or --job-type arguments in sys.argv.

        Returns:
            The job type string if found, None otherwise
        """
        job_type = None

        # Search through command line arguments for job type
        for i, arg in enumerate(sys.argv):
            if arg in ("--JOB_TYPE", "--job_type", "--job-type") and i + 1 < len(
                sys.argv
            ):
                job_type = sys.argv[i + 1]
                break

        return job_type

    def _handle_result_output(self, etl_job, result) -> None:
        """
        Handle output of job results to appropriate storage location.

        Args:
            etl_job: The executed job instance
            result: The result data from the job execution
        """
        if result is None:
            print("No result data to output")
            return

        try:
            # Try to find an appropriate S3 bucket from job arguments
            s3_bucket = (
                etl_job.args.get("data_lake_bucket")
                or etl_job.args.get("cleaned_data")
                or etl_job.args.get("curated_data")
            )

            # Write results to S3 if bucket is configured and result is a DataFrame
            if s3_bucket and hasattr(result, "write"):
                output_path = (
                    f"s3://{s3_bucket}/results/{etl_job.__class__.__name__.lower()}/"
                )

                result.write.mode("overwrite").parquet(output_path)
                print(f"Results written to {output_path}")

            else:
                print("No valid output bucket configured or result is not a DataFrame")

        except Exception as e:
            print(f"Warning: Failed to write results to S3: {e}")

    def list_jobs(self) -> Dict[str, str]:
        """
        Get a list of all available job types with descriptions.

        Returns:
            Dictionary mapping job type strings to their descriptions
        """
        return self.factory.list_available_jobs()
