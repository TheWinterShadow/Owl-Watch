"""
Main Entry Point for ETL Job Execution

This module provides the main entry point for executing ETL jobs in AWS Glue.
It orchestrates the configuration parsing and job execution workflow.
"""

import sys

from execution.core.config_manager import JobConfigManager
from execution.core.job_runner import JobRunner


def main():
    """
    Main entry point for ETL job execution.

    This function coordinates the entire ETL job lifecycle:
    1. Parse job arguments from command line
    2. Create job configuration
    3. Initialize and run the job

    The function handles exceptions gracefully and exits with appropriate
    status codes for monitoring and alerting purposes.
    """
    try:
        # Initialize configuration manager and parse arguments
        config_manager = JobConfigManager()
        job_args = config_manager.parse_job_arguments()
        job_config = config_manager.create_job_config(job_args)

        # Initialize job runner and execute the job
        runner = JobRunner(job_config)
        runner.run()

    except Exception as e:
        # Log error and exit with non-zero status code for monitoring
        print(f"ETL execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
