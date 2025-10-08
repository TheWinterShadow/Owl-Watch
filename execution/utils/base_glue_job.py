from abc import ABC, abstractmethod
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SparkContext
from typing import List, Dict, Any


class BaseGlueETLJob(ABC):
    """
    Base class for AWS Glue ETL Jobs.
    Provides common setup and utility methods for Glue jobs.
    """

    def __init__(self, args_to_extract: List[str]):
        """
        Initialize the Glue ETL job with given arguments.
        """
        self.args_to_extract = args_to_extract + ["JOB_NAME"]
        self.args = self._get_args()
        self._initialize_glue_context()
        self.job = Job(self.glue_context)

    def _get_args(self) -> Dict[str, Any]:
        """
        Extract and return the required arguments for the Glue job.
        """
        from awsglue.utils import getResolvedOptions
        import sys

        return getResolvedOptions(sys.argv, self.args_to_extract)

    def _initialize_glue_context(self):
        """
        Initialize Glue context and job.
        """
        self.sc = SparkContext()
        self.glue_context = GlueContext(self.sc)
        self.spark = self.glue_context.spark_session
        self.job = Job(self.glue_context)
        self.job.init(self.args['JOB_NAME'], self.args)

    @abstractmethod
    def run(self):
        """
        Main ETL logic to be implemented by subclasses.
        """
        pass

    def commit(self):
        """
        Commit the Glue job.
        """
        self.job.commit()

    def cleanup(self):
        """
        Cleanup resources if needed.
        """
        self.sc.stop()
