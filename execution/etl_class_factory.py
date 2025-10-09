
"""
ETL class factory for selecting and returning the appropriate ETL class object from etl/data_classes.
"""

from typing import Type
from utils.base_glue_job import BaseGlueETLJob
from etl.jobs.email_communication import EmailCommunicationETL
from etl.jobs.slack_communication import SlackCommunicationETL


class ETLClassFactory:

    def __init__(self):
        # Map job_type to ETL class
        self.etl_class_map = {
            'email_communication': EmailCommunicationETL,
            'slack_communication': SlackCommunicationETL,
            # Add more job types and their classes here
        }

    def get_etl_class(self, job_type: str) -> Type[BaseGlueETLJob]:
        """
        Return the ETL class object for the given job_type.
        """
        etl_class = self.etl_class_map.get(job_type)
        if etl_class is None:
            raise ImportError(f"No ETL class found for job type '{job_type}'")
        return etl_class
