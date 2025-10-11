"""
Secure Logger Utility

This module provides a secure logging implementation that sanitizes sensitive data
and integrates with AWS Lambda Powertools for structured logging.
"""

from typing import Any, Dict

import boto3
from aws_lambda_powertools import Logger as PowertoolsLogger
from mypy_boto3_sts import STSClient


def get_account_id():
    """
    Retrieve the current AWS account ID using STS.

    Returns:
        String containing the AWS account ID
    """
    sts_client: STSClient = boto3.client("sts")
    return sts_client.get_caller_identity()["Account"]


class SecureLogger(PowertoolsLogger):
    """
    Secure logger that sanitizes sensitive data before logging.

    Extends AWS Lambda Powertools Logger with data sanitization capabilities
    to prevent accidental logging of sensitive information like credentials,
    personal data, or other confidential information.

    Attributes:
        _max_length: Maximum length for string values before truncation
        stack_level: Stack level for location tracking in logs
    """

    def __init__(self):
        super().__init__(
            service="data_sentinel",
            level="DEBUG",
            disable_log_level_warning=True,
            location="[%(filename)s:%(lineno)s][%(funcName)s]",
        )
        self._max_length = 1000
        self.stack_level = 3

    def _sanitize_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (int, float, bool)):
            return value

        value_str = str(value)
        value_str = (
            value_str.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
        )

        if len(value_str) > self._max_length:
            value_str = value_str[: self._max_length] + "... (truncated)"

        return value_str

    def _sanitize_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return {k: self._sanitize_value(v) for k, v in kwargs.items()}

    def secure_debug(self, message: str, **kwargs) -> None:
        self.debug(
            message, extra=self._sanitize_kwargs(kwargs), stacklevel=self.stack_level
        )

    def secure_info(self, message: str, **kwargs) -> None:
        self.info(
            message, extra=self._sanitize_kwargs(kwargs), stacklevel=self.stack_level
        )

    def secure_warning(self, message: str, **kwargs) -> None:
        self.warning(
            message, extra=self._sanitize_kwargs(kwargs), stacklevel=self.stack_level
        )

    def secure_error(self, message: str, **kwargs) -> None:
        self.error(
            message, extra=self._sanitize_kwargs(kwargs), stacklevel=self.stack_level
        )

    def secure_exception(self, message: str, **kwargs) -> None:
        self.exception(
            message, extra=self._sanitize_kwargs(kwargs), stacklevel=self.stack_level
        )


logger = SecureLogger()
