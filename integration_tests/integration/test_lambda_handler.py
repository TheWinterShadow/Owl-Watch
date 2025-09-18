"""Integration tests for Lambda handler."""

import json
from unittest.mock import MagicMock

import pytest
from owl_watch.main import handler


class TestLambdaHandler:
    """Test cases for Lambda handler integration."""

    def test_handler_success(self, lambda_event):
        """Test successful Lambda handler execution."""
        context = MagicMock()
        context.function_name = "test-function"
        context.request_id = "test-request-id"

        response = handler(lambda_event, context)

        assert response["statusCode"] == 200
        assert "application/json" in response["headers"]["Content-Type"]

        body = json.loads(response["body"])
        assert "event_id" in body
        assert body["status"] == "processed"

    def test_handler_error_handling(self):
        """Test Lambda handler error handling."""
        context = MagicMock()
        invalid_event = {"invalid": "data"}

        response = handler(invalid_event, context)

        assert response["statusCode"] == 500
        body = json.loads(response["body"])
        assert "error" in body
