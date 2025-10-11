import unittest
from unittest.mock import Mock, patch

from execution.utils.logger import SecureLogger, get_account_id, logger


class TestGetAccountId(unittest.TestCase):

    @patch("execution.utils.logger.boto3.client")
    def test_get_account_id_success(self, mock_boto3_client):
        mock_sts_client = Mock()
        mock_sts_client.get_caller_identity.return_value = {"Account": "123456789012"}
        mock_boto3_client.return_value = mock_sts_client

        account_id = get_account_id()

        self.assertEqual(account_id, "123456789012")
        mock_boto3_client.assert_called_once_with("sts")
        mock_sts_client.get_caller_identity.assert_called_once()

    @patch("execution.utils.logger.boto3.client")
    def test_get_account_id_failure(self, mock_boto3_client):
        mock_sts_client = Mock()
        mock_sts_client.get_caller_identity.side_effect = Exception("STS error")
        mock_boto3_client.return_value = mock_sts_client

        with self.assertRaises(Exception):
            get_account_id()


class TestSecureLogger(unittest.TestCase):

    def setUp(self):
        with patch(
            "execution.utils.logger.get_account_id", return_value="774787676070"
        ):
            self.logger = SecureLogger()

    def test_init_production_account(self):
        with patch(
            "execution.utils.logger.get_account_id", return_value="774787676070"
        ):
            secure_logger = SecureLogger()

        self.assertEqual(secure_logger.stack_level, 3)
        self.assertEqual(secure_logger._max_length, 1000)

    def test_init_non_production_account(self):
        with patch(
            "execution.utils.logger.get_account_id", return_value="123456789012"
        ):
            secure_logger = SecureLogger()

        self.assertEqual(secure_logger.stack_level, 3)
        self.assertEqual(secure_logger._max_length, 1000)

    def test_init_account_id_failure(self):
        with patch(
            "execution.utils.logger.get_account_id", side_effect=Exception("STS error")
        ):
            secure_logger = SecureLogger()

        self.assertEqual(secure_logger.stack_level, 3)
        self.assertEqual(secure_logger._max_length, 1000)

    def test_sanitize_value_none(self):
        result = self.logger._sanitize_value(None)

        self.assertIsNone(result)

    def test_sanitize_value_primitive_types(self):
        test_cases = [
            (42, 42),
            (3.14, 3.14),
            (True, True),
            (False, False),
        ]

        for input_value, expected in test_cases:
            with self.subTest(input_value=input_value):
                result = self.logger._sanitize_value(input_value)

                self.assertEqual(result, expected)

    def test_sanitize_value_string_with_control_chars(self):
        input_string = "Hello\nWorld\r\tTest"

        result = self.logger._sanitize_value(input_string)

        self.assertEqual(result, "Hello\\nWorld\\r\\tTest")

    def test_sanitize_value_long_string_truncation(self):
        long_string = "x" * 1500

        result = self.logger._sanitize_value(long_string)

        self.assertEqual(len(result), 1000 + len("... (truncated)"))
        self.assertTrue(result.endswith("... (truncated)"))

    def test_sanitize_value_object_to_string(self):
        test_object = {"key": "value"}

        result = self.logger._sanitize_value(test_object)

        self.assertEqual(result, "{'key': 'value'}")

    def test_sanitize_kwargs(self):
        kwargs = {
            "string_value": "test\nstring",
            "int_value": 42,
            "none_value": None,
            "long_string": "x" * 1500,
        }

        result = self.logger._sanitize_kwargs(kwargs)

        self.assertEqual(result["string_value"], "test\\nstring")
        self.assertEqual(result["int_value"], 42)
        self.assertIsNone(result["none_value"])
        self.assertTrue(result["long_string"].endswith("... (truncated)"))

    @patch.object(SecureLogger, "debug")
    def test_secure_debug(self, mock_debug):
        message = "Debug message"
        kwargs = {"key": "value\nwith\rcontrol\tchars"}

        self.logger.secure_debug(message, **kwargs)

        mock_debug.assert_called_once()
        call_args = mock_debug.call_args
        self.assertEqual(call_args[0][0], message)
        self.assertEqual(call_args[1]["stacklevel"], 3)
        extra = call_args[1]["extra"]
        self.assertEqual(extra["key"], "value\\nwith\\rcontrol\\tchars")

    @patch.object(SecureLogger, "info")
    def test_secure_info(self, mock_info):
        message = "Info message"
        kwargs = {"count": 100, "status": "success"}

        self.logger.secure_info(message, **kwargs)

        mock_info.assert_called_once()
        call_args = mock_info.call_args
        self.assertEqual(call_args[0][0], message)
        self.assertEqual(call_args[1]["stacklevel"], 3)
        extra = call_args[1]["extra"]
        self.assertEqual(extra["count"], 100)
        self.assertEqual(extra["status"], "success")

    @patch.object(SecureLogger, "warning")
    def test_secure_warning(self, mock_warning):
        message = "Warning message"
        kwargs = {"error_code": "W001", "details": "Some warning details"}

        self.logger.secure_warning(message, **kwargs)

        mock_warning.assert_called_once()
        call_args = mock_warning.call_args
        self.assertEqual(call_args[0][0], message)
        self.assertEqual(call_args[1]["stacklevel"], 3)
        extra = call_args[1]["extra"]
        self.assertEqual(extra["error_code"], "W001")
        self.assertEqual(extra["details"], "Some warning details")

    @patch.object(SecureLogger, "error")
    def test_secure_error(self, mock_error):
        message = "Error message"
        kwargs = {"error_type": "validation", "field": "email"}

        self.logger.secure_error(message, **kwargs)

        mock_error.assert_called_once()
        call_args = mock_error.call_args
        self.assertEqual(call_args[0][0], message)
        self.assertEqual(call_args[1]["stacklevel"], 3)
        extra = call_args[1]["extra"]
        self.assertEqual(extra["error_type"], "validation")
        self.assertEqual(extra["field"], "email")

    @patch.object(SecureLogger, "exception")
    def test_secure_exception(self, mock_exception):
        message = "Exception occurred"
        kwargs = {"exception_type": "ValueError", "context": "data processing"}

        self.logger.secure_exception(message, **kwargs)

        mock_exception.assert_called_once()
        call_args = mock_exception.call_args
        self.assertEqual(call_args[0][0], message)
        self.assertEqual(call_args[1]["stacklevel"], 3)
        extra = call_args[1]["extra"]
        self.assertEqual(extra["exception_type"], "ValueError")
        self.assertEqual(extra["context"], "data processing")

    def test_secure_methods_with_empty_kwargs(self):
        message = "Test message"

        with patch.object(self.logger, "debug") as mock_debug:
            self.logger.secure_debug(message)

            mock_debug.assert_called_once()
            call_args = mock_debug.call_args
            self.assertEqual(call_args[0][0], message)
            self.assertEqual(call_args[1]["extra"], {})

    def test_secure_methods_with_complex_kwargs(self):
        message = "Complex data test"
        kwargs = {
            "list_data": [1, 2, 3],
            "dict_data": {"nested": {"key": "value"}},
            "mixed": ["string", 42, None],
        }

        with patch.object(self.logger, "info") as mock_info:
            self.logger.secure_info(message, **kwargs)

            mock_info.assert_called_once()
            call_args = mock_info.call_args
            extra = call_args[1]["extra"]
            self.assertEqual(extra["list_data"], "[1, 2, 3]")
            self.assertEqual(extra["dict_data"], "{'nested': {'key': 'value'}}")
            self.assertEqual(extra["mixed"], "['string', 42, None]")


class TestGlobalLoggerInstance(unittest.TestCase):

    def test_global_logger_exists(self):
        self.assertIsInstance(logger, SecureLogger)

    def test_global_logger_has_secure_methods(self):
        self.assertTrue(hasattr(logger, "secure_debug"))
        self.assertTrue(hasattr(logger, "secure_info"))
        self.assertTrue(hasattr(logger, "secure_warning"))
        self.assertTrue(hasattr(logger, "secure_error"))
        self.assertTrue(hasattr(logger, "secure_exception"))

    def test_global_logger_methods_callable(self):
        self.assertTrue(callable(logger.secure_debug))
        self.assertTrue(callable(logger.secure_info))
        self.assertTrue(callable(logger.secure_warning))
        self.assertTrue(callable(logger.secure_error))
        self.assertTrue(callable(logger.secure_exception))


if __name__ == "__main__":
    unittest.main()
