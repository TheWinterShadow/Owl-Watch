import unittest

from execution.schemas.communication_schema import (
    COMMUNICATION_SCHEMA,
    COMMUNICATION_SCHEMA_JSON,
)


class TestCommunicationSchema(unittest.TestCase):

    def test_communication_schema_structure(self):
        from pyspark.sql.types import StructType

        self.assertIsInstance(COMMUNICATION_SCHEMA, StructType)

        field_names = [field.name for field in COMMUNICATION_SCHEMA.fields]
        expected_fields = [
            "id",
            "sender",
            "recipient",
            "timestamp",
            "body",
            "subject",
            "channel",
            "attachments",
            "sentiment",
            "language",
        ]

        for expected_field in expected_fields:
            self.assertIn(expected_field, field_names)

    def test_communication_schema_field_types(self):
        from pyspark.sql.types import ArrayType, StringType, TimestampType

        field_types = {
            field.name: field.dataType for field in COMMUNICATION_SCHEMA.fields
        }

        string_fields = [
            "id",
            "sender",
            "recipient",
            "body",
            "subject",
            "channel",
            "sentiment",
            "language",
        ]
        for field_name in string_fields:
            self.assertIsInstance(field_types[field_name], StringType)

        self.assertIsInstance(field_types["timestamp"], TimestampType)

        self.assertIsInstance(field_types["attachments"], ArrayType)
        self.assertIsInstance(field_types["attachments"].elementType, StringType)

    def test_communication_schema_nullable_fields(self):
        for field in COMMUNICATION_SCHEMA.fields:
            self.assertTrue(field.nullable, f"Field {field.name} should be nullable")

    def test_communication_schema_json_structure(self):
        self.assertIsInstance(COMMUNICATION_SCHEMA_JSON, dict)

        expected_fields = [
            "id",
            "sender",
            "recipient",
            "timestamp",
            "body",
            "subject",
            "channel",
            "attachments",
            "sentiment",
            "language",
        ]

        for expected_field in expected_fields:
            self.assertIn(expected_field, COMMUNICATION_SCHEMA_JSON)

    def test_communication_schema_json_types(self):
        expected_types = {
            "id": "string",
            "sender": "string",
            "recipient": "string",
            "timestamp": "timestamp",
            "body": "string",
            "subject": "string",
            "channel": "string",
            "attachments": "array<string>",
            "sentiment": "string",
            "language": "string",
        }

        for field_name, expected_type in expected_types.items():
            self.assertEqual(COMMUNICATION_SCHEMA_JSON[field_name], expected_type)

    def test_schema_consistency(self):
        pyspark_fields = set(field.name for field in COMMUNICATION_SCHEMA.fields)
        json_fields = set(COMMUNICATION_SCHEMA_JSON.keys())

        self.assertEqual(
            pyspark_fields,
            json_fields,
            "PySpark schema and JSON schema should have the same fields",
        )

    def test_required_communication_fields_present(self):
        required_fields = ["sender", "recipient", "body", "timestamp"]

        pyspark_field_names = [field.name for field in COMMUNICATION_SCHEMA.fields]
        json_field_names = list(COMMUNICATION_SCHEMA_JSON.keys())

        for required_field in required_fields:
            self.assertIn(
                required_field,
                pyspark_field_names,
                f"Required field {required_field} missing from PySpark schema",
            )
            self.assertIn(
                required_field,
                json_field_names,
                f"Required field {required_field} missing from JSON schema",
            )

    def test_optional_communication_fields_present(self):
        optional_fields = ["subject", "channel", "attachments", "sentiment", "language"]

        pyspark_field_names = [field.name for field in COMMUNICATION_SCHEMA.fields]
        json_field_names = list(COMMUNICATION_SCHEMA_JSON.keys())

        for optional_field in optional_fields:
            self.assertIn(
                optional_field,
                pyspark_field_names,
                f"Optional field {optional_field} missing from PySpark schema",
            )
            self.assertIn(
                optional_field,
                json_field_names,
                f"Optional field {optional_field} missing from JSON schema",
            )

    def test_schema_field_count(self):
        expected_field_count = 10

        self.assertEqual(
            len(COMMUNICATION_SCHEMA.fields),
            expected_field_count,
            f"PySpark schema should have {expected_field_count} fields",
        )
        self.assertEqual(
            len(COMMUNICATION_SCHEMA_JSON),
            expected_field_count,
            f"JSON schema should have {expected_field_count} fields",
        )


if __name__ == "__main__":
    unittest.main()
