import unittest
from unittest.mock import Mock, patch

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from execution.models.exceptions import (
    MissingFieldError,
    SchemaValidationError,
    TypeMismatchError,
)
from execution.utils.validation import SchemaValidator, ValidationResult


class TestValidationResult(unittest.TestCase):

    def test_init_with_required_fields(self):
        result = ValidationResult(
            is_valid=True, errors=["error1", "error2"], warnings=["warning1"]
        )

        self.assertTrue(result.is_valid)
        self.assertEqual(result.errors, ["error1", "error2"])
        self.assertEqual(result.warnings, ["warning1"])
        self.assertEqual(result.missing_fields, [])
        self.assertEqual(result.type_mismatches, [])

    def test_init_with_optional_fields(self):
        result = ValidationResult(
            is_valid=False,
            errors=["error1"],
            warnings=[],
            missing_fields=["field1", "field2"],
            type_mismatches=["mismatch1"],
            field_name="test_field",
        )

        self.assertFalse(result.is_valid)
        self.assertEqual(result.missing_fields, ["field1", "field2"])
        self.assertEqual(result.type_mismatches, ["mismatch1"])
        self.assertEqual(result.field_name, "test_field")

    def test_post_init_initializes_lists(self):
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            missing_fields=None,
            type_mismatches=None,
        )

        self.assertEqual(result.missing_fields, [])
        self.assertEqual(result.type_mismatches, [])

    def test_add_error(self):
        result = ValidationResult(is_valid=True, errors=[], warnings=[])

        result.add_error("New error")

        self.assertIn("New error", result.errors)
        self.assertFalse(result.is_valid)

    def test_add_warning(self):
        result = ValidationResult(is_valid=True, errors=[], warnings=[])

        result.add_warning("New warning")

        self.assertIn("New warning", result.warnings)
        self.assertTrue(result.is_valid)

    def test_get_method(self):
        result = ValidationResult(
            is_valid=True, errors=[], warnings=[], field_name="test_field"
        )

        self.assertTrue(result.get("is_valid"))
        self.assertEqual(result.get("field_name"), "test_field")
        self.assertIsNone(result.get("nonexistent_field"))
        self.assertEqual(result.get("nonexistent_field", "default"), "default")

    def test_getitem_method(self):
        result = ValidationResult(
            is_valid=False, errors=["error1"], warnings=["warning1"]
        )

        self.assertFalse(result["is_valid"])
        self.assertEqual(result["errors"], ["error1"])
        self.assertEqual(result["warnings"], ["warning1"])


class TestSchemaValidator(unittest.TestCase):

    def setUp(self):
        self.validator = SchemaValidator(check_nullable=False)
        self.strict_validator = SchemaValidator(check_nullable=True)

    def test_init_default_settings(self):
        validator = SchemaValidator()

        self.assertFalse(validator.check_nullable)

    def test_init_with_nullable_check(self):
        validator = SchemaValidator(check_nullable=True)

        self.assertTrue(validator.check_nullable)

    def test_validate_generic_method(self):
        result = self.validator.validate("test_data")

        self.assertIsInstance(result, ValidationResult)
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.errors), 0)

    def test_get_schema_generic_method(self):
        schema = self.validator.get_schema()

        self.assertEqual(schema, {})

    @patch("execution.utils.validation.logger")
    def test_validate_schema_matching_schemas(self, mock_logger):
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )

        mock_df = Mock()
        mock_df.schema = expected_schema

        result = self.validator.validate_schema(mock_df, expected_schema)

        self.assertIsInstance(result, ValidationResult)
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.errors), 0)
        self.assertEqual(len(result.warnings), 0)

    @patch("execution.utils.validation.logger")
    def test_validate_schema_missing_fields(self, mock_logger):
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("email", StringType(), True),
            ]
        )

        actual_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )

        mock_df = Mock()
        mock_df.schema = actual_schema

        result = self.validator.validate_schema(mock_df, expected_schema)

        self.assertFalse(result.is_valid)
        self.assertTrue(
            any("Missing required fields" in error for error in result.errors)
        )
        self.assertIn("email", result.missing_fields)

    @patch("execution.utils.validation.logger")
    def test_validate_schema_extra_fields(self, mock_logger):
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )

        actual_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("extra_field", StringType(), True),
            ]
        )

        mock_df = Mock()
        mock_df.schema = actual_schema

        result = self.validator.validate_schema(mock_df, expected_schema)

        self.assertTrue(result.is_valid)
        self.assertTrue(
            any("Extra fields found" in warning for warning in result.warnings)
        )

    @patch("execution.utils.validation.logger")
    def test_validate_schema_type_mismatches(self, mock_logger):
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("count", IntegerType(), True),
            ]
        )

        actual_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("count", StringType(), True),
            ]
        )

        mock_df = Mock()
        mock_df.schema = actual_schema

        result = self.validator.validate_schema(mock_df, expected_schema)

        self.assertFalse(result.is_valid)
        self.assertTrue(any("count" in error for error in result.errors))
        self.assertTrue(
            any("IntegerType" in mismatch for mismatch in result.type_mismatches)
        )

    def test_types_match_identical_types(self):
        self.assertTrue(self.validator._types_match(StringType(), StringType()))
        self.assertTrue(self.validator._types_match(IntegerType(), IntegerType()))

    def test_types_match_different_types(self):
        self.assertFalse(self.validator._types_match(StringType(), IntegerType()))
        self.assertFalse(self.validator._types_match(IntegerType(), StringType()))

    @patch("execution.utils.validation.logger")
    def test_validate_curated_schema(self, mock_logger):
        expected_schema = StructType([StructField("id", StringType(), True)])
        mock_df = Mock()
        mock_df.schema = expected_schema

        result = self.validator.validate_curated_schema(mock_df, expected_schema)

        self.assertIsInstance(result, ValidationResult)
        self.assertTrue(result.is_valid)

    @patch("execution.utils.validation.logger")
    def test_validate_transformed_schema(self, mock_logger):
        expected_schema = StructType([StructField("id", StringType(), True)])
        mock_df = Mock()
        mock_df.schema = expected_schema

        result = self.validator.validate_transformed_schema(mock_df, expected_schema)

        self.assertIsInstance(result, ValidationResult)
        self.assertTrue(result.is_valid)

    def test_get_schema_diff_identical_schemas(self):
        schema1 = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )

        diff = self.validator.get_schema_diff(schema1, schema2)

        self.assertEqual(diff["only_in_schema1"], [])
        self.assertEqual(diff["only_in_schema2"], [])
        self.assertEqual(diff["type_differences"], [])
        self.assertTrue(diff["schemas_identical"])

    def test_get_schema_diff_different_schemas(self):
        schema1 = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("unique_field1", StringType(), True),
            ]
        )
        schema2 = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("unique_field2", StringType(), True),
            ]
        )

        diff = self.validator.get_schema_diff(schema1, schema2)

        self.assertIn("unique_field1", diff["only_in_schema1"])
        self.assertIn("unique_field2", diff["only_in_schema2"])
        self.assertEqual(len(diff["type_differences"]), 1)
        self.assertFalse(diff["schemas_identical"])

    @patch("execution.utils.validation.logger")
    def test_select_expected_fields_all_present(self, mock_logger):
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )

        mock_df = Mock()
        mock_df.columns = ["id", "name", "extra_field"]
        mock_result_df = Mock()
        mock_df.select.return_value = mock_result_df

        result = self.validator.select_expected_fields(mock_df, expected_schema)

        mock_df.select.assert_called_once_with("id", "name")
        self.assertEqual(result, mock_result_df)

    @patch("execution.utils.validation.logger")
    def test_select_expected_fields_some_missing(self, mock_logger):
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("missing_field", StringType(), True),
            ]
        )

        mock_df = Mock()
        mock_df.columns = ["id", "name"]
        mock_result_df = Mock()
        mock_df.select.return_value = mock_result_df

        result = self.validator.select_expected_fields(mock_df, expected_schema)

        mock_df.select.assert_called_once_with("id", "name")
        self.assertEqual(result, mock_result_df)

    def test_validate_required_fields_all_present(self):
        mock_df = Mock()
        mock_df.columns = ["id", "name", "email"]

        def mock_getitem(self, key):
            mock_column = Mock()
            mock_column.isNull.return_value = Mock()
            return mock_column

        mock_df.__getitem__ = mock_getitem
        mock_df.filter.return_value.count.return_value = 0
        required_fields = ["id", "name", "email"]

        result = self.validator.validate_required_fields(mock_df, required_fields)

        self.assertTrue(result["is_valid"])
        self.assertEqual(result["missing_fields"], [])
        self.assertEqual(result["null_counts"], {})

    def test_validate_required_fields_missing_fields(self):
        mock_df = Mock()
        mock_df.columns = ["id", "name"]  # Missing "email"
        mock_df.count.return_value = 100

        def mock_getitem(self, key):
            mock_column = Mock()
            mock_column.isNull.return_value = Mock()
            return mock_column

        mock_df.__getitem__ = mock_getitem
        mock_df.filter.return_value.count.return_value = 0
        required_fields = ["id", "name", "email"]

        result = self.validator.validate_required_fields(mock_df, required_fields)

        self.assertFalse(result["is_valid"])
        self.assertIn("email", result["missing_fields"])

    def test_validate_required_fields_with_nulls(self):
        mock_df = Mock()
        mock_df.columns = ["id", "name", "email"]
        mock_df.count.return_value = 100

        def mock_getitem(self, key):
            mock_column = Mock()
            mock_column.isNull.return_value = Mock()
            return mock_column

        mock_df.__getitem__ = mock_getitem

        def mock_filter_side_effect(condition):
            mock_filtered = Mock()
            if "name" in str(condition):
                mock_filtered.count.return_value = 5
            else:
                mock_filtered.count.return_value = 0
            return mock_filtered

        mock_df.filter.side_effect = mock_filter_side_effect
        required_fields = ["id", "name", "email"]

        result = self.validator.validate_required_fields(mock_df, required_fields)
        self.assertFalse(result["is_valid"])
        self.assertEqual(result["null_counts"]["name"], 5)

    def test_validate_schema_strict_success(self):
        expected_schema = StructType([StructField("id", StringType(), True)])
        mock_df = Mock()
        mock_df.schema = expected_schema

        self.validator.validate_schema_strict(mock_df, expected_schema, "test_schema")

    def test_validate_schema_strict_missing_fields(self):
        expected_schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("missing_field", StringType(), True),
            ]
        )
        actual_schema = StructType([StructField("id", StringType(), True)])
        mock_df = Mock()
        mock_df.schema = actual_schema

        with self.assertRaises(MissingFieldError):
            self.validator.validate_schema_strict(
                mock_df, expected_schema, "test_schema"
            )

    def test_validate_schema_strict_type_mismatch(self):
        expected_schema = StructType([StructField("id", IntegerType(), True)])
        actual_schema = StructType([StructField("id", StringType(), True)])
        mock_df = Mock()
        mock_df.schema = actual_schema

        with self.assertRaises(TypeMismatchError):
            self.validator.validate_schema_strict(
                mock_df, expected_schema, "test_schema"
            )

    def test_validate_required_fields_strict_success(self):
        mock_df = Mock()
        mock_df.columns = ["id", "name"]

        def mock_getitem(self, key):
            mock_column = Mock()
            mock_column.isNull.return_value = Mock()
            return mock_column

        mock_df.__getitem__ = mock_getitem
        mock_df.filter.return_value.count.return_value = 0
        required_fields = ["id", "name"]

        self.validator.validate_required_fields_strict(mock_df, required_fields)

    def test_validate_required_fields_strict_missing_fields(self):
        mock_df = Mock()
        mock_df.columns = ["id"]  # Missing "name"
        mock_df.count.return_value = 100

        def mock_getitem(self, key):
            mock_column = Mock()
            mock_column.isNull.return_value = Mock()
            return mock_column

        mock_df.__getitem__ = mock_getitem
        mock_df.filter.return_value.count.return_value = 0
        required_fields = ["id", "name"]

        with self.assertRaises(MissingFieldError):
            self.validator.validate_required_fields_strict(mock_df, required_fields)

    def test_validate_required_fields_strict_null_values(self):
        mock_df = Mock()
        mock_df.columns = ["id", "name"]
        mock_df.count.return_value = 100

        def mock_getitem(self, key):
            mock_column = Mock()
            mock_column.isNull.return_value = Mock()
            return mock_column

        mock_df.__getitem__ = mock_getitem

        mock_filtered = Mock()
        mock_filtered.count.return_value = 5
        mock_df.filter.return_value = mock_filtered

        required_fields = ["id", "name"]

        with self.assertRaises(SchemaValidationError):
            self.validator.validate_required_fields_strict(mock_df, required_fields)

    def test_get_schema_summary(self):
        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("name", StringType(), True),
                StructField("count", IntegerType(), True),
            ]
        )

        summary = self.validator.get_schema_summary(schema)

        self.assertEqual(summary["total_fields"], 3)
        self.assertIn("StringType()", summary["field_types"])
        self.assertEqual(summary["field_types"]["StringType()"], 2)
        self.assertEqual(summary["field_types"]["IntegerType()"], 1)
        self.assertIn("name", summary["nullable_fields"])
        self.assertIn("count", summary["nullable_fields"])
        self.assertIn("id", summary["non_nullable_fields"])
        self.assertEqual(summary["field_names"], ["id", "name", "count"])


if __name__ == "__main__":
    unittest.main()
