import unittest
from datetime import datetime

from execution.models.data_quality import (
    DataQualityMetrics,
    DataQualityValidator,
    FieldQualityMetrics,
    QualityDimension,
    ValidationResult,
    ValidationRule,
    ValidationSeverity,
    data_quality_validator,
)


class TestValidationRule(unittest.TestCase):

    def test_init(self):
        rule = ValidationRule(
            name="test_rule",
            description="Test validation rule",
            severity=ValidationSeverity.ERROR,
            dimension=QualityDimension.VALIDITY,
        )

        self.assertEqual(rule.name, "test_rule")
        self.assertEqual(rule.description, "Test validation rule")
        self.assertEqual(rule.severity, ValidationSeverity.ERROR)
        self.assertEqual(rule.dimension, QualityDimension.VALIDITY)
        self.assertIsNone(rule.rule_function)
        self.assertEqual(len(rule.parameters), 0)

    def test_validate_no_function(self):
        rule = ValidationRule(
            name="test_rule",
            description="Test rule",
            severity=ValidationSeverity.ERROR,
            dimension=QualityDimension.VALIDITY,
        )

        result = rule.validate("test_value")

        self.assertEqual(result.rule_name, "test_rule")
        self.assertTrue(result.passed)
        self.assertEqual(result.message, "No validation function defined")

    def test_validate_with_function_success(self):

        def mock_rule_function(value, params, context):
            return True, "Validation passed"

        rule = ValidationRule(
            name="test_rule",
            description="Test rule",
            severity=ValidationSeverity.INFO,
            dimension=QualityDimension.COMPLETENESS,
            rule_function=mock_rule_function,
        )

        result = rule.validate("test_value")

        self.assertEqual(result.rule_name, "test_rule")
        self.assertTrue(result.passed)
        self.assertEqual(result.message, "Validation passed")
        self.assertEqual(result.severity, ValidationSeverity.INFO)
        self.assertEqual(result.dimension, QualityDimension.COMPLETENESS)

    def test_validate_with_function_failure(self):

        def mock_rule_function(value, params, context):
            return False, "Validation failed"

        rule = ValidationRule(
            name="test_rule",
            description="Test rule",
            severity=ValidationSeverity.WARNING,
            dimension=QualityDimension.ACCURACY,
            rule_function=mock_rule_function,
        )

        result = rule.validate("test_value")

        self.assertEqual(result.rule_name, "test_rule")
        self.assertFalse(result.passed)
        self.assertEqual(result.message, "Validation failed")
        self.assertEqual(result.severity, ValidationSeverity.WARNING)
        self.assertEqual(result.dimension, QualityDimension.ACCURACY)

    def test_validate_with_exception(self):

        def mock_rule_function(value, params, context):
            raise Exception("Rule execution error")

        rule = ValidationRule(
            name="test_rule",
            description="Test rule",
            severity=ValidationSeverity.ERROR,
            dimension=QualityDimension.VALIDITY,
            rule_function=mock_rule_function,
        )

        result = rule.validate("test_value")

        self.assertEqual(result.rule_name, "test_rule")
        self.assertFalse(result.passed)
        self.assertIn("Validation rule execution failed", result.message)
        self.assertEqual(result.severity, ValidationSeverity.ERROR)


class TestValidationResult(unittest.TestCase):

    def test_init(self):
        result = ValidationResult(
            rule_name="test_rule", passed=True, message="Test message"
        )

        self.assertEqual(result.rule_name, "test_rule")
        self.assertTrue(result.passed)
        self.assertEqual(result.message, "Test message")
        self.assertEqual(result.severity, ValidationSeverity.INFO)
        self.assertEqual(result.dimension, QualityDimension.VALIDITY)
        self.assertIsInstance(result.timestamp, datetime)

    def test_to_dict(self):
        result = ValidationResult(
            rule_name="test_rule",
            passed=False,
            message="Test failure",
            severity=ValidationSeverity.ERROR,
            dimension=QualityDimension.COMPLETENESS,
            record_id="rec-123",
            field_name="email",
        )

        result_dict = result.to_dict()

        expected_keys = [
            "rule_name",
            "passed",
            "message",
            "severity",
            "dimension",
            "timestamp",
            "record_id",
            "field_name",
        ]

        for key in expected_keys:
            self.assertIn(key, result_dict)

        self.assertEqual(result_dict["rule_name"], "test_rule")
        self.assertFalse(result_dict["passed"])
        self.assertEqual(result_dict["message"], "Test failure")
        self.assertEqual(result_dict["severity"], ValidationSeverity.ERROR.value)
        self.assertEqual(result_dict["dimension"], QualityDimension.COMPLETENESS.value)
        self.assertEqual(result_dict["record_id"], "rec-123")
        self.assertEqual(result_dict["field_name"], "email")


class TestFieldQualityMetrics(unittest.TestCase):

    def test_init(self):
        metrics = FieldQualityMetrics(
            field_name="email",
            total_records=1000,
            non_null_count=950,
            unique_count=900,
            completeness_score=95.0,
            uniqueness_score=94.7,
            format_compliance_score=98.5,
        )

        self.assertEqual(metrics.field_name, "email")
        self.assertEqual(metrics.total_records, 1000)
        self.assertEqual(metrics.non_null_count, 950)
        self.assertEqual(metrics.unique_count, 900)
        self.assertEqual(metrics.completeness_score, 95.0)
        self.assertEqual(metrics.uniqueness_score, 94.7)
        self.assertEqual(metrics.format_compliance_score, 98.5)

    def test_null_count_property(self):
        metrics = FieldQualityMetrics(
            field_name="email",
            total_records=1000,
            non_null_count=950,
            unique_count=900,
            completeness_score=95.0,
            uniqueness_score=94.7,
            format_compliance_score=98.5,
        )

        self.assertEqual(metrics.null_count, 50)

    def test_duplicate_count_property(self):
        metrics = FieldQualityMetrics(
            field_name="email",
            total_records=1000,
            non_null_count=950,
            unique_count=900,
            completeness_score=95.0,
            uniqueness_score=94.7,
            format_compliance_score=98.5,
        )

        self.assertEqual(metrics.duplicate_count, 50)

    def test_overall_quality_score_property(self):
        metrics = FieldQualityMetrics(
            field_name="email",
            total_records=1000,
            non_null_count=950,
            unique_count=900,
            completeness_score=90.0,
            uniqueness_score=85.0,
            format_compliance_score=95.0,
        )

        expected_score = (90.0 + 85.0 + 95.0) / 3
        self.assertEqual(metrics.overall_quality_score, expected_score)


class TestDataQualityMetrics(unittest.TestCase):

    def _create_metrics(self, **kwargs):
        from execution.models.base import RecordType

        defaults = {"id": "test-record-123", "record_type": RecordType.DATA_QUALITY}
        defaults.update(kwargs)
        return DataQualityMetrics(**defaults)

    def test_init_with_defaults(self):
        from execution.models.base import RecordType

        metrics = DataQualityMetrics(
            id="test-record-123", record_type=RecordType.DATA_QUALITY
        )

        self.assertEqual(metrics.dataset_name, "")
        self.assertEqual(metrics.total_records, 0)
        self.assertEqual(metrics.valid_records, 0)
        self.assertEqual(metrics.invalid_records, 0)
        self.assertEqual(metrics.overall_quality_score, 0.0)

    def test_validate_success(self):
        metrics = self._create_metrics(
            dataset_name="test_dataset",
            total_records=1000,
            valid_records=950,
            invalid_records=50,
        )

        errors = metrics.validate()

        self.assertEqual(len(errors), 0)

    def test_validate_missing_dataset_name(self):
        metrics = self._create_metrics()

        errors = metrics.validate()

        self.assertIn("dataset_name is required", errors)

    def test_validate_negative_total_records(self):
        metrics = self._create_metrics(dataset_name="test_dataset", total_records=-10)

        errors = metrics.validate()

        self.assertIn("total_records cannot be negative", errors)

    def test_validate_invalid_record_counts(self):
        metrics = self._create_metrics(
            dataset_name="test_dataset",
            total_records=100,
            valid_records=80,
            invalid_records=30,
        )

        errors = metrics.validate()

        self.assertIn(
            "valid_records + invalid_records cannot exceed total_records", errors
        )

    def test_validate_score_out_of_range(self):
        metrics = self._create_metrics(
            dataset_name="test_dataset", completeness_score=150.0
        )

        errors = metrics.validate()

        self.assertTrue(
            any(
                "completeness_score must be between 0.0 and 100.0" in error
                for error in errors
            )
        )

    def test_calculate_overall_score_all_dimensions(self):
        metrics = self._create_metrics(
            dataset_name="test_dataset",
            completeness_score=90.0,
            accuracy_score=85.0,
            consistency_score=88.0,
            validity_score=92.0,
            uniqueness_score=87.0,
            timeliness_score=95.0,
            integrity_score=89.0,
        )

        metrics.calculate_overall_score()

        expected_score = (90.0 + 85.0 + 88.0 + 92.0 + 87.0 + 95.0 + 89.0) / 7
        self.assertEqual(metrics.overall_quality_score, expected_score)

    def test_calculate_overall_score_partial_dimensions(self):
        metrics = self._create_metrics(
            dataset_name="test_dataset",
            completeness_score=90.0,
            accuracy_score=85.0,
            validity_score=92.0,
        )

        metrics.calculate_overall_score()

        expected_score = (90.0 + 85.0 + 92.0) / 3
        self.assertEqual(metrics.overall_quality_score, expected_score)

    def test_add_field_metrics(self):
        metrics = self._create_metrics(dataset_name="test_dataset")
        field_metrics = FieldQualityMetrics(
            field_name="email",
            total_records=100,
            non_null_count=95,
            unique_count=90,
            completeness_score=95.0,
            uniqueness_score=94.7,
            format_compliance_score=98.5,
        )

        metrics.add_field_metrics("email", field_metrics)

        self.assertIn("email", metrics.field_metrics)
        self.assertEqual(metrics.field_metrics["email"], field_metrics)

    def test_add_validation_result(self):
        metrics = self._create_metrics(
            dataset_name="test_dataset", completeness_score=100.0
        )
        validation_result = ValidationResult(
            rule_name="test_rule",
            passed=False,
            message="Validation failed",
            dimension=QualityDimension.COMPLETENESS,
        )

        metrics.add_validation_result(validation_result)

        self.assertIn(validation_result, metrics.validation_results)
        self.assertEqual(metrics.completeness_score, 95.0)

    def test_get_quality_summary(self):
        metrics = self._create_metrics(
            dataset_name="test_dataset",
            total_records=1000,
            valid_records=950,
            invalid_records=50,
            duplicate_records=10,
            completeness_score=95.0,
            accuracy_score=90.0,
            overall_quality_score=92.5,
        )

        failed_result = ValidationResult(
            "rule1", False, "Failed", ValidationSeverity.ERROR
        )
        critical_result = ValidationResult(
            "rule2", False, "Critical", ValidationSeverity.CRITICAL
        )
        metrics.validation_results = [failed_result, critical_result]

        summary = metrics.get_quality_summary()

        expected_keys = [
            "dataset_name",
            "total_records",
            "valid_records",
            "invalid_records",
            "duplicate_records",
            "success_rate",
            "dimension_scores",
            "overall_quality_score",
            "validation_issues",
            "critical_issues",
        ]

        for key in expected_keys:
            self.assertIn(key, summary)

        self.assertEqual(summary["dataset_name"], "test_dataset")
        self.assertEqual(summary["total_records"], 1000)
        self.assertEqual(summary["success_rate"], 95.0)
        self.assertEqual(summary["validation_issues"], 2)
        self.assertEqual(summary["critical_issues"], 1)


class TestDataQualityValidator(unittest.TestCase):

    def test_init_registers_common_rules(self):
        validator = DataQualityValidator()

        self.assertIn("email_format", validator.rules)
        self.assertIn("non_empty", validator.rules)
        self.assertIn("positive_number", validator.rules)

    def test_register_rule(self):
        validator = DataQualityValidator()
        custom_rule = ValidationRule(
            name="custom_rule",
            description="Custom test rule",
            severity=ValidationSeverity.WARNING,
            dimension=QualityDimension.CONSISTENCY,
        )

        validator.register_rule(custom_rule)

        self.assertIn("custom_rule", validator.rules)
        self.assertEqual(validator.rules["custom_rule"], custom_rule)

    def test_validate_record_specific_rules(self):
        validator = DataQualityValidator()
        record = {"email": "test@example.com", "record_id": "rec-123"}

        results = validator.validate_record(record, ["email_format"])

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].rule_name, "email_format")
        self.assertTrue(results[0].passed)
        self.assertEqual(results[0].record_id, "rec-123")

    def test_validate_record_all_rules(self):
        validator = DataQualityValidator()
        record = {"email": "test@example.com", "name": "John Doe", "age": 25}

        results = validator.validate_record(record)

        self.assertGreaterEqual(len(results), 3)
        rule_names = [r.rule_name for r in results]
        self.assertIn("email_format", rule_names)
        self.assertIn("non_empty", rule_names)
        self.assertIn("positive_number", rule_names)

    def test_email_validation_rule_valid(self):
        validator = DataQualityValidator()
        email_rule = validator.rules["email_format"]

        result = email_rule.validate("user@example.com")

        self.assertTrue(result.passed)
        self.assertEqual(result.message, "Valid email format")

    def test_email_validation_rule_invalid(self):
        validator = DataQualityValidator()
        email_rule = validator.rules["email_format"]

        result = email_rule.validate("invalid-email")

        self.assertFalse(result.passed)
        self.assertEqual(result.message, "Invalid email format")

    def test_non_empty_validation_rule_valid(self):
        validator = DataQualityValidator()
        non_empty_rule = validator.rules["non_empty"]

        result = non_empty_rule.validate("non-empty string")

        self.assertTrue(result.passed)
        self.assertEqual(result.message, "Value is not empty")

    def test_non_empty_validation_rule_empty(self):
        validator = DataQualityValidator()
        non_empty_rule = validator.rules["non_empty"]

        result = non_empty_rule.validate("")

        self.assertFalse(result.passed)
        self.assertEqual(result.message, "Value cannot be empty")

    def test_positive_number_validation_rule_valid(self):
        validator = DataQualityValidator()
        positive_rule = validator.rules["positive_number"]

        result = positive_rule.validate(42)

        self.assertTrue(result.passed)
        self.assertEqual(result.message, "Value is a positive number")

    def test_positive_number_validation_rule_invalid(self):
        validator = DataQualityValidator()
        positive_rule = validator.rules["positive_number"]

        result = positive_rule.validate(-5)

        self.assertFalse(result.passed)
        self.assertEqual(result.message, "Value must be positive")


class TestGlobalValidator(unittest.TestCase):

    def test_global_validator_exists(self):
        self.assertIsInstance(data_quality_validator, DataQualityValidator)
        self.assertIn("email_format", data_quality_validator.rules)
        self.assertIn("non_empty", data_quality_validator.rules)
        self.assertIn("positive_number", data_quality_validator.rules)


class TestEnums(unittest.TestCase):

    def test_validation_severity_values(self):
        self.assertEqual(ValidationSeverity.INFO.value, "info")
        self.assertEqual(ValidationSeverity.WARNING.value, "warning")
        self.assertEqual(ValidationSeverity.ERROR.value, "error")
        self.assertEqual(ValidationSeverity.CRITICAL.value, "critical")

    def test_quality_dimension_values(self):
        self.assertEqual(QualityDimension.COMPLETENESS.value, "completeness")
        self.assertEqual(QualityDimension.ACCURACY.value, "accuracy")
        self.assertEqual(QualityDimension.CONSISTENCY.value, "consistency")
        self.assertEqual(QualityDimension.VALIDITY.value, "validity")
        self.assertEqual(QualityDimension.UNIQUENESS.value, "uniqueness")
        self.assertEqual(QualityDimension.TIMELINESS.value, "timeliness")
        self.assertEqual(QualityDimension.INTEGRITY.value, "integrity")


if __name__ == "__main__":
    unittest.main()
