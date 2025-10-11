"""
Data Quality Models and Validation Framework

This module provides comprehensive data quality validation capabilities including
validation rules, quality dimensions, metrics tracking, and reporting functionality.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from execution.models.base import (
    BaseRecord,
    ErrorInfo,
    ProcessingStats,
    RecordType,
)


class ValidationSeverity(Enum):
    """
    Enumeration of validation severity levels.

    Used to categorize the importance and impact of data quality issues.
    """

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class QualityDimension(Enum):
    """
    Data quality dimensions based on industry standards.

    Defines the different aspects of data quality that can be measured:
    - COMPLETENESS: Data has all required values
    - ACCURACY: Data values are correct and precise
    - CONSISTENCY: Data is consistent across systems/time
    - VALIDITY: Data conforms to defined formats/rules
    - UNIQUENESS: No duplicate records exist
    - TIMELINESS: Data is up-to-date and available when needed
    - INTEGRITY: Data maintains referential integrity
    """

    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"


@dataclass
class ValidationRule:
    """
    Represents a single data validation rule.

    Contains the metadata and function needed to validate data quality
    against specific business or technical requirements.

    Attributes:
        name: Unique identifier for the rule
        description: Human-readable description of what the rule validates
        severity: Impact level of validation failures
        dimension: Quality dimension this rule addresses
        rule_function: Optional callable that performs the validation
        parameters: Configuration parameters for the validation function
    """

    name: str
    description: str
    severity: ValidationSeverity
    dimension: QualityDimension
    rule_function: Optional[Callable] = None
    parameters: Dict[str, Any] = field(default_factory=dict)

    def validate(
        self, value: Any, context: Optional[Dict[str, Any]] = None
    ) -> "ValidationResult":
        if not self.rule_function:
            return ValidationResult(
                rule_name=self.name,
                passed=True,
                message="No validation function defined",
            )

        try:
            passed, message = self.rule_function(value, self.parameters, context or {})
            return ValidationResult(
                rule_name=self.name,
                passed=passed,
                message=message,
                severity=self.severity,
                dimension=self.dimension,
            )
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                message=f"Validation rule execution failed: {str(e)}",
                severity=ValidationSeverity.ERROR,
                dimension=self.dimension,
            )


@dataclass
class ValidationResult:

    rule_name: str
    passed: bool
    message: str
    severity: ValidationSeverity = ValidationSeverity.INFO
    dimension: QualityDimension = QualityDimension.VALIDITY
    timestamp: datetime = field(default_factory=datetime.utcnow)
    record_id: Optional[str] = None
    field_name: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "passed": self.passed,
            "message": self.message,
            "severity": self.severity.value,
            "dimension": self.dimension.value,
            "timestamp": self.timestamp.isoformat(),
            "record_id": self.record_id,
            "field_name": self.field_name,
        }


@dataclass
class FieldQualityMetrics:

    field_name: str
    total_records: int
    non_null_count: int
    unique_count: int
    completeness_score: float
    uniqueness_score: float
    format_compliance_score: float
    validation_results: List[ValidationResult] = field(default_factory=list)

    @property
    def null_count(self) -> int:
        return self.total_records - self.non_null_count

    @property
    def duplicate_count(self) -> int:
        return self.non_null_count - self.unique_count

    @property
    def overall_quality_score(self) -> float:
        return (
            self.completeness_score
            + self.uniqueness_score
            + self.format_compliance_score
        ) / 3


@dataclass
class DataQualityMetrics(BaseRecord):

    dataset_name: str = ""
    schema_name: Optional[str] = None

    total_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    duplicate_records: int = 0

    field_metrics: Dict[str, FieldQualityMetrics] = field(default_factory=dict)

    completeness_score: float = 0.0
    accuracy_score: float = 0.0
    consistency_score: float = 0.0
    validity_score: float = 0.0
    uniqueness_score: float = 0.0
    timeliness_score: float = 0.0
    integrity_score: float = 0.0

    overall_quality_score: float = 0.0

    validation_results: List[ValidationResult] = field(default_factory=list)

    processing_stats: Optional[ProcessingStats] = None
    errors: List[ErrorInfo] = field(default_factory=list)

    def __post_init__(self):
        self.record_type = RecordType.DATA_QUALITY
        self.calculate_overall_score()

    def validate(self) -> List[str]:
        errors: List[str] = []

        if not self.dataset_name:
            errors.append("dataset_name is required")

        if self.total_records < 0:
            errors.append("total_records cannot be negative")

        if self.valid_records + self.invalid_records > self.total_records:
            errors.append("valid_records + invalid_records cannot exceed total_records")

        score_fields = [
            "completeness_score",
            "accuracy_score",
            "consistency_score",
            "validity_score",
            "uniqueness_score",
            "timeliness_score",
            "integrity_score",
            "overall_quality_score",
        ]

        for score_field in score_fields:
            score = getattr(self, score_field)
            if not 0.0 <= score <= 100.0:
                errors.append(f"{score_field} must be between 0.0 and 100.0")

        return errors

    def calculate_overall_score(self):
        dimension_scores = [
            self.completeness_score,
            self.accuracy_score,
            self.consistency_score,
            self.validity_score,
            self.uniqueness_score,
            self.timeliness_score,
            self.integrity_score,
        ]

        non_zero_scores = [score for score in dimension_scores if score > 0]

        if non_zero_scores:
            self.overall_quality_score = sum(non_zero_scores) / len(non_zero_scores)
        else:
            self.overall_quality_score = 0.0

    def add_field_metrics(self, field_name: str, metrics: FieldQualityMetrics):
        self.field_metrics[field_name] = metrics

    def add_validation_result(self, result: ValidationResult):
        self.validation_results.append(result)

        if not result.passed:
            self._update_dimension_score(result.dimension, False)

    def _update_dimension_score(self, dimension: QualityDimension, passed: bool):
        penalty = 0 if passed else 5.0

        if dimension == QualityDimension.COMPLETENESS:
            self.completeness_score = max(0.0, self.completeness_score - penalty)
        elif dimension == QualityDimension.ACCURACY:
            self.accuracy_score = max(0.0, self.accuracy_score - penalty)
        elif dimension == QualityDimension.CONSISTENCY:
            self.consistency_score = max(0.0, self.consistency_score - penalty)
        elif dimension == QualityDimension.VALIDITY:
            self.validity_score = max(0.0, self.validity_score - penalty)
        elif dimension == QualityDimension.UNIQUENESS:
            self.uniqueness_score = max(0.0, self.uniqueness_score - penalty)
        elif dimension == QualityDimension.TIMELINESS:
            self.timeliness_score = max(0.0, self.timeliness_score - penalty)
        elif dimension == QualityDimension.INTEGRITY:
            self.integrity_score = max(0.0, self.integrity_score - penalty)

        self.calculate_overall_score()

    def get_quality_summary(self) -> Dict[str, Any]:
        return {
            "dataset_name": self.dataset_name,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "duplicate_records": self.duplicate_records,
            "success_rate": (
                (self.valid_records / self.total_records * 100)
                if self.total_records > 0
                else 0
            ),
            "dimension_scores": {
                "completeness": self.completeness_score,
                "accuracy": self.accuracy_score,
                "consistency": self.consistency_score,
                "validity": self.validity_score,
                "uniqueness": self.uniqueness_score,
                "timeliness": self.timeliness_score,
                "integrity": self.integrity_score,
            },
            "overall_quality_score": self.overall_quality_score,
            "validation_issues": len(
                [r for r in self.validation_results if not r.passed]
            ),
            "critical_issues": len(
                [
                    r
                    for r in self.validation_results
                    if not r.passed and r.severity == ValidationSeverity.CRITICAL
                ]
            ),
        }


class DataQualityValidator:

    def __init__(self):
        self.rules: Dict[str, ValidationRule] = {}
        self._register_common_rules()

    def register_rule(self, rule: ValidationRule):
        self.rules[rule.name] = rule

    def validate_record(
        self, record: Dict[str, Any], rule_names: Optional[List[str]] = None
    ) -> List[ValidationResult]:
        rules_to_apply = rule_names or list(self.rules.keys())
        results = []

        for rule_name in rules_to_apply:
            if rule_name in self.rules:
                rule = self.rules[rule_name]
                result = rule.validate(record)
                result.record_id = record.get("record_id")
                results.append(result)

        return results

    def _register_common_rules(self):

        def validate_email(record, params, context):
            import re

            if isinstance(record, dict):
                value = record.get("email")
            else:
                value = record

            if value is None:
                return False, "Email field not found"

            if not isinstance(value, str):
                return False, "Email value must be a string"

            pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            if re.match(pattern, value):
                return True, "Valid email format"
            return False, "Invalid email format"

        self.register_rule(
            ValidationRule(
                name="email_format",
                description="Validate email address format",
                severity=ValidationSeverity.ERROR,
                dimension=QualityDimension.VALIDITY,
                rule_function=validate_email,
            )
        )

        def validate_non_empty(value, params, context):
            if value is None:
                return False, "Value cannot be null"
            if isinstance(value, str) and not value.strip():
                return False, "Value cannot be empty"
            return True, "Value is not empty"

        self.register_rule(
            ValidationRule(
                name="non_empty",
                description="Validate that value is not empty",
                severity=ValidationSeverity.WARNING,
                dimension=QualityDimension.COMPLETENESS,
                rule_function=validate_non_empty,
            )
        )

        def validate_positive_number(value, params, context):
            if not isinstance(value, (int, float)):
                return False, "Value must be a number"
            if value <= 0:
                return False, "Value must be positive"
            return True, "Value is a positive number"

        self.register_rule(
            ValidationRule(
                name="positive_number",
                description="Validate that number is positive",
                severity=ValidationSeverity.ERROR,
                dimension=QualityDimension.VALIDITY,
                rule_function=validate_positive_number,
            )
        )


data_quality_validator = DataQualityValidator()
