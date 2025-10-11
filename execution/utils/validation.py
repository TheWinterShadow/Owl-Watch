"""
Data Validation Utilities

This module provides comprehensive data validation functionality including
schema validation, data type checking, field validation, and result tracking.
Used throughout the ETL pipeline to ensure data quality and consistency.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from execution.models.exceptions import (
    MissingFieldError,
    SchemaValidationError,
    TypeMismatchError,
)
from execution.utils.logger import logger


@dataclass
class ValidationResult:
    """
    Container for validation results and metadata.

    Tracks validation status, errors, warnings, and specific issues
    found during data validation processes.

    Attributes:
        is_valid: Boolean indicating if validation passed
        errors: List of error messages found during validation
        warnings: List of warning messages
        missing_fields: Fields that were expected but not found
        type_mismatches: Fields with incorrect data types
        field_name: Specific field being validated (optional)
    """

    is_valid: bool
    errors: List[str]
    warnings: List[str]
    missing_fields: Optional[List[str]] = None
    type_mismatches: Optional[List[str]] = None
    field_name: Optional[str] = None

    def __post_init__(self) -> None:
        if self.missing_fields is None:
            self.missing_fields = []
        if self.type_mismatches is None:
            self.type_mismatches = []

    def add_error(self, error: str) -> None:
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        self.warnings.append(warning)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


class SchemaValidator:
    def __init__(self, check_nullable: bool = False):
        self.check_nullable = check_nullable
        logger.secure_debug(
            "SchemaValidator initialized", check_nullable=check_nullable
        )

    def validate(self, data: Any) -> ValidationResult:
        return ValidationResult(
            is_valid=True, errors=[], warnings=[], missing_fields=[], type_mismatches=[]
        )

    def get_schema(self) -> Any:
        return {}

    def validate_schema(
        self, df: DataFrame, expected_schema: StructType
    ) -> ValidationResult:
        logger.secure_debug("Starting schema validation")

        actual_schema = df.schema
        expected_fields = {
            field.name: field for field in expected_schema.fields}
        actual_fields = {field.name: field for field in actual_schema.fields}

        expected_field_names = set(expected_fields.keys())
        actual_field_names = set(actual_fields.keys())

        missing_fields = list(expected_field_names - actual_field_names)
        extra_fields = list(actual_field_names - expected_field_names)

        type_mismatches = []
        warnings = []
        common_fields = expected_field_names & actual_field_names

        for field_name in common_fields:
            expected_field = expected_fields[field_name]
            actual_field = actual_fields[field_name]

            if not self._types_match(expected_field.dataType, actual_field.dataType):
                type_mismatches.append(
                    f"Field '{field_name}': expected {expected_field.dataType}, got {actual_field.dataType}"
                )
            elif (
                self.check_nullable and expected_field.nullable != actual_field.nullable
            ):
                type_mismatches.append(
                    f"Field '{field_name}': nullable mismatch - "
                    f"expected {expected_field.nullable}, got {actual_field.nullable}"
                )

        is_valid = len(missing_fields) == 0 and len(type_mismatches) == 0

        errors = []
        if missing_fields:
            errors.append(
                f"Missing required fields: {', '.join(missing_fields)}")
        if type_mismatches:
            errors.extend(type_mismatches)

        if extra_fields:
            warnings.append(f"Extra fields found: {', '.join(extra_fields)}")

        logger.secure_debug(
            "Schema validation completed",
            is_valid=is_valid,
            missing_fields_count=len(missing_fields),
            extra_fields_count=len(extra_fields),
            type_mismatches_count=len(type_mismatches),
        )

        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            missing_fields=missing_fields,
            type_mismatches=type_mismatches,
        )

    def _types_match(self, expected_type, actual_type) -> bool:
        from pyspark.sql.types import ArrayType, MapType, StructType

        if type(expected_type) is not type(actual_type):
            return False

        if isinstance(expected_type, StructType):
            expected_struct_fields = {f.name: f for f in expected_type.fields}
            actual_struct_fields = {f.name: f for f in actual_type.fields}

            if set(expected_struct_fields.keys()) != set(actual_struct_fields.keys()):
                return False

            for field_name in expected_struct_fields.keys():
                expected_field = expected_struct_fields[field_name]
                actual_field = actual_struct_fields[field_name]

                if not self._types_match(
                    expected_field.dataType, actual_field.dataType
                ):
                    return False

                if (
                    self.check_nullable
                    and expected_field.nullable != actual_field.nullable
                ):
                    return False

            return True

        if isinstance(expected_type, ArrayType):
            if not self._types_match(
                expected_type.elementType, actual_type.elementType
            ):
                return False

            if (
                self.check_nullable
                and expected_type.containsNull != actual_type.containsNull
            ):
                return False

            return True

        if isinstance(expected_type, MapType):
            if not self._types_match(expected_type.keyType, actual_type.keyType):
                return False

            if not self._types_match(expected_type.valueType, actual_type.valueType):
                return False

            if (
                self.check_nullable
                and expected_type.valueContainsNull != actual_type.valueContainsNull
            ):
                return False

            return True

        return expected_type == actual_type

    def validate_curated_schema(
        self, df: DataFrame, expected_schema: StructType
    ) -> ValidationResult:
        logger.secure_debug("Starting curated schema validation")
        return self.validate_schema(df, expected_schema)

    def validate_transformed_schema(
        self, df: DataFrame, expected_schema: StructType
    ) -> ValidationResult:
        logger.secure_debug("Starting transformed schema validation")
        return self.validate_schema(df, expected_schema)

    def get_schema_diff(
        self, schema1: StructType, schema2: StructType
    ) -> Dict[str, Any]:
        fields1 = {field.name: field for field in schema1.fields}
        fields2 = {field.name: field for field in schema2.fields}

        field_names1 = set(fields1.keys())
        field_names2 = set(fields2.keys())

        only_in_schema1 = list(field_names1 - field_names2)
        only_in_schema2 = list(field_names2 - field_names1)
        common_fields = field_names1 & field_names2

        type_differences = []
        for field_name in common_fields:
            field1 = fields1[field_name]
            field2 = fields2[field_name]

            if field1.dataType != field2.dataType or field1.nullable != field2.nullable:
                type_differences.append(
                    {
                        "field": field_name,
                        "schema1_type": str(field1.dataType),
                        "schema1_nullable": field1.nullable,
                        "schema2_type": str(field2.dataType),
                        "schema2_nullable": field2.nullable,
                    }
                )

        return {
            "only_in_schema1": only_in_schema1,
            "only_in_schema2": only_in_schema2,
            "type_differences": type_differences,
            "schemas_identical": len(only_in_schema1) == 0
            and len(only_in_schema2) == 0
            and len(type_differences) == 0,
        }

    def select_expected_fields(
        self, df: DataFrame, expected_schema: StructType
    ) -> DataFrame:
        expected_field_names = [field.name for field in expected_schema.fields]
        available_fields = df.columns

        fields_to_select = [
            field for field in expected_field_names if field in available_fields
        ]

        if len(fields_to_select) != len(expected_field_names):
            missing_fields = set(expected_field_names) - set(available_fields)
            logger.secure_warning(
                "Some expected fields are missing from DataFrame",
                missing_fields=list(missing_fields),
            )

        logger.secure_debug(
            "Selecting expected fields",
            expected_count=len(expected_field_names),
            available_count=len(fields_to_select),
        )

        return df.select(*fields_to_select)

    def validate_required_fields(
        self, df: DataFrame, required_fields: List[str]
    ) -> Dict[str, Any]:
        available_fields = set(df.columns)
        missing_fields = [
            field for field in required_fields if field not in available_fields
        ]

        null_counts = {}
        present_required_fields = [
            field for field in required_fields if field in available_fields
        ]

        if present_required_fields:
            for field in present_required_fields:
                null_count = df.filter(df[field].isNull()).count()
                if null_count > 0:
                    null_counts[field] = null_count

        is_valid = len(missing_fields) == 0 and len(null_counts) == 0

        return {
            "is_valid": is_valid,
            "missing_fields": missing_fields,
            "null_counts": null_counts,
            "total_records": df.count(),
        }

    def validate_schema_strict(
        self, df: DataFrame, expected_schema: StructType, schema_name: str
    ) -> None:
        result = self.validate_schema(df, expected_schema)

        if not result.is_valid:
            if result.missing_fields:
                raise MissingFieldError(
                    missing_fields=result.missing_fields,
                    context={"expected_schema": str(expected_schema)},
                )

            if result.type_mismatches:
                first_mismatch = result.type_mismatches[0]
                import re

                match = re.match(
                    r"Field '(.+?)': expected (.+?), got (.+)", first_mismatch
                )
                if match:
                    field_name, expected_type, actual_type = match.groups()
                    raise TypeMismatchError(
                        field_name=field_name,
                        expected_type=expected_type,
                        actual_type=actual_type,
                        context={"all_mismatches": result.type_mismatches},
                    )
                else:
                    raise SchemaValidationError(
                        schema_name=schema_name,
                        validation_errors=result.type_mismatches,
                        context={"validation_type": "type_mismatch"},
                    )

    def validate_required_fields_strict(
        self, df: DataFrame, required_fields: List[str]
    ) -> None:
        result = self.validate_required_fields(df, required_fields)

        if not result["is_valid"]:
            if result["missing_fields"]:
                raise MissingFieldError(
                    missing_fields=result["missing_fields"],
                    context={"total_records": result["total_records"]},
                )

            if result["null_counts"]:
                null_details = [
                    f"{field}: {count} nulls"
                    for field, count in result["null_counts"].items()
                ]
                raise SchemaValidationError(
                    schema_name="RequiredFields",
                    validation_errors=[
                        f"Required fields contain null values: {'; '.join(null_details)}"
                    ],
                    context={
                        "null_counts": result["null_counts"],
                        "total_records": result["total_records"],
                    },
                )

    def get_schema_summary(self, schema: StructType) -> Dict[str, Any]:
        field_types: Dict[str, int] = {}
        nullable_fields = []
        non_nullable_fields = []

        for field in schema.fields:
            field_type = str(field.dataType)
            if field_type in field_types:
                field_types[field_type] += 1
            else:
                field_types[field_type] = 1

            if field.nullable:
                nullable_fields.append(field.name)
            else:
                non_nullable_fields.append(field.name)

        return {
            "total_fields": len(schema.fields),
            "field_types": field_types,
            "nullable_fields": nullable_fields,
            "non_nullable_fields": non_nullable_fields,
            "field_names": [field.name for field in schema.fields],
        }
