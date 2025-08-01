"""Custom exceptions.

This module provides a comprehensive exception hierarchy, designed to give users
clear, actionable error messages while enabling programmatic error handling.
The hierarchy is organized by functional areas (schema, types, constraints, etc.)
and includes suggestions for resolution.

Example:
    >>> try:
    ...     spec = from_dict(invalid_data)
    ... except UnknownTypeError as e:
    ...     print(f"Error: {e}")
    ...     print(f"Suggestions: {e.suggestions}")
"""

from __future__ import annotations


class YadsError(Exception):
    """Base exception for all yads-related errors.

    This is the root exception that all other yads exceptions inherit from.
    It provides enhanced error reporting with suggestions for resolution.

    Attributes:
        suggestions: List of suggested fixes or actions.

    Example:
        >>> raise YadsError(
        ...     "Something went wrong with field 'user_id' at line 42",
        ...     suggestions=["Check the field name", "Verify the type definition"]
        ... )
    """

    def __init__(
        self,
        message: str,
        suggestions: list[str] | None = None,
    ):
        """Initialize a YadsError.

        Args:
            message: The error message.
            suggestions: Optional list of suggestions to fix the error.
        """
        super().__init__(message)
        self.suggestions = suggestions or []

    def __str__(self) -> str:
        """Return a formatted error message with suggestions."""
        result = super().__str__()

        if self.suggestions:
            suggestions_text = "; ".join(self.suggestions)
            result += f" | {suggestions_text}"

        return result


class YadsValidationError(YadsError):
    """Base for all validation-related errors.

    This exception is raised when validation fails during schema parsing,
    type checking, constraint validation, or other validation operations.
    """


# Schema/Spec Exceptions
class SchemaError(YadsValidationError):
    """Schema definition and validation errors.

    Raised when there are issues with schema structure, field definitions,
    or overall schema consistency.
    """


class SchemaParsingError(SchemaError):
    """Errors during schema parsing from YAML/JSON.

    Raised when the input format is invalid, required fields are missing,
    or the structure doesn't conform to the expected schema format.

    Example:
        >>> raise SchemaParsingError(
        ...     "Missing required field 'version' in schema 'users'",
        ...     suggestions=["Add a 'version' field to your schema definition"]
        ... )
    """


class SchemaValidationError(SchemaError):
    """Schema consistency and integrity validation errors.

    Raised when the schema is structurally valid but has logical inconsistencies,
    such as referential integrity violations, duplicate columns, or conflicting
    constraints.

    Example:
        >>> raise SchemaValidationError(
        ...     "Partition column 'status' is not defined in the schema (defined columns: id, name)",
        ...     suggestions=["Add 'status' column to schema", "Remove 'status' from partitioned_by"]
        ... )
    """


# Type System Exceptions
class TypeDefinitionError(YadsValidationError):
    """Invalid type definitions and parameters.

    Raised when type definitions have invalid parameters, conflicting settings,
    or other structural issues.

    Example:
        >>> raise TypeDefinitionError(
        ...     "String 'length' must be a positive integer, got -5",
        ...     suggestions=["Use a positive integer for string length"]
        ... )
    """


class UnknownTypeError(TypeDefinitionError):
    """Unknown or unsupported type name.

    Raised when attempting to use a type that is not recognized by yads.

    Example:
        >>> raise UnknownTypeError(
        ...     "Unknown type 'invalid_type' for field 'user_id'",
        ...     suggestions=[
        ...         "Check for typos in the type name",
        ...         "Use one of the supported types: string, integer, float, etc."
        ...     ]
        ... )
    """


# Constraint Exceptions
class ConstraintError(YadsValidationError):
    """Constraint definition and validation errors.

    Base exception for all constraint-related issues including unknown constraints,
    invalid parameters, and constraint conflicts.
    """


class UnknownConstraintError(ConstraintError):
    """Unknown constraint type.

    Raised when attempting to use a constraint that is not recognized by yads.

    Example:
        >>> raise UnknownConstraintError(
        ...     "Unknown constraint 'invalid_constraint' for field 'user_id'",
        ...     suggestions=[
        ...         "Check for typos in the constraint name",
        ...         "Use one of the supported constraints: not_null, primary_key, etc."
        ...     ]
        ... )
    """


class InvalidConstraintError(ConstraintError):
    """Invalid constraint parameters or configuration.

    Raised when constraint parameters are invalid, missing, or have incorrect types.

    Example:
        >>> raise InvalidConstraintError(
        ...     "The 'not_null' constraint expects a boolean value, got 'yes' for field 'email'",
        ...     suggestions=["Use true or false for the not_null constraint"]
        ... )
    """


class ConstraintConflictError(ConstraintError):
    """Conflicting constraints.

    Raised when constraints are defined in conflicting ways, such as the same
    constraint being defined at both column and table level.

    Example:
        >>> raise ConstraintConflictError(
        ...     "Column 'id' has primary_key defined at both column and table level",
        ...     suggestions=[
        ...         "Remove the column-level primary_key constraint",
        ...         "Remove the table-level primary_key constraint"
        ...     ]
        ... )
    """


# Converter Exceptions
class ConverterError(YadsError):
    """Base for converter-related errors.

    Raised when there are issues during the conversion process from yads specs
    to target formats (SQL, PyArrow, PySpark, etc.).
    """


class ConversionError(ConverterError):
    """Errors during conversion process.

    Raised when the conversion process fails due to incompatible data structures,
    missing handlers, or other conversion issues.

    Example:
        >>> raise ConversionError(
        ...     "Failed to convert Map type to SQL representation for field 'metadata'",
        ...     suggestions=["Use a supported type for SQL conversion"]
        ... )
    """


class UnsupportedFeatureError(ConverterError):
    """Feature not supported by target converter/dialect.

    Raised when attempting to convert a yads feature that is not supported
    by the target format or dialect.

    Example:
        >>> raise UnsupportedFeatureError(
        ...     "Fixed-length strings are not supported in Spark SQL for field 'code'",
        ...     suggestions=[
        ...         "Remove the length parameter",
        ...         "Use mode='fix' to automatically adjust the schema"
        ...     ]
        ... )
    """


# Validator Exceptions
class ValidationRuleError(YadsValidationError):
    """Validation rule processing errors.

    Raised when there are issues with validation rule definition, execution,
    or processing.

    Example:
        >>> raise ValidationRuleError(
        ...     "Validation rule 'NoFixedLengthStringRule' failed to process DataType node",
        ...     suggestions=["Check the rule implementation", "Verify the input AST"]
        ... )
    """
