"""Custom yads exceptions."""

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
        result = super().__str__()

        if self.suggestions:
            suggestions_text = "; ".join(self.suggestions)
            result += f" | {suggestions_text}"

        return result


class YadsValidationError(YadsError):
    """Base for all validation-related errors.

    This exception is raised when validation fails during spec parsing,
    type checking, constraint validation, or other validation operations.
    """


# Spec Exceptions
class SpecError(YadsValidationError):
    """Spec definition and validation errors.

    Raised when there are issues with spec structure, field definitions,
    or overall spec consistency.
    """


class SpecParsingError(SpecError):
    """Errors during spec parsing from YAML/JSON.

    Raised when the input format is invalid, required fields are missing,
    or the structure doesn't conform to the expected spec format.
    """


class SpecValidationError(SpecError):
    """Spec consistency and integrity validation errors.

    Raised when the spec is structurally valid but has logical inconsistencies,
    such as referential integrity violations, duplicate columns, or conflicting
    constraints.
    """


# Type System Exceptions
class TypeDefinitionError(YadsValidationError):
    """Invalid type definitions and parameters.

    Raised when type definitions have invalid parameters, conflicting settings,
    or other structural issues.
    """


class UnknownTypeError(TypeDefinitionError):
    """Unknown or unsupported type name.

    Raised when attempting to use a type that is not recognized by yads.
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
    """


class InvalidConstraintError(ConstraintError):
    """Invalid constraint parameters or configuration.

    Raised when constraint parameters are invalid, missing, or have incorrect types.
    """


class ConstraintConflictError(ConstraintError):
    """Conflicting constraints.

    Raised when constraints are defined in conflicting ways, such as the same
    constraint being defined at both column and table level.
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
    """


class UnsupportedFeatureError(ConverterError):
    """Feature not supported by target converter/dialect.

    Raised when attempting to convert a yads feature that is not supported
    by the target format or dialect.
    """


# Validator Exceptions
class AstValidationError(YadsValidationError):
    """Validation rule processing errors.

    Raised when there are issues with validation rule definition, execution,
    or processing.
    """
