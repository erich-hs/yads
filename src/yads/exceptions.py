"""Custom yads exceptions and shared warning utilities."""

from __future__ import annotations

import warnings


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


# Cross-cutting Exceptions (used across multiple domains)
class ConfigError(YadsError):
    """Base for configuration-related errors.

    Raised when there are issues with configuration parameters, invalid
    settings, or conflicting configuration options across different
    components (converters, loaders, etc.).
    """


class UnsupportedFeatureError(YadsError):
    """Feature not supported by target system/component.

    Raised when attempting to use a feature that is not supported by the
    target format, dialect, or component. This is a cross-cutting concern
    that can occur in converters, loaders, and other components.
    """


# Dependency Exceptions
class DependencyError(YadsError):
    """Base for optional dependency errors.

    Raised when an optional runtime dependency is missing or does not meet
    the required version constraints for a specific feature.
    """


class MissingDependencyError(DependencyError):
    """Required optional dependency is not installed."""


class DependencyVersionError(DependencyError):
    """Installed dependency version does not meet requirements."""


# Converter Exceptions
class ConverterError(YadsError):
    """Base for converter-related errors.

    Raised when there are issues during the conversion process from yads specs
    to target formats (SQL, PyArrow, PySpark, etc.).
    """


class ConverterConfigError(ConfigError):
    """Errors during converter configuration.

    Raised when there are issues with converter configuration, such as invalid
    configuration parameters or conflicting settings.
    """


class ConversionError(ConverterError):
    """Errors during conversion process.

    Raised when the conversion process fails due to incompatible data structures,
    missing handlers, or other conversion issues.
    """


# Loader Exceptions
class LoaderError(YadsError):
    """Base for loader-related errors.

    Raised when there are issues during the loading process from external
    sources to yads specs.
    """


class LoaderConfigError(ConfigError):
    """Errors during loader configuration.

    Raised when there are issues with loader configuration, such as invalid
    configuration parameters or conflicting settings.
    """


# Validator Exceptions
class AstValidationError(YadsValidationError):
    """Validation rule processing errors.

    Raised when there are issues with validation rule definition, execution,
    or processing.
    """


# Shared warnings
class ValidationWarning(UserWarning):
    """Warning emitted when validation rules fail in converters or validators."""


def validation_warning(message: str, *, filename: str, module: str | None = None) -> None:
    """Emit a categorized validation warning with a concise origin.

    Args:
        message: Human-readable warning message.
        filename: Logical filename/module label to display as the source.
        module: Module name override. Defaults to the caller's module if not provided.
    """
    warnings.warn_explicit(
        message=message,
        category=ValidationWarning,
        filename=filename,
        lineno=1,
        module=module or __name__,
    )
