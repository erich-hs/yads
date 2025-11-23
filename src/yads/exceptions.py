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
    """Base for all validation-related errors."""


# Spec Exceptions
class SpecError(YadsValidationError):
    """Spec definition and validation errors."""


class SpecParsingError(SpecError):
    """Errors during spec parsing from YAML/JSON."""


class SpecValidationError(SpecError):
    """Spec consistency and integrity validation errors."""


class SpecSerializationError(SpecError):
    """Errors encountered while serializing specs to dictionaries."""


# Type System Exceptions
class TypeDefinitionError(YadsValidationError):
    """Invalid type definitions and parameters."""


class UnknownTypeError(TypeDefinitionError):
    """Unknown or unsupported type name."""


# Constraint Exceptions
class ConstraintError(YadsValidationError):
    """Constraint definition and validation errors."""


class UnknownConstraintError(ConstraintError):
    """Unknown constraint type."""


class InvalidConstraintError(ConstraintError):
    """Invalid constraint parameters or configuration."""


class ConstraintConflictError(ConstraintError):
    """Conflicting constraints."""


# Cross-cutting Exceptions (used across multiple domains)
class ConfigError(YadsError):
    """Base for configuration-related errors."""


class UnsupportedFeatureError(YadsError):
    """Feature not supported by target system/component."""


# Dependency Exceptions
class DependencyError(YadsError):
    """Base for optional dependency errors."""


class MissingDependencyError(DependencyError):
    """Required optional dependency is not installed."""


class DependencyVersionError(DependencyError):
    """Installed dependency version does not meet requirements."""


# Converter Exceptions
class ConverterError(YadsError):
    """Base for converter-related errors."""


class ConverterConfigError(ConfigError):
    """Errors during converter configuration."""


class ConversionError(ConverterError):
    """Errors during conversion process."""


# Loader Exceptions
class LoaderError(YadsError):
    """Base for loader-related errors."""


class LoaderConfigError(ConfigError):
    """Errors during loader configuration."""


# Registry Exceptions
class RegistryError(YadsError):
    """Base exception for registry operations."""


class RegistryConnectionError(RegistryError):
    """Failed to connect or validate registry base path."""


class SpecNotFoundError(RegistryError):
    """Spec name or version not found in registry."""


class InvalidSpecNameError(RegistryError):
    """Spec name contains invalid characters."""


# Validator Exceptions
class AstValidationError(YadsValidationError):
    """SQL AST validation rule processing errors."""


# Shared warnings
class ValidationWarning(UserWarning):
    """Warning emitted when validation rules fail in converters or validators."""


class DuplicateSpecWarning(UserWarning):
    """Warning emitted when registering a spec with identical content to latest version."""


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
