"""Base converter interface for spec transformations.

This module defines the abstract base class for all yads converters. Converters
are responsible for transforming YadsSpec objects into target formats such as
SQL DDL, framework-specific schemas or other representations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Generic,
    Literal,
    TypeVar,
    cast,
)

from ..spec import Field
from ..exceptions import ConverterConfigError

if TYPE_CHECKING:
    from ..spec import YadsSpec

T = TypeVar("T")


@dataclass(frozen=True)
class BaseConverterConfig(Generic[T]):
    """Base configuration for all yads converters.

    Args:
        mode: Conversion mode. "raise" will raise exceptions on unsupported features,
            "coerce" will attempt to coerce unsupported features to supported ones
            with warnings. Defaults to "coerce".
        ignore_columns: Set of column names to ignore during conversion.
            These columns will be excluded from the output. Defaults to empty set.
        include_columns: Optional set of column names to include during conversion.
            If specified, only these columns will be included in the output.
            If None, all columns (except ignored ones) are included. Defaults to None.
        column_overrides: Dictionary mapping column names to custom conversion
            functions. Provides column-specific conversion logic with complete
            control over field conversion. Function signature:
            (field, converter) -> converted_result. Defaults to empty dict.
    """

    mode: Literal["raise", "coerce"] = "coerce"
    ignore_columns: set[str] = field(default_factory=set)
    include_columns: set[str] | None = None
    # Keep permissive typing at the base to avoid contravariance issues.
    # Subclasses may narrow this for better IDE hints.
    column_overrides: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mode not in {"raise", "coerce"}:
            raise ConverterConfigError("mode must be one of 'raise' or 'coerce'.")

        if self.include_columns is not None and self.ignore_columns:
            overlap = self.ignore_columns & self.include_columns
            if overlap:
                raise ConverterConfigError(
                    f"Columns cannot be both ignored and included: {sorted(overlap)}"
                )


class BaseConverter(Generic[T], ABC):
    """Abstract base class for spec converters."""

    def __init__(self, config: BaseConverterConfig[T] | None = None) -> None:
        """Initialize the BaseConverter.

        Args:
            config: Configuration object. If None, uses default BaseConverterConfig.
        """
        self.config = config or BaseConverterConfig()
        self._current_field_name: str | None = None

    @abstractmethod
    def convert(
        self, spec: YadsSpec, *, mode: Literal["raise", "coerce"] | None = None
    ) -> Any:
        """Convert a YadsSpec to the target format."""
        ...

    def _filter_columns(self, spec: YadsSpec) -> Generator[Field, None, None]:
        for column in spec.columns:
            name = column.name
            if name in self.config.ignore_columns:
                continue
            if self.config.include_columns is not None and (
                name not in self.config.include_columns
            ):
                continue
            yield column

    def _validate_column_filters(self, spec: YadsSpec) -> None:
        column_names = {c.name for c in spec.columns}
        unknown_ignored = self.config.ignore_columns - column_names
        unknown_included = (self.config.include_columns or set()) - column_names

        messages: list[str] = []
        if unknown_ignored:
            messages.append(
                "Unknown columns in ignore_columns: " + ", ".join(sorted(unknown_ignored))
            )
        if unknown_included:
            messages.append(
                "Unknown columns in include_columns: "
                + ", ".join(sorted(unknown_included))
            )
        if not messages:
            return
        raise ConverterConfigError("; ".join(messages))

    def _has_column_override(self, column_name: str) -> bool:
        return column_name in self.config.column_overrides

    def _apply_column_override(self, field: Field) -> T:
        override_func = cast(
            Callable[[Field, Any], T], self.config.column_overrides[field.name]
        )
        return override_func(field, self)

    def _convert_field_with_overrides(self, field: Field) -> T:
        if self._has_column_override(field.name):
            return self._apply_column_override(field)
        return self._convert_field_default(field)

    def _convert_field_default(self, field: Field) -> Any:
        """Convert field using default conversion logic. Subclasses that use
        the _convert_field_with_overrides method must implement this.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _convert_field_default "
            "to use the centralized override resolution"
        )

    @contextmanager
    def conversion_context(
        self,
        *,
        mode: Literal["raise", "coerce"] | None = None,
        field: str | None = None,
    ) -> Generator[None, None, None]:
        """Temporarily set conversion mode and field context.

        This context manager centralizes handling of converter state used for
        warnings and coercions, ensuring that values are restored afterwards.

        Args:
            mode: Optional override for the current conversion mode.
            field: Optional field name for contextual warnings.
        """
        # Snapshot current state
        previous_config = self.config
        previous_field = self._current_field_name

        try:
            if mode is not None:
                if mode not in ("raise", "coerce"):
                    raise ConverterConfigError("mode must be one of 'raise' or 'coerce'.")
                self.config = replace(self.config, mode=mode)
            if field is not None:
                self._current_field_name = field
            yield
        finally:
            # Restore prior state
            self.config = previous_config
            self._current_field_name = previous_field
