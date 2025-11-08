"""Base converter interface for spec transformations.

This module defines the abstract base class for all yads converters. Converters
are responsible for transforming YadsSpec objects into target formats such as
SQL DDL, framework-specific schemas or other representations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generator,
    Generic,
    Literal,
    Mapping,
    TypeVar,
    cast,
)

from ..spec import Field
from ..exceptions import ConverterConfigError, UnsupportedFeatureError, validation_warning

if TYPE_CHECKING:
    from ..spec import YadsSpec

T = TypeVar("T")
# Keep permissive here to avoid contravariance issues. Subclasses may narrow.
ColumnOverrideFunc = Callable[[Field, Any], T]
ColumnOverrides = Mapping[str, ColumnOverrideFunc[T]]


@dataclass(frozen=True)
class BaseConverterConfig(Generic[T]):
    """Base configuration for all yads converters.

    This configuration is immutable and safely copies user-provided containers.

    Args:
        mode: Conversion mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        ignore_columns: Iterable of column names to ignore during conversion.
            These columns will be excluded from the output. Accepts any iterable
            and is stored immutably. Defaults to None (treated as empty).
        include_columns: Optional iterable of column names to include during
            conversion. If specified, only these columns will be included in the
            output. If None, all columns (except ignored ones) are included.
            Accepts any iterable and is stored immutably. Defaults to None.
        column_overrides: Mapping of column names to custom conversion functions.
            These provide column-specific conversion logic with complete control
            over field conversion. Accepts mutable mappings and is stored as an
            immutable mapping internally. Function signature:
            `(field, converter) -> converted_result`.
    """

    # Public fields (inputs are coerced to immutable in __post_init__)
    mode: Literal["raise", "coerce"] = "coerce"
    ignore_columns: frozenset[str] = field(default_factory=frozenset)
    include_columns: frozenset[str] | None = None
    column_overrides: Mapping[str, ColumnOverrideFunc[T]] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        # Convert user inputs to immutable, detached containers
        object.__setattr__(self, "ignore_columns", frozenset(self.ignore_columns))
        if self.include_columns is not None:
            object.__setattr__(self, "include_columns", frozenset(self.include_columns))
        object.__setattr__(
            self,
            "column_overrides",
            MappingProxyType(dict(self.column_overrides)),
        )

        # Validation
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

    @property
    def _field_context(self) -> str:
        """Current field name or '<unknown>' for error messages."""
        return self._current_field_name or "<unknown>"

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
        if self.config.include_columns is None:
            unknown_included: set[str] = set()
        else:
            unknown_included = set(self.config.include_columns - column_names)

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
            ColumnOverrideFunc[T], self.config.column_overrides[field.name]
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

    def raise_or_coerce(
        self,
        yads_type: Any | None = None,
        *,
        coerce_type: T | None = None,
        error_msg: str | None = None,
    ) -> T:
        """Handle raise or coerce mode for unsupported type features.

        This public method provides a consistent way to handle unsupported types
        based on the converter's mode. It can be used within converters and in
        custom column override functions.

        The method uses the template method pattern with several hook methods
        that subclasses can override to customize behavior:
        - `_format_type_for_display`: Customize how types appear in warnings
        - `_emit_warning`: Customize warning emission
        - `_get_fallback_type`: Customize fallback type resolution
        - `_generate_error_message`: Customize error message generation

        Args:
            yads_type: The yads type that is not supported. Can be None if
                error_msg is explicitly provided.
            coerce_type: The type to coerce to in coerce mode. If None, uses
                the converter's configured fallback type.
            error_msg: Custom error message. If None, uses a default message
                based on the converter class name and yads_type. When providing
                a custom error_msg, yads_type can be None.

        Returns:
            The coerced type in coerce mode.

        Raises:
            UnsupportedFeatureError: In raise mode when the feature is not supported,
                or in coerce mode when fallback_type is None.
            ValueError: If both yads_type and error_msg are None.
        """
        # Resolve error message once
        if error_msg is None:
            if yads_type is None:
                raise ValueError(
                    "Either yads_type or error_msg must be provided to raise_or_coerce"
                )
            error_msg = self._generate_error_message(yads_type)

        # Resolve coerce_type (fallback to config if not provided)
        if coerce_type is None:
            try:
                coerce_type = self._get_fallback_type()
            except ValueError:
                # fallback_type is None - must raise even in coerce mode
                if self.config.mode == "coerce":
                    error_msg = f"{error_msg} Specify a fallback_type to enable coercion of unsupported types."
                raise UnsupportedFeatureError(error_msg)

        # Handle based on mode
        if self.config.mode == "coerce":
            display_type = self._format_type_for_display(coerce_type)
            warning_msg = f"{error_msg} The data type will be coerced to {display_type}."
            self._emit_warning(warning_msg)
            return coerce_type
        else:
            raise UnsupportedFeatureError(error_msg)

    def _format_type_for_display(self, type_obj: Any) -> str:
        return str(type_obj)

    def _emit_warning(self, message: str) -> None:
        validation_warning(
            message=message,
            filename=self.__class__.__module__,
            module=self.__class__.__module__,
        )

    def _get_fallback_type(self) -> T:
        fallback_type = getattr(self.config, "fallback_type", None)
        if fallback_type is None:
            raise ValueError(
                f"{self.__class__.__name__} config must have a fallback_type "
                "attribute to use raise_or_coerce without explicit coerce_type"
            )
        return fallback_type

    def _generate_error_message(self, yads_type: Any) -> str:
        return (
            f"{self.__class__.__name__} does not support type: {yads_type}"
            f" for '{self._field_context}'."
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
