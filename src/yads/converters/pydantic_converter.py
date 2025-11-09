"""Pydantic converter from yads `YadsSpec` to Pydantic `BaseModel`.

This module defines the `PydanticConverter`, responsible for producing a
Pydantic `BaseModel` class from yads' canonical `YadsSpec` format.

Example:
    >>> import yads.types as ytypes
    >>> from yads.spec import Column, YadsSpec
    >>> from yads.converters import PydanticConverter
    >>> spec = YadsSpec(
    ...     name="catalog.db.table",
    ...     version="0.0.1",
    ...     columns=[
    ...         Column(name="id", type=ytypes.Integer(bits=64)),
    ...         Column(name="name", type=ytypes.String()),
    ...     ],
    ... )
    >>> converter = PydanticConverter()
    >>> model_cls = converter.convert(spec)
    >>> instance = model_cls(id=1, name="test")
    >>> print(instance.model_dump())
    {'id': 1, 'name': 'test'}
"""

from __future__ import annotations

from functools import singledispatchmethod, lru_cache
from datetime import date, datetime, time, timedelta
from decimal import Decimal as PythonDecimal
from typing import Any, Callable, Literal, Optional, Type, Mapping, cast, TYPE_CHECKING
from uuid import UUID as PythonUUID
from dataclasses import dataclass, field
from types import MappingProxyType

from ..constraints import (
    ColumnConstraint,
    DefaultConstraint,
    ForeignKeyConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)
from ..exceptions import UnsupportedFeatureError
from .._dependencies import (
    requires_dependency,
    _get_installed_version,
    _meets_min_version,
)
from .base import BaseConverter, BaseConverterConfig

from .. import spec as yspec
from .. import types as ytypes

if TYPE_CHECKING:
    from pydantic import BaseModel  # type: ignore[import-untyped]
    from pydantic.fields import FieldInfo  # type: ignore[import-untyped]


# %% ---- Configuration --------------------------------------------------------------
@dataclass(frozen=True)
class PydanticConverterConfig(BaseConverterConfig):
    """Configuration for PydanticConverter.

    Args:
        mode: Conversion mode. One of "raise" or "coerce". Inherited from
            BaseConverterConfig. Defaults to "coerce".
        ignore_columns: Column names to exclude from conversion. Inherited from
            BaseConverterConfig. Defaults to empty.
        include_columns: If provided, only these columns are included. Inherited
            from BaseConverterConfig. Defaults to None.
        column_overrides: Mapping of column name to a callable that returns a
            custom Pydantic field conversion. Inherited from BaseConverterConfig.
            Defaults to empty mapping.
        model_name: Custom name for the generated model class. If None, uses
            the spec name. Defaults to None.
        model_config: Dictionary of Pydantic model configuration options.
            Defaults to empty dict.
        fallback_type: Python type to use for unsupported types in coerce mode.
            Must be one of: str, dict, bytes, or None. Defaults to None.
    """

    model_name: str | None = None
    model_config: dict[str, Any] | None = None
    fallback_type: type | None = None
    column_overrides: Mapping[
        str, Callable[[yspec.Field, PydanticConverter], tuple[Any, FieldInfo]]
    ] = field(default_factory=lambda: MappingProxyType({}))  # type: ignore[assignment]

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()

        # Validate fallback_type if provided
        if self.fallback_type is not None:
            valid_fallback_types = {str, dict, bytes}
            if self.fallback_type not in valid_fallback_types:
                raise UnsupportedFeatureError(
                    f"fallback_type must be one of: str, dict, bytes, or None. Got: {self.fallback_type}"
                )


# %% ---- Converter ------------------------------------------------------------------
class PydanticConverter(BaseConverter):
    """Convert a yads `YadsSpec` into a Pydantic `BaseModel` class.

    The converter maps each yads column to a Pydantic field and assembles a
    `BaseModel` class. Complex types such as arrays, structs, and maps are
    recursively converted to their Pydantic equivalents.

    Notes:
        - Complex types (Array, Struct, Map) are converted to their Pydantic
          equivalents using nested models and typing constructs.
        - Geometry and Geography types are not supported and raise
          `UnsupportedFeatureError` unless in coerce mode.
    """

    def __init__(self, config: PydanticConverterConfig | None = None) -> None:
        """Initialize the PydanticConverter.

        Args:
            config: Configuration object. If None, uses default PydanticConverterConfig.
        """
        self.config: PydanticConverterConfig = config or PydanticConverterConfig()
        super().__init__(self.config)

    @requires_dependency("pydantic", min_version="2.0.0", import_name="pydantic")
    def convert(
        self,
        spec: yspec.YadsSpec,
        *,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> Type[BaseModel]:
        """Convert a yads `YadsSpec` into a Pydantic `BaseModel` class.

        Args:
            spec: The yads spec as a `YadsSpec` object.
            mode: Optional conversion mode override for this call. When not
                provided, the converter's configured mode is used. If provided:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid model and emit warnings.

        Returns:
            A Pydantic `BaseModel` class with fields mapped from the spec columns.
        """
        from pydantic import create_model, ConfigDict  # type: ignore[import-untyped]

        model_name: str = self.config.model_name or spec.name.replace(".", "_")
        model_config: dict[str, Any] = self.config.model_config or {}

        fields: dict[str, Any] = {}
        with self.conversion_context(mode=mode):
            self._validate_column_filters(spec)
            for col in self._filter_columns(spec):
                with self.conversion_context(field=col.name):
                    field_type, field_info = self._convert_field_with_overrides(col)

                    # Pydantic expects (annotation, FieldInfo) for dynamic models
                    fields[col.name] = (field_type, field_info)

        config_dict: ConfigDict | None = None
        if model_config:
            config_dict = cast(ConfigDict, model_config)

        model = create_model(
            model_name,
            __config__=config_dict,
            **fields,
        )

        return model

    # %% ---- Type conversion ---------------------------------------------------------
    @singledispatchmethod
    def _convert_type(self, yads_type: ytypes.YadsType) -> tuple[Any, dict[str, Any]]:
        # Fallback for currently unsupported:
        # - Geometry
        # - Geography
        # - Tensor
        fallback_type: Any = self.raise_or_coerce(yads_type)
        return fallback_type, {}

    @_convert_type.register(ytypes.String)
    def _(self, yads_type: ytypes.String) -> tuple[Any, dict[str, Any]]:
        params: dict[str, Any] = {}
        if yads_type.length:
            params["max_length"] = yads_type.length
        return str, params

    @_convert_type.register(ytypes.Integer)
    def _(self, yads_type: ytypes.Integer) -> tuple[Any, dict[str, Any]]:
        params: dict[str, Any] = {}
        if yads_type.bits:
            if yads_type.signed:
                min_val = -(2 ** (yads_type.bits - 1))
                max_val = 2 ** (yads_type.bits - 1) - 1
            else:  # unsigned
                min_val = 0
                max_val = 2**yads_type.bits - 1
            params["ge"] = min_val
            params["le"] = max_val
        else:
            # Unsigned without bit width: enforce non-negative only.
            if not yads_type.signed:
                params["ge"] = 0
        return int, params

    @_convert_type.register(ytypes.Float)
    def _(self, yads_type: ytypes.Float) -> tuple[Any, dict[str, Any]]:
        # Python's float is typically 64-bit; emit warning when a narrower
        # bit-width is requested, since precision cannot be enforced.
        if yads_type.bits is not None and yads_type.bits != 64:
            # Use raise_or_coerce for consistent warning/error handling
            self.raise_or_coerce(
                coerce_type=float,
                error_msg=(
                    f"Float(bits={yads_type.bits}) cannot be represented exactly"
                    f" in Pydantic; Python float is 64-bit for '{self._field_context}'."
                ),
            )
        return float, {}

    @_convert_type.register(ytypes.Decimal)
    def _(self, yads_type: ytypes.Decimal) -> tuple[Any, dict[str, Any]]:
        params: dict[str, Any] = {}
        if yads_type.precision is not None and self._supports_decimal_constraints():
            params["max_digits"] = yads_type.precision
            params["decimal_places"] = yads_type.scale
        elif yads_type.precision is not None and not self._supports_decimal_constraints():
            # Decimal constraints not supported in this Pydantic version
            # Use raise_or_coerce to emit warning, but continue with PythonDecimal
            self.raise_or_coerce(
                coerce_type=PythonDecimal,
                error_msg=(
                    f"Decimal precision and scale constraints require Pydantic >= 2.8.0"
                    f" for '{self._field_context}'. "
                    f"Found version {_get_installed_version('pydantic') or 'unknown'}."
                ),
            )
        return PythonDecimal, params

    @_convert_type.register(ytypes.Boolean)
    def _(self, yads_type: ytypes.Boolean) -> tuple[Any, dict[str, Any]]:
        return bool, {}

    @_convert_type.register(ytypes.Binary)
    def _(self, yads_type: ytypes.Binary) -> tuple[Any, dict[str, Any]]:
        params: dict[str, Any] = {}
        if yads_type.length:
            params["min_length"] = yads_type.length
            params["max_length"] = yads_type.length
        return bytes, params

    @_convert_type.register(ytypes.Date)
    def _(self, yads_type: ytypes.Date) -> tuple[Any, dict[str, Any]]:
        # Ignore bit-width parameter
        if yads_type.bits is not None:
            self.raise_or_coerce(
                coerce_type=date,
                error_msg=(
                    f"{yads_type} cannot be represented in Pydantic; "
                    f"bits constraint will be lost for '{self._field_context}'."
                ),
            )
        return date, {}

    @_convert_type.register(ytypes.Time)
    def _(self, yads_type: ytypes.Time) -> tuple[Any, dict[str, Any]]:
        # Ignore bit-width parameter
        # Ignore unit parameter
        if yads_type.bits is not None or yads_type.unit is not None:
            self.raise_or_coerce(
                coerce_type=time,
                error_msg=(
                    f"{yads_type} cannot be represented in Pydantic; "
                    f"bits and/or unit constraints will be lost for '{self._field_context}'."
                ),
            )
        return time, {}

    @_convert_type.register(ytypes.Timestamp)
    def _(self, yads_type: ytypes.Timestamp) -> tuple[Any, dict[str, Any]]:
        # Ignore unit parameter
        if yads_type.unit is not None:
            self.raise_or_coerce(
                coerce_type=datetime,
                error_msg=(
                    f"{yads_type} cannot be represented in Pydantic; "
                    f"unit constraint will be lost for '{self._field_context}'."
                ),
            )
        return datetime, {}

    @_convert_type.register(ytypes.TimestampTZ)
    def _(self, yads_type: ytypes.TimestampTZ) -> tuple[Any, dict[str, Any]]:
        # Ignore unit parameter
        # Ignore tz parameter
        if yads_type.unit is not None or yads_type.tz is not None:
            self.raise_or_coerce(
                coerce_type=datetime,
                error_msg=(
                    f"{yads_type} cannot be represented in Pydantic; "
                    f"unit and/or tz constraints will be lost for '{self._field_context}'."
                ),
            )
        return datetime, {}

    @_convert_type.register(ytypes.TimestampLTZ)
    def _(self, yads_type: ytypes.TimestampLTZ) -> tuple[Any, dict[str, Any]]:
        # Ignore unit parameter
        if yads_type.unit is not None:
            self.raise_or_coerce(
                coerce_type=datetime,
                error_msg=(
                    f"{yads_type} cannot be represented in Pydantic; "
                    f"unit constraint will be lost for '{self._field_context}'."
                ),
            )
        return datetime, {}

    @_convert_type.register(ytypes.TimestampNTZ)
    def _(self, yads_type: ytypes.TimestampNTZ) -> tuple[Any, dict[str, Any]]:
        # Ignore unit parameter
        if yads_type.unit is not None:
            self.raise_or_coerce(
                coerce_type=datetime,
                error_msg=(
                    f"{yads_type} cannot be represented in Pydantic; "
                    f"unit constraint will be lost for '{self._field_context}'."
                ),
            )
        return datetime, {}

    @_convert_type.register(ytypes.Duration)
    def _(self, yads_type: ytypes.Duration) -> tuple[Any, dict[str, Any]]:
        # Ignore unit parameter
        if yads_type.unit is not None:
            self.raise_or_coerce(
                coerce_type=timedelta,
                error_msg=(
                    f"{yads_type} cannot be represented in Pydantic; "
                    f"unit constraint will be lost for '{self._field_context}'."
                ),
            )
        return timedelta, {}

    @_convert_type.register(ytypes.Interval)
    def _(self, yads_type: ytypes.Interval) -> tuple[Any, dict[str, Any]]:
        from pydantic import Field, create_model  # type: ignore[import-untyped]

        # Represent as a structured Month-Day-Nano interval, matching PyArrow's
        # month_day_nano_interval layout: (months, days, nanoseconds)
        interval_model_name = self._nested_model_name("MonthDayNanoInterval")
        months_field = (int, Field(default=...))
        days_field = (int, Field(default=...))
        nanos_field = (int, Field(default=...))
        interval_model = create_model(
            interval_model_name,
            months=months_field,
            days=days_field,
            nanoseconds=nanos_field,
        )
        return interval_model, {}

    @_convert_type.register(ytypes.Array)
    def _(self, yads_type: ytypes.Array) -> tuple[Any, dict[str, Any]]:
        element_type, _ = self._convert_type(yads_type.element)
        list_type = list[element_type]  # type: ignore[valid-type]

        params: dict[str, Any] = {}
        if yads_type.size:
            params["min_length"] = yads_type.size
            params["max_length"] = yads_type.size

        return list_type, params

    @_convert_type.register(ytypes.Struct)
    def _(self, yads_type: ytypes.Struct) -> tuple[Any, dict[str, Any]]:
        from pydantic import create_model  # type: ignore[import-untyped]

        # Create nested model for struct
        nested_fields = {}
        for yads_field in yads_type.fields:
            with self.conversion_context(field=yads_field.name):
                field_type, field_info = self._convert_field(yads_field)
                nested_fields[yads_field.name] = (field_type, field_info)

        # Create nested model class
        struct_model_name = self._nested_model_name(yads_type.__class__.__name__)
        # Preserve FieldInfo for nested fields
        nested_kwargs: dict[str, Any] = {
            name: (ftype, finfo) for name, (ftype, finfo) in nested_fields.items()
        }
        nested_model = create_model(struct_model_name, **nested_kwargs)

        return nested_model, {}

    @_convert_type.register(ytypes.Map)
    def _(self, yads_type: ytypes.Map) -> tuple[Any, dict[str, Any]]:
        key_type, _ = self._convert_type(yads_type.key)
        value_type, _ = self._convert_type(yads_type.value)

        dict_type = dict[key_type, value_type]  # type: ignore[valid-type]

        if yads_type.keys_sorted:
            self.raise_or_coerce(
                coerce_type=dict_type,
                error_msg=(
                    f"{yads_type} cannot be represented in Pydantic; "
                    f"keys_sorted parameter will be lost for '{self._field_context}'."
                ),
            )
        return dict_type, {}

    @_convert_type.register(ytypes.JSON)
    def _(self, yads_type: ytypes.JSON) -> tuple[Any, dict[str, Any]]:
        # Map to dict for JSON data
        return dict, {}

    @_convert_type.register(ytypes.UUID)
    def _(self, yads_type: ytypes.UUID) -> tuple[Any, dict[str, Any]]:
        return PythonUUID, {}

    @_convert_type.register(ytypes.Void)
    def _(self, yads_type: ytypes.Void) -> tuple[Any, dict[str, Any]]:
        # Represent a NULL/VOID value
        return type(None), {"default": None}

    @_convert_type.register(ytypes.Variant)
    def _(self, yads_type: ytypes.Variant) -> tuple[Any, dict[str, Any]]:
        return Any, {}

    def _convert_field(self, field: yspec.Field) -> tuple[Any, FieldInfo]:
        from pydantic import Field  # type: ignore[import-untyped]

        field_type, field_params = self._convert_type(field.type)

        if field.is_nullable:
            field_type = Optional[field_type]

        if field.description:
            field_params["description"] = field.description

        json_schema_extra: dict[str, Any] = {}
        if field.metadata:
            json_schema_extra["metadata"] = field.metadata

        for constraint in field.constraints:
            field_params, json_schema_extra = self._apply_constraint(
                constraint, field_params, json_schema_extra
            )

        if json_schema_extra:
            # Wrap in "yads" key to avoid collisions
            field_params["json_schema_extra"] = {"yads": json_schema_extra}

        if "default" not in field_params:
            field_params["default"] = ...

        field_info: FieldInfo = Field(**field_params)  # type: ignore[assignment]
        return field_type, field_info

    def _convert_field_default(self, field: yspec.Field) -> tuple[Any, FieldInfo]:
        return self._convert_field(field)

    def _apply_column_override(self, field: yspec.Field) -> tuple[Any, FieldInfo]:
        from pydantic.fields import FieldInfo  # type: ignore[import-untyped]

        result = self.config.column_overrides[field.name](field, self)
        if not (isinstance(result, tuple) and len(result) == 2):
            raise UnsupportedFeatureError(
                "Pydantic column override must return (annotation, FieldInfo)."
            )
        annotation, field_info = result
        if not isinstance(field_info, FieldInfo):
            raise UnsupportedFeatureError(
                "Pydantic column override second element must be a FieldInfo."
            )
        return annotation, field_info

    # %% ---- Constraint conversion ---------------------------------------------------
    @singledispatchmethod
    def _apply_constraint(
        self,
        constraint: ColumnConstraint,
        field_params: dict[str, Any],
        json_schema_extra: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        # Fallback for unknown constraints does nothing
        return field_params, json_schema_extra

    @_apply_constraint.register(NotNullConstraint)
    def _(
        self,
        constraint: NotNullConstraint,
        field_params: dict[str, Any],
        json_schema_extra: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        # Nullability is handled by default=...
        return field_params, json_schema_extra

    @_apply_constraint.register(PrimaryKeyConstraint)
    def _(
        self,
        constraint: PrimaryKeyConstraint,
        field_params: dict[str, Any],
        json_schema_extra: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        # Capture primary key metadata in schema extras
        json_schema_extra["primary_key"] = True
        return field_params, json_schema_extra

    @_apply_constraint.register(DefaultConstraint)
    def _(
        self,
        constraint: DefaultConstraint,
        field_params: dict[str, Any],
        json_schema_extra: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        field_params["default"] = constraint.value
        return field_params, json_schema_extra

    @_apply_constraint.register(ForeignKeyConstraint)
    def _(
        self,
        constraint: ForeignKeyConstraint,
        field_params: dict[str, Any],
        json_schema_extra: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        # Capture foreign key metadata in schema extras
        fk_metadata: dict[str, Any] = {
            "table": constraint.references.table,
        }
        if constraint.references.columns:
            fk_metadata["columns"] = list(constraint.references.columns)
        if constraint.name:
            fk_metadata["name"] = constraint.name
        json_schema_extra["foreign_key"] = fk_metadata
        return field_params, json_schema_extra

    @_apply_constraint.register(IdentityConstraint)
    def _(
        self,
        constraint: IdentityConstraint,
        field_params: dict[str, Any],
        json_schema_extra: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        # Capture identity/auto-increment metadata in schema extras
        identity_metadata: dict[str, Any] = {"always": constraint.always}
        if constraint.start is not None:
            identity_metadata["start"] = constraint.start
        if constraint.increment is not None:
            identity_metadata["increment"] = constraint.increment
        json_schema_extra["identity"] = identity_metadata
        return field_params, json_schema_extra

    # %% ---- Helpers -----------------------------------------------------------------
    @staticmethod
    @lru_cache(maxsize=1)
    def _supports_decimal_constraints() -> bool:
        """Check if the installed Pydantic version supports Decimal constraints.

        Decimal max_digits and decimal_places constraints were introduced in
        Pydantic 2.8.0.
        """
        pydantic_version = _get_installed_version("pydantic")
        if pydantic_version is None:
            return False
        return _meets_min_version(pydantic_version, "2.8.0")

    def _nested_model_name(self, suffix: str) -> str:
        base = self.config.model_name or "Model"
        return f"{base}_{suffix}"
