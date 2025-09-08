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

from functools import singledispatchmethod
from datetime import date, datetime, time, timedelta
from decimal import Decimal as PythonDecimal
from typing import Any, Callable, Literal, Optional, Type, cast, Mapping
from uuid import UUID as PythonUUID
from dataclasses import dataclass, field
from types import MappingProxyType

from pydantic import BaseModel, Field, create_model, ConfigDict  # type: ignore[import-untyped]
from pydantic.fields import FieldInfo  # type: ignore[import-untyped]

from ..constraints import (
    ColumnConstraint,
    DefaultConstraint,
    ForeignKeyConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)
from ..exceptions import UnsupportedFeatureError, validation_warning
from .base import BaseConverter, BaseConverterConfig

from .. import spec
from .. import types as ytypes


# %% ---- Configuration --------------------------------------------------------------
@dataclass(frozen=True)
class PydanticConverterConfig(BaseConverterConfig[tuple[Any, FieldInfo]]):
    """Configuration for PydanticConverter.

    Args:
        model_name: Custom name for the generated model class. If None, uses
            the spec name. Defaults to None.
        model_config: Dictionary of Pydantic model configuration options.
            Defaults to empty dict.
        fallback_type: Python type to use for unsupported types in coerce mode.
            Must be one of: str, dict, bytes. Defaults to str.
    """

    model_name: str | None = None
    model_config: dict[str, Any] | None = None
    fallback_type: type = str
    column_overrides: Mapping[
        str, Callable[[spec.Field, PydanticConverter], tuple[Any, FieldInfo]]
    ] = field(default_factory=lambda: MappingProxyType({}))  # type: ignore[assignment]

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()

        # Validate fallback_type
        valid_fallback_types = {str, dict, bytes}
        if self.fallback_type not in valid_fallback_types:
            raise UnsupportedFeatureError(
                f"fallback_type must be one of: str, dict, bytes. Got: {self.fallback_type}"
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

    def convert(
        self,
        spec: spec.YadsSpec,
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
        model_name: str = self.config.model_name or spec.name.replace(".", "_")
        model_config: dict[str, Any] = self.config.model_config or {}

        fields: dict[str, Any] = {}
        # Set mode for this conversion call
        with self.conversion_context(mode=mode):
            self._validate_column_filters(spec)
            for col in self._filter_columns(spec):
                try:
                    # Set field context during conversion
                    with self.conversion_context(field=col.name):
                        # Use centralized override resolution
                        field_type, field_info = self._convert_field_with_overrides(col)

                        # Pydantic expects (annotation, FieldInfo) for dynamic models
                        fields[col.name] = (field_type, field_info)
                except UnsupportedFeatureError:
                    if self.config.mode == "coerce":
                        fallback_name = self.config.fallback_type.__name__.upper()
                        validation_warning(
                            message=(
                                f"Data type '{type(col.type).__name__.upper()}' is not supported"
                                f" for column '{col.name}'. The field will be represented as"
                                f" {fallback_name} in the model."
                            ),
                            filename="yads.converters.pydantic_converter",
                            module=__name__,
                        )
                        fields[col.name] = (
                            self.config.fallback_type,
                            self._create_fallback_field_info(col),
                        )
                        continue
                    raise

        model = create_model(
            model_name,
            **fields,
        )

        if model_config:
            # In Pydantic v2, set BaseModel.model_config (ConfigDict) for dynamic models.
            # We cast here to avoid over-constraining keys at type-check time.
            setattr(model, "model_config", cast(ConfigDict, model_config))

        return model

    # %% ---- Type conversion ---------------------------------------------------------
    @singledispatchmethod
    def _convert_type(self, yads_type: ytypes.YadsType) -> tuple[Any, FieldInfo]:
        # Unsupported logical types will be handled by the caller depending on mode.
        raise UnsupportedFeatureError(
            f"PydanticConverter does not support type: {type(yads_type).__name__}."
        )

    @_convert_type.register(ytypes.String)
    def _(self, yads_type: ytypes.String) -> tuple[Any, FieldInfo]:
        if yads_type.length:
            field_info = self.required(max_length=yads_type.length)
        else:
            field_info = self.required()
        return str, field_info

    @_convert_type.register(ytypes.Integer)
    def _(self, yads_type: ytypes.Integer) -> tuple[Any, FieldInfo]:
        if yads_type.bits:
            if yads_type.signed:
                if yads_type.bits == 8:
                    field_info = self.required(ge=-(2 ** (8 - 1)), le=2 ** (8 - 1) - 1)
                elif yads_type.bits == 16:
                    field_info = self.required(ge=-(2 ** (16 - 1)), le=2 ** (16 - 1) - 1)
                elif yads_type.bits == 32:
                    field_info = self.required(ge=-(2 ** (32 - 1)), le=2 ** (32 - 1) - 1)
                elif yads_type.bits == 64:
                    field_info = self.required(ge=-(2 ** (64 - 1)), le=2 ** (64 - 1) - 1)
            else:  # unsigned
                if yads_type.bits == 8:
                    field_info = self.required(ge=0, le=2**8 - 1)
                elif yads_type.bits == 16:
                    field_info = self.required(ge=0, le=2**16 - 1)
                elif yads_type.bits == 32:
                    field_info = self.required(ge=0, le=2**32 - 1)
                elif yads_type.bits == 64:
                    field_info = self.required(ge=0, le=2**64 - 1)
        else:
            # Unsigned without bit width: enforce non-negative only.
            if not yads_type.signed:
                field_info = self.required(ge=0)
            else:
                field_info = self.required()
        return int, field_info

    @_convert_type.register(ytypes.Float)
    def _(self, yads_type: ytypes.Float) -> tuple[Any, FieldInfo]:
        # Python's float is typically 64-bit; emit warning when a narrower
        # bit-width is requested, since precision cannot be enforced.
        if yads_type.bits is not None and yads_type.bits != 64:
            if self.config.mode == "coerce":
                validation_warning(
                    message=(
                        f"Float(bits={yads_type.bits}) cannot be represented exactly"
                        f" in Pydantic; Python float is 64-bit. The data type will be"
                        f" represented as 64-bit float."
                    ),
                    filename="yads.converters.pydantic_converter",
                    module=__name__,
                )
            else:
                raise UnsupportedFeatureError(
                    f"Float(bits={yads_type.bits}) cannot be represented exactly"
                    f" in Pydantic; Python float is 64-bit."
                )
        field_info = self.required()
        return float, field_info

    @_convert_type.register(ytypes.Decimal)
    def _(self, yads_type: ytypes.Decimal) -> tuple[Any, FieldInfo]:
        if yads_type.precision is not None:
            field_info = self.required(
                max_digits=yads_type.precision,
                decimal_places=yads_type.scale,
            )
        else:
            field_info = self.required()
        return PythonDecimal, field_info

    @_convert_type.register(ytypes.Boolean)
    def _(self, yads_type: ytypes.Boolean) -> tuple[Any, FieldInfo]:
        field_info = self.required()
        return bool, field_info

    @_convert_type.register(ytypes.Binary)
    def _(self, yads_type: ytypes.Binary) -> tuple[Any, FieldInfo]:
        if yads_type.length:
            field_info = self.required(
                min_length=yads_type.length,
                max_length=yads_type.length,
            )
        else:
            field_info = self.required()
        return bytes, field_info

    @_convert_type.register(ytypes.Date)
    def _(self, yads_type: ytypes.Date) -> tuple[Any, FieldInfo]:
        # Ignore bit-width parameter
        field_info = self.required()
        return date, field_info

    @_convert_type.register(ytypes.Time)
    def _(self, yads_type: ytypes.Time) -> tuple[Any, FieldInfo]:
        # Ignore bit-width parameter
        # Ignore unit parameter
        field_info = self.required()
        return time, field_info

    @_convert_type.register(ytypes.Timestamp)
    def _(self, yads_type: ytypes.Timestamp) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        field_info = self.required()
        return datetime, field_info

    @_convert_type.register(ytypes.TimestampTZ)
    def _(self, yads_type: ytypes.TimestampTZ) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        # Ignore tz parameter
        field_info = self.required()
        return datetime, field_info

    @_convert_type.register(ytypes.TimestampLTZ)
    def _(self, yads_type: ytypes.TimestampLTZ) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        field_info = self.required()
        return datetime, field_info

    @_convert_type.register(ytypes.TimestampNTZ)
    def _(self, yads_type: ytypes.TimestampNTZ) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        field_info = self.required()
        return datetime, field_info

    @_convert_type.register(ytypes.Duration)
    def _(self, yads_type: ytypes.Duration) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        field_info = self.required()
        return timedelta, field_info

    @_convert_type.register(ytypes.Interval)
    def _(self, yads_type: ytypes.Interval) -> tuple[Any, FieldInfo]:
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
        field_info = self.required()
        return interval_model, field_info

    @_convert_type.register(ytypes.Array)
    def _(self, yads_type: ytypes.Array) -> tuple[Any, FieldInfo]:
        element_type, _ = self._convert_type(yads_type.element)
        list_type = list[element_type]  # type: ignore[valid-type]

        if yads_type.size:
            field_info = self.required(
                min_length=yads_type.size,
                max_length=yads_type.size,
            )
        else:
            field_info = self.required()

        return list_type, field_info

    @_convert_type.register(ytypes.Struct)
    def _(self, yads_type: ytypes.Struct) -> tuple[Any, FieldInfo]:
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

        field_info = self.required()
        return nested_model, field_info

    @_convert_type.register(ytypes.Map)
    def _(self, yads_type: ytypes.Map) -> tuple[Any, FieldInfo]:
        key_type, _ = self._convert_type(yads_type.key)
        value_type, _ = self._convert_type(yads_type.value)

        dict_type = dict[key_type, value_type]  # type: ignore[valid-type]

        # Ignore keys_sorted parameter
        field_info = self.required()
        return dict_type, field_info

    @_convert_type.register(ytypes.JSON)
    def _(self, yads_type: ytypes.JSON) -> tuple[Any, FieldInfo]:
        # Map to dict for JSON data
        field_info = self.required()
        return dict, field_info

    @_convert_type.register(ytypes.Geometry)
    def _(self, yads_type: ytypes.Geometry) -> tuple[Any, FieldInfo]:
        raise UnsupportedFeatureError(
            f"PydanticConverter does not support type: {type(yads_type).__name__}."
        )

    @_convert_type.register(ytypes.Geography)
    def _(self, yads_type: ytypes.Geography) -> tuple[Any, FieldInfo]:
        raise UnsupportedFeatureError(
            f"PydanticConverter does not support type: {type(yads_type).__name__}."
        )

    @_convert_type.register(ytypes.UUID)
    def _(self, yads_type: ytypes.UUID) -> tuple[Any, FieldInfo]:
        field_info = self.required()
        return PythonUUID, field_info

    @_convert_type.register(ytypes.Void)
    def _(self, yads_type: ytypes.Void) -> tuple[Any, FieldInfo]:
        # Represent a NULL/VOID value
        field_info = cast(FieldInfo, Field(default=None))
        return type(None), field_info

    @_convert_type.register(ytypes.Variant)
    def _(self, yads_type: ytypes.Variant) -> tuple[Any, FieldInfo]:
        field_info = self.required()
        return Any, field_info

    def _convert_field(self, field: spec.Field) -> tuple[Any, FieldInfo]:
        field_type, field_info = self._convert_type(field.type)

        if field.is_nullable:
            field_type = Optional[field_type]

        if field.description:
            field_info.description = field.description

        if field.metadata:
            field_info = self._merge_schema_extra(
                field_info, {"metadata": field.metadata}
            )

        for constraint in field.constraints:
            field_info = self._apply_constraint(constraint, field_info)

        return field_type, field_info

    def _convert_field_default(self, field: spec.Field) -> tuple[Any, FieldInfo]:
        return self._convert_field(field)

    def _apply_column_override(self, field: spec.Field) -> tuple[Any, FieldInfo]:
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
        self, constraint: ColumnConstraint, field_info: FieldInfo
    ) -> FieldInfo:
        # Fallback for unknown constraints does nothing
        return field_info

    @_apply_constraint.register(NotNullConstraint)
    def _(self, constraint: NotNullConstraint, field_info: FieldInfo) -> FieldInfo:
        # Nullability is handled by default=...
        return field_info

    @_apply_constraint.register(PrimaryKeyConstraint)
    def _(self, constraint: PrimaryKeyConstraint, field_info: FieldInfo) -> FieldInfo:
        # Capture primary key metadata in schema extras
        return self._merge_schema_extra(field_info, {"primary_key": True})

    @_apply_constraint.register(DefaultConstraint)
    def _(self, constraint: DefaultConstraint, field_info: FieldInfo) -> FieldInfo:
        field_info.default = constraint.value
        return field_info

    @_apply_constraint.register(ForeignKeyConstraint)
    def _(self, constraint: ForeignKeyConstraint, field_info: FieldInfo) -> FieldInfo:
        # Capture foreign key metadata in schema extras
        fk_metadata: dict[str, Any] = {
            "table": constraint.references.table,
        }
        if constraint.references.columns:
            fk_metadata["columns"] = list(constraint.references.columns)
        if constraint.name:
            fk_metadata["name"] = constraint.name
        return self._merge_schema_extra(field_info, {"foreign_key": fk_metadata})

    @_apply_constraint.register(IdentityConstraint)
    def _(self, constraint: IdentityConstraint, field_info: FieldInfo) -> FieldInfo:
        # Capture identity/auto-increment metadata in schema extras
        identity_metadata: dict[str, Any] = {"always": constraint.always}
        if constraint.start is not None:
            identity_metadata["start"] = constraint.start
        if constraint.increment is not None:
            identity_metadata["increment"] = constraint.increment
        return self._merge_schema_extra(field_info, {"identity": identity_metadata})

    # %% ---- Helpers -----------------------------------------------------------------
    @staticmethod
    def required(**kwargs: Any) -> FieldInfo:
        return Field(default=..., **kwargs)

    def _create_fallback_field_info(self, field: spec.Field) -> FieldInfo:
        field_info = Field(default=...)
        if field.description:
            field_info.description = field.description
        if field.metadata:
            field_info = self._merge_schema_extra(
                field_info, {"metadata": field.metadata}
            )
        return field_info

    def _nested_model_name(self, suffix: str) -> str:
        base = self.config.model_name or "Model"
        return f"{base}_{suffix}"

    def _merge_schema_extra(
        self, field_info: FieldInfo, updates: dict[str, Any]
    ) -> FieldInfo:
        """Merge yads-specific metadata into json_schema_extra.

        The metadata is stored under the "yads" key to avoid collisions.
        """
        current: dict[str, Any] = {}
        extra = getattr(field_info, "json_schema_extra", None)
        if extra is not None:
            if callable(extra):
                # Cannot merge into a callable
                # Start fresh while preserving callable separately
                current = {}
            else:
                current = dict(cast(dict[str, Any], extra) or {})
        yads_metadata: dict[str, Any] = dict(current.get("yads", {}))
        yads_metadata.update(updates)
        current["yads"] = yads_metadata
        field_info.json_schema_extra = current
        return field_info
