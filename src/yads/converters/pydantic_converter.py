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
from typing import Any, Literal, Optional, Type, cast
from uuid import UUID as PythonUUID

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
from .base import BaseConverter

from .. import spec
from .. import types as ytypes


class PydanticConverter(BaseConverter):
    """Convert a yads `YadsSpec` into a Pydantic `BaseModel` class.

    The converter maps each yads column to a Pydantic field and assembles a
    `BaseModel` class. Complex types such as arrays, structs, and maps are
    recursively converted to their Pydantic equivalents.

    The following options are supported via `**kwargs` to customize
    conversion:

    - model_name: Custom name for the generated model class. If None, uses
      the spec name. Default None.
    - model_config: Dictionary of Pydantic model configuration options.
      Default empty dict.

    Notes:
        - Complex types (Array, Struct, Map) are converted to their Pydantic
          equivalents using nested models and typing constructs.
        - Geometry, Geography, and Variant types are not supported and raise
          `UnsupportedFeatureError` unless in coerce mode.
    """

    def __init__(self, mode: Literal["raise", "coerce"] = "coerce") -> None:
        """Initialize the PydanticConverter.

        Args:
            mode: "raise" or "coerce". When "coerce", unsupported or
                incompatible constructs are coerced to a valid Pydantic
                type with a warning. Defaults to "coerce".
        """
        super().__init__(mode=mode)

    def convert(self, spec: spec.YadsSpec, **kwargs: Any) -> Type[BaseModel]:
        """Convert a yads `YadsSpec` into a Pydantic `BaseModel` class.

        Args:
            spec: The yads spec as a `YadsSpec` object.
            **kwargs: Optional conversion modifiers:
                model_name: Custom name for the generated model class.
                    If None, uses the spec name. Defaults to None.
                model_config: Dictionary of Pydantic model configuration
                    options. Defaults to empty dict.
                mode: "raise" or "coerce". When "coerce", unsupported or
                    incompatible constructs are coerced to a valid Pydantic
                    type with a warning. Defaults to "coerce".

        Returns:
            A Pydantic `BaseModel` class with fields mapped from the spec columns.
        """
        mode_override = kwargs.get("mode", None)
        self._model_name: str = kwargs.get("model_name") or spec.name.replace(".", "_")
        self._model_config: dict[str, Any] = kwargs.get("model_config", {})

        fields: dict[str, Any] = {}
        # Set mode for this conversion call
        with self.conversion_context(mode=mode_override):
            for col in spec.columns:
                try:
                    # Set field context during conversion
                    with self.conversion_context(field=col.name):
                        field_type, field_info = self._convert_field(col)
                        # Pydantic expects (annotation, FieldInfo) for dynamic models
                        fields[col.name] = (field_type, field_info)
                except UnsupportedFeatureError:
                    if self._mode == "coerce":
                        validation_warning(
                            message=(
                                f"Data type '{type(col.type).__name__.upper()}' is not supported"
                                f" for column '{col.name}'. The field will be represented as an"
                                f" object in the model."
                            ),
                            filename="yads.converters.pydantic_converter",
                            module=__name__,
                        )
                        fields[col.name] = (
                            dict,
                            Field(default=...),
                        )  # coerce to object (dict)
                        continue
                    raise

        model = create_model(
            self._model_name,
            **fields,
        )

        if self._model_config:
            # In Pydantic v2, set BaseModel.model_config (ConfigDict) for dynamic models.
            # We cast here to avoid over-constraining keys at type-check time.
            setattr(model, "model_config", cast(ConfigDict, self._model_config))

        return model

    # Type conversion
    @singledispatchmethod
    def _convert_type(self, yads_type: ytypes.YadsType) -> tuple[Any, FieldInfo]:
        # Unsupported logical types will be handled by the caller depending on mode.
        raise UnsupportedFeatureError(
            f"PydanticConverter does not support type: {type(yads_type).__name__}."
        )

    @_convert_type.register(ytypes.String)
    def _(self, yads_type: ytypes.String) -> tuple[Any, FieldInfo]:
        if yads_type.length:
            field_info = Field(
                default=...,
                max_length=yads_type.length,
            )
        else:
            field_info = Field(default=...)
        return str, field_info

    @_convert_type.register(ytypes.Integer)
    def _(self, yads_type: ytypes.Integer) -> tuple[Any, FieldInfo]:
        if yads_type.bits:
            if yads_type.signed:
                if yads_type.bits == 8:
                    field_info = Field(default=..., ge=-(2**7), le=2**7 - 1)
                elif yads_type.bits == 16:
                    field_info = Field(default=..., ge=-(2**15), le=2**15 - 1)
                elif yads_type.bits == 32:
                    field_info = Field(default=..., ge=-(2**31), le=2**31 - 1)
                elif yads_type.bits == 64:
                    field_info = Field(default=..., ge=-(2**63), le=2**63 - 1)
            else:  # unsigned
                if yads_type.bits == 8:
                    field_info = Field(default=..., ge=0, le=2**8 - 1)
                elif yads_type.bits == 16:
                    field_info = Field(default=..., ge=0, le=2**16 - 1)
                elif yads_type.bits == 32:
                    field_info = Field(default=..., ge=0, le=2**32 - 1)
                elif yads_type.bits == 64:
                    field_info = Field(default=..., ge=0, le=2**64 - 1)
        else:
            # Unsigned without bit width: enforce non-negative only.
            if not yads_type.signed:
                field_info = Field(default=..., ge=0)
            else:
                field_info = Field(default=...)
        return int, field_info

    @_convert_type.register(ytypes.Float)
    def _(self, yads_type: ytypes.Float) -> tuple[Any, FieldInfo]:
        # Python's float is typically 64-bit; emit warning when a narrower
        # bit-width is requested, since precision cannot be enforced.
        if yads_type.bits is not None and yads_type.bits != 64:
            if self._mode == "coerce":
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
        field_info = Field(default=...)
        return float, field_info

    @_convert_type.register(ytypes.Decimal)
    def _(self, yads_type: ytypes.Decimal) -> tuple[Any, FieldInfo]:
        if yads_type.precision is not None:
            field_info = Field(
                default=...,
                max_digits=yads_type.precision,
                decimal_places=yads_type.scale,
            )
        else:
            field_info = Field(default=...)
        return PythonDecimal, field_info

    @_convert_type.register(ytypes.Boolean)
    def _(self, yads_type: ytypes.Boolean) -> tuple[Any, FieldInfo]:
        field_info = Field(default=...)
        return bool, field_info

    @_convert_type.register(ytypes.Binary)
    def _(self, yads_type: ytypes.Binary) -> tuple[Any, FieldInfo]:
        if yads_type.length:
            field_info = Field(
                default=...,
                min_length=yads_type.length,
                max_length=yads_type.length,
            )
        else:
            field_info = Field(default=...)
        return bytes, field_info

    @_convert_type.register(ytypes.Date)
    def _(self, yads_type: ytypes.Date) -> tuple[Any, FieldInfo]:
        # Ignore bit-width parameter
        field_info = Field(default=...)
        return date, field_info

    @_convert_type.register(ytypes.Time)
    def _(self, yads_type: ytypes.Time) -> tuple[Any, FieldInfo]:
        # Ignore bit-width parameter
        # Ignore unit parameter
        field_info = Field(default=...)
        return time, field_info

    @_convert_type.register(ytypes.Timestamp)
    def _(self, yads_type: ytypes.Timestamp) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        field_info = Field(default=...)
        return datetime, field_info

    @_convert_type.register(ytypes.TimestampTZ)
    def _(self, yads_type: ytypes.TimestampTZ) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        # Ignore tz parameter
        field_info = Field(default=...)
        return datetime, field_info

    @_convert_type.register(ytypes.TimestampLTZ)
    def _(self, yads_type: ytypes.TimestampLTZ) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        field_info = Field(default=...)
        return datetime, field_info

    @_convert_type.register(ytypes.TimestampNTZ)
    def _(self, yads_type: ytypes.TimestampNTZ) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        field_info = Field(default=...)
        return datetime, field_info

    @_convert_type.register(ytypes.Duration)
    def _(self, yads_type: ytypes.Duration) -> tuple[Any, FieldInfo]:
        # Ignore unit parameter
        field_info = Field(default=...)
        return timedelta, field_info

    @_convert_type.register(ytypes.Interval)
    def _(self, yads_type: ytypes.Interval) -> tuple[Any, FieldInfo]:
        # Represent as a structured Month-Day-Nano interval, matching PyArrow's
        # month_day_nano_interval layout: (months, days, nanoseconds)
        interval_model_name = f"{self._model_name}_MonthDayNanoInterval"
        months_field = (int, Field(default=...))
        days_field = (int, Field(default=...))
        nanos_field = (int, Field(default=...))
        interval_model = create_model(
            interval_model_name,
            months=months_field,
            days=days_field,
            nanoseconds=nanos_field,
        )
        field_info = Field(default=...)
        return interval_model, field_info

    @_convert_type.register(ytypes.Array)
    def _(self, yads_type: ytypes.Array) -> tuple[Any, FieldInfo]:
        element_type, _ = self._convert_type(yads_type.element)
        list_type = list[element_type]  # type: ignore[valid-type]

        if yads_type.size:
            field_info = Field(
                default=...,
                min_length=yads_type.size,
                max_length=yads_type.size,
            )
        else:
            field_info = Field(default=...)

        return list_type, field_info

    @_convert_type.register(ytypes.Struct)
    def _(self, yads_type: ytypes.Struct) -> tuple[Any, FieldInfo]:
        # Create nested model for struct
        nested_fields = {}
        for field in yads_type.fields:
            with self.conversion_context(field=field.name):
                field_type, field_info = self._convert_field(field)
                nested_fields[field.name] = (field_type, field_info)

        # Create nested model class
        struct_model_name = f"{self._model_name}_{yads_type.__class__.__name__}"
        # Preserve FieldInfo for nested fields
        nested_kwargs: dict[str, Any] = {
            name: (ftype, finfo) for name, (ftype, finfo) in nested_fields.items()
        }
        nested_model = create_model(struct_model_name, **nested_kwargs)

        field_info = Field(default=...)
        return nested_model, field_info

    @_convert_type.register(ytypes.Map)
    def _(self, yads_type: ytypes.Map) -> tuple[Any, FieldInfo]:
        key_type, _ = self._convert_type(yads_type.key)
        value_type, _ = self._convert_type(yads_type.value)

        dict_type = dict[key_type, value_type]  # type: ignore[valid-type]

        # Ignore keys_sorted parameter
        field_info = Field(default=...)
        return dict_type, field_info

    @_convert_type.register(ytypes.JSON)
    def _(self, yads_type: ytypes.JSON) -> tuple[Any, FieldInfo]:
        # Map to dict for JSON data
        field_info = Field(default=...)
        return dict, field_info

    @_convert_type.register(ytypes.Geometry)
    def _(self, yads_type: ytypes.Geometry) -> tuple[Any, FieldInfo]:
        if self._mode == "coerce":
            validation_warning(
                message=(
                    f"PydanticConverter does not support type: {type(yads_type).__name__}"
                    f" for column '{self._current_field_name or '<unknown>'}'. The data type"
                    f" will be represented as OBJECT."
                ),
                filename="yads.converters.pydantic_converter",
                module=__name__,
            )
            field_info = Field(default=...)
            return dict, field_info
        raise UnsupportedFeatureError(
            f"PydanticConverter does not support type: {type(yads_type).__name__}."
        )

    @_convert_type.register(ytypes.Geography)
    def _(self, yads_type: ytypes.Geography) -> tuple[Any, FieldInfo]:
        if self._mode == "coerce":
            validation_warning(
                message=(
                    f"PydanticConverter does not support type: {type(yads_type).__name__}"
                    f" for column '{self._current_field_name or '<unknown>'}'. The data type"
                    f" will be represented as OBJECT."
                ),
                filename="yads.converters.pydantic_converter",
                module=__name__,
            )
            field_info = Field(default=...)
            return dict, field_info
        raise UnsupportedFeatureError(
            f"PydanticConverter does not support type: {type(yads_type).__name__}."
        )

    @_convert_type.register(ytypes.UUID)
    def _(self, yads_type: ytypes.UUID) -> tuple[Any, FieldInfo]:
        field_info = Field(default=...)
        return PythonUUID, field_info

    @_convert_type.register(ytypes.Void)
    def _(self, yads_type: ytypes.Void) -> tuple[Any, FieldInfo]:
        # Represent a NULL/VOID value
        field_info = cast(FieldInfo, Field(default=None))
        return type(None), field_info

    @_convert_type.register(ytypes.Variant)
    def _(self, yads_type: ytypes.Variant) -> tuple[Any, FieldInfo]:
        field_info = Field(default=...)
        return Any, field_info

    # Helpers
    def _convert_field(self, field: spec.Field) -> tuple[Any, FieldInfo]:
        field_type, field_info = self._convert_type(field.type)

        if not field.is_nullable:
            pass  # Not null handled by default=...
        else:
            field_type = Optional[field_type]  # type: ignore[assignment]

        if field.description:
            field_info.description = field.description

        for constraint in field.constraints:
            field_info = self._apply_constraint(constraint, field_info)

        return field_type, field_info

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
