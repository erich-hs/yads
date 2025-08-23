"""PyArrow converter from yads `YadsSpec` to `pyarrow.Schema`.

This module defines the `PyArrowConverter`, responsible for producing a
`pyarrow.Schema` from yads' canonical `YadsSpec`.

Example:
    >>> import yads.types as ytypes
    >>> from yads.spec import Column, YadsSpec
    >>> from yads.converters import PyArrowConverter
    >>> spec = YadsSpec(
    ...     name="catalog.db.table",
    ...     version="0.0.1",
    ...     columns=[
    ...         Column(name="id", type=ytypes.Integer(bits=64)),
    ...         Column(name="name", type=ytypes.String()),
    ...     ],
    ... )
    >>> pa_schema = PyArrowConverter().convert(spec)
    >>> pa_schema.names
    ['id', 'name']
"""

from __future__ import annotations

from functools import singledispatchmethod
import json
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
from ..exceptions import validation_warning

from ..exceptions import UnsupportedFeatureError
from ..spec import Field, YadsSpec
from ..types import (
    YadsType,
    String,
    Integer,
    Float,
    Decimal,
    Boolean,
    Binary,
    Date,
    TimeUnit,
    Time,
    Timestamp,
    TimestampTZ,
    TimestampLTZ,
    TimestampNTZ,
    Duration,
    Interval,
    Array,
    Struct,
    Map,
    JSON,
    UUID,
    Void,
)
from .base import BaseConverter


class PyArrowConverter(BaseConverter):
    """Convert a yads `YadsSpec` into a `pyarrow.Schema`.

    The converter maps each yads column to a `pyarrow.Field` and assembles a
    `pyarrow.Schema`. Complex types such as arrays, structs, and maps are
    recursively converted.

    The following options are supported via `**kwargs` to customize
    conversion:

    - `mode`: Controls validation/coercion behavior for incompatible
      parameter combinations. One of `"raise"` or `"coerce"` (default).
      In `"raise"` mode, incompatible parameters raise
      `UnsupportedFeatureError`. In `"coerce"` mode, the converter attempts
      to coerce to a compatible target (e.g., promote decimal to 256-bit or
      time to 64-bit when units require it). If a logical type is unsupported
      by PyArrow, it is mapped to a canonical placeholder `pa.binary()`.
    - `use_large_string`: If `True`, use `pa.large_string()` for
      `String`. Default `False`.
    - `use_large_binary`: If `True`, use `pa.large_binary()` for
      `Binary(length=None)`. When a fixed `length` is provided, a fixed-size
      `pa.binary(length)` is always used. Default `False`.
    - `use_large_list`: If `True`, use `pa.large_list(element)` for
      variable-length `Array` (i.e., `size is None`). For fixed-size arrays
      (`size` set), `pa.list_(element, list_size=size)` is used. Default
      `False`.

    Notes:
        - Arrow strings are variable-length; any `String.length` hint is
          ignored in the resulting Arrow schema.
        - `Geometry`, `Geography`, and `Variant` are not supported and raise
          `UnsupportedFeatureError`.
    """

    def convert(self, spec: YadsSpec, **kwargs: Any) -> pa.Schema:
        """Convert a yads `YadsSpec` into a `pyarrow.Schema`.

        Args:
            spec: The yads spec as a `YadsSpec` object.
            **kwargs: Optional conversion modifiers:
                mode: `"raise"`, `"coerce"`, or `"ignore"`. Controls how
                    incompatible type parameters are handled and whether
                    unsupported columns are skipped. Defaults to `"raise"`.
                use_large_string: If `True`, maps `String` to
                    `pa.large_string()`. Defaults to `False`.
                use_large_binary: If `True`, maps `Binary(length=None)` to
                    `pa.large_binary()`. Fixed-size binaries always use
                    `pa.binary(length)`. Defaults to `False`.
                use_large_list: If `True`, maps variable-length `Array` to
                    `pa.large_list(element)`. Fixed-size arrays always use
                    `pa.list_(element, list_size)`. Defaults to `False`.

        Returns:
            A `pyarrow.Schema` with fields mapped from the spec columns.
        """
        self._mode: str = kwargs.get("mode", "coerce")
        if self._mode not in {"raise", "coerce"}:
            raise UnsupportedFeatureError("mode must be one of 'raise' or 'coerce'.")
        self._use_large_string: bool = bool(kwargs.get("use_large_string", False))
        self._use_large_binary: bool = bool(kwargs.get("use_large_binary", False))
        self._use_large_list: bool = bool(kwargs.get("use_large_list", False))

        # Track current field name for contextual warnings during type coercions.
        self._current_field_name: str | None = None

        fields: list[pa.Field] = []
        for col in spec.columns:
            try:
                self._current_field_name = col.name
                fields.append(self._convert_field(col))
            except UnsupportedFeatureError:
                if self._mode == "coerce":
                    # Map unsupported logical types to a fallback type
                    validation_warning(
                        message=(
                            f"Data type '{type(col.type).__name__.upper()}' is not supported"
                            f" for column '{col.name}'. The data type will be replaced with pyarrow.binary()."
                        ),
                        filename="yads.converters.pyarrow",
                        module=__name__,
                    )
                    fallback = pa.field(col.name, pa.binary(), nullable=col.is_nullable)
                    fields.append(fallback)
                    continue
                raise
            finally:
                self._current_field_name = None
        # Attach schema-level metadata if present, coercing values to strings
        schema_metadata = self._coerce_metadata(spec.metadata) if spec.metadata else None
        return pa.schema(fields, metadata=schema_metadata)

    # Type conversion
    @singledispatchmethod
    def _convert_type(self, yads_type: YadsType) -> pa.DataType:
        # Unsupported logical types will be handled by the caller depending on mode.
        raise UnsupportedFeatureError(
            f"PyArrowConverter does not support type: {type(yads_type).__name__}."
        )

    @_convert_type.register(String)
    def _(self, yads_type: String) -> pa.DataType:
        # Arrow strings are variable-length. Optionally use large_string.
        return pa.large_string() if self._use_large_string else pa.string()

    @_convert_type.register(Integer)
    def _(self, yads_type: Integer) -> pa.DataType:
        bits = yads_type.bits or 32
        if yads_type.signed:
            if bits == 8:
                return pa.int8()
            if bits == 16:
                return pa.int16()
            if bits == 32:
                return pa.int32()
            if bits == 64:
                return pa.int64()
            raise UnsupportedFeatureError(
                f"Unsupported Integer bits: {bits}. Expected 8/16/32/64."
            )
        if bits == 8:
            return pa.uint8()
        if bits == 16:
            return pa.uint16()
        if bits == 32:
            return pa.uint32()
        if bits == 64:
            return pa.uint64()
        raise UnsupportedFeatureError(
            f"Unsupported Integer bits: {bits}. Expected 8/16/32/64."
        )

    @_convert_type.register(Float)
    def _(self, yads_type: Float) -> pa.DataType:
        bits = yads_type.bits or 64
        if bits == 16:
            return pa.float16()
        if bits == 32:
            return pa.float32()
        if bits == 64:
            return pa.float64()
        raise UnsupportedFeatureError(
            f"Unsupported Float bits: {bits}. Expected 16/32/64."
        )

    @_convert_type.register(Decimal)
    def _(self, yads_type: Decimal) -> pa.DataType:
        # Determine width function first, considering precision constraints.
        precision = yads_type.precision if yads_type.precision is not None else 38
        scale = yads_type.scale if yads_type.scale is not None else 0
        bits = yads_type.bits

        def build_decimal(width_bits: int) -> pa.DataType:
            if width_bits == 128:
                return pa.decimal128(precision, scale)
            if width_bits == 256:
                return pa.decimal256(precision, scale)
            raise UnsupportedFeatureError(
                f"Unsupported Decimal bits: {width_bits}. Expected 128/256."
            )

        if bits is None:
            # Choose width based on precision threshold.
            return build_decimal(256 if precision > 38 else 128)

        if bits == 128 and precision > 38:
            if self._mode == "coerce":
                validation_warning(
                    message=(
                        "Precision greater than 38 is incompatible with Decimal(bits=128)"
                        f" for column '{self._current_field_name or '<unknown>'}'."
                        f" The data type will be replaced with pyarrow.decimal256({precision=}, {scale=})."
                    ),
                    filename="yads.converters.pyarrow",
                    module=__name__,
                )
                return build_decimal(256)
            raise UnsupportedFeatureError(
                "precision > 38 is incompatible with Decimal(bits=128)."
            )
        return build_decimal(bits)

    @_convert_type.register(Boolean)
    def _(self, yads_type: Boolean) -> pa.DataType:
        return pa.bool_()

    @_convert_type.register(Binary)
    def _(self, yads_type: Binary) -> pa.DataType:
        if yads_type.length is not None:
            return pa.binary(yads_type.length)
        return pa.large_binary() if self._use_large_binary else pa.binary()

    @_convert_type.register(Date)
    def _(self, yads_type: Date) -> pa.DataType:
        bits = yads_type.bits or 32
        if bits == 32:
            return pa.date32()
        if bits == 64:
            return pa.date64()
        raise UnsupportedFeatureError(f"Unsupported Date bits: {bits}. Expected 32/64.")

    @_convert_type.register(Time)
    def _(self, yads_type: Time) -> pa.DataType:
        unit = self._to_pa_time_unit(yads_type.unit)
        bits = yads_type.bits

        if bits is None:
            # Infer from unit
            if unit in {"s", "ms"}:
                return pa.time32(unit)
            return pa.time64(unit)

        if bits == 32:
            if unit not in {"s", "ms"}:
                if self._mode == "coerce":
                    validation_warning(
                        message=(
                            "time32 supports only 's' or 'ms' units"
                            f" (got '{unit}') for column '{self._current_field_name or '<unknown>'}'."
                            f" The data type will be replaced with pyarrow.time64({unit=})."
                        ),
                        filename="yads.converters.pyarrow",
                        module=__name__,
                    )
                    return pa.time64(unit)
                raise UnsupportedFeatureError(
                    "time32 supports only 's' or 'ms' units (got '" + unit + "')."
                )
            return pa.time32(unit)
        if bits == 64:
            if unit not in {"us", "ns"}:
                if self._mode == "coerce":
                    # Promote coarse units to 32 if asked for 64 but unit is s/ms
                    validation_warning(
                        message=(
                            "time64 supports only 'us' or 'ns' units"
                            f" (got '{unit}') for column '{self._current_field_name or '<unknown>'}'."
                            f" The data type will be replaced with pyarrow.time32({unit=})."
                        ),
                        filename="yads.converters.pyarrow",
                        module=__name__,
                    )
                    return pa.time32(unit)
                raise UnsupportedFeatureError(
                    "time64 supports only 'us' or 'ns' units (got '" + unit + "')."
                )
            return pa.time64(unit)
        raise UnsupportedFeatureError(f"Unsupported Time bits: {bits}. Expected 32/64.")

    @_convert_type.register(Timestamp)
    def _(self, yads_type: Timestamp) -> pa.DataType:
        unit = self._to_pa_time_unit(yads_type.unit)
        return pa.timestamp(unit)

    @_convert_type.register(TimestampTZ)
    def _(self, yads_type: TimestampTZ) -> pa.DataType:
        unit = self._to_pa_time_unit(yads_type.unit)
        return pa.timestamp(unit, tz=yads_type.tz)

    @_convert_type.register(TimestampLTZ)
    def _(self, yads_type: TimestampLTZ) -> pa.DataType:
        unit = self._to_pa_time_unit(yads_type.unit)
        return pa.timestamp(unit, tz=None)

    @_convert_type.register(TimestampNTZ)
    def _(self, yads_type: TimestampNTZ) -> pa.DataType:
        unit = self._to_pa_time_unit(yads_type.unit)
        return pa.timestamp(unit, tz=None)

    @_convert_type.register(Duration)
    def _(self, yads_type: Duration) -> pa.DataType:
        unit = self._to_pa_time_unit(yads_type.unit)
        return pa.duration(unit)

    @_convert_type.register(Interval)
    def _(self, yads_type: Interval) -> pa.DataType:
        # Arrow currently represents intervals with month_day_nano layout.
        return pa.month_day_nano_interval()

    @_convert_type.register(Array)
    def _(self, yads_type: Array) -> pa.DataType:
        value_type = self._convert_type(yads_type.element)
        if yads_type.size is not None:
            # Fixed-size arrays use list_ with list_size
            return pa.list_(value_type, list_size=yads_type.size)
        # Variable-size arrays can optionally use large_list
        return pa.large_list(value_type) if self._use_large_list else pa.list_(value_type)

    @_convert_type.register(Struct)
    def _(self, yads_type: Struct) -> pa.DataType:
        fields = [self._convert_field(f) for f in yads_type.fields]
        return pa.struct(fields)

    @_convert_type.register(Map)
    def _(self, yads_type: Map) -> pa.DataType:
        key_type = self._convert_type(yads_type.key)
        item_type = self._convert_type(yads_type.value)
        return pa.map_(key_type, item_type, keys_sorted=yads_type.keys_sorted)

    @_convert_type.register(JSON)
    def _(self, yads_type: JSON) -> pa.DataType:
        return pa.json_(storage_type=pa.utf8())

    @_convert_type.register(UUID)
    def _(self, yads_type: UUID) -> pa.DataType:
        return pa.uuid()

    @_convert_type.register(Void)
    def _(self, yads_type: Void) -> pa.DataType:
        return pa.null()

    # Helpers
    def _convert_field(self, field: Field) -> pa.Field:
        """Convert a yads `Field` into a `pyarrow.Field`.

        Args:
            field: The yads field to convert.

        Returns:
            A `pyarrow.Field` with mapped type, nullability and metadata.
        """
        pa_type = self._convert_type(field.type)
        metadata = self._coerce_metadata(field.metadata) if field.metadata else None
        return pa.field(
            field.name, pa_type, nullable=field.is_nullable, metadata=metadata
        )

    @staticmethod
    def _to_pa_time_unit(unit: TimeUnit | None) -> str:
        """Map yads `TimeUnit` to a PyArrow unit string.

        Args:
            unit: The yads time unit value.

        Returns:
            One of `"s"`, `"ms"`, `"us"`, or `"ns"`.
        """
        # Default semantics per yads types: see definitions in `types.py`
        if unit is None:
            # Fallback to the most common Arrow default for timestamp/time conversions
            return "ms"
        return unit.value

    # Metadata helpers
    @staticmethod
    def _coerce_metadata(metadata: dict[str, Any]) -> dict[str, str]:
        """Coerce arbitrary metadata values to strings for PyArrow.

        PyArrow's KeyValueMetadata requires both keys and values to be
        strings (or bytes). This helper converts keys via `str(key)` and
        values as follows:

        - If the value is already a string, use it as-is
        - Otherwise, JSON-encode the value to preserve structure and types

        Args:
            metadata: Arbitrary key-value metadata mapping.

        Returns:
            A mapping of `str` to `str` suitable for pyarrow.
        """
        coerced: dict[str, str] = {}
        for k, v in metadata.items():
            sk = str(k)
            if isinstance(v, str):
                coerced[sk] = v
            else:
                coerced[sk] = json.dumps(v, separators=(",", ":"))
        return coerced
