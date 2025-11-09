"""Polars converter from yads `YadsSpec` to `polars.Schema`.

This module defines the `PolarsConverter`, responsible for producing a
`polars.Schema` from yads' canonical `YadsSpec`.

Example:
    >>> import yads.types as ytypes
    >>> from yads.spec import Column, YadsSpec
    >>> from yads.converters import PolarsConverter
    >>> spec = YadsSpec(
    ...     name="catalog.db.table",
    ...     version="0.0.1",
    ...     columns=[
    ...         Column(name="id", type=ytypes.Integer(bits=64)),
    ...         Column(name="name", type=ytypes.String()),
    ...     ],
    ... )
    >>> pl_schema = PolarsConverter().convert(spec)
    >>> pl_schema.names()
    ['id', 'name']
"""

from __future__ import annotations

from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from ..exceptions import UnsupportedFeatureError
from .._dependencies import requires_dependency
from .. import spec as yspec
from .. import types as ytypes
from .base import BaseConverter, BaseConverterConfig

if TYPE_CHECKING:
    import polars as pl  # type: ignore[import-untyped]


# %% ---- Configuration --------------------------------------------------------------
@dataclass(frozen=True)
class PolarsConverterConfig(BaseConverterConfig):
    """Configuration for PolarsConverter.

    Args:
        mode: Conversion mode. One of "raise" or "coerce". Inherited from
            BaseConverterConfig. Defaults to "coerce".
        ignore_columns: Column names to exclude from conversion. Inherited from
            BaseConverterConfig. Defaults to empty.
        include_columns: If provided, only these columns are included. Inherited
            from BaseConverterConfig. Defaults to None.
        column_overrides: Mapping of column name to a callable that returns a
            custom Polars field conversion. Inherited from BaseConverterConfig.
            Defaults to empty mapping.
        fallback_type: Polars data type to use for unsupported types in coerce mode.
            Must be one of: pl.String, pl.Binary, or None.
            Defaults to None.
    """

    fallback_type: Any | None = None
    column_overrides: Mapping[str, Callable[[yspec.Field, PolarsConverter], Any]] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()

        # Validate fallback_type if provided
        if self.fallback_type is not None:
            import polars as pl  # type: ignore[import-untyped]

            valid_fallback_types = {pl.String, pl.Binary}
            if self.fallback_type not in valid_fallback_types:
                raise UnsupportedFeatureError(
                    f"fallback_type must be one of: pl.String, pl.Binary, or None. "
                    f"Got: {self.fallback_type}"
                )


# %% ---- Converter ------------------------------------------------------------------
class PolarsConverter(BaseConverter):
    """Convert a yads `YadsSpec` into a `polars.Schema`.

    The converter maps each yads column to a `polars.Field` and assembles a
    `polars.Schema`. Complex types such as arrays and structs are
    recursively converted.

    In "raise" mode, incompatible types raise `UnsupportedFeatureError`.
    In "coerce" mode, the converter attempts to coerce to a compatible target
    with warnings. If a logical type is unsupported by Polars, it is mapped to
    the configured fallback type.

    Notes:
        - Polars strings are variable-length; any `String.length` hint is
          ignored in the resulting Polars schema.
        - `Float(bits=16)` is not supported and coerces to Float32.
        - `Tensor` is converted to `pl.Array` with multi-dimensional shape support.
        - `Map`, `UUID`, `JSON`, `Geometry`, `Geography`, and `Variant`
          are not supported and coerce to the fallback type.
        - `Interval` is not supported and coerces to the fallback type (Polars
          Duration only supports subsecond units).
        - `TimestampLTZ` loses local timezone semantics and coerces to Datetime
          without timezone.
    """

    def __init__(self, config: PolarsConverterConfig | None = None) -> None:
        """Initialize the PolarsConverter.

        Args:
            config: Configuration object. If None, uses default PolarsConverterConfig.
        """
        self.config: PolarsConverterConfig = config or PolarsConverterConfig()
        super().__init__(self.config)

    @requires_dependency("polars", min_version="1.0.0", import_name="polars")
    def convert(
        self,
        spec: yspec.YadsSpec,
        *,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> pl.Schema:
        """Convert a yads `YadsSpec` into a `polars.Schema`.

        Args:
            spec: The yads spec as a `YadsSpec` object.
            mode: Optional conversion mode override for this call. When not
                provided, the converter's configured mode is used. If provided:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid schema and emit warnings.

        Returns:
            A `polars.Schema` with fields mapped from the spec columns.
        """
        import polars as pl  # type: ignore[import-untyped]

        fields: dict[str, pl.DataType] = {}
        with self.conversion_context(mode=mode):
            self._validate_column_filters(spec)
            for col in self._filter_columns(spec):
                with self.conversion_context(field=col.name):
                    field_result = self._convert_field_with_overrides(col)
                    fields[field_result.name] = field_result.dtype
        return pl.Schema(fields)

    # %% ---- Type conversion ---------------------------------------------------------
    @singledispatchmethod
    def _convert_type(self, yads_type: ytypes.YadsType) -> Any:
        # Fallback for currently unsupported:
        # - Geometry
        # - Geography
        # - Variant
        # - Interval
        # - JSON
        # - UUID
        return self.raise_or_coerce(yads_type)

    @_convert_type.register(ytypes.String)
    def _(self, yads_type: ytypes.String) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        # Polars strings are variable-length. Length hint is ignored.
        if yads_type.length is not None:
            self.raise_or_coerce(
                coerce_type=pl.String,
                error_msg=(
                    f"{yads_type} cannot be represented in Polars; "
                    f"length constraint will be lost for '{self._field_context}'."
                ),
            )
        return pl.String

    @_convert_type.register(ytypes.Integer)
    def _(self, yads_type: ytypes.Integer) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        bits = yads_type.bits or 32
        signed_map = {
            8: pl.Int8,
            16: pl.Int16,
            32: pl.Int32,
            64: pl.Int64,
        }
        unsigned_map = {
            8: pl.UInt8,
            16: pl.UInt16,
            32: pl.UInt32,
            64: pl.UInt64,
        }
        mapping = signed_map if yads_type.signed else unsigned_map
        try:
            return mapping[bits]
        except KeyError as e:
            raise UnsupportedFeatureError(
                f"Unsupported Integer bits: {bits}. Expected 8/16/32/64"
                f" for '{self._field_context}'."
            ) from e

    @_convert_type.register(ytypes.Float)
    def _(self, yads_type: ytypes.Float) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        bits = yads_type.bits or 32
        mapping = {32: pl.Float32, 64: pl.Float64}

        if bits == 16:
            return self.raise_or_coerce(yads_type, coerce_type=pl.Float32)

        try:
            return mapping[bits]
        except KeyError as e:
            raise UnsupportedFeatureError(
                f"Unsupported Float bits: {bits}. Expected 32/64"
                f" for '{self._field_context}'."
            ) from e

    @_convert_type.register(ytypes.Decimal)
    def _(self, yads_type: ytypes.Decimal) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        precision = yads_type.precision
        scale = yads_type.scale if yads_type.scale is not None else 0

        return pl.Decimal(precision=precision, scale=scale)

    @_convert_type.register(ytypes.Boolean)
    def _(self, yads_type: ytypes.Boolean) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        return pl.Boolean

    @_convert_type.register(ytypes.Binary)
    def _(self, yads_type: ytypes.Binary) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        # Polars binary is variable-length. Length hint is ignored.
        if yads_type.length is not None:
            self.raise_or_coerce(
                coerce_type=pl.Binary,
                error_msg=(
                    f"{yads_type} cannot be represented in Polars; "
                    f"length constraint will be lost for '{self._field_context}'."
                ),
            )
        return pl.Binary

    @_convert_type.register(ytypes.Date)
    def _(self, yads_type: ytypes.Date) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        # Polars has a single Date type. Ignore bits parameter.
        if yads_type.bits is not None:
            self.raise_or_coerce(
                coerce_type=pl.Date,
                error_msg=(
                    f"{yads_type} cannot be represented in Polars; "
                    f"bits constraint will be lost for '{self._field_context}'."
                ),
            )
        return pl.Date

    @_convert_type.register(ytypes.Time)
    def _(self, yads_type: ytypes.Time) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        # Polars Time indicates nanoseconds since midnight, only supports NS unit
        time_unit = self._to_pl_time_unit(yads_type.unit)

        if time_unit != "ns":
            return self.raise_or_coerce(
                error_msg=(
                    f"Polars Time only supports nanosecond precision (unit='ns')"
                    f" for '{self._field_context}'."
                ),
            )

        return pl.Time

    @_convert_type.register(ytypes.Timestamp)
    def _(self, yads_type: ytypes.Timestamp) -> Any:
        return self._build_datetime(yads_type.unit, time_zone=None)

    @_convert_type.register(ytypes.TimestampTZ)
    def _(self, yads_type: ytypes.TimestampTZ) -> Any:
        return self._build_datetime(yads_type.unit, time_zone=yads_type.tz)

    @_convert_type.register(ytypes.TimestampLTZ)
    def _(self, yads_type: ytypes.TimestampLTZ) -> Any:
        # Polars doesn't have explicit LTZ semantics, use None for timezone
        # This loses local timezone semantics
        return self.raise_or_coerce(
            coerce_type=self._build_datetime(yads_type.unit, time_zone=None),
            error_msg=(
                f"{yads_type} cannot be represented in Polars; "
                f"local timezone semantics will be lost for '{self._field_context}'."
            ),
        )

    @_convert_type.register(ytypes.TimestampNTZ)
    def _(self, yads_type: ytypes.TimestampNTZ) -> Any:
        return self._build_datetime(yads_type.unit, time_zone=None)

    @_convert_type.register(ytypes.Duration)
    def _(self, yads_type: ytypes.Duration) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        time_unit = self._to_pl_time_unit(yads_type.unit)

        # Polars Duration only supports ms, us, ns
        if time_unit == "s":
            return self.raise_or_coerce(
                error_msg=(
                    f"Polars Duration does not support 's' (second) time unit"
                    f" for '{self._field_context}'."
                ),
            )

        # Cast to Any to avoid type checker issues with Polars types
        return pl.Duration(time_unit=time_unit)  # type: ignore[call-arg,arg-type]

    @_convert_type.register(ytypes.Array)
    def _(self, yads_type: ytypes.Array) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        value_type = self._convert_type(yads_type.element)
        if yads_type.size is not None:
            # Fixed-size array - use 'shape' parameter (width is deprecated)
            return pl.Array(value_type, shape=yads_type.size)
        # Variable-length list
        return pl.List(value_type)

    @_convert_type.register(ytypes.Tensor)
    def _(self, yads_type: ytypes.Tensor) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        element_type = self._convert_type(yads_type.element)
        return pl.Array(element_type, shape=yads_type.shape)

    @_convert_type.register(ytypes.Struct)
    def _(self, yads_type: ytypes.Struct) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        fields = []
        for yads_field in yads_type.fields:
            with self.conversion_context(field=yads_field.name):
                field_type = self._convert_type(yads_field.type)
                fields.append(pl.Field(yads_field.name, field_type))
        return pl.Struct(fields)

    @_convert_type.register(ytypes.Map)
    def _(self, yads_type: ytypes.Map) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        # Polars doesn't have a native Map type
        # Coerce to Struct with key/value fields to preserve structure
        key_type = self._convert_type(yads_type.key)
        value_type = self._convert_type(yads_type.value)
        struct_type = pl.Struct(
            [pl.Field("key", key_type), pl.Field("value", value_type)]
        )

        return self.raise_or_coerce(yads_type, coerce_type=struct_type)

    @_convert_type.register(ytypes.Void)
    def _(self, yads_type: ytypes.Void) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        return pl.Null

    def _convert_field(self, field: yspec.Field) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        pl_type = self._convert_type(field.type)
        return pl.Field(field.name, pl_type)

    def _convert_field_default(self, field: yspec.Field) -> Any:
        return self._convert_field(field)

    # %% ---- Helpers -----------------------------------------------------------------
    @staticmethod
    def _to_pl_time_unit(unit: ytypes.TimeUnit | None) -> str:
        if unit is None:
            return "ns"
        return unit.value

    def _build_datetime(self, unit: ytypes.TimeUnit | None, time_zone: str | None) -> Any:
        import polars as pl  # type: ignore[import-untyped]

        time_unit = self._to_pl_time_unit(unit)

        # Polars Datetime only supports ms, us, ns (not s)
        if time_unit == "s":
            return self.raise_or_coerce(
                error_msg=(
                    f"Polars Datetime does not support 's' (second) time unit"
                    f" for '{self._field_context}'."
                ),
            )

        # Cast to Any to avoid type checker issues with Polars types
        return pl.Datetime(time_unit=time_unit, time_zone=time_zone)  # type: ignore[call-arg,arg-type]
