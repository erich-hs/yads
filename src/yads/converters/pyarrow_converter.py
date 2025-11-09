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
from typing import Any, Callable, Literal, Mapping, TYPE_CHECKING
from dataclasses import dataclass, field
from types import MappingProxyType

from ..exceptions import UnsupportedFeatureError
from .._dependencies import requires_dependency, try_import_optional
from .. import spec as yspec
from .. import types as ytypes
from .base import BaseConverter, BaseConverterConfig

if TYPE_CHECKING:
    import pyarrow as pa  # type: ignore[import-untyped]


# %% ---- Configuration --------------------------------------------------------------
@dataclass(frozen=True)
class PyArrowConverterConfig(BaseConverterConfig):
    """Configuration for PyArrowConverter.

    Args:
        mode: Conversion mode. One of "raise" or "coerce". Inherited from
            BaseConverterConfig. Defaults to "coerce".
        ignore_columns: Column names to exclude from conversion. Inherited from
            BaseConverterConfig. Defaults to empty.
        include_columns: If provided, only these columns are included. Inherited
            from BaseConverterConfig. Defaults to None.
        column_overrides: Mapping of column name to a callable that returns a
            custom PyArrow field conversion. Inherited from BaseConverterConfig.
            Defaults to empty mapping.
        use_large_string: If True, use `pa.large_string()` for
            `String`. Defaults to False.
        use_large_binary: If True, use `pa.large_binary()` for
            `Binary(length=None)`. When a fixed `length` is provided, a fixed-size
            `pa.binary(length)` is always used. Defaults to False.
        use_large_list: If True, use `pa.large_list(element)` for
            variable-length `Array` (i.e., `size is None`). For fixed-size arrays
            (`size` set), `pa.list_(element, list_size=size)` is used. Defaults to False.
        fallback_type: PyArrow data type to use for unsupported types in coerce mode.
            Must be one of: pa.binary(), pa.large_binary(), pa.string(), pa.large_string(), or None.
            Defaults to None.
    """

    use_large_string: bool = False
    use_large_binary: bool = False
    use_large_list: bool = False
    fallback_type: pa.DataType | None = None
    column_overrides: Mapping[
        str, Callable[[yspec.Field, PyArrowConverter], pa.Field]
    ] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()

        # Validate fallback_type if provided
        if self.fallback_type is not None:
            import pyarrow as pa  # type: ignore[import-untyped]

            valid_fallback_types = {
                pa.binary(),
                pa.large_binary(),
                pa.string(),
                pa.large_string(),
            }
            if self.fallback_type not in valid_fallback_types:
                raise UnsupportedFeatureError(
                    f"fallback_type must be one of: pa.binary(), pa.large_binary(), "
                    f"pa.string(), pa.large_string(), or None. Got: {self.fallback_type}"
                )


# %% ---- Converter ------------------------------------------------------------------
class PyArrowConverter(BaseConverter):
    """Convert a yads `YadsSpec` into a `pyarrow.Schema`.

    The converter maps each yads column to a `pyarrow.Field` and assembles a
    `pyarrow.Schema`. Complex types such as arrays, structs, and maps are
    recursively converted.

    In "raise" mode, incompatible parameters raise `UnsupportedFeatureError`.
    In "coerce" mode, the converter attempts to coerce to a compatible target
    (e.g., promote decimal to 256-bit or time to 64-bit when units require it).
    If a logical type is unsupported by PyArrow, it is mapped to a canonical
    fallback `pa.binary()`.

    Notes:
        - Arrow strings are variable-length; any `String.length` hint is
          ignored in the resulting Arrow schema.
        - `Geometry`, `Geography`, and `Variant` are not supported and raise
          `UnsupportedFeatureError` unless in coerce mode.
    """

    def __init__(self, config: PyArrowConverterConfig | None = None) -> None:
        """Initialize the PyArrowConverter.

        Args:
            config: Configuration object. If None, uses default PyArrowConverterConfig.
        """
        self.config: PyArrowConverterConfig = config or PyArrowConverterConfig()
        super().__init__(self.config)

    @requires_dependency("pyarrow", import_name="pyarrow")
    def convert(
        self,
        spec: yspec.YadsSpec,
        *,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> pa.Schema:
        """Convert a yads `YadsSpec` into a `pyarrow.Schema`.

        Args:
            spec: The yads spec as a `YadsSpec` object.
            mode: Optional conversion mode override for this call. When not
                provided, the converter's configured mode is used. If provided:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid schema and emit warnings.

        Returns:
            A `pyarrow.Schema` with fields mapped from the spec columns.
        """
        import pyarrow as pa  # type: ignore[import-untyped]

        fields: list[pa.Field] = []
        with self.conversion_context(mode=mode):
            self._validate_column_filters(spec)
            for col in self._filter_columns(spec):
                with self.conversion_context(field=col.name):
                    field_result = self._convert_field_with_overrides(col)
                    fields.append(field_result)
        schema_metadata = self._coerce_metadata(spec.metadata) if spec.metadata else None
        return pa.schema(fields, metadata=schema_metadata)

    # %% ---- Type conversion ---------------------------------------------------------
    # Time unit constraints for Arrow
    _TIME32_UNITS: frozenset[str] = frozenset({"s", "ms"})
    _TIME64_UNITS: frozenset[str] = frozenset({"us", "ns"})

    @singledispatchmethod
    def _convert_type(self, yads_type: ytypes.YadsType) -> pa.DataType:
        # Fallback for currently unsupported:
        # - Geometry
        # - Geography
        # - Variant
        return self.raise_or_coerce(yads_type)

    @_convert_type.register(ytypes.String)
    def _(self, yads_type: ytypes.String) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        # Arrow strings are variable-length. Optionally use large_string.
        if yads_type.length is not None:
            self.raise_or_coerce(
                coerce_type=pa.large_string()
                if self.config.use_large_string
                else pa.string(),
                error_msg=(
                    f"{yads_type} cannot be represented in PyArrow; "
                    f"length constraint will be lost for '{self._field_context}'."
                ),
            )
        return pa.large_string() if self.config.use_large_string else pa.string()

    @_convert_type.register(ytypes.Integer)
    def _(self, yads_type: ytypes.Integer) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        bits = yads_type.bits or 32
        signed_map = {
            8: pa.int8(),
            16: pa.int16(),
            32: pa.int32(),
            64: pa.int64(),
        }
        unsigned_map = {
            8: pa.uint8(),
            16: pa.uint16(),
            32: pa.uint32(),
            64: pa.uint64(),
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
    def _(self, yads_type: ytypes.Float) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        bits = yads_type.bits or 32
        mapping = {16: pa.float16(), 32: pa.float32(), 64: pa.float64()}
        try:
            return mapping[bits]
        except KeyError as e:
            raise UnsupportedFeatureError(
                f"Unsupported Float bits: {bits}. Expected 16/32/64"
                f" for '{self._field_context}'."
            ) from e

    @_convert_type.register(ytypes.Decimal)
    def _(self, yads_type: ytypes.Decimal) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        # Determine width function first, considering precision constraints.
        precision = yads_type.precision or 38
        scale = yads_type.scale or 18
        bits = yads_type.bits

        def build_decimal(width_bits: int) -> pa.DataType:
            if width_bits == 128:
                return pa.decimal128(precision, scale)
            if width_bits == 256:
                return pa.decimal256(precision, scale)
            raise UnsupportedFeatureError(
                f"Unsupported Decimal bits: {width_bits}. Expected 128/256"
                f" for '{self._field_context}'."
            )

        if bits is None:
            # Choose width based on precision threshold.
            return build_decimal(256 if precision > 38 else 128)

        if bits == 128 and precision > 38:
            return self.raise_or_coerce(
                coerce_type=build_decimal(256),
                error_msg=(
                    "precision > 38 is incompatible with Decimal(bits=128)"
                    f" for '{self._field_context}'."
                ),
            )
        return build_decimal(bits)

    @_convert_type.register(ytypes.Boolean)
    def _(self, yads_type: ytypes.Boolean) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        return pa.bool_()

    @_convert_type.register(ytypes.Binary)
    def _(self, yads_type: ytypes.Binary) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        if yads_type.length is not None:
            return pa.binary(yads_type.length)
        return pa.large_binary() if self.config.use_large_binary else pa.binary()

    @_convert_type.register(ytypes.Date)
    def _(self, yads_type: ytypes.Date) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        bits = yads_type.bits or 32
        mapping = {32: pa.date32(), 64: pa.date64()}
        try:
            return mapping[bits]
        except KeyError as e:
            raise UnsupportedFeatureError(
                f"Unsupported Date bits: {bits}. Expected 32/64"
                f" for '{self._field_context}'."
            ) from e

    @_convert_type.register(ytypes.Time)
    def _(self, yads_type: ytypes.Time) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        unit = self._to_pa_time_unit(yads_type.unit)
        bits = yads_type.bits

        if bits is None:
            # Infer from unit
            if unit in self._TIME32_UNITS:
                return pa.time32(unit)
            return pa.time64(unit)

        if bits == 32:
            if unit not in self._TIME32_UNITS:
                return self.raise_or_coerce(
                    coerce_type=pa.time64(unit),
                    error_msg=(
                        "time32 supports only 's' or 'ms' units"
                        f" (got '{unit}') for '{self._field_context}'."
                    ),
                )
            return pa.time32(unit)
        elif bits == 64:
            if unit not in self._TIME64_UNITS:
                # Promote coarse units to 32 if asked for 64 but unit is s/ms
                return self.raise_or_coerce(
                    coerce_type=pa.time32(unit),
                    error_msg=(
                        "time64 supports only 'us' or 'ns' units"
                        f" (got '{unit}') for '{self._field_context}'."
                    ),
                )
            return pa.time64(unit)
        raise UnsupportedFeatureError(
            f"Unsupported Time bits: {bits}. Expected 32/64 for '{self._field_context}'."
        )

    @_convert_type.register(ytypes.Timestamp)
    def _(self, yads_type: ytypes.Timestamp) -> pa.DataType:
        return self._build_timestamp(yads_type.unit, tz=None)

    @_convert_type.register(ytypes.TimestampTZ)
    def _(self, yads_type: ytypes.TimestampTZ) -> pa.DataType:
        return self._build_timestamp(yads_type.unit, tz=yads_type.tz)

    @_convert_type.register(ytypes.TimestampLTZ)
    def _(self, yads_type: ytypes.TimestampLTZ) -> pa.DataType:
        return self._build_timestamp(yads_type.unit, tz=None)

    @_convert_type.register(ytypes.TimestampNTZ)
    def _(self, yads_type: ytypes.TimestampNTZ) -> pa.DataType:
        return self._build_timestamp(yads_type.unit, tz=None)

    @_convert_type.register(ytypes.Duration)
    def _(self, yads_type: ytypes.Duration) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        unit = self._to_pa_time_unit(yads_type.unit)
        return pa.duration(unit)

    @_convert_type.register(ytypes.Interval)
    def _(self, yads_type: ytypes.Interval) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        return pa.month_day_nano_interval()

    @_convert_type.register(ytypes.Array)
    def _(self, yads_type: ytypes.Array) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        value_type = self._convert_type(yads_type.element)
        if yads_type.size is not None:
            return pa.list_(value_type, list_size=yads_type.size)
        return (
            pa.large_list(value_type)
            if self.config.use_large_list
            else pa.list_(value_type)
        )

    @_convert_type.register(ytypes.Struct)
    def _(self, yads_type: ytypes.Struct) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        fields = []
        for yads_field in yads_type.fields:
            with self.conversion_context(field=yads_field.name):
                field_result = self._convert_field(yads_field)
                fields.append(field_result)
        return pa.struct(fields)

    @_convert_type.register(ytypes.Map)
    def _(self, yads_type: ytypes.Map) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        key_type = self._convert_type(yads_type.key)
        item_type = self._convert_type(yads_type.value)
        return pa.map_(key_type, item_type, keys_sorted=yads_type.keys_sorted)

    @_convert_type.register(ytypes.JSON)
    def _(self, yads_type: ytypes.JSON) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        json_constructor = self._get_version_gated_constructor(
            constructor_name="json_",
            min_version="19.0.0",
            feature_description="JSON type",
        )
        if json_constructor is None:
            return self.config.fallback_type
        return json_constructor(storage_type=pa.utf8())

    @_convert_type.register(ytypes.UUID)
    def _(self, yads_type: ytypes.UUID) -> pa.DataType:
        uuid_constructor = self._get_version_gated_constructor(
            constructor_name="uuid",
            min_version="18.0.0",
            feature_description="UUID type",
        )
        if uuid_constructor is None:
            return self.config.fallback_type
        return uuid_constructor()

    @_convert_type.register(ytypes.Void)
    def _(self, yads_type: ytypes.Void) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        return pa.null()

    @_convert_type.register(ytypes.Tensor)
    def _(self, yads_type: ytypes.Tensor) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        element_type = self._convert_type(yads_type.element)
        return pa.fixed_shape_tensor(element_type, yads_type.shape)

    def _convert_field(self, field: yspec.Field) -> pa.Field:
        import pyarrow as pa  # type: ignore[import-untyped]

        pa_type = self._convert_type(field.type)
        metadata = self._build_field_metadata(field)
        return pa.field(
            field.name,
            pa_type,
            nullable=field.is_nullable,
            metadata=metadata,
        )

    def _convert_field_default(self, field: yspec.Field) -> pa.Field:
        return self._convert_field(field)

    # %% ---- Helpers -----------------------------------------------------------------
    @staticmethod
    def _to_pa_time_unit(unit: ytypes.TimeUnit | None) -> str:
        if unit is None:
            return "ms"
        return unit.value

    def _build_timestamp(
        self, unit: ytypes.TimeUnit | None, tz: str | None
    ) -> pa.DataType:
        import pyarrow as pa  # type: ignore[import-untyped]

        pa_unit = self._to_pa_time_unit(unit)
        return pa.timestamp(pa_unit, tz=tz)

    def _build_field_metadata(self, field: yspec.Field) -> dict[str, str] | None:
        metadata: dict[str, Any] = {}
        if field.description is not None:
            metadata["description"] = field.description
        if field.metadata is not None:
            metadata.update(field.metadata)
        return self._coerce_metadata(metadata) if metadata else None

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

    def _get_version_gated_constructor(
        self,
        *,
        constructor_name: str,
        min_version: str,
        feature_description: str,
    ) -> Any | None:
        """Attempt to import a version-gated PyArrow type constructor with mode-aware fallback."""
        context = f"{feature_description} for '{self._field_context}'"

        imported_constructor, error_msg = try_import_optional(
            "pyarrow",
            required_import=constructor_name,
            package_name="pyarrow",
            min_version=min_version,
            context=context,
        )

        if imported_constructor is not None:
            return imported_constructor

        # Constructor is unavailable - handle based on mode
        self.raise_or_coerce(error_msg=error_msg)
        return None
