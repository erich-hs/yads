"""PySpark converter from yads `YadsSpec` to PySpark `StructType`.

This module defines the `PySparkConverter`, responsible for producing a
PySpark `StructType` schema from yads' canonical `YadsSpec`.

Example:
    >>> import yads.types as ytypes
    >>> from yads.spec import Column, YadsSpec
    >>> from yads.converters import PySparkConverter
    >>> spec = YadsSpec(
    ...     name="catalog.db.table",
    ...     version="0.0.1",
    ...     columns=[
    ...         Column(name="id", type=ytypes.Integer(bits=64)),
    ...         Column(name="name", type=ytypes.String()),
    ...     ],
    ... )
    >>> spark_schema = PySparkConverter().convert(spec)
    >>> spark_schema.fieldNames()
    ['id', 'name']
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import singledispatchmethod
from typing import Any, Callable, Literal, Mapping, TYPE_CHECKING

from .base import BaseConverter, BaseConverterConfig
from ..exceptions import UnsupportedFeatureError, validation_warning
from .._dependencies import requires_dependency, try_import_optional
import yads.spec as yspec
import yads.types as ytypes

if TYPE_CHECKING:
    from pyspark.sql.types import DataType, StructField, StructType
    from yads.spec import Field


@requires_dependency("pyspark", import_name="pyspark.sql.types")
def _default_fallback_type() -> DataType:
    from pyspark.sql.types import StringType

    return StringType()


# %% ---- Configuration --------------------------------------------------------------
@dataclass(frozen=True)
class PySparkConverterConfig(BaseConverterConfig):
    """Configuration for PySparkConverter.

    Args:
        fallback_type: PySpark data type to use for unsupported types in coerce mode.
            Must be one of: StringType(), BinaryType(). Defaults to StringType().
    """

    fallback_type: DataType = field(default_factory=_default_fallback_type)
    column_overrides: Mapping[str, Callable[[Field, PySparkConverter], StructField]] = (
        field(default_factory=dict)
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        # Validate fallback_type
        from pyspark.sql.types import (
            StringType,
            BinaryType,
        )

        valid_fallback_types = (StringType, BinaryType)
        if not isinstance(self.fallback_type, valid_fallback_types):
            raise UnsupportedFeatureError(
                "fallback_type must be one of: StringType(), BinaryType(). "
                f"Got: {self.fallback_type}"
            )


# %% ---- Converter ------------------------------------------------------------------
class PySparkConverter(BaseConverter):
    """Convert a yads `YadsSpec` into a PySpark `StructType`.

    The converter maps each yads column to a `StructField` and assembles a
    `StructType`. Complex types such as arrays, structs, and maps are
    recursively converted.

    In "raise" mode, incompatible parameters raise `UnsupportedFeatureError`.
    In "coerce" mode, the converter attempts to coerce to a compatible target
    (e.g., promote unsigned integers to signed ones, or map unsupported types
    to StringType).

    Notes:
        - Time types are not supported by PySpark and raise `UnsupportedFeatureError`
          unless in coerce mode.
        - Duration types are not supported by PySpark and raise `UnsupportedFeatureError`
          unless in coerce mode.
        - Geometry, Geography, JSON, UUID, and Tensor types are not supported and raise
          `UnsupportedFeatureError` unless in coerce mode.
        - Variant type maps to VariantType if available in the PySpark version.
    """

    def __init__(self, config: PySparkConverterConfig | None = None) -> None:
        """Initialize the PySparkConverter.

        Args:
            config: Configuration object. If None, uses default PySparkConverterConfig.
        """
        self.config: PySparkConverterConfig = config or PySparkConverterConfig()
        super().__init__(self.config)

    @requires_dependency("pyspark", import_name="pyspark.sql.types")
    def convert(
        self,
        spec: yspec.YadsSpec,
        *,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> StructType:
        """Convert a yads `YadsSpec` into a PySpark `StructType`.

        Args:
            spec: The yads spec as a `YadsSpec` object.
            mode: Optional conversion mode override for this call. When not
                provided, the converter's configured mode is used. If provided:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid schema and emit warnings.

        Returns:
            A PySpark `StructType` with fields mapped from the spec columns.
        """
        from pyspark.sql.types import StructType

        fields: list[StructField] = []
        # Set mode for this conversion call
        with self.conversion_context(mode=mode):
            self._validate_column_filters(spec)
            for col in self._filter_columns(spec):
                # Set field context during conversion
                with self.conversion_context(field=col.name):
                    # Use centralized override resolution
                    field_result = self._convert_field_with_overrides(col)
                    fields.append(field_result)
        return StructType(fields)

    # %% ---- Type conversion ---------------------------------------------------------
    @singledispatchmethod
    def _convert_type(self, yads_type: ytypes.YadsType) -> DataType:
        # Fallback for currently unsupported types
        # - Time
        # - Duration
        # - JSON
        # - Geometry
        # - Geography
        # - UUID
        # - Tensor
        if self.config.mode == "coerce":
            validation_warning(
                message=(
                    f"PySparkConverter does not support type: {yads_type}"
                    f" for '{self._current_field_name or '<unknown>'}'."
                    f" The data type will be coerced to {self.config.fallback_type}."
                ),
                filename="yads.converters.pyspark_converter",
                module=__name__,
            )
            return self.config.fallback_type
        raise UnsupportedFeatureError(
            f"PySparkConverter does not support type: {yads_type}"
            f" for '{self._current_field_name or '<unknown>'}'."
        )

    @_convert_type.register(ytypes.String)
    def _(self, yads_type: ytypes.String) -> DataType:
        from pyspark.sql.types import (
            VarcharType,
            StringType,
        )

        if yads_type.length is not None:
            return VarcharType(yads_type.length)
        return StringType()

    @_convert_type.register(ytypes.Integer)
    def _(self, yads_type: ytypes.Integer) -> DataType:
        from pyspark.sql.types import (
            ByteType,
            ShortType,
            IntegerType,
            LongType,
            DecimalType,
        )

        bits = yads_type.bits or 32
        signed = yads_type.signed

        if signed:
            mapping = {
                8: ByteType(),
                16: ShortType(),
                32: IntegerType(),
                64: LongType(),
            }
            try:
                return mapping[bits]
            except KeyError as e:
                raise UnsupportedFeatureError(
                    f"Unsupported Integer bits: {bits}. Expected 8/16/32/64."
                ) from e
        else:
            # Handle unsigned integers
            if self.config.mode == "coerce":
                if bits == 8:
                    validation_warning(
                        message=(
                            f"Unsigned Integer(bits=8) is not supported by PySpark"
                            f" for '{self._current_field_name or '<unknown>'}'."
                            f" The data type will be coerced to ShortType()."
                        ),
                        filename="yads.converters.pyspark_converter",
                        module=__name__,
                    )
                    return ShortType()
                elif bits == 16:
                    validation_warning(
                        message=(
                            f"Unsigned Integer(bits=16) is not supported by PySpark"
                            f" for '{self._current_field_name or '<unknown>'}'."
                            f" The data type will be coerced to IntegerType()."
                        ),
                        filename="yads.converters.pyspark_converter",
                        module=__name__,
                    )
                    return IntegerType()
                elif bits == 32:
                    validation_warning(
                        message=(
                            f"Unsigned Integer(bits=32) is not supported by PySpark"
                            f" for '{self._current_field_name or '<unknown>'}'."
                            f" The data type will be coerced to LongType()."
                        ),
                        filename="yads.converters.pyspark_converter",
                        module=__name__,
                    )
                    return LongType()
                elif bits == 64:
                    validation_warning(
                        message=(
                            f"Unsigned Integer(bits=64) is not supported by PySpark"
                            f" for '{self._current_field_name or '<unknown>'}'."
                            f" The data type will be coerced to DecimalType(20, 0)."
                        ),
                        filename="yads.converters.pyspark_converter",
                        module=__name__,
                    )
                    return DecimalType(20, 0)
                else:
                    raise UnsupportedFeatureError(
                        f"Unsupported Integer bits: {bits}. Expected 8/16/32/64."
                    )
            else:
                raise UnsupportedFeatureError(
                    (
                        f"Unsigned Integer(bits={bits}) is not supported by PySpark"
                        f" for '{self._current_field_name or '<unknown>'}'."
                    )
                )

    @_convert_type.register(ytypes.Float)
    def _(self, yads_type: ytypes.Float) -> DataType:
        from pyspark.sql.types import (
            FloatType,
            DoubleType,
        )

        bits = yads_type.bits or 32

        if bits == 16:
            if self.config.mode == "coerce":
                validation_warning(
                    message=(
                        f"Float(bits=16) is not supported by PySpark"
                        f" for '{self._current_field_name or '<unknown>'}'."
                        f" The data type will be coerced to FloatType()."
                    ),
                    filename="yads.converters.pyspark_converter",
                    module=__name__,
                )
                return FloatType()
            else:
                raise UnsupportedFeatureError(
                    (
                        f"Float(bits=16) is not supported by PySpark"
                        f" for '{self._current_field_name or '<unknown>'}'."
                    )
                )
        elif bits == 32:
            return FloatType()
        elif bits == 64:
            return DoubleType()
        else:
            raise UnsupportedFeatureError(
                f"Unsupported Float bits: {bits}. Expected 16/32/64."
            )

    @_convert_type.register(ytypes.Decimal)
    def _(self, yads_type: ytypes.Decimal) -> DataType:
        from pyspark.sql.types import DecimalType

        precision = yads_type.precision or 38
        scale = yads_type.scale or 18
        return DecimalType(precision, scale)

    @_convert_type.register(ytypes.Boolean)
    def _(self, yads_type: ytypes.Boolean) -> DataType:
        from pyspark.sql.types import BooleanType

        return BooleanType()

    @_convert_type.register(ytypes.Binary)
    def _(self, yads_type: ytypes.Binary) -> DataType:
        from pyspark.sql.types import BinaryType

        # Ignore length parameter
        return BinaryType()

    @_convert_type.register(ytypes.Date)
    def _(self, yads_type: ytypes.Date) -> DataType:
        from pyspark.sql.types import DateType

        # Ignore bit-width parameter
        return DateType()

    @_convert_type.register(ytypes.Timestamp)
    def _(self, yads_type: ytypes.Timestamp) -> DataType:
        from pyspark.sql.types import TimestampType

        # Ignore unit parameter
        return TimestampType()

    @_convert_type.register(ytypes.TimestampTZ)
    def _(self, yads_type: ytypes.TimestampTZ) -> DataType:
        from pyspark.sql.types import TimestampType

        # Ignore unit parameter
        # Ignore tz parameter
        return TimestampType()

    @_convert_type.register(ytypes.TimestampLTZ)
    def _(self, yads_type: ytypes.TimestampLTZ) -> DataType:
        from pyspark.sql.types import TimestampType

        # Ignore unit parameter
        return TimestampType()

    @_convert_type.register(ytypes.TimestampNTZ)
    def _(self, yads_type: ytypes.TimestampNTZ) -> DataType:
        from pyspark.sql.types import TimestampNTZType

        # Ignore unit parameter
        return TimestampNTZType()

    @_convert_type.register(ytypes.Interval)
    def _(self, yads_type: ytypes.Interval) -> DataType:
        from pyspark.sql.types import (
            YearMonthIntervalType,
            DayTimeIntervalType,
        )

        start_field = yads_type.interval_start
        end_field = yads_type.interval_end or start_field

        # Map interval units to PySpark constants
        year_month_units = {
            ytypes.IntervalTimeUnit.YEAR,
            ytypes.IntervalTimeUnit.MONTH,
        }
        day_time_units = {
            ytypes.IntervalTimeUnit.DAY,
            ytypes.IntervalTimeUnit.HOUR,
            ytypes.IntervalTimeUnit.MINUTE,
            ytypes.IntervalTimeUnit.SECOND,
        }

        # PySpark interval field constants
        YEAR = 0
        MONTH = 1
        DAY = 0
        HOUR = 1
        MINUTE = 2
        SECOND = 3

        if start_field in year_month_units:
            # Validate end_field is compatible
            if end_field not in year_month_units:
                raise UnsupportedFeatureError(
                    f"Invalid interval combination: {start_field} to {end_field}. "
                    "Year-Month intervals must use YEAR or MONTH units only."
                )

            start_val = YEAR if start_field == ytypes.IntervalTimeUnit.YEAR else MONTH
            end_val = YEAR if end_field == ytypes.IntervalTimeUnit.YEAR else MONTH
            return YearMonthIntervalType(start_val, end_val)
        elif start_field in day_time_units:
            # Validate end_field is compatible
            if end_field not in day_time_units:
                raise UnsupportedFeatureError(
                    f"Invalid interval combination: {start_field} to {end_field}. "
                    "Day-Time intervals must use DAY, HOUR, MINUTE, or SECOND units only."
                )

            start_val = {
                ytypes.IntervalTimeUnit.DAY: DAY,
                ytypes.IntervalTimeUnit.HOUR: HOUR,
                ytypes.IntervalTimeUnit.MINUTE: MINUTE,
                ytypes.IntervalTimeUnit.SECOND: SECOND,
            }[start_field]
            end_val = {
                ytypes.IntervalTimeUnit.DAY: DAY,
                ytypes.IntervalTimeUnit.HOUR: HOUR,
                ytypes.IntervalTimeUnit.MINUTE: MINUTE,
                ytypes.IntervalTimeUnit.SECOND: SECOND,
            }[end_field]
            return DayTimeIntervalType(startField=start_val, endField=end_val)
        else:
            raise UnsupportedFeatureError(
                f"Unsupported interval start field: {start_field}"
            )

    @_convert_type.register(ytypes.Array)
    def _(self, yads_type: ytypes.Array) -> DataType:
        from pyspark.sql.types import ArrayType

        # Ignore size parameter
        element_type = self._convert_type(yads_type.element)
        return ArrayType(element_type, True)

    @_convert_type.register(ytypes.Struct)
    def _(self, yads_type: ytypes.Struct) -> DataType:
        from pyspark.sql.types import StructType

        fields = []
        for yads_field in yads_type.fields:
            with self.conversion_context(field=yads_field.name):
                field_result = self._convert_field(yads_field)
                fields.append(field_result)
        return StructType(fields)

    @_convert_type.register(ytypes.Map)
    def _(self, yads_type: ytypes.Map) -> DataType:
        from pyspark.sql.types import MapType

        key_type = self._convert_type(yads_type.key)
        value_type = self._convert_type(yads_type.value)
        return MapType(keyType=key_type, valueType=value_type, valueContainsNull=True)

    @_convert_type.register(ytypes.Void)
    def _(self, yads_type: ytypes.Void) -> DataType:
        from pyspark.sql.types import NullType

        return NullType()

    @_convert_type.register(ytypes.Variant)
    def _(self, yads_type: ytypes.Variant) -> DataType:
        VariantType, msg = try_import_optional(
            "pyspark.sql.types",
            required_import="VariantType",
            package_name="pyspark",
            min_version="4.0.0",
            context=f"Variant type for '{self._current_field_name or '<unknown>'}'",
        )
        if VariantType is None:
            if self.config.mode == "coerce":
                validation_warning(
                    message=f"{msg}\nThe data type will be coerced to {self.config.fallback_type}.",
                    filename="yads.converters.pyspark_converter",
                    module=__name__,
                )
                return self.config.fallback_type
            raise UnsupportedFeatureError(
                "Variant type requires PySpark with VariantType support (>= 4.0)"
                f" for '{self._current_field_name or '<unknown>'}'."
            )
        return VariantType()

    def _convert_field(self, field: yspec.Field) -> StructField:
        from pyspark.sql.types import StructField

        spark_type = self._convert_type(field.type)
        metadata: dict[str, Any] = {}
        if field.description is not None:
            metadata["description"] = field.description
        if field.metadata is not None:
            metadata.update(field.metadata)

        return StructField(
            field.name,
            spark_type,
            nullable=field.is_nullable,
            metadata=metadata or None,
        )

    def _convert_field_default(self, field: Field) -> StructField:
        return self._convert_field(field)
