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

from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    VarcharType,
    VariantType,
    YearMonthIntervalType,
)

from ..exceptions import UnsupportedFeatureError, validation_warning
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
    Timestamp,
    TimestampTZ,
    TimestampLTZ,
    TimestampNTZ,
    Interval,
    IntervalTimeUnit,
    Array,
    Struct,
    Map,
    Void,
    Variant,
)
from .base import BaseConverter, BaseConverterConfig

if TYPE_CHECKING:
    from pyspark.sql.types import DataType


# %% ---- Configuration --------------------------------------------------------------
@dataclass(frozen=True)
class PySparkConverterConfig(BaseConverterConfig[StructField]):
    """Configuration for PySparkConverter.

    Args:
        fallback_type: PySpark data type to use for unsupported types in coerce mode.
            Must be one of: StringType(), BinaryType(). Defaults to StringType().
    """

    fallback_type: DataType = field(default_factory=StringType)
    column_overrides: Mapping[str, Callable[[Field, PySparkConverter], StructField]] = (
        field(default_factory=dict)
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        # Validate fallback_type
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

    def convert(
        self,
        spec: YadsSpec,
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
    def _convert_type(self, yads_type: YadsType) -> DataType:
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

    @_convert_type.register(String)
    def _(self, yads_type: String) -> DataType:
        if yads_type.length is not None:
            return VarcharType(yads_type.length)
        return StringType()

    @_convert_type.register(Integer)
    def _(self, yads_type: Integer) -> DataType:
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
                        f"Unsigned Integer(bits={bits}) is not supported by PySpark."
                        f" for '{self._current_field_name or '<unknown>'}'."
                    )
                )

    @_convert_type.register(Float)
    def _(self, yads_type: Float) -> DataType:
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
                        f"Float(bits=16) is not supported by PySpark."
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

    @_convert_type.register(Decimal)
    def _(self, yads_type: Decimal) -> DataType:
        precision = yads_type.precision or 38
        scale = yads_type.scale or 18
        return DecimalType(precision, scale)

    @_convert_type.register(Boolean)
    def _(self, yads_type: Boolean) -> DataType:
        return BooleanType()

    @_convert_type.register(Binary)
    def _(self, yads_type: Binary) -> DataType:
        # Ignore length parameter
        return BinaryType()

    @_convert_type.register(Date)
    def _(self, yads_type: Date) -> DataType:
        # Ignore bit-width parameter
        return DateType()

    @_convert_type.register(Timestamp)
    def _(self, yads_type: Timestamp) -> DataType:
        # Ignore unit parameter
        return TimestampType()

    @_convert_type.register(TimestampTZ)
    def _(self, yads_type: TimestampTZ) -> DataType:
        # Ignore unit parameter
        # Ignore tz parameter
        return TimestampType()

    @_convert_type.register(TimestampLTZ)
    def _(self, yads_type: TimestampLTZ) -> DataType:
        # Ignore unit parameter
        return TimestampType()

    @_convert_type.register(TimestampNTZ)
    def _(self, yads_type: TimestampNTZ) -> DataType:
        # Ignore unit parameter
        return TimestampNTZType()

    @_convert_type.register(Interval)
    def _(self, yads_type: Interval) -> DataType:
        start_field = yads_type.interval_start
        end_field = yads_type.interval_end or start_field

        # Map interval units to PySpark constants
        year_month_units = {
            IntervalTimeUnit.YEAR,
            IntervalTimeUnit.MONTH,
        }
        day_time_units = {
            IntervalTimeUnit.DAY,
            IntervalTimeUnit.HOUR,
            IntervalTimeUnit.MINUTE,
            IntervalTimeUnit.SECOND,
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

            start_val = YEAR if start_field == IntervalTimeUnit.YEAR else MONTH
            end_val = YEAR if end_field == IntervalTimeUnit.YEAR else MONTH
            return YearMonthIntervalType(start_val, end_val)
        elif start_field in day_time_units:
            # Validate end_field is compatible
            if end_field not in day_time_units:
                raise UnsupportedFeatureError(
                    f"Invalid interval combination: {start_field} to {end_field}. "
                    "Day-Time intervals must use DAY, HOUR, MINUTE, or SECOND units only."
                )

            start_val = {
                IntervalTimeUnit.DAY: DAY,
                IntervalTimeUnit.HOUR: HOUR,
                IntervalTimeUnit.MINUTE: MINUTE,
                IntervalTimeUnit.SECOND: SECOND,
            }[start_field]
            end_val = {
                IntervalTimeUnit.DAY: DAY,
                IntervalTimeUnit.HOUR: HOUR,
                IntervalTimeUnit.MINUTE: MINUTE,
                IntervalTimeUnit.SECOND: SECOND,
            }[end_field]
            return DayTimeIntervalType(startField=start_val, endField=end_val)
        else:
            raise UnsupportedFeatureError(
                f"Unsupported interval start field: {start_field}"
            )

    @_convert_type.register(Array)
    def _(self, yads_type: Array) -> DataType:
        # Ignore size parameter
        element_type = self._convert_type(yads_type.element)
        return ArrayType(element_type, True)

    @_convert_type.register(Struct)
    def _(self, yads_type: Struct) -> DataType:
        fields = []
        for yads_field in yads_type.fields:
            with self.conversion_context(field=yads_field.name):
                field_result = self._convert_field(yads_field)
                fields.append(field_result)
        return StructType(fields)

    @_convert_type.register(Map)
    def _(self, yads_type: Map) -> DataType:
        key_type = self._convert_type(yads_type.key)
        value_type = self._convert_type(yads_type.value)
        return MapType(keyType=key_type, valueType=value_type, valueContainsNull=True)

    @_convert_type.register(Void)
    def _(self, yads_type: Void) -> DataType:
        return NullType()

    @_convert_type.register(Variant)
    def _(self, yads_type: Variant) -> DataType:
        return VariantType()

    def _convert_field(self, field: Field) -> StructField:
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
