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
from ..exceptions import UnsupportedFeatureError
from .._dependencies import requires_dependency, try_import_optional
import yads.spec as yspec
import yads.types as ytypes

if TYPE_CHECKING:
    from pyspark.sql.types import DataType, StructField, StructType
    from yads.spec import Field


# %% ---- Configuration --------------------------------------------------------------
@dataclass(frozen=True)
class PySparkConverterConfig(BaseConverterConfig):
    """Configuration for PySparkConverter.

    Args:
        mode: Conversion mode. One of "raise" or "coerce". Inherited from
            BaseConverterConfig. Defaults to "coerce".
        ignore_columns: Column names to exclude from conversion. Inherited from
            BaseConverterConfig. Defaults to empty.
        include_columns: If provided, only these columns are included. Inherited
            from BaseConverterConfig. Defaults to None.
        column_overrides: Mapping of column name to a callable that returns a
            custom PySpark field conversion. Inherited from BaseConverterConfig.
            Defaults to empty mapping.
        fallback_type: PySpark data type to use for unsupported types in coerce mode.
            Must be one of: StringType(), BinaryType(), or None. Defaults to None.
    """

    fallback_type: DataType | None = None
    column_overrides: Mapping[str, Callable[[Field, PySparkConverter], StructField]] = (
        field(default_factory=dict)
    )

    def __post_init__(self) -> None:
        super().__post_init__()
        # Validate fallback_type if provided
        if self.fallback_type is not None:
            from pyspark.sql.types import (
                StringType,
                BinaryType,
            )

            valid_fallback_types = (StringType, BinaryType)
            if not isinstance(self.fallback_type, valid_fallback_types):
                raise UnsupportedFeatureError(
                    "fallback_type must be one of: StringType(), BinaryType(), or None. "
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
        with self.conversion_context(mode=mode):
            self._validate_column_filters(spec)
            for col in self._filter_columns(spec):
                with self.conversion_context(field=col.name):
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
        return self.raise_or_coerce(yads_type)

    @_convert_type.register(ytypes.String)
    def _(self, yads_type: ytypes.String) -> DataType:
        from pyspark.sql.types import StringType

        if yads_type.length is not None:
            # VarcharType/CharType types are not supported in PySpark DataFrame schemas
            return self.raise_or_coerce(
                coerce_type=StringType(),
                error_msg=(
                    f"String with fixed length is not supported in PySpark "
                    f"DataFrame schemas for '{self._field_context}'."
                ),
            )

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
                    f"Unsupported Integer bits: {bits}. Expected 8/16/32/64"
                    f" for '{self._field_context}'."
                ) from e
        else:
            # Handle unsigned integers
            if bits == 8:
                return self.raise_or_coerce(
                    coerce_type=ShortType(),
                    error_msg=(
                        f"Unsigned Integer(bits=8) is not supported by PySpark"
                        f" for '{self._field_context}'."
                    ),
                )
            elif bits == 16:
                return self.raise_or_coerce(
                    coerce_type=IntegerType(),
                    error_msg=(
                        f"Unsigned Integer(bits=16) is not supported by PySpark"
                        f" for '{self._field_context}'."
                    ),
                )
            elif bits == 32:
                return self.raise_or_coerce(
                    coerce_type=LongType(),
                    error_msg=(
                        f"Unsigned Integer(bits=32) is not supported by PySpark"
                        f" for '{self._field_context}'."
                    ),
                )
            elif bits == 64:
                return self.raise_or_coerce(
                    coerce_type=DecimalType(20, 0),
                    error_msg=(
                        f"Unsigned Integer(bits=64) is not supported by PySpark"
                        f" for '{self._field_context}'."
                    ),
                )
            else:
                raise UnsupportedFeatureError(
                    f"Unsupported Integer bits: {bits}. Expected 8/16/32/64"
                    f" for '{self._field_context}'."
                )

    @_convert_type.register(ytypes.Float)
    def _(self, yads_type: ytypes.Float) -> DataType:
        from pyspark.sql.types import (
            FloatType,
            DoubleType,
        )

        bits = yads_type.bits or 32

        if bits == 16:
            return self.raise_or_coerce(yads_type, coerce_type=FloatType())
        elif bits == 32:
            return FloatType()
        elif bits == 64:
            return DoubleType()
        else:
            raise UnsupportedFeatureError(
                f"Unsupported Float bits: {bits}. Expected 16/32/64"
                f" for '{self._field_context}'."
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
        if yads_type.length is not None:
            self.raise_or_coerce(
                coerce_type=BinaryType(),
                error_msg=(
                    f"{yads_type} cannot be represented in PySpark; "
                    f"length constraint will be lost for '{self._field_context}'."
                ),
            )
        return BinaryType()

    @_convert_type.register(ytypes.Date)
    def _(self, yads_type: ytypes.Date) -> DataType:
        from pyspark.sql.types import DateType

        # Ignore bit-width parameter
        if yads_type.bits is not None:
            self.raise_or_coerce(
                coerce_type=DateType(),
                error_msg=(
                    f"{yads_type} cannot be represented in PySpark; "
                    f"bits constraint will be lost for '{self._field_context}'."
                ),
            )
        return DateType()

    @_convert_type.register(ytypes.Timestamp)
    def _(self, yads_type: ytypes.Timestamp) -> DataType:
        from pyspark.sql.types import TimestampType

        # Ignore unit parameter
        if yads_type.unit is not None:
            self.raise_or_coerce(
                coerce_type=TimestampType(),
                error_msg=(
                    f"{yads_type} cannot be represented in PySpark; "
                    f"unit constraint will be lost for '{self._field_context}'."
                ),
            )
        return TimestampType()

    @_convert_type.register(ytypes.TimestampTZ)
    def _(self, yads_type: ytypes.TimestampTZ) -> DataType:
        from pyspark.sql.types import TimestampType

        # Ignore unit parameter
        # Ignore tz parameter
        if yads_type.unit is not None or yads_type.tz is not None:
            self.raise_or_coerce(
                coerce_type=TimestampType(),
                error_msg=(
                    f"{yads_type} cannot be represented in PySpark; "
                    f"unit and/or tz constraints will be lost for '{self._field_context}'."
                ),
            )
        return TimestampType()

    @_convert_type.register(ytypes.TimestampLTZ)
    def _(self, yads_type: ytypes.TimestampLTZ) -> DataType:
        from pyspark.sql.types import TimestampType

        # Ignore unit parameter
        if yads_type.unit is not None:
            self.raise_or_coerce(
                coerce_type=TimestampType(),
                error_msg=(
                    f"{yads_type} cannot be represented in PySpark; "
                    f"unit constraint will be lost for '{self._field_context}'."
                ),
            )
        return TimestampType()

    @_convert_type.register(ytypes.TimestampNTZ)
    def _(self, yads_type: ytypes.TimestampNTZ) -> DataType:
        TimestampNTZType, error_msg = self._get_version_gated_type(
            type_name="TimestampNTZType",
            min_version="3.4.0",
            feature_description="TimestampNTZ type",
        )
        if TimestampNTZType is None:
            return self.raise_or_coerce(yads_type, error_msg=error_msg)
        # Ignore unit parameter
        if yads_type.unit is not None:
            self.raise_or_coerce(
                coerce_type=TimestampNTZType(),
                error_msg=(
                    f"{yads_type} cannot be represented in PySpark; "
                    f"unit constraint will be lost for '{self._field_context}'."
                ),
            )
        return TimestampNTZType()

    @_convert_type.register(ytypes.Interval)
    def _(self, yads_type: ytypes.Interval) -> DataType:
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
                    f"Year-Month intervals must use YEAR or MONTH units only"
                    f" for '{self._field_context}'."
                )

            YearMonthIntervalType, error_msg = self._get_version_gated_type(
                type_name="YearMonthIntervalType",
                min_version="3.5.0",
                feature_description="Interval type with year-month units",
            )
            if YearMonthIntervalType is None:
                return self.raise_or_coerce(yads_type, error_msg=error_msg)

            start_val = YEAR if start_field == ytypes.IntervalTimeUnit.YEAR else MONTH
            end_val = YEAR if end_field == ytypes.IntervalTimeUnit.YEAR else MONTH
            return YearMonthIntervalType(start_val, end_val)
        elif start_field in day_time_units:
            # Validate end_field is compatible
            if end_field not in day_time_units:
                raise UnsupportedFeatureError(
                    f"Invalid interval combination: {start_field} to {end_field}. "
                    f"Day-Time intervals must use DAY, HOUR, MINUTE, or SECOND units only"
                    f" for '{self._field_context}'."
                )

            DayTimeIntervalType, error_msg = self._get_version_gated_type(
                type_name="DayTimeIntervalType",
                min_version="3.2.0",
                feature_description="Interval type with day-time units",
            )
            if DayTimeIntervalType is None:
                return self.raise_or_coerce(yads_type, error_msg=error_msg)

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
                f" for '{self._field_context}'."
            )

    @_convert_type.register(ytypes.Array)
    def _(self, yads_type: ytypes.Array) -> DataType:
        from pyspark.sql.types import ArrayType

        # Ignore size parameter
        element_type = self._convert_type(yads_type.element)
        if yads_type.size is not None:
            self.raise_or_coerce(
                coerce_type=ArrayType(element_type, True),
                error_msg=(
                    f"{yads_type} cannot be represented in PySpark; "
                    f"size constraint will be lost for '{self._field_context}'."
                ),
            )
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
        VariantType, error_msg = self._get_version_gated_type(
            type_name="VariantType",
            min_version="4.0.0",
            feature_description="Variant type",
        )
        if VariantType is None:
            return self.raise_or_coerce(yads_type, error_msg=error_msg)
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

    # %% ---- Helpers -----------------------------------------------------------------
    def _get_version_gated_type(
        self,
        *,
        type_name: str,
        min_version: str,
        feature_description: str,
    ) -> tuple[type | None, str | None]:
        """Attempt to import a version-gated PySpark type."""
        context = f"{feature_description} for '{self._field_context}'"

        imported_type, error_msg = try_import_optional(
            "pyspark.sql.types",
            required_import=type_name,
            package_name="pyspark",
            min_version=min_version,
            context=context,
        )

        return imported_type, error_msg
