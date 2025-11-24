"""Load a `YadsSpec` from a `pyspark.sql.types.StructType`.

This loader converts PySpark schemas to yads specifications by building a
normalized dictionary representation and delegating spec construction to
`yads.spec.from_dict`. It preserves column-level nullability and propagates
field metadata when available.

Example:
    >>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    >>> from yads.loaders import PySparkLoader
    >>> schema = StructType([
    ...     StructField("id", IntegerType(), nullable=False),
    ...     StructField("name", StringType(), nullable=True),
    ... ])
    >>> loader = PySparkLoader()
    >>> spec = loader.load(schema, name="test.table", version=1)
    >>> spec.name
    'test.table'
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, Literal

import pyspark.sql.types as pyspark_types

from .. import spec as yspec
from .. import types as ytypes
from ..constraints import ColumnConstraint, NotNullConstraint
from ..exceptions import LoaderConfigError, UnsupportedFeatureError, validation_warning
from ..serializers import ConstraintSerializer, TypeSerializer
from .._dependencies import ensure_dependency
from .base import BaseLoaderConfig, ConfigurableLoader

ensure_dependency("pyspark", min_version="3.1.1")

if TYPE_CHECKING:
    from ..spec import YadsSpec
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        ByteType,
        CharType,
        DataType,
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


@dataclass(frozen=True)
class PySparkLoaderConfig(BaseLoaderConfig):
    """Configuration for PySparkLoader.

    Args:
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        fallback_type: A yads type to use as fallback when an unsupported
            PySpark type is encountered. Only used when mode is "coerce".
            Must be either String or Binary, or None. Defaults to None.
    """

    fallback_type: ytypes.YadsType | None = None

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()
        if self.fallback_type is not None:
            if not isinstance(self.fallback_type, (ytypes.String, ytypes.Binary)):
                raise LoaderConfigError(
                    "fallback_type must be either String or Binary type, or None."
                )


class PySparkLoader(ConfigurableLoader):
    """Load a `YadsSpec` from a `pyspark.sql.types.StructType`.

    The loader converts PySpark schemas to yads specifications by building a
    normalized dictionary representation and delegating spec construction to
    `yads.spec.from_dict`. It preserves column-level nullability and propagates
    field metadata when available.

    In "raise" mode, incompatible PySpark types raise `UnsupportedFeatureError`.
    In "coerce" mode, the loader attempts to coerce unsupported types to
    compatible fallback types (String or Binary) with warnings.
    """

    def __init__(self, config: PySparkLoaderConfig | None = None) -> None:
        """Initialize the PySparkLoader.

        Args:
            config: Configuration object. If None, uses default PySparkLoaderConfig.
        """
        self.config: PySparkLoaderConfig = config or PySparkLoaderConfig()
        self._type_serializer = TypeSerializer()
        self._type_serializer.bind_field_serializer(self._serialize_field_definition)
        self._constraint_serializer = ConstraintSerializer()
        super().__init__(self.config)

    def load(
        self,
        schema: StructType,
        *,
        name: str,
        version: int = 1,
        description: str | None = None,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> YadsSpec:
        """Convert the PySpark schema to `YadsSpec`.

        Args:
            schema: Source PySpark StructType schema.
            name: Fully-qualified spec name to assign.
            version: Spec version integer. Defaults to 1 for newly loaded specs.
            description: Optional human-readable description.
            mode: Optional override for the loading mode. When not provided, the
                loader's configured mode is used. If provided:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid spec and emit warnings.

        Returns:
            A validated immutable `YadsSpec` instance.
        """
        with self.load_context(mode=mode):
            columns: list[dict[str, Any]] = []
            for field in schema.fields:
                with self.load_context(field=field.name):
                    column_def = self._convert_field(field)
                    columns.append(column_def)

            data: dict[str, Any] = {
                "name": name,
                "version": version,
                "columns": columns,
            }

            if description:
                data["description"] = description

            return yspec.from_dict(data)

    # %% ---- Field and type conversion -----------------------------------------------
    def _convert_field(self, field: StructField) -> dict[str, Any]:
        """Convert a PySpark StructField to a normalized column definition."""
        field_model = self._build_field_model(field)
        return self._serialize_field_definition(field_model)

    def _build_field_model(self, field: StructField) -> yspec.Field:
        metadata = dict(field.metadata) if field.metadata else {}
        description = metadata.pop("description", None)
        constraints: list[ColumnConstraint] = []
        if field.nullable is False:
            constraints.append(NotNullConstraint())
        return yspec.Field(
            name=field.name,
            type=self._convert_type(field.dataType),
            description=description,
            metadata=metadata,
            constraints=constraints,
        )

    def _serialize_field_definition(self, field: yspec.Field) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": field.name}
        payload.update(self._type_serializer.serialize(field.type))
        if field.description:
            payload["description"] = field.description
        if field.metadata:
            payload["metadata"] = dict(field.metadata)
        constraints = self._constraint_serializer.serialize_column_constraints(
            field.constraints
        )
        if constraints:
            payload["constraints"] = constraints
        return payload

    @singledispatchmethod
    def _convert_type(self, dtype: DataType) -> ytypes.YadsType:
        """Convert a PySpark data type to a normalized type definition.

        Maps PySpark types to yads types according to the specification in TODO.md.

        Currently unsupported:
            - CalendarIntervalType
        """
        error_msg = (
            f"PySparkLoader does not support PySpark type: '{dtype}' ({type(dtype).__name__})"
            f" for '{self._current_field_name or '<unknown>'}'"
        )

        if self.config.mode == "coerce":
            if self.config.fallback_type is None:
                raise UnsupportedFeatureError(
                    f"{error_msg}. Specify a fallback_type to enable coercion of unsupported types."
                )
            validation_warning(
                message=f"{error_msg}. The data type will be coerced to {self.config.fallback_type}.",
                filename="yads.loaders.pyspark_loader",
                module=__name__,
            )
            return self.config.fallback_type

        raise UnsupportedFeatureError(f"{error_msg}.")

    @_convert_type.register(pyspark_types.NullType)
    def _(self, dtype: NullType) -> ytypes.YadsType:
        return ytypes.Void()

    @_convert_type.register(pyspark_types.BooleanType)
    def _(self, dtype: BooleanType) -> ytypes.YadsType:
        return ytypes.Boolean()

    @_convert_type.register(pyspark_types.ByteType)
    def _(self, dtype: ByteType) -> ytypes.YadsType:
        return ytypes.Integer(bits=8, signed=True)

    @_convert_type.register(pyspark_types.ShortType)
    def _(self, dtype: ShortType) -> ytypes.YadsType:
        return ytypes.Integer(bits=16, signed=True)

    @_convert_type.register(pyspark_types.IntegerType)
    def _(self, dtype: IntegerType) -> ytypes.YadsType:
        return ytypes.Integer(bits=32, signed=True)

    @_convert_type.register(pyspark_types.LongType)
    def _(self, dtype: LongType) -> ytypes.YadsType:
        return ytypes.Integer(bits=64, signed=True)

    @_convert_type.register(pyspark_types.FloatType)
    def _(self, dtype: FloatType) -> ytypes.YadsType:
        return ytypes.Float(bits=32)

    @_convert_type.register(pyspark_types.DoubleType)
    def _(self, dtype: DoubleType) -> ytypes.YadsType:
        return ytypes.Float(bits=64)

    @_convert_type.register(pyspark_types.DecimalType)
    def _(self, dtype: DecimalType) -> ytypes.YadsType:
        return ytypes.Decimal(precision=dtype.precision, scale=dtype.scale)

    @_convert_type.register(pyspark_types.StringType)
    def _(self, dtype: StringType) -> ytypes.YadsType:
        return ytypes.String()

    @_convert_type.register(pyspark_types.BinaryType)
    def _(self, dtype: BinaryType) -> ytypes.YadsType:
        return ytypes.Binary()

    @_convert_type.register(pyspark_types.DateType)
    def _(self, dtype: DateType) -> ytypes.YadsType:
        return ytypes.Date(bits=32)

    @_convert_type.register(pyspark_types.TimestampType)
    def _(self, dtype: TimestampType) -> ytypes.YadsType:
        return ytypes.TimestampLTZ(unit=ytypes.TimeUnit.NS)

    @_convert_type.register(pyspark_types.ArrayType)
    def _(self, dtype: ArrayType) -> ytypes.YadsType:
        with self.load_context(field="<array_element>"):
            element_type = self._convert_type(dtype.elementType)
        return ytypes.Array(element=element_type)

    @_convert_type.register(pyspark_types.MapType)
    def _(self, dtype: MapType) -> ytypes.YadsType:
        with self.load_context(field="<map_key>"):
            key_type = self._convert_type(dtype.keyType)
        with self.load_context(field="<map_value>"):
            value_type = self._convert_type(dtype.valueType)
        return ytypes.Map(key=key_type, value=value_type)

    @_convert_type.register(pyspark_types.StructType)
    def _(self, dtype: StructType) -> ytypes.YadsType:
        fields: list[yspec.Field] = []
        for field in dtype.fields:
            with self.load_context(field=field.name):
                fields.append(self._build_field_model(field))
        return ytypes.Struct(fields=fields)

    # Version-gated type registrations for types not available in earlier PySpark versions

    if hasattr(pyspark_types, "DayTimeIntervalType"):  # Added in pyspark 3.2.0

        @_convert_type.register(pyspark_types.DayTimeIntervalType)  # type: ignore[misc]
        def _convert_daytime_interval(
            self, dtype: DayTimeIntervalType
        ) -> ytypes.YadsType:
            start_field: int | None = dtype.startField
            end_field: int | None = dtype.endField
            field_names: dict[int, str] = {0: "DAY", 1: "HOUR", 2: "MINUTE", 3: "SECOND"}
            start_key: int = start_field if start_field is not None else 0
            start_name: str = field_names.get(start_key, "DAY")
            if end_field is None:
                return ytypes.Interval(interval_start=ytypes.IntervalTimeUnit[start_name])
            end_key: int = end_field
            if start_key == end_key:
                return ytypes.Interval(interval_start=ytypes.IntervalTimeUnit[start_name])
            end_name: str = field_names.get(end_key, "SECOND")
            return ytypes.Interval(
                interval_start=ytypes.IntervalTimeUnit[start_name],
                interval_end=ytypes.IntervalTimeUnit[end_name],
            )

    if hasattr(pyspark_types, "CharType"):  # Added in pyspark 3.4.0

        @_convert_type.register(pyspark_types.CharType)  # type: ignore[misc]
        def _convert_char(self, dtype: CharType) -> ytypes.YadsType:
            return ytypes.String(length=dtype.length)

    if hasattr(pyspark_types, "VarcharType"):  # Added in pyspark 3.4.0

        @_convert_type.register(pyspark_types.VarcharType)  # type: ignore[misc]
        def _convert_varchar(self, dtype: VarcharType) -> ytypes.YadsType:
            return ytypes.String(length=dtype.length)

    if hasattr(pyspark_types, "TimestampNTZType"):  # Added in pyspark 3.4.0

        @_convert_type.register(pyspark_types.TimestampNTZType)  # type: ignore[misc]
        def _convert_timestamp_ntz(self, dtype: TimestampNTZType) -> ytypes.YadsType:
            return ytypes.TimestampNTZ(unit=ytypes.TimeUnit.NS)

    if hasattr(pyspark_types, "YearMonthIntervalType"):  # Added in pyspark 3.5.0

        @_convert_type.register(pyspark_types.YearMonthIntervalType)  # type: ignore[misc]
        def _convert_yearmonth_interval(
            self, dtype: YearMonthIntervalType
        ) -> ytypes.YadsType:
            start_field: int | None = dtype.startField
            end_field: int | None = dtype.endField
            field_names: dict[int, str] = {0: "YEAR", 1: "MONTH"}
            start_key: int = start_field if start_field is not None else 0
            start_name: str = field_names.get(start_key, "YEAR")
            if end_field is None:
                return ytypes.Interval(interval_start=ytypes.IntervalTimeUnit[start_name])
            end_key: int = end_field
            if start_key == end_key:
                return ytypes.Interval(interval_start=ytypes.IntervalTimeUnit[start_name])
            end_name: str = field_names.get(end_key, "MONTH")
            return ytypes.Interval(
                interval_start=ytypes.IntervalTimeUnit[start_name],
                interval_end=ytypes.IntervalTimeUnit[end_name],
            )

    if hasattr(pyspark_types, "VariantType"):  # Added in pyspark 4.0.0

        @_convert_type.register(pyspark_types.VariantType)  # type: ignore[misc]
        def _convert_variant(self, dtype: VariantType) -> ytypes.YadsType:
            return ytypes.Variant()
