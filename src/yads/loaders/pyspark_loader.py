"""Load a `YadsSpec` from a `pyspark.sql.types.StructType`.

This loader converts PySpark schemas to yads specifications by building a
normalized dictionary representation and delegating spec construction and
validation to `SpecBuilder`. It preserves column-level nullability and
propagates field metadata when available.

Example:
    >>> from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    >>> from yads.loaders import PySparkLoader
    >>> schema = StructType([
    ...     StructField("id", IntegerType(), nullable=False),
    ...     StructField("name", StringType(), nullable=True),
    ... ])
    >>> loader = PySparkLoader()
    >>> spec = loader.load(schema, name="test.table", version="1.0.0")
    >>> spec.name
    'test.table'
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, Literal

import importlib

from .. import types as ytypes
from ..exceptions import LoaderConfigError, UnsupportedFeatureError, validation_warning
from .._dependencies import ensure_dependency
from .base import BaseLoaderConfig, ConfigurableLoader
from .common import SpecBuilder

ensure_dependency("pyspark", min_version="3.1.1")

T = importlib.import_module("pyspark.sql.types")

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
    normalized dictionary representation and delegating spec construction and
    validation to `SpecBuilder`. It preserves column-level nullability and
    propagates field metadata when available.

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
        super().__init__(self.config)

    def load(
        self,
        schema: StructType,
        *,
        name: str,
        version: str,
        description: str | None = None,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> YadsSpec:
        """Convert the PySpark schema to `YadsSpec`.

        Args:
            schema: Source PySpark StructType schema.
            name: Fully-qualified spec name to assign.
            version: Spec version string.
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

            return SpecBuilder(data).build()

    # %% ---- Field and type conversion -----------------------------------------------
    def _convert_field(self, field: StructField) -> dict[str, Any]:
        """Convert a PySpark StructField to a normalized column definition."""
        field_metadata = dict(field.metadata) if field.metadata else {}
        description = field_metadata.pop("description", None)

        col: dict[str, Any] = {"name": field.name}

        type_def = self._convert_type(field.dataType)
        col.update(type_def)

        if description is not None:
            col["description"] = description
        if field_metadata:
            col["metadata"] = field_metadata
        if field.nullable is False:
            col["constraints"] = {"not_null": True}

        return col

    @singledispatchmethod
    def _convert_type(self, dtype: DataType) -> dict[str, Any]:
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
            return self._get_fallback_type_definition()

        raise UnsupportedFeatureError(f"{error_msg}.")

    @_convert_type.register(T.NullType)
    def _(self, dtype: NullType) -> dict[str, Any]:
        return {"type": "void"}

    @_convert_type.register(T.BooleanType)
    def _(self, dtype: BooleanType) -> dict[str, Any]:
        return {"type": "boolean"}

    @_convert_type.register(T.ByteType)
    def _(self, dtype: ByteType) -> dict[str, Any]:
        return {"type": "integer", "params": {"bits": 8, "signed": True}}

    @_convert_type.register(T.ShortType)
    def _(self, dtype: ShortType) -> dict[str, Any]:
        return {"type": "integer", "params": {"bits": 16, "signed": True}}

    @_convert_type.register(T.IntegerType)
    def _(self, dtype: IntegerType) -> dict[str, Any]:
        return {"type": "integer", "params": {"bits": 32, "signed": True}}

    @_convert_type.register(T.LongType)
    def _(self, dtype: LongType) -> dict[str, Any]:
        return {"type": "integer", "params": {"bits": 64, "signed": True}}

    @_convert_type.register(T.FloatType)
    def _(self, dtype: FloatType) -> dict[str, Any]:
        return {"type": "float", "params": {"bits": 32}}

    @_convert_type.register(T.DoubleType)
    def _(self, dtype: DoubleType) -> dict[str, Any]:
        return {"type": "float", "params": {"bits": 64}}

    @_convert_type.register(T.DecimalType)
    def _(self, dtype: DecimalType) -> dict[str, Any]:
        return {
            "type": "decimal",
            "params": {
                "precision": dtype.precision,
                "scale": dtype.scale,
            },
        }

    @_convert_type.register(T.StringType)
    def _(self, dtype: StringType) -> dict[str, Any]:
        return {"type": "string"}

    @_convert_type.register(T.BinaryType)
    def _(self, dtype: BinaryType) -> dict[str, Any]:
        return {"type": "binary"}

    @_convert_type.register(T.DateType)
    def _(self, dtype: DateType) -> dict[str, Any]:
        return {"type": "date", "params": {"bits": 32}}

    @_convert_type.register(T.TimestampType)
    def _(self, dtype: TimestampType) -> dict[str, Any]:
        return {"type": "timestampltz", "params": {"unit": "ns"}}

    @_convert_type.register(T.ArrayType)
    def _(self, dtype: ArrayType) -> dict[str, Any]:
        with self.load_context(field="<array_element>"):
            elem_def = self._convert_type(dtype.elementType)
        return {"type": "array", "element": elem_def}

    @_convert_type.register(T.MapType)
    def _(self, dtype: MapType) -> dict[str, Any]:
        with self.load_context(field="<map_key>"):
            key_def = self._convert_type(dtype.keyType)
        with self.load_context(field="<map_value>"):
            val_def = self._convert_type(dtype.valueType)
        return {"type": "map", "key": key_def, "value": val_def}

    @_convert_type.register(T.StructType)
    def _(self, dtype: StructType) -> dict[str, Any]:
        fields: list[dict[str, Any]] = []
        for field in dtype.fields:
            with self.load_context(field=field.name):
                field_def = self._convert_field(field)
                fields.append(field_def)
        return {"type": "struct", "fields": fields}

    # Version-gated type registrations for types not available in earlier PySpark versions

    if hasattr(T, "DayTimeIntervalType"):  # Added in pyspark 3.2.0

        @_convert_type.register(T.DayTimeIntervalType)  # type: ignore[misc]
        def _convert_daytime_interval(self, dtype: DayTimeIntervalType) -> dict[str, Any]:
            start_field = dtype.startField
            end_field = dtype.endField
            # Map integer field values to names
            field_names = {0: "DAY", 1: "HOUR", 2: "MINUTE", 3: "SECOND"}
            start_name = field_names.get(start_field, "DAY")
            if end_field is None or start_field == end_field:
                return {
                    "type": "interval",
                    "params": {"interval_start": start_name},
                }
            end_name = field_names.get(end_field, "SECOND")
            return {
                "type": "interval",
                "params": {
                    "interval_start": start_name,
                    "interval_end": end_name,
                },
            }

    if hasattr(T, "CharType"):  # Added in pyspark 3.4.0

        @_convert_type.register(T.CharType)  # type: ignore[misc]
        def _convert_char(self, dtype: CharType) -> dict[str, Any]:
            return {"type": "string", "params": {"length": dtype.length}}

    if hasattr(T, "VarcharType"):  # Added in pyspark 3.4.0

        @_convert_type.register(T.VarcharType)  # type: ignore[misc]
        def _convert_varchar(self, dtype: VarcharType) -> dict[str, Any]:
            return {"type": "string", "params": {"length": dtype.length}}

    if hasattr(T, "TimestampNTZType"):  # Added in pyspark 3.4.0

        @_convert_type.register(T.TimestampNTZType)  # type: ignore[misc]
        def _convert_timestamp_ntz(self, dtype: TimestampNTZType) -> dict[str, Any]:
            return {"type": "timestampntz", "params": {"unit": "ns"}}

    if hasattr(T, "YearMonthIntervalType"):  # Added in pyspark 3.5.0

        @_convert_type.register(T.YearMonthIntervalType)  # type: ignore[misc]
        def _convert_yearmonth_interval(
            self, dtype: YearMonthIntervalType
        ) -> dict[str, Any]:
            start_field = dtype.startField
            end_field = dtype.endField
            # Map integer field values to names
            field_names = {0: "YEAR", 1: "MONTH"}
            start_name = field_names.get(start_field, "YEAR")
            if end_field is None or start_field == end_field:
                return {
                    "type": "interval",
                    "params": {"interval_start": start_name},
                }
            end_name = field_names.get(end_field, "MONTH")
            return {
                "type": "interval",
                "params": {
                    "interval_start": start_name,
                    "interval_end": end_name,
                },
            }

    if hasattr(T, "VariantType"):  # Added in pyspark 4.0.0

        @_convert_type.register(T.VariantType)  # type: ignore[misc]
        def _convert_variant(self, dtype: VariantType) -> dict[str, Any]:
            return {"type": "variant"}

    def _get_fallback_type_definition(self) -> dict[str, Any]:
        if isinstance(self.config.fallback_type, ytypes.Binary):
            binary_type = self.config.fallback_type
            if binary_type.length is not None:
                return {"type": "binary", "params": {"length": binary_type.length}}
            return {"type": "binary"}
        else:  # String (default)
            string_type = self.config.fallback_type
            assert isinstance(string_type, ytypes.String)  # Validated in __post_init__
            if string_type.length is not None:
                return {"type": "string", "params": {"length": string_type.length}}
            return {"type": "string"}
