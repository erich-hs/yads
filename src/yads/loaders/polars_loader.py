"""Load a `YadsSpec` from a `polars.Schema`.

This loader converts Polars schemas to yads specifications by building a
normalized dictionary representation and delegating spec construction to
`yads.spec.from_dict`. It preserves column-level nullability where possible and
handles Polars-specific type features.

Example:
    >>> import polars as pl
    >>> from yads.loaders import PolarsLoader
    >>> schema = pl.Schema({
    ...     "id": pl.Int64,
    ...     "name": pl.String,
    ... })
    >>> loader = PolarsLoader()
    >>> spec = loader.load(schema, name="test.table", version=1)
    >>> spec.name
    'test.table'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from .. import spec as yspec
from .. import types as ytypes
from ..exceptions import LoaderConfigError, UnsupportedFeatureError, validation_warning
from ..serializers import ConstraintSerializer, TypeSerializer
from .._dependencies import ensure_dependency
from .base import BaseLoaderConfig, ConfigurableLoader

ensure_dependency("polars", min_version="1.0.0")

import polars as pl  # type: ignore[import-untyped] # noqa: E402

if TYPE_CHECKING:
    from ..spec import YadsSpec

ArrayShapeSequence = tuple[int, ...] | list[int]


@dataclass(frozen=True)
class PolarsLoaderConfig(BaseLoaderConfig):
    """Configuration for PolarsLoader.

    Args:
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        fallback_type: A yads type to use as fallback when an unsupported
            Polars type is encountered. Only used when mode is "coerce".
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


class PolarsLoader(ConfigurableLoader):
    """Load a `YadsSpec` from a `polars.Schema`.

    The loader converts Polars schemas to yads specifications by building a
    normalized dictionary representation and delegating spec construction to
    `yads.spec.from_dict`.

    In "raise" mode, incompatible Polars types raise `UnsupportedFeatureError`.
    In "coerce" mode, the loader attempts to coerce unsupported types to
    compatible fallback types (String or Binary) with warnings.

    Notes:
        - Polars Schema doesn't track nullability at the schema level, so all
          fields are treated as nullable by default.
    """

    def __init__(self, config: PolarsLoaderConfig | None = None) -> None:
        """Initialize the PolarsLoader.

        Args:
            config: Configuration object. If None, uses default PolarsLoaderConfig.
        """
        self.config: PolarsLoaderConfig = config or PolarsLoaderConfig()
        self._type_serializer = TypeSerializer()
        self._type_serializer.bind_field_serializer(self._serialize_field_definition)
        self._constraint_serializer = ConstraintSerializer()
        super().__init__(self.config)

    def load(
        self,
        schema: pl.Schema,
        *,
        name: str,
        version: int = 1,
        description: str | None = None,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> YadsSpec:
        """Convert the Polars schema to `YadsSpec`.

        Args:
            schema: Source Polars schema.
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
            for field_name, dtype in schema.items():
                with self.load_context(field=field_name):
                    column_def = self._convert_field(field_name, dtype)
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
    def _convert_field(self, field_name: str, dtype: pl.DataType) -> dict[str, Any]:
        """Convert a Polars field to a normalized column definition."""
        field_model = self._build_field_model(field_name, dtype)
        return self._serialize_field_definition(field_model)

    def _build_field_model(self, field_name: str, dtype: pl.DataType) -> yspec.Field:
        return yspec.Field(name=field_name, type=self._convert_type(dtype))

    def _serialize_field_definition(self, field: yspec.Field) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": field.name}
        payload.update(self._type_serializer.serialize(field.type))
        constraints = self._constraint_serializer.serialize_column_constraints(
            field.constraints
        )
        if constraints:
            payload["constraints"] = constraints
        if field.metadata:
            payload["metadata"] = dict(field.metadata)
        if field.description:
            payload["description"] = field.description
        return payload

    def _convert_type(self, dtype: pl.DataType) -> ytypes.YadsType:
        """Convert a Polars data type to a `YadsType`."""
        if dtype is pl.Null or isinstance(dtype, pl.Null):
            return ytypes.Void()
        if dtype is pl.Boolean or isinstance(dtype, pl.Boolean):
            return ytypes.Boolean()
        if dtype is pl.Int8 or isinstance(dtype, pl.Int8):
            return ytypes.Integer(bits=8, signed=True)
        if dtype is pl.Int16 or isinstance(dtype, pl.Int16):
            return ytypes.Integer(bits=16, signed=True)
        if dtype is pl.Int32 or isinstance(dtype, pl.Int32):
            return ytypes.Integer(bits=32, signed=True)
        if dtype is pl.Int64 or isinstance(dtype, pl.Int64):
            return ytypes.Integer(bits=64, signed=True)
        if dtype is pl.UInt8 or isinstance(dtype, pl.UInt8):
            return ytypes.Integer(bits=8, signed=False)
        if dtype is pl.UInt16 or isinstance(dtype, pl.UInt16):
            return ytypes.Integer(bits=16, signed=False)
        if dtype is pl.UInt32 or isinstance(dtype, pl.UInt32):
            return ytypes.Integer(bits=32, signed=False)
        if dtype is pl.UInt64 or isinstance(dtype, pl.UInt64):
            return ytypes.Integer(bits=64, signed=False)
        if dtype is pl.Float32 or isinstance(dtype, pl.Float32):
            return ytypes.Float(bits=32)
        if dtype is pl.Float64 or isinstance(dtype, pl.Float64):
            return ytypes.Float(bits=64)
        if (
            dtype is pl.String
            or dtype is pl.Utf8
            or isinstance(dtype, (pl.String, pl.Utf8))
        ):
            return ytypes.String()
        if dtype is pl.Binary or isinstance(dtype, pl.Binary):
            return ytypes.Binary()
        if dtype is pl.Date or isinstance(dtype, pl.Date):
            return ytypes.Date(bits=32)
        if dtype is pl.Time or isinstance(dtype, pl.Time):
            return ytypes.Time(unit=ytypes.TimeUnit.NS, bits=64)

        if isinstance(dtype, pl.Duration):
            time_unit = self._normalize_time_unit(self._extract_duration_unit(dtype))
            return ytypes.Duration(unit=time_unit)

        if isinstance(dtype, pl.Datetime):
            time_unit = self._normalize_time_unit(self._extract_datetime_unit(dtype))
            time_zone = self._extract_datetime_timezone(dtype)
            if time_zone is None:
                return ytypes.Timestamp(unit=time_unit)
            return ytypes.TimestampTZ(unit=time_unit, tz=time_zone)

        if isinstance(dtype, pl.Decimal):
            precision = getattr(dtype, "precision", None)
            scale = getattr(dtype, "scale", None)
            if precision is not None and scale is not None:
                return ytypes.Decimal(precision=precision, scale=scale)
            return ytypes.Decimal()

        if isinstance(dtype, pl.List):
            inner_type = getattr(dtype, "inner", None)
            if inner_type is None:
                raise UnsupportedFeatureError(
                    f"Cannot extract inner type from List type: {dtype} "
                    f"for '{self._current_field_name or '<unknown>'}'"
                )
            with self.load_context(field="<array_element>"):
                element_type = self._convert_type(inner_type)
            return ytypes.Array(element=element_type)

        if isinstance(dtype, pl.Array):
            inner_type = getattr(dtype, "inner", None)
            shape = cast(ArrayShapeSequence | int | None, getattr(dtype, "shape", None))
            if inner_type is None:
                raise UnsupportedFeatureError(
                    f"Cannot extract inner type from Array type: {dtype} "
                    f"for '{self._current_field_name or '<unknown>'}'"
                )

            if shape is None:
                with self.load_context(field="<array_element>"):
                    element_type = self._convert_type(inner_type)
                return ytypes.Array(element=element_type)

            if isinstance(shape, (list, tuple)):
                shape_tuple: tuple[int, ...] = tuple(shape)
            else:
                shape_tuple = (shape,)

            base_inner_type = inner_type
            while isinstance(base_inner_type, pl.Array):
                base_inner_type = getattr(base_inner_type, "inner", None)
                if base_inner_type is None:
                    raise UnsupportedFeatureError(
                        f"Cannot extract base inner type from nested Array type: {dtype} "
                        f"for '{self._current_field_name or '<unknown>'}'"
                    )

            with self.load_context(field="<array_element>"):
                element_type = self._convert_type(base_inner_type)

            if len(shape_tuple) == 1:
                return ytypes.Array(element=element_type, size=shape_tuple[0])
            return ytypes.Tensor(element=element_type, shape=shape_tuple)

        if isinstance(dtype, pl.Struct):
            fields_list = getattr(dtype, "fields", None)
            if fields_list is None:
                raise UnsupportedFeatureError(
                    f"Cannot extract fields from Struct type: {dtype} "
                    f"for '{self._current_field_name or '<unknown>'}'"
                )

            fields: list[yspec.Field] = []
            for pl_field in fields_list:
                field_name = getattr(pl_field, "name", None)
                field_dtype = getattr(pl_field, "dtype", None)
                if field_name is None or field_dtype is None:
                    raise UnsupportedFeatureError(
                        f"Cannot extract field name or dtype from Struct field: {pl_field} "
                        f"for '{self._current_field_name or '<unknown>'}'"
                    )
                with self.load_context(field=field_name):
                    fields.append(self._build_field_model(field_name, field_dtype))
            return ytypes.Struct(fields=fields)

        if hasattr(pl, "Object") and (dtype is pl.Object or isinstance(dtype, pl.Object)):
            return ytypes.Variant()

        if hasattr(pl, "Categorical") and isinstance(dtype, pl.Categorical):
            error_msg = (
                f"PolarsLoader does not support Polars type: '{dtype}' "
                f"({type(dtype).__name__}) for "
                f"'{self._current_field_name or '<unknown>'}'"
            )
            return self._handle_unsupported_type(error_msg)

        if hasattr(pl, "Enum") and isinstance(dtype, pl.Enum):
            error_msg = (
                f"PolarsLoader does not support Polars type: '{dtype}' "
                f"({type(dtype).__name__}) for "
                f"'{self._current_field_name or '<unknown>'}'"
            )
            return self._handle_unsupported_type(error_msg)

        error_msg = (
            f"PolarsLoader does not support Polars type: '{dtype}' "
            f"({type(dtype).__name__}) for "
            f"'{self._current_field_name or '<unknown>'}'"
        )
        return self._handle_unsupported_type(error_msg)

    def _handle_unsupported_type(self, error_msg: str) -> ytypes.YadsType:
        """Handle unsupported types based on mode configuration."""
        if self.config.mode == "coerce":
            if self.config.fallback_type is None:
                raise UnsupportedFeatureError(
                    f"{error_msg}. Specify a fallback_type to enable coercion "
                    "of unsupported types."
                )
            validation_warning(
                message=(
                    f"{error_msg}. The data type will be coerced to "
                    f"{self.config.fallback_type}."
                ),
                filename="yads.loaders.polars_loader",
                module=__name__,
            )
            return self.config.fallback_type

        raise UnsupportedFeatureError(f"{error_msg}.")

    # %% ---- Helpers -----------------------------------------------------------------
    @staticmethod
    def _normalize_time_unit(unit: Any) -> ytypes.TimeUnit:
        if isinstance(unit, ytypes.TimeUnit):
            return unit
        if unit is None:
            return ytypes.TimeUnit.NS
        return ytypes.TimeUnit(str(unit))

    @staticmethod
    def _extract_duration_unit(dtype: pl.DataType) -> str:
        time_unit = getattr(dtype, "time_unit", None)
        if time_unit is None:
            return "ns"
        return str(time_unit)

    @staticmethod
    def _extract_datetime_unit(dtype: pl.DataType) -> str:
        time_unit = getattr(dtype, "time_unit", None)
        if time_unit is None:
            return "ns"
        return str(time_unit)

    @staticmethod
    def _extract_datetime_timezone(dtype: pl.DataType) -> str | None:
        time_zone = getattr(dtype, "time_zone", None)
        if time_zone is None:
            return None
        return str(time_zone)
