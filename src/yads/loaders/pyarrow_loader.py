"""Load a `YadsSpec` from a `pyarrow.Schema`.

This loader converts PyArrow schemas to yads specifications by building a
normalized dictionary representation and delegating spec construction to
`yads.spec.from_dict`. It preserves column-level nullability and propagates
field and schema metadata when available.

Example:
    >>> import pyarrow as pa
    >>> from yads.loaders import PyArrowLoader
    >>> schema = pa.schema([
    ...     pa.field("id", pa.int64(), nullable=False),
    ...     pa.field("name", pa.string()),
    ... ])
    >>> loader = PyArrowLoader()
    >>> spec = loader.load(schema, name="test.table", version=1)
    >>> spec.name
    'test.table'
"""

from __future__ import annotations

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none, reportUnknownParameterType=none
# PyArrow typing stubs progress: https://github.com/apache/arrow/pull/47609

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Mapping

from .. import spec as yspec
from .. import types as ytypes
from ..constraints import NotNullConstraint
from ..exceptions import LoaderConfigError, UnsupportedFeatureError, validation_warning
from ..serializers import ConstraintSerializer, TypeSerializer
from .._dependencies import ensure_dependency
from .base import BaseLoaderConfig, ConfigurableLoader

ensure_dependency("pyarrow", min_version="15.0.0")

import pyarrow as pa  # type: ignore[import-untyped] # noqa: E402

if TYPE_CHECKING:
    from ..spec import YadsSpec


@dataclass(frozen=True)
class PyArrowLoaderConfig(BaseLoaderConfig):
    """Configuration for PyArrowLoader.

    Args:
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        fallback_type: A yads type to use as fallback when an unsupported
            PyArrow type is encountered. Only used when mode is "coerce".
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


class PyArrowLoader(ConfigurableLoader):
    """Load a `YadsSpec` from a `pyarrow.Schema`.

    The loader converts PyArrow schemas to yads specifications by building a
    normalized dictionary representation and delegating spec construction to
    `yads.spec.from_dict`. It preserves column-level nullability and propagates
    field and schema metadata when available.

    In "raise" mode, incompatible Arrow types raise `UnsupportedFeatureError`.
    In "coerce" mode, the loader attempts to coerce unsupported types to
    compatible fallback types (String or Binary) with warnings.
    """

    def __init__(self, config: PyArrowLoaderConfig | None = None) -> None:
        """Initialize the PyArrowLoader.

        Args:
            config: Configuration object. If None, uses default PyArrowLoaderConfig.
        """
        self.config: PyArrowLoaderConfig = config or PyArrowLoaderConfig()
        self._type_serializer = TypeSerializer()
        self._type_serializer.bind_field_serializer(self._serialize_field_definition)
        self._constraint_serializer = ConstraintSerializer()
        super().__init__(self.config)

    def load(
        self,
        schema: pa.Schema,
        *,
        name: str,
        version: int = 1,
        description: str | None = None,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> YadsSpec:
        """Convert the Arrow schema to `YadsSpec`.

        Args:
            schema: Source Arrow schema.
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
            for field in schema:
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

            if schema.metadata:
                data["metadata"] = self._decode_key_value_metadata(schema.metadata)

            return yspec.from_dict(data)

    # %% ---- Field and type conversion -----------------------------------------------
    def _convert_field(self, field: pa.Field) -> dict[str, Any]:
        """Convert an Arrow field to a normalized column definition."""
        field_model = self._build_field_model(field)
        return self._serialize_field_definition(field_model)

    def _build_field_model(self, field: pa.Field) -> yspec.Field:
        metadata = self._decode_key_value_metadata(field.metadata)
        description = metadata.pop("description", None)
        constraints = []
        if field.nullable is False:
            constraints.append(NotNullConstraint())
        return yspec.Field(
            name=field.name,
            type=self._convert_type(field.type),
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

    def _convert_type(self, dtype: pa.DataType) -> ytypes.YadsType:
        """Convert an Arrow data type to a normalized type definition.

        Currently unsupported:
            - pa.DictionaryType
            - pa.RunEndEncodedType
            - pa.UnionType
            - pa.DenseUnionType
            - pa.SparseUnionType
        """
        t = dtype
        types = pa.types

        # Null / Boolean
        if types.is_null(t):
            return ytypes.Void()
        if types.is_boolean(t):
            return ytypes.Boolean()

        # Integers
        if types.is_int8(t):
            return ytypes.Integer(bits=8, signed=True)
        if types.is_int16(t):
            return ytypes.Integer(bits=16, signed=True)
        if types.is_int32(t):
            return ytypes.Integer(bits=32, signed=True)
        if types.is_int64(t):
            return ytypes.Integer(bits=64, signed=True)
        if types.is_uint8(t):
            return ytypes.Integer(bits=8, signed=False)
        if types.is_uint16(t):
            return ytypes.Integer(bits=16, signed=False)
        if types.is_uint32(t):
            return ytypes.Integer(bits=32, signed=False)
        if types.is_uint64(t):
            return ytypes.Integer(bits=64, signed=False)

        # Floats
        if types.is_float16(t):
            return ytypes.Float(bits=16)
        if types.is_float32(t):
            return ytypes.Float(bits=32)
        if types.is_float64(t):
            return ytypes.Float(bits=64)

        # Strings / Binary
        if types.is_string(t):
            return ytypes.String()
        if getattr(types, "is_large_string", self._type_predicate_default)(t):
            return ytypes.String()
        if hasattr(types, "is_string_view") and types.is_string_view(
            t
        ):  # Added in pyarrow 16.0.0
            return ytypes.String()
        if types.is_fixed_size_binary(t):
            # pyarrow.FixedSizeBinaryType exposes byte_width
            return ytypes.Binary(length=getattr(t, "byte_width", None))
        if types.is_binary(t):
            return ytypes.Binary()
        if getattr(types, "is_large_binary", self._type_predicate_default)(t):
            return ytypes.Binary()
        if hasattr(types, "is_binary_view") and types.is_binary_view(
            t
        ):  # Added in pyarrow 16.0.0
            return ytypes.Binary()

        # Decimal
        if types.is_decimal128(t):
            return ytypes.Decimal(precision=t.precision, scale=t.scale, bits=128)
        if types.is_decimal256(t):
            return ytypes.Decimal(precision=t.precision, scale=t.scale, bits=256)

        # Date / Time / Timestamp / Duration / Interval
        if types.is_date32(t):
            return ytypes.Date(bits=32)
        if types.is_date64(t):
            return ytypes.Date(bits=64)
        if types.is_time32(t):
            return ytypes.Time(unit=self._normalize_time_unit(t.unit), bits=32)
        if types.is_time64(t):
            return ytypes.Time(unit=self._normalize_time_unit(t.unit), bits=64)
        if types.is_timestamp(t):
            unit = t.unit
            tz = getattr(t, "tz", None)
            if tz is None:
                return ytypes.Timestamp(unit=self._normalize_time_unit(unit))
            return ytypes.TimestampTZ(
                unit=self._normalize_time_unit(unit),
                tz=tz,
            )
        if types.is_duration(t):
            return ytypes.Duration(unit=self._normalize_time_unit(t.unit))
        # Only M/D/N interval exists in Arrow; default to DAY as start unit
        if getattr(types, "is_interval", self._type_predicate_default)(t):
            return ytypes.Interval(interval_start=ytypes.IntervalTimeUnit.DAY)

        # Complex: Array / Struct / Map
        if (
            types.is_list(t)
            or getattr(types, "is_large_list", self._type_predicate_default)(t)
            or (
                hasattr(types, "is_list_view") and types.is_list_view(t)
            )  # Added in pyarrow 16.0.0
            or (
                hasattr(types, "is_large_list_view") and types.is_large_list_view(t)
            )  # Added in pyarrow 16.0.0
        ):
            with self.load_context(field="<array_element>"):
                elem_type = self._convert_type(t.value_type)
            return ytypes.Array(element=elem_type)

        if getattr(types, "is_fixed_size_list", self._type_predicate_default)(t):
            with self.load_context(field="<array_element>"):
                elem_type = self._convert_type(t.value_type)
            return ytypes.Array(element=elem_type, size=t.list_size)

        if types.is_struct(t):
            # t is a StructType; iterate contained pa.Field entries
            fields: list[yspec.Field] = []
            for f in t:
                with self.load_context(field=f.name):
                    fields.append(self._build_field_model(f))
            return ytypes.Struct(fields=fields)

        if types.is_map(t):
            with self.load_context(field="<map_key>"):
                key_type = self._convert_type(t.key_type)
            with self.load_context(field="<map_value>"):
                val_type = self._convert_type(t.item_type)
            if t.keys_sorted:
                return ytypes.Map(key=key_type, value=val_type, keys_sorted=True)
            return ytypes.Map(key=key_type, value=val_type)

        # Canonical extension types supported by checking the typeclass
        # https://arrow.apache.org/docs/format/CanonicalExtensions.html
        if hasattr(pa, "UuidType") and isinstance(
            t, pa.UuidType
        ):  # Added in pyarrow 18.0.0
            return ytypes.UUID()
        if hasattr(pa, "JsonType") and isinstance(
            t, pa.JsonType
        ):  # Added in pyarrow 19.0.0
            return ytypes.JSON()
        if hasattr(pa, "Bool8Type") and isinstance(
            t, pa.Bool8Type
        ):  # Added in pyarrow 18.0.0
            return ytypes.Boolean()
        if isinstance(t, pa.FixedShapeTensorType):
            with self.load_context(field="<tensor_element>"):
                element_type = self._convert_type(t.value_type)
            return ytypes.Tensor(element=element_type, shape=tuple(t.shape))

        error_msg = (
            f"PyArrowLoader does not support PyArrow type: '{t}' ({type(t).__name__})"
            f" for '{self._current_field_name or '<unknown>'}'"
        )
        if self.config.mode == "coerce":
            if self.config.fallback_type is None:
                raise UnsupportedFeatureError(
                    f"{error_msg}. Specify a fallback_type to enable coercion of unsupported types."
                )
            validation_warning(
                message=f"{error_msg}. The data type will be coerced to {self.config.fallback_type}.",
                filename="yads.loaders.pyarrow_loader",
                module=__name__,
            )
            return self.config.fallback_type

        raise UnsupportedFeatureError(f"{error_msg}.")

    @staticmethod
    def _normalize_time_unit(unit: Any) -> ytypes.TimeUnit:
        if isinstance(unit, ytypes.TimeUnit):
            return unit
        if hasattr(unit, "value"):
            normalized = getattr(unit, "value")
        else:
            normalized = unit
        if normalized is None:
            return ytypes.TimeUnit.NS
        return ytypes.TimeUnit(str(normalized))

    # %% ---- Helpers -----------------------------------------------------------------
    @staticmethod
    def _type_predicate_default(_dtype: pa.DataType) -> bool:
        return False

    @staticmethod
    def _decode_key_value_metadata(
        metadata: Mapping[bytes | str, bytes | str] | None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        if not metadata:
            return result

        def _to_str(value: bytes | str) -> str:
            if isinstance(value, bytes):
                try:
                    return value.decode("utf-8")
                except Exception:
                    # Fallback representation
                    return value.decode("utf-8", errors="ignore")
            return value

        import json

        for k, v in metadata.items():
            sk = _to_str(k)
            sv = _to_str(v)
            # Best-effort JSON parsing
            try:
                result[sk] = json.loads(sv)
            except Exception:
                result[sk] = sv

        return result
