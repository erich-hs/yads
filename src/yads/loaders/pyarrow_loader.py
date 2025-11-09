"""Load a `YadsSpec` from a `pyarrow.Schema`.

This loader converts PyArrow schemas to yads specifications by building a
normalized dictionary representation and delegating spec construction and
validation to `SpecBuilder`. It preserves column-level nullability and
propagates field and schema metadata when available.

Example:
    >>> import pyarrow as pa
    >>> from yads.loaders import PyArrowLoader
    >>> schema = pa.schema([
    ...     pa.field("id", pa.int64(), nullable=False),
    ...     pa.field("name", pa.string()),
    ... ])
    >>> loader = PyArrowLoader()
    >>> spec = loader.load(schema, name="test.table", version="1.0.0")
    >>> spec.name
    'test.table'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, Mapping

from .. import types as ytypes
from ..exceptions import LoaderConfigError, UnsupportedFeatureError, validation_warning
from .._dependencies import ensure_dependency
from .base import BaseLoaderConfig, ConfigurableLoader
from .common import SpecBuilder

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
    normalized dictionary representation and delegating spec construction and
    validation to `SpecBuilder`. It preserves column-level nullability and
    propagates field and schema metadata when available.

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
        super().__init__(self.config)

    def load(
        self,
        schema: pa.Schema,
        *,
        name: str,
        version: str,
        description: str | None = None,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> YadsSpec:
        """Convert the Arrow schema to `YadsSpec`.

        Args:
            schema: Source Arrow schema.
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

            return SpecBuilder(data).build()

    # %% ---- Field and type conversion -----------------------------------------------
    def _convert_field(self, field: pa.Field) -> dict[str, Any]:
        """Convert an Arrow field to a normalized column definition."""
        field_meta = self._decode_key_value_metadata(field.metadata)

        # Lift description out of metadata if present
        description = field_meta.pop("description", None)

        col: dict[str, Any] = {
            "name": field.name,
        }

        type_def = self._convert_type(field.type)
        col.update(type_def)

        if description is not None:
            col["description"] = description
        if field_meta:
            col["metadata"] = field_meta

        # Nullability -> not_null constraint
        if field.nullable is False:
            col["constraints"] = {"not_null": True}

        return col

    def _convert_type(self, dtype: pa.DataType) -> dict[str, Any]:
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
            return {"type": "void"}
        if types.is_boolean(t):
            return {"type": "boolean"}

        # Integers
        if types.is_int8(t):
            return {"type": "integer", "params": {"bits": 8, "signed": True}}
        if types.is_int16(t):
            return {"type": "integer", "params": {"bits": 16, "signed": True}}
        if types.is_int32(t):
            return {"type": "integer", "params": {"bits": 32, "signed": True}}
        if types.is_int64(t):
            return {"type": "integer", "params": {"bits": 64, "signed": True}}
        if types.is_uint8(t):
            return {"type": "integer", "params": {"bits": 8, "signed": False}}
        if types.is_uint16(t):
            return {"type": "integer", "params": {"bits": 16, "signed": False}}
        if types.is_uint32(t):
            return {"type": "integer", "params": {"bits": 32, "signed": False}}
        if types.is_uint64(t):
            return {"type": "integer", "params": {"bits": 64, "signed": False}}

        # Floats
        if types.is_float16(t):
            return {"type": "float", "params": {"bits": 16}}
        if types.is_float32(t):
            return {"type": "float", "params": {"bits": 32}}
        if types.is_float64(t):
            return {"type": "float", "params": {"bits": 64}}

        # Strings / Binary
        if types.is_string(t):
            return {"type": "string"}
        if getattr(types, "is_large_string", lambda _t: False)(t):
            return {"type": "string"}
        if hasattr(types, "is_string_view") and types.is_string_view(
            t
        ):  # Added in pyarrow 16.0.0
            return {"type": "string"}
        if types.is_fixed_size_binary(t):
            # pyarrow.FixedSizeBinaryType exposes byte_width
            return {
                "type": "binary",
                "params": {"length": getattr(t, "byte_width", None)},
            }
        if types.is_binary(t):
            return {"type": "binary"}
        if getattr(types, "is_large_binary", lambda _t: False)(t):
            return {"type": "binary"}
        if hasattr(types, "is_binary_view") and types.is_binary_view(
            t
        ):  # Added in pyarrow 16.0.0
            return {"type": "binary"}

        # Decimal
        if types.is_decimal128(t):
            return {
                "type": "decimal",
                "params": {
                    "precision": t.precision,
                    "scale": t.scale,
                    "bits": 128,
                },
            }
        if types.is_decimal256(t):
            return {
                "type": "decimal",
                "params": {
                    "precision": t.precision,
                    "scale": t.scale,
                    "bits": 256,
                },
            }

        # Date / Time / Timestamp / Duration / Interval
        if types.is_date32(t):
            return {"type": "date", "params": {"bits": 32}}
        if types.is_date64(t):
            return {"type": "date", "params": {"bits": 64}}
        if types.is_time32(t):
            return {"type": "time", "params": {"unit": t.unit, "bits": 32}}
        if types.is_time64(t):
            return {"type": "time", "params": {"unit": t.unit, "bits": 64}}
        if types.is_timestamp(t):
            unit = t.unit
            tz = getattr(t, "tz", None)
            if tz is None:
                return {"type": "timestamp", "params": {"unit": unit}}
            return {"type": "timestamptz", "params": {"unit": unit, "tz": tz}}
        if types.is_duration(t):
            return {"type": "duration", "params": {"unit": t.unit}}
        # Only M/D/N interval exists in Arrow; default to DAY as start unit
        if getattr(types, "is_interval", lambda _t: False)(t):
            return {
                "type": "interval",
                "params": {"interval_start": "DAY"},
            }

        # Complex: Array / Struct / Map
        if (
            types.is_list(t)
            or getattr(types, "is_large_list", lambda _t: False)(t)
            or (
                hasattr(types, "is_list_view") and types.is_list_view(t)
            )  # Added in pyarrow 16.0.0
            or (
                hasattr(types, "is_large_list_view") and types.is_large_list_view(t)
            )  # Added in pyarrow 16.0.0
        ):
            with self.load_context(field="<array_element>"):
                elem_def = self._convert_type(t.value_type)
            return {"type": "array", "element": elem_def}

        if getattr(types, "is_fixed_size_list", lambda _t: False)(t):
            with self.load_context(field="<array_element>"):
                elem_def = self._convert_type(t.value_type)
            return {"type": "array", "element": elem_def, "params": {"size": t.list_size}}

        if types.is_struct(t):
            # t is a StructType; iterate contained pa.Field entries
            fields: list[dict[str, Any]] = []
            for f in t:
                with self.load_context(field=f.name):
                    field_def = self._convert_field(f)
                    fields.append(field_def)
            return {"type": "struct", "fields": fields}

        if types.is_map(t):
            with self.load_context(field="<map_key>"):
                key_def = self._convert_type(t.key_type)
            with self.load_context(field="<map_value>"):
                val_def = self._convert_type(t.item_type)
            if t.keys_sorted:
                return {
                    "type": "map",
                    "key": key_def,
                    "value": val_def,
                    "params": {"keys_sorted": True},
                }
            return {"type": "map", "key": key_def, "value": val_def}

        # Canonical extension types supported by checking the typeclass
        # https://arrow.apache.org/docs/format/CanonicalExtensions.html
        if hasattr(pa, "UuidType") and isinstance(
            t, pa.UuidType
        ):  # Added in pyarrow 18.0.0
            return {"type": "uuid"}
        if hasattr(pa, "JsonType") and isinstance(
            t, pa.JsonType
        ):  # Added in pyarrow 19.0.0
            return {"type": "json"}
        if hasattr(pa, "Bool8Type") and isinstance(
            t, pa.Bool8Type
        ):  # Added in pyarrow 18.0.0
            return {"type": "boolean"}
        if isinstance(t, pa.FixedShapeTensorType):
            with self.load_context(field="<tensor_element>"):
                element_def = self._convert_type(t.value_type)
            return {
                "type": "tensor",
                "element": element_def,
                "params": {"shape": list(t.shape)},
            }

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
            return self._get_fallback_type_definition()

        raise UnsupportedFeatureError(f"{error_msg}.")

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

    # %% ---- Helpers -----------------------------------------------------------------
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
