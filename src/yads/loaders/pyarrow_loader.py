"""Load a `YadsSpec` from a `pyarrow.Schema`.

This loader builds a normalized dictionary from a PyArrow schema and delegates
spec construction and validation to `SpecBuilder`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

import pyarrow as pa  # type: ignore[import-untyped]

from ..exceptions import UnsupportedFeatureError
from .base import BaseLoader
from .common import SpecBuilder

if TYPE_CHECKING:
    from ..spec import YadsSpec


class PyArrowLoader(BaseLoader):
    """Load a `YadsSpec` from a `pyarrow.Schema`.

    The loader converts the Arrow schema to the normalized dictionary format
    expected by `SpecBuilder`. It preserves column-level nullability and
    propagates field and schema metadata when available.
    """

    def load(
        self,
        schema: pa.Schema,
        *,
        name: str,
        version: str,
        description: str | None = None,
    ) -> "YadsSpec":
        """Convert the Arrow schema to `YadsSpec`.

        Args:
            schema: Source Arrow schema.
            name: Fully-qualified spec name to assign.
            version: Spec version string.
            description: Optional human-readable description.

        Returns:
            A validated immutable `YadsSpec` instance.
        """
        data: dict[str, Any] = {
            "name": name,
            "version": version,
            "columns": [self._convert_field(f) for f in schema],
        }

        if description:
            data["description"] = description

        if schema.metadata:
            data["metadata"] = self._decode_key_value_metadata(schema.metadata)

        return SpecBuilder(data).build()

    # %% ---- Field and type conversion -----------------------------------------------
    def _convert_field(self, field: pa.Field) -> dict[str, Any]:
        """Convert an Arrow field to a normalized column definition.

        Returns:
            A dictionary with keys compatible with `SpecBuilder` column parsing.
        """
        field_meta = self._decode_key_value_metadata(field.metadata)

        # Lift description out of metadata if present
        description = field_meta.pop("description", None)

        col: dict[str, Any] = {
            "name": field.name,
        }

        type_def = self._convert_type(field.type, field_name=field.name)
        col.update(type_def)

        if description is not None:
            col["description"] = description
        if field_meta:
            col["metadata"] = field_meta

        # Nullability -> not_null constraint
        if field.nullable is False:
            col["constraints"] = {"not_null": True}

        return col

    def _convert_type(
        self, dtype: pa.DataType, field_name: str | None = None
    ) -> dict[str, Any]:
        """Convert an Arrow data type to a normalized type definition.

        The returned mapping is shaped for `SpecBuilder`:
          - Simple types: {"type": "string_name", "params": {...}}
          - Array: {"type": "array", "element": {<type def>}}
          - Struct: {"type": "struct", "fields": [{<field def>}, ...]}
          - Map: {"type": "map", "key": {<type def>}, "value": {<type def>}}
          - Interval: {"type": "interval", "params": {"interval_start": ...}}

        Args:
            dtype: The Arrow data type to convert.
            field_name: Optional field name for improved error context.
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
        if types.is_string_view(t):
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
        if getattr(types, "is_binary_view", lambda _t: False)(t):
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
            or getattr(types, "is_list_view", lambda _t: False)(t)
            or getattr(types, "is_large_list_view", lambda _t: False)(t)
        ):
            elem_def = self._convert_type(t.value_type, field_name=field_name)
            return {"type": "array", "element": elem_def}

        if types.is_struct(t):
            # t is a StructType; iterate contained pa.Field entries
            fields: list[dict[str, Any]] = [self._convert_field(f) for f in t]
            return {"type": "struct", "fields": fields}

        if types.is_map(t):
            key_def = self._convert_type(t.key_type, field_name=field_name)
            val_def = self._convert_type(t.item_type, field_name=field_name)
            if t.keys_sorted:
                return {
                    "type": "map",
                    "key": key_def,
                    "value": val_def,
                    "params": {"keys_sorted": True},
                }
            return {"type": "map", "key": key_def, "value": val_def}

        # Unsupported Arrow constructs (dictionary, unions, tensors, etc.)
        if types.is_dictionary(t):
            raise UnsupportedFeatureError(
                self._format_error_with_field_context(
                    "Dictionary-encoded types are not supported", field_name
                )
            )
        if getattr(types, "is_run_end_encoded", lambda _t: False)(t):
            raise UnsupportedFeatureError(
                self._format_error_with_field_context(
                    "Run-end encoded types are not supported", field_name
                )
            )
        if getattr(types, "is_fixed_size_list", lambda _t: False)(t):
            elem_def = self._convert_type(t.value_type, field_name=field_name)
            return {"type": "array", "element": elem_def, "params": {"size": t.list_size}}
        if getattr(types, "is_union", lambda _t: False)(t):
            raise UnsupportedFeatureError(
                self._format_error_with_field_context(
                    "Union types are not supported", field_name
                )
            )

        # Canonical extension types supported by checking the typeclass
        # https://arrow.apache.org/docs/format/CanonicalExtensions.html
        if isinstance(t, pa.UuidType):
            return {"type": "uuid"}
        if isinstance(t, pa.JsonType):
            return {"type": "json"}
        if isinstance(t, pa.Bool8Type):
            return {"type": "boolean"}

        raise UnsupportedFeatureError(
            self._format_error_with_field_context(
                f"Unsupported or unknown Arrow type: {t} ({type(t).__name__})", field_name
            )
        )

    # %% ------------- Helpers --------------------------------------------------------
    def _format_error_with_field_context(
        self, message: str, field_name: str | None = None
    ) -> str:
        if field_name:
            return f"{message} for field '{field_name}'."
        return f"{message}."

    @staticmethod
    def _decode_key_value_metadata(
        metadata: Mapping[bytes | str, bytes | str] | None,
    ) -> dict[str, Any]:
        """Decode Arrow KeyValueMetadata to a JSON-friendly dict.

        - Keys are coerced to `str`.
        - Byte values are decoded as UTF-8 when possible.
        - Values that look like JSON are parsed; otherwise left as strings.
        """
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
