"""Type deserialization helpers."""

from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence, cast

from ..exceptions import TypeDefinitionError, UnknownTypeError
from .. import types as ytypes
from .. import spec as yspec

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none, reportUnknownParameterType=none

FieldFactory = Callable[[dict[str, Any]], yspec.Field]


class TypeDeserializer:
    """Parse type definitions into `YadsType` instances."""

    _TYPE_PARSERS: dict[type[ytypes.YadsType], str] = {
        ytypes.Interval: "_parse_interval_type",
        ytypes.Array: "_parse_array_type",
        ytypes.Struct: "_parse_struct_type",
        ytypes.Map: "_parse_map_type",
        ytypes.Tensor: "_parse_tensor_type",
    }

    def __init__(
        self,
        *,
        field_factory: FieldFactory | None = None,
    ) -> None:
        self._field_factory = field_factory
        self._type_aliases = ytypes.TYPE_ALIASES

    def set_field_factory(self, field_factory: FieldFactory) -> None:
        """Assign the factory used to build Field instances in struct types."""
        self._field_factory = field_factory

    def parse(self, type_name: str, type_def: Mapping[str, Any]) -> ytypes.YadsType:
        """Parse a type definition dictionary."""
        type_name_lower = type_name.lower()
        if (alias := self._type_aliases.get(type_name_lower)) is None:
            raise UnknownTypeError(f"Unknown type: '{type_name}'.")

        base_type_class = alias[0]
        parser_method_name = self._TYPE_PARSERS.get(base_type_class)
        if parser_method_name:
            parser_method = getattr(self, parser_method_name)
            return parser_method(dict(type_def))

        final_params = self._get_processed_type_params(type_name, type_def)
        if "unit" in final_params and isinstance(final_params["unit"], str):
            final_params["unit"] = ytypes.TimeUnit(final_params["unit"])
        return base_type_class(**final_params)

    # ---- Helpers -----------------------------------------------------------------
    def _get_processed_type_params(
        self, type_name: str, type_def: Mapping[str, Any]
    ) -> dict[str, Any]:
        type_params = type_def.get("params", {})
        default_params = self._type_aliases[type_name.lower()][1]
        return {**default_params, **type_params}

    def _parse_interval_type(self, type_def: Mapping[str, Any]) -> ytypes.Interval:
        type_name = type_def.get("type", "")
        final_params = self._get_processed_type_params(type_name, type_def)
        if "interval_start" not in final_params:
            raise TypeDefinitionError(
                "Interval type definition must include 'interval_start'."
            )
        final_params["interval_start"] = ytypes.IntervalTimeUnit(
            final_params["interval_start"].upper()
        )
        if end_field_val := final_params.get("interval_end"):
            final_params["interval_end"] = ytypes.IntervalTimeUnit(end_field_val.upper())
        return ytypes.Interval(**final_params)

    def _parse_array_type(self, type_def: Mapping[str, Any]) -> ytypes.Array:
        if "element" not in type_def:
            raise TypeDefinitionError("Array type definition must include 'element'.")

        element_def = type_def["element"]
        if not isinstance(element_def, Mapping) or "type" not in element_def:
            raise TypeDefinitionError(
                "The 'element' of an array must be a dictionary with a 'type' key."
            )
        element_def = cast(dict[str, Any], dict(element_def))
        element_type_name = cast(str, element_def["type"])
        final_params = self._get_processed_type_params(type_def.get("type", ""), type_def)
        return ytypes.Array(
            element=self.parse(element_type_name, element_def),
            **final_params,
        )

    def _parse_struct_type(self, type_def: Mapping[str, Any]) -> ytypes.Struct:
        if not self._field_factory:
            raise TypeDefinitionError(
                "Struct type parsing requires a field factory to be configured."
            )
        if "fields" not in type_def:
            raise TypeDefinitionError("Struct type definition must include 'fields'")
        fields_value = type_def["fields"]
        if not isinstance(fields_value, Sequence):
            raise TypeDefinitionError("Struct 'fields' must be a sequence of objects.")
        struct_fields = [
            self._field_factory(cast(dict[str, Any], field_def))
            for field_def in fields_value
        ]
        return ytypes.Struct(fields=struct_fields)

    def _parse_map_type(self, type_def: Mapping[str, Any]) -> ytypes.Map:
        if "key" not in type_def or "value" not in type_def:
            raise TypeDefinitionError(
                "Map type definition must include 'key' and 'value'."
            )

        key_def = type_def["key"]
        value_def = type_def["value"]
        if not isinstance(key_def, Mapping) or "type" not in key_def:
            raise TypeDefinitionError(
                "Map key definition must be a dictionary that includes 'type'."
            )
        if not isinstance(value_def, Mapping) or "type" not in value_def:
            raise TypeDefinitionError(
                "Map value definition must be a dictionary that includes 'type'."
            )
        key_def = cast(dict[str, Any], dict(key_def))
        value_def = cast(dict[str, Any], dict(value_def))
        final_params = self._get_processed_type_params(type_def.get("type", ""), type_def)
        return ytypes.Map(
            key=self.parse(cast(str, key_def["type"]), key_def),
            value=self.parse(cast(str, value_def["type"]), value_def),
            **final_params,
        )

    def _parse_tensor_type(self, type_def: Mapping[str, Any]) -> ytypes.Tensor:
        if "element" not in type_def:
            raise TypeDefinitionError("Tensor type definition must include 'element'.")

        element_def = type_def["element"]
        if not isinstance(element_def, Mapping) or "type" not in element_def:
            raise TypeDefinitionError(
                "The 'element' of a tensor must be a dictionary with a 'type' key."
            )

        element_def = cast(dict[str, Any], dict(element_def))
        element_type_name = cast(str, element_def["type"])
        final_params = self._get_processed_type_params(type_def.get("type", ""), type_def)

        if "shape" not in final_params:
            raise TypeDefinitionError("Tensor type definition must include 'shape'.")

        raw_shape = final_params["shape"]
        if not isinstance(raw_shape, (list, tuple)):
            raise TypeDefinitionError(
                f"Tensor 'shape' must be a list or tuple, got {type(raw_shape).__name__}."
            )

        raw_shape_sequence = cast(Sequence[Any], raw_shape)
        normalized_shape: list[int] = []
        for raw_dim in raw_shape_sequence:
            if isinstance(raw_dim, int):
                normalized_shape.append(raw_dim)
                continue
            raise TypeDefinitionError("Tensor 'shape' elements must be integers.")

        return ytypes.Tensor(
            element=self.parse(element_type_name, element_def),
            shape=tuple(normalized_shape),
        )
