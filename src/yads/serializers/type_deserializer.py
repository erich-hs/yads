"""Type deserialization helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Callable, Mapping as TypingMapping, TypeVar, cast

from ..exceptions import TypeDefinitionError, UnknownTypeError
from .. import types as ytypes
from .. import spec as yspec

FieldFactory = Callable[[dict[str, Any]], yspec.Field]
TypeParser = Callable[
    [str, Mapping[str, Any], TypingMapping[str, Any], FieldFactory],
    ytypes.YadsType,
]
TypeT = TypeVar("TypeT", bound=ytypes.YadsType)


class TypeDeserializer:
    """Parse type definitions into `YadsType` instances."""

    def __init__(
        self,
        *,
        type_aliases: TypingMapping[str, tuple[type[ytypes.YadsType], dict[str, Any]]]
        | None = None,
        type_parsers: TypingMapping[type[ytypes.YadsType], TypeParser] | None = None,
    ) -> None:
        self._type_aliases = type_aliases or ytypes.TYPE_ALIASES
        self._type_parsers: dict[type[ytypes.YadsType], TypeParser] = (
            dict(type_parsers) if type_parsers is not None else {}
        )
        if not self._type_parsers:
            self._register_default_parsers()

    def register_parser(
        self, target_type: type[ytypes.YadsType], parser: TypeParser
    ) -> None:
        """Register a parser callable for a concrete `YadsType` subclass."""
        self._type_parsers[target_type] = parser

    def parse(
        self,
        type_name: str,
        type_def: Mapping[str, Any],
        *,
        field_factory: FieldFactory,
    ) -> ytypes.YadsType:
        """Parse a type definition dictionary."""
        type_name_lower = type_name.lower()
        if (alias := self._type_aliases.get(type_name_lower)) is None:
            raise UnknownTypeError(f"Unknown type: '{type_name}'.")

        base_type_class, default_params = alias
        parser = self._type_parsers.get(base_type_class)
        normalized_type_def = dict(type_def)
        if parser:
            return parser(
                type_name_lower,
                normalized_type_def,
                default_params,
                field_factory,
            )

        final_params = self._get_processed_type_params(
            type_name=type_name_lower,
            type_def=normalized_type_def,
            default_params=default_params,
        )
        if "unit" in final_params and isinstance(final_params["unit"], str):
            final_params["unit"] = ytypes.TimeUnit(final_params["unit"])
        return self._instantiate_type(
            base_type_class,
            params=final_params,
            type_name=type_name,
        )

    # ---- Helpers -----------------------------------------------------------------
    def _register_default_parsers(self) -> None:
        self.register_parser(ytypes.Interval, self._parse_interval_type)
        self.register_parser(ytypes.Array, self._parse_array_type)
        self.register_parser(ytypes.Struct, self._parse_struct_type)
        self.register_parser(ytypes.Map, self._parse_map_type)
        self.register_parser(ytypes.Tensor, self._parse_tensor_type)

    def _type_label(self, type_def: Mapping[str, Any], default: str) -> str:
        raw_label = type_def.get("type")
        if isinstance(raw_label, str) and raw_label:
            return raw_label
        return default

    def _instantiate_type(
        self,
        type_cls: type[TypeT],
        *,
        params: Mapping[str, Any],
        type_name: str,
    ) -> TypeT:
        try:
            return type_cls(**params)
        except (TypeError, ValueError) as exc:
            raise TypeDefinitionError(
                f"Failed to instantiate type '{type_name}': {exc}"
            ) from exc

    def _get_processed_type_params(
        self,
        *,
        type_name: str,
        type_def: Mapping[str, Any],
        default_params: TypingMapping[str, Any],
    ) -> dict[str, Any]:
        type_params_raw = type_def.get("params")
        if type_params_raw is None:
            type_params_raw = {}
        if not isinstance(type_params_raw, Mapping):
            raise TypeDefinitionError("'params' must be a mapping of parameter names.")

        validated_params: dict[str, Any] = {}
        typed_params = cast(Mapping[Any, Any], type_params_raw)
        for raw_key, raw_value in typed_params.items():
            if not isinstance(raw_key, str):
                raise TypeDefinitionError("'params' must be a mapping of string keys.")
            validated_params[raw_key] = raw_value

        merged_params: dict[str, Any] = {**default_params, **validated_params}
        return merged_params

    def _parse_interval_type(
        self,
        type_name: str,
        type_def: Mapping[str, Any],
        default_params: TypingMapping[str, Any],
        _: FieldFactory,
    ) -> ytypes.Interval:
        final_params = self._get_processed_type_params(
            type_name=type_name,
            type_def=type_def,
            default_params=default_params,
        )
        if "interval_start" not in final_params:
            raise TypeDefinitionError(
                "Interval type definition must include 'interval_start'."
            )
        final_params["interval_start"] = ytypes.IntervalTimeUnit(
            str(final_params["interval_start"]).upper()
        )
        if end_field_val := final_params.get("interval_end"):
            final_params["interval_end"] = ytypes.IntervalTimeUnit(
                str(end_field_val).upper()
            )
        return self._instantiate_type(
            ytypes.Interval,
            params=final_params,
            type_name=self._type_label(type_def, type_name),
        )

    def _parse_array_type(
        self,
        type_name: str,
        type_def: Mapping[str, Any],
        default_params: TypingMapping[str, Any],
        field_factory: FieldFactory,
    ) -> ytypes.Array:
        if "element" not in type_def:
            raise TypeDefinitionError("Array type definition must include 'element'.")

        element_def = type_def["element"]
        if not isinstance(element_def, Mapping):
            raise TypeDefinitionError(
                "The 'element' of an array must be a dictionary with a 'type' key."
            )
        if "type" not in element_def or not isinstance(element_def["type"], str):
            raise TypeDefinitionError(
                "The 'element' definition must include a string 'type' value."
            )
        normalized_element = dict(cast(Mapping[str, Any], element_def))
        element_type_name = cast(str, normalized_element["type"])
        final_params = self._get_processed_type_params(
            type_name=type_name,
            type_def=type_def,
            default_params=default_params,
        )
        array_params: dict[str, Any] = {
            "element": self.parse(
                element_type_name,
                normalized_element,
                field_factory=field_factory,
            ),
            **final_params,
        }
        return self._instantiate_type(
            ytypes.Array,
            params=array_params,
            type_name=self._type_label(type_def, type_name),
        )

    def _parse_struct_type(
        self,
        type_name: str,
        type_def: Mapping[str, Any],
        default_params: TypingMapping[str, Any],
        field_factory: FieldFactory,
    ) -> ytypes.Struct:
        if "fields" not in type_def:
            raise TypeDefinitionError("Struct type definition must include 'fields'.")
        fields_value = type_def["fields"]
        if not isinstance(fields_value, Sequence) or isinstance(
            fields_value, (str, bytes)
        ):
            raise TypeDefinitionError("Struct 'fields' must be a sequence of objects.")
        sequence_fields = cast(Sequence[object], fields_value)
        struct_fields: list[yspec.Field] = []
        for index, field_def in enumerate(sequence_fields):
            if not isinstance(field_def, Mapping):
                raise TypeDefinitionError(
                    f"Struct field at index {index} must be a dictionary."
                )
            struct_fields.append(field_factory(dict(cast(Mapping[str, Any], field_def))))
        return self._instantiate_type(
            ytypes.Struct,
            params={"fields": struct_fields},
            type_name=self._type_label(type_def, type_name),
        )

    def _parse_map_type(
        self,
        type_name: str,
        type_def: Mapping[str, Any],
        default_params: TypingMapping[str, Any],
        field_factory: FieldFactory,
    ) -> ytypes.Map:
        if "key" not in type_def or "value" not in type_def:
            raise TypeDefinitionError(
                "Map type definition must include 'key' and 'value'."
            )

        key_def = type_def["key"]
        value_def = type_def["value"]
        if not isinstance(key_def, Mapping):
            raise TypeDefinitionError(
                "Map key definition must be a dictionary that includes 'type'."
            )
        if not isinstance(value_def, Mapping):
            raise TypeDefinitionError(
                "Map value definition must be a dictionary that includes 'type'."
            )
        if "type" not in key_def or not isinstance(key_def["type"], str):
            raise TypeDefinitionError("Map key definition must include a string 'type'.")
        if "type" not in value_def or not isinstance(value_def["type"], str):
            raise TypeDefinitionError(
                "Map value definition must include a string 'type'."
            )
        key_def_normalized = dict(cast(Mapping[str, Any], key_def))
        value_def_normalized = dict(cast(Mapping[str, Any], value_def))
        final_params = self._get_processed_type_params(
            type_name=type_name,
            type_def=type_def,
            default_params=default_params,
        )
        map_params: dict[str, Any] = {
            "key": self.parse(
                cast(str, key_def_normalized["type"]),
                key_def_normalized,
                field_factory=field_factory,
            ),
            "value": self.parse(
                cast(str, value_def_normalized["type"]),
                value_def_normalized,
                field_factory=field_factory,
            ),
            **final_params,
        }
        return self._instantiate_type(
            ytypes.Map,
            params=map_params,
            type_name=self._type_label(type_def, type_name),
        )

    def _parse_tensor_type(
        self,
        type_name: str,
        type_def: Mapping[str, Any],
        default_params: TypingMapping[str, Any],
        field_factory: FieldFactory,
    ) -> ytypes.Tensor:
        if "element" not in type_def:
            raise TypeDefinitionError("Tensor type definition must include 'element'.")

        element_def = type_def["element"]
        if not isinstance(element_def, Mapping):
            raise TypeDefinitionError(
                "The 'element' of a tensor must be a dictionary with a 'type' key."
            )
        if "type" not in element_def or not isinstance(element_def["type"], str):
            raise TypeDefinitionError(
                "Tensor element definition must include a string 'type'."
            )

        element_def = dict(cast(Mapping[str, Any], element_def))
        element_type_name = cast(str, element_def["type"])
        final_params = self._get_processed_type_params(
            type_name=type_name,
            type_def=type_def,
            default_params=default_params,
        )

        if "shape" not in final_params:
            raise TypeDefinitionError("Tensor type definition must include 'shape'.")

        raw_shape = final_params["shape"]
        if not isinstance(raw_shape, Sequence) or isinstance(raw_shape, (str, bytes)):
            raise TypeDefinitionError("Tensor 'shape' must be a list or tuple of ints.")

        raw_shape_sequence = cast(Sequence[object], raw_shape)
        normalized_shape: list[int] = []
        for index, raw_dim in enumerate(raw_shape_sequence):
            if isinstance(raw_dim, int):
                normalized_shape.append(raw_dim)
                continue
            raise TypeDefinitionError(
                f"Tensor 'shape' elements must be integers (failed at index {index})."
            )

        tensor_params: dict[str, Any] = {
            "element": self.parse(
                element_type_name,
                element_def,
                field_factory=field_factory,
            ),
            **final_params,
            "shape": tuple(normalized_shape),
        }
        return self._instantiate_type(
            ytypes.Tensor,
            params=tensor_params,
            type_name=self._type_label(type_def, type_name),
        )
