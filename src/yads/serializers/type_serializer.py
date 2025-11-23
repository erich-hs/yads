"""Type serialization and deserialization helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import MISSING, fields as dataclass_fields, is_dataclass
from enum import Enum
from typing import Any, Callable, Mapping as TypingMapping, TypeVar, cast

from ..exceptions import SpecSerializationError, TypeDefinitionError, UnknownTypeError
from .. import types as ytypes
from .. import spec as yspec

FieldFactory = Callable[[dict[str, Any]], yspec.Field]
FieldSerializer = Callable[[yspec.Field], dict[str, Any]]
TypeParser = Callable[
    [str, Mapping[str, Any], TypingMapping[str, Any], FieldFactory], ytypes.YadsType
]
TypeSerializeHandler = Callable[
    [ytypes.YadsType, dict[str, Any], FieldSerializer | None], dict[str, Any]
]
TypeT = TypeVar("TypeT", bound=ytypes.YadsType)


class TypeSerializer:
    """Serialize `YadsType` instances into dictionary definitions."""

    def __init__(
        self,
        *,
        type_aliases: TypingMapping[str, tuple[type[ytypes.YadsType], dict[str, Any]]]
        | None = None,
    ) -> None:
        self._type_aliases = type_aliases or ytypes.TYPE_ALIASES
        self._canonical_aliases, self._alias_defaults = self._build_alias_metadata()
        self._type_serializers: dict[type[ytypes.YadsType], TypeSerializeHandler] = {}
        self._default_field_serializer: FieldSerializer | None = None
        self._register_default_serializers()

    def bind_field_serializer(self, serializer: FieldSerializer) -> None:
        """Set the callable used to serialize nested `Field` instances."""
        self._default_field_serializer = serializer

    def register_serializer(
        self,
        target_type: type[ytypes.YadsType],
        serializer: TypeSerializeHandler,
    ) -> None:
        """Register a serializer callable for a concrete `YadsType` subclass."""
        self._type_serializers[target_type] = serializer

    def serialize(
        self,
        yads_type: ytypes.YadsType,
        *,
        field_serializer: FieldSerializer | None = None,
    ) -> dict[str, Any]:
        """Serialize a `YadsType` into its dictionary representation."""
        type_name, alias_defaults = self._select_alias(yads_type)
        payload: dict[str, Any] = {"type": type_name}
        params = self._collect_params(yads_type, alias_defaults)
        nested_field_serializer = field_serializer or self._default_field_serializer
        handler = self._type_serializers.get(type(yads_type))
        if handler:
            payload.update(handler(yads_type, params, nested_field_serializer))
        elif params:
            payload["params"] = params
        return payload

    # ---- Serializer registration --------------------------------------------------
    def _register_default_serializers(self) -> None:
        self.register_serializer(ytypes.Array, self._serialize_array)
        self.register_serializer(ytypes.Tensor, self._serialize_tensor)
        self.register_serializer(ytypes.Struct, self._serialize_struct)
        self.register_serializer(ytypes.Map, self._serialize_map)

    # ---- Alias resolution helpers -------------------------------------------------
    def _build_alias_metadata(
        self,
    ) -> tuple[
        dict[type[ytypes.YadsType], str],
        dict[type[ytypes.YadsType], dict[str, Any]],
    ]:
        canonical: dict[type[ytypes.YadsType], str] = {}
        defaults: dict[type[ytypes.YadsType], dict[str, Any]] = {}
        candidates: dict[type[ytypes.YadsType], list[tuple[str, dict[str, Any]]]] = {}
        for alias, (type_cls, default_params) in self._type_aliases.items():
            # Track each alias so we can fall back to the first one if no canonical
            # alias (matching the dataclass name) was provided.
            candidates.setdefault(type_cls, []).append((alias, dict(default_params)))
            canonical_label = type_cls.__name__.lower()
            if alias == canonical_label and type_cls not in canonical:
                canonical[type_cls] = alias
                defaults[type_cls] = dict(default_params)
        for type_cls, alias_list in candidates.items():
            if type_cls not in canonical and alias_list:
                alias, alias_defaults = alias_list[0]
                canonical[type_cls] = alias
                defaults[type_cls] = dict(alias_defaults)
        return canonical, defaults

    def _select_alias(self, yads_type: ytypes.YadsType) -> tuple[str, dict[str, Any]]:
        type_cls = type(yads_type)
        alias = self._canonical_aliases.get(type_cls, type_cls.__name__.lower())
        defaults = self._alias_defaults.get(type_cls, {})
        return alias, defaults

    # ---- Parameter gathering helpers ---------------------------------------------
    def _collect_params(
        self,
        yads_type: ytypes.YadsType,
        alias_defaults: Mapping[str, Any],
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        normalized_alias_defaults = {
            key: self._normalize_param_value(value)
            for key, value in alias_defaults.items()
        }
        data_cls = type(yads_type)
        if not is_dataclass(data_cls):
            raise SpecSerializationError(
                f"Type {data_cls.__name__} must be a dataclass to be serialized."
            )
        for data_field in dataclass_fields(cast(type[Any], data_cls)):
            name = data_field.name
            value = getattr(yads_type, name)
            normalized_value = self._normalize_param_value(value)
            if normalized_value is None:
                continue
            include_param = False
            if name in normalized_alias_defaults:
                # Alias-provided defaults override dataclass defaults when deciding
                # whether a parameter is redundant.
                if normalized_value != normalized_alias_defaults[name]:
                    include_param = True
            elif data_field.default is not MISSING:
                default_value = self._normalize_param_value(data_field.default)
                if normalized_value != default_value:
                    include_param = True
            elif data_field.default_factory is not MISSING:
                default_factory_value = self._normalize_param_value(
                    data_field.default_factory()  # type: ignore[misc]
                )
                if normalized_value != default_factory_value:
                    include_param = True
            else:
                # Required field without defaults must be emitted.
                include_param = True
            if include_param and not self._is_complex_attribute(normalized_value):
                params[name] = normalized_value
        return params

    def _is_complex_attribute(self, value: Any) -> bool:
        if isinstance(value, ytypes.YadsType):
            return True
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            sequence_value = cast(Sequence[object], value)
            # Field sequences are serialized through the field serializer, not as
            # inline `params` entries, so skip them here.
            return all(isinstance(item, yspec.Field) for item in sequence_value)
        return False

    def _normalize_param_value(self, value: Any) -> Any:
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, tuple):
            return list(cast(tuple[Any, ...], value))
        return value

    # ---- Concrete type serializers -----------------------------------------------
    def _serialize_array(
        self,
        yads_type: ytypes.YadsType,
        params: dict[str, Any],
        field_serializer: FieldSerializer | None,
    ) -> dict[str, Any]:
        array_type = cast(ytypes.Array, yads_type)
        payload: dict[str, Any] = {}
        if params:
            payload["params"] = params
        payload["element"] = self.serialize(
            array_type.element, field_serializer=field_serializer
        )
        return payload

    def _serialize_tensor(
        self,
        yads_type: ytypes.YadsType,
        params: dict[str, Any],
        field_serializer: FieldSerializer | None,
    ) -> dict[str, Any]:
        tensor_type = cast(ytypes.Tensor, yads_type)
        payload: dict[str, Any] = {}
        if params:
            payload["params"] = params
        payload["element"] = self.serialize(
            tensor_type.element, field_serializer=field_serializer
        )
        return payload

    def _serialize_struct(
        self,
        yads_type: ytypes.YadsType,
        params: dict[str, Any],
        field_serializer: FieldSerializer | None,
    ) -> dict[str, Any]:
        if field_serializer is None:
            raise SpecSerializationError(
                "Struct serialization requires a bound field serializer."
            )
        struct_type = cast(ytypes.Struct, yads_type)
        payload: dict[str, Any] = {}
        if params:
            payload["params"] = params
        payload["fields"] = [field_serializer(field) for field in struct_type.fields]
        return payload

    def _serialize_map(
        self,
        yads_type: ytypes.YadsType,
        params: dict[str, Any],
        field_serializer: FieldSerializer | None,
    ) -> dict[str, Any]:
        map_type = cast(ytypes.Map, yads_type)
        payload: dict[str, Any] = {}
        if params:
            payload["params"] = params
        payload["key"] = self.serialize(map_type.key, field_serializer=field_serializer)
        payload["value"] = self.serialize(
            map_type.value, field_serializer=field_serializer
        )
        return payload


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

    # ---- Parser registration helpers ---------------------------------------------
    def _register_default_parsers(self) -> None:
        self.register_parser(ytypes.Interval, self._parse_interval_type)
        self.register_parser(ytypes.Array, self._parse_array_type)
        self.register_parser(ytypes.Struct, self._parse_struct_type)
        self.register_parser(ytypes.Map, self._parse_map_type)
        self.register_parser(ytypes.Tensor, self._parse_tensor_type)

    # ---- Generic parsing helpers --------------------------------------------------
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
            # Keep parameters as provided; type-specific parsers handle validation.
            validated_params[raw_key] = raw_value

        merged_params: dict[str, Any] = {**default_params, **validated_params}
        return merged_params

    def _normalize_nested_type_def(
        self,
        value: Any,
        *,
        mapping_message: str,
        missing_type_message: str,
    ) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise TypeDefinitionError(mapping_message)
        normalized = dict(cast(Mapping[str, Any], value))
        if "type" not in normalized or not isinstance(normalized["type"], str):
            raise TypeDefinitionError(missing_type_message)
        return normalized

    # ---- Concrete type parsers ---------------------------------------------------
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
        # Interval enums expect uppercase strings; normalize as part of parsing.
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

        normalized_element = self._normalize_nested_type_def(
            type_def["element"],
            mapping_message=(
                "The 'element' of an array must be a dictionary with a 'type' key."
            ),
            missing_type_message=(
                "The 'element' definition must include a string 'type' value."
            ),
        )
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

        key_def_normalized = self._normalize_nested_type_def(
            type_def["key"],
            mapping_message="Map key definition must be a dictionary that includes 'type'.",
            missing_type_message="Map key definition must include a string 'type'.",
        )
        value_def_normalized = self._normalize_nested_type_def(
            type_def["value"],
            mapping_message=(
                "Map value definition must be a dictionary that includes 'type'."
            ),
            missing_type_message="Map value definition must include a string 'type'.",
        )
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

        element_def = self._normalize_nested_type_def(
            type_def["element"],
            mapping_message=(
                "The 'element' of a tensor must be a dictionary with a 'type' key."
            ),
            missing_type_message="Tensor element definition must include a string 'type'.",
        )
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
