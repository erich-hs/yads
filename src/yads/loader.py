# src/yads/loader.py
from __future__ import annotations

import yaml
from typing import Any, Callable

from .constraints import (
    BaseConstraint,
    DefaultConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)
from .spec import Field, Options, PartitionColumn, Properties, SchemaSpec
from .types import (
    Array,
    Binary,
    Boolean,
    Date,
    Decimal,
    Float,
    Integer,
    JSON,
    Map,
    String,
    Struct,
    Timestamp,
    TimestampTZ,
    Type,
    UUID,
)

# A dictionary mapping type names to their corresponding classes.
_TYPE_MAP: dict[str, type[Type]] = {
    "string": String,
    "integer": Integer,
    "float": Float,
    "boolean": Boolean,
    "decimal": Decimal,
    "date": Date,
    "timestamp": Timestamp,
    "timestamp_tz": TimestampTZ,
    "binary": Binary,
    "json": JSON,
    "uuid": UUID,
    "array": Array,
    "struct": Struct,
    "map": Map,
}

# A type alias for the dictionary that defines a type's properties.
TypeDef = dict[str, Any]


def _parse_type(type_name: str, type_def: TypeDef) -> Type:
    """
    Parses a type definition from its name and definition dictionary.

    This function looks up the type class in `_TYPE_MAP` and dispatches to
    specialized parser functions for complex types like 'array', 'struct', or 'map'.
    """
    if not (type_class := _TYPE_MAP.get(type_name)):
        raise ValueError(f"Unknown type: '{type_name}'")

    if type_name in _COMPLEX_TYPE_PARSERS:
        params = _COMPLEX_TYPE_PARSERS[type_name](type_def)
    else:
        params = type_def.get("params", {})

    return type_class(**params)


def _parse_array_type(type_def: TypeDef) -> dict[str, Any]:
    """Parses the parameters for an 'array' type."""
    if "element" not in type_def:
        raise ValueError("Array type definition must include 'element'")

    element_def = type_def["element"]
    if not isinstance(element_def, dict) or "type" not in element_def:
        raise ValueError(
            "The 'element' of an array must be a dictionary with a 'type' key"
        )

    element_type_name = element_def["type"]
    return {"element": _parse_type(element_type_name, element_def)}


def _parse_struct_type(type_def: TypeDef) -> dict[str, Any]:
    """Parses the parameters for a 'struct' type."""
    if "fields" not in type_def:
        raise ValueError("Struct type definition must include 'fields'")
    return {"fields": [_parse_column(f) for f in type_def["fields"]]}


def _parse_map_type(type_def: TypeDef) -> dict[str, Any]:
    """Parses the parameters for a 'map' type."""
    if "key" not in type_def or "value" not in type_def:
        raise ValueError("Map type definition must include 'key' and 'value'")

    key_def = type_def["key"]
    value_def = type_def["value"]

    return {
        "key": _parse_type(key_def["type"], key_def),
        "value": _parse_type(value_def["type"], value_def),
    }


# A dictionary mapping complex type names to their parser functions.
_COMPLEX_TYPE_PARSERS: dict[str, Callable[[TypeDef], dict[str, Any]]] = {
    "array": _parse_array_type,
    "struct": _parse_struct_type,
    "map": _parse_map_type,
}


def _parse_constraints(
    constraints_def: dict[str, Any] | None,
) -> list[BaseConstraint]:
    """Parses constraint definitions, returning a list of constraint objects."""
    constraints: list[BaseConstraint] = []
    if not constraints_def:
        return constraints

    if constraints_def.get("not_null"):
        constraints.append(NotNullConstraint())
    if constraints_def.get("primary_key"):
        constraints.append(PrimaryKeyConstraint())
    if "default" in constraints_def:
        constraints.append(DefaultConstraint(constraints_def["default"]))

    return constraints


def _parse_column(col_def: dict[str, Any]) -> Field:
    """Parses a column definition dictionary and returns a Field object."""
    for required_field in ("name", "type"):
        if required_field not in col_def:
            raise ValueError(
                f"'{required_field}' is a required field in a column definition"
            )
    type_name = col_def["type"]

    if not isinstance(type_name, str):
        raise ValueError("The 'type' of a field must be a string")

    return Field(
        name=col_def["name"],
        type=_parse_type(type_name, col_def),
        description=col_def.get("description"),
        constraints=_parse_constraints(col_def.get("constraints")),
        metadata=col_def.get("metadata", {}),
    )


def _parse_options(options_def: dict[str, Any] | None) -> Options:
    """Parses the options dictionary and returns an Options object."""
    if not options_def:
        return Options()
    return Options(**options_def)


def _parse_properties(properties_def: dict[str, Any] | None) -> Properties:
    """Parses the properties dictionary and returns a Properties object."""
    if not properties_def:
        return Properties()

    if "partitioned_by" in properties_def:
        properties_def["partitioned_by"] = [
            PartitionColumn(**pc) for pc in properties_def["partitioned_by"]
        ]

    return Properties(**properties_def)


def from_dict(data: dict[str, Any]) -> SchemaSpec:
    """Instantiates a SchemaSpec from a pre-parsed Python dictionary."""
    for required_field in ("name", "version", "columns"):
        if required_field not in data:
            raise ValueError(f"'{required_field}' is a required field")
    return SchemaSpec(
        name=data["name"],
        version=data["version"],
        description=data.get("description"),
        options=_parse_options(data.get("options")),
        properties=_parse_properties(data.get("properties")),
        metadata=data.get("metadata", {}),
        columns=[_parse_column(c) for c in data["columns"]],
    )


def from_string(content: str) -> SchemaSpec:
    """Parses a YAML string and returns a SchemaSpec."""
    data = yaml.safe_load(content)
    if not isinstance(data, dict):
        raise TypeError("Loaded YAML content did not parse to a dictionary")
    return from_dict(data)


def from_yaml(path: str) -> SchemaSpec:
    """Reads a YAML file from a given path and returns a SchemaSpec object."""
    with open(path) as f:
        content = f.read()
    return from_string(content)
