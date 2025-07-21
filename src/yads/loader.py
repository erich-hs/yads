from __future__ import annotations

import yaml
import warnings
from typing import Any, Callable

from .constraints import (
    CONSTRAINT_EQUIVALENTS,
    ColumnConstraint,
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    Reference,
    TableConstraint,
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


_COMPLEX_TYPE_PARSERS: dict[str, Callable[[TypeDef], dict[str, Any]]] = {
    "array": _parse_array_type,
    "struct": _parse_struct_type,
    "map": _parse_map_type,
}


def _parse_not_null_constraint(value: Any) -> NotNullConstraint:
    """Parses a not null constraint."""
    if not isinstance(value, bool):
        raise ValueError("The 'not_null' constraint expects a boolean value")
    return NotNullConstraint()


def _parse_primary_key_constraint(value: Any) -> PrimaryKeyConstraint:
    """Parses a primary key constraint."""
    if not isinstance(value, bool):
        raise ValueError("The 'primary_key' constraint expects a boolean value")
    return PrimaryKeyConstraint()


def _parse_default_constraint(value: Any) -> DefaultConstraint:
    """Parses a default constraint."""
    return DefaultConstraint(value=value)


def _parse_foreign_key_constraint(value: Any) -> ForeignKeyConstraint:
    """Parses a foreign key constraint."""
    if not isinstance(value, dict):
        raise ValueError("The 'foreign_key' constraint expects a dictionary")
    if "references" not in value:
        raise ValueError("The 'foreign_key' constraint must specify 'references'")
    return ForeignKeyConstraint(
        name=value.get("name"),
        references=_parse_references(value["references"]),
    )


_COLUMN_CONSTRAINT_PARSERS: dict[str, Callable[[Any], ColumnConstraint]] = {
    "not_null": _parse_not_null_constraint,
    "primary_key": _parse_primary_key_constraint,
    "default": _parse_default_constraint,
    "foreign_key": _parse_foreign_key_constraint,
}


def _parse_constraints(
    constraints_def: dict[str, Any] | None,
) -> list[ColumnConstraint]:
    """Parses constraint definitions, returning a list of column constraint objects."""
    constraints: list[ColumnConstraint] = []
    if not constraints_def:
        return constraints

    for key, value in constraints_def.items():
        if key in _COLUMN_CONSTRAINT_PARSERS:
            constraints.append(_COLUMN_CONSTRAINT_PARSERS[key](value))
        else:
            raise ValueError(f"Unknown column constraint: {key}")

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


def _parse_primary_key_table_constraint(
    const_def: dict[str, Any],
) -> PrimaryKeyTableConstraint:
    """Parses a primary key table constraint."""
    if "columns" not in const_def:
        raise ValueError("Primary key table constraint must specify 'columns'")
    return PrimaryKeyTableConstraint(
        columns=const_def["columns"], name=const_def.get("name")
    )


def _parse_foreign_key_table_constraint(
    const_def: dict[str, Any],
) -> ForeignKeyTableConstraint:
    """Parses a foreign key table constraint."""
    for required_field in ("columns", "references"):
        if required_field not in const_def:
            raise ValueError(
                f"Foreign key table constraint must specify '{required_field}'"
            )
    return ForeignKeyTableConstraint(
        columns=const_def["columns"],
        name=const_def.get("name"),
        references=_parse_references(const_def["references"]),
    )


def _parse_references(references_def: dict[str, Any]) -> Reference:
    """Parses a references dictionary and returns a Reference object."""
    if "table" not in references_def:
        raise ValueError(
            "The 'references' of a foreign key must be a dictionary with a 'table' key"
        )
    return Reference(
        table=references_def["table"], columns=references_def.get("columns")
    )


_TABLE_CONSTRAINT_PARSERS: dict[str, Callable[[Any], TableConstraint]] = {
    "primary_key": _parse_primary_key_table_constraint,
    "foreign_key": _parse_foreign_key_table_constraint,
}


def _parse_table_constraints(
    table_constraints_def: list[dict[str, Any]] | None,
) -> list[TableConstraint]:
    """Parses table constraint definitions, returning a list of constraint objects."""
    if not table_constraints_def:
        return []

    constraints: list[TableConstraint] = []
    for const_def in table_constraints_def:
        if not (constraint_type := const_def.get("type")):
            raise ValueError("Table constraint definition must have a 'type'")

        if parser := _TABLE_CONSTRAINT_PARSERS.get(constraint_type):
            constraints.append(parser(const_def))
        else:
            raise ValueError(f"Unknown table constraint type: {constraint_type}")
    return constraints


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


def _check_for_duplicate_constraint_definitions(spec: SchemaSpec) -> None:
    """
    Warns if equivalent constraints are defined at both the column and table levels.

    This function iterates through a mapping of equivalent constraint types and
    checks for columns that have both types of constraints applied.
    """
    for col_const_type, tbl_const_type in CONSTRAINT_EQUIVALENTS.items():
        # Get columns with the specified column-level constraint
        constrained_cols = {
            c.name
            for c in spec.columns
            if any(isinstance(const, col_const_type) for const in c.constraints)
        }

        # Get columns with the equivalent table-level constraint
        table_constrained_cols = set()
        for const in spec.table_constraints:
            if isinstance(const, tbl_const_type):
                table_constrained_cols.update(const.get_constrained_columns())

        # Find and warn about duplicates
        if duplicates := constrained_cols.intersection(table_constrained_cols):
            warnings.warn(
                f"Columns {sorted(list(duplicates))} have a "
                f"{col_const_type.__name__} defined at both the column and table "
                "level.",
                UserWarning,
                stacklevel=2,
            )


def _check_for_undefined_columns_in_table_constraints(spec: SchemaSpec) -> None:
    """
    Warns if table constraints reference columns that are not defined in the spec.

    This function iterates through all table constraints and checks if the columns
    they apply to exist in the list of defined columns in the schema.
    """
    defined_columns = {c.name for c in spec.columns}
    for constraint in spec.table_constraints:
        constrained_columns = set(constraint.get_constrained_columns())
        if not_defined := constrained_columns - defined_columns:
            constraint_name = (
                getattr(constraint, "name", None)
                or f"of type {type(constraint).__name__}"
            )
            warnings.warn(
                f"Table constraint '{constraint_name}' references undefined columns: "
                f"{sorted(list(not_defined))}",
                UserWarning,
                stacklevel=2,
            )


def from_dict(data: dict[str, Any]) -> SchemaSpec:
    """Instantiates a SchemaSpec from a pre-parsed Python dictionary."""
    for required_field in ("name", "version", "columns"):
        if required_field not in data:
            raise ValueError(f"'{required_field}' is a required field")

    spec = SchemaSpec(
        name=data["name"],
        version=data["version"],
        description=data.get("description"),
        options=_parse_options(data.get("options")),
        properties=_parse_properties(data.get("properties")),
        table_constraints=_parse_table_constraints(data.get("table_constraints")),
        metadata=data.get("metadata", {}),
        columns=[_parse_column(c) for c in data["columns"]],
    )
    _check_for_duplicate_constraint_definitions(spec)
    _check_for_undefined_columns_in_table_constraints(spec)
    return spec


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
