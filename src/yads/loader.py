from __future__ import annotations

import warnings
from typing import Any, Callable

import yaml

from .constraints import (
    CONSTRAINT_EQUIVALENTS,
    ColumnConstraint,
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    Reference,
    TableConstraint,
)
from .spec import (
    Field,
    GenerationClause,
    Options,
    SchemaSpec,
    Storage,
    TransformedColumn,
)
from .types import (
    TYPE_ALIASES,
    Array,
    Map,
    Struct,
    Type,
)


class SpecLoader:
    def __init__(self, data: dict[str, Any]):
        self.data = data
        self.spec: SchemaSpec | None = None
        self._column_constraint_parsers: dict[
            str, Callable[[Any], ColumnConstraint]
        ] = {
            "not_null": self._parse_not_null_constraint,
            "primary_key": self._parse_primary_key_constraint,
            "default": self._parse_default_constraint,
            "foreign_key": self._parse_foreign_key_constraint,
            "identity": self._parse_identity_constraint,
        }
        self._table_constraint_parsers: dict[str, Callable[[Any], TableConstraint]] = {
            "primary_key": self._parse_primary_key_table_constraint,
            "foreign_key": self._parse_foreign_key_table_constraint,
        }

    def load(self) -> SchemaSpec:
        for required_field in ("name", "version", "columns"):
            if required_field not in self.data:
                raise ValueError(f"'{required_field}' is a required field")
        self.spec = SchemaSpec(
            name=self.data["name"],
            version=self.data["version"],
            description=self.data.get("description"),
            options=self._parse_options(self.data.get("options")),
            storage=self._parse_storage(self.data.get("storage")),
            partitioned_by=self._parse_partitioned_by(self.data.get("partitioned_by")),
            table_constraints=self._parse_table_constraints(
                self.data.get("table_constraints")
            ),
            metadata=self.data.get("metadata", {}),
            columns=[self._parse_column(c) for c in self.data["columns"]],
        )
        self._validate_spec()
        return self.spec

    def _parse_type(self, type_name: str, type_def: dict[str, Any]) -> Type:
        type_params = type_def.get("params", {})
        type_name_lower = type_name.lower()

        if (alias := TYPE_ALIASES.get(type_name_lower)) is None:
            raise ValueError(f"Unknown type: '{type_name}'")

        base_type_class, default_params = alias
        # Allow explicit params to override the alias's params
        final_params = {**default_params, **type_params}

        if issubclass(base_type_class, Array):
            if "element" not in type_def:
                raise ValueError("Array type definition must include 'element'")
            element_def = type_def["element"]
            if not isinstance(element_def, dict) or "type" not in element_def:
                raise ValueError(
                    "The 'element' of an array must be a dictionary with a 'type' key"
                )
            element_type_name = element_def["type"]
            return Array(element=self._parse_type(element_type_name, element_def))

        if issubclass(base_type_class, Struct):
            if "fields" not in type_def:
                raise ValueError("Struct type definition must include 'fields'")
            return Struct(fields=[self._parse_column(f) for f in type_def["fields"]])

        if issubclass(base_type_class, Map):
            if "key" not in type_def or "value" not in type_def:
                raise ValueError("Map type definition must include 'key' and 'value'")
            key_def = type_def["key"]
            value_def = type_def["value"]
            return Map(
                key=self._parse_type(key_def["type"], key_def),
                value=self._parse_type(value_def["type"], value_def),
            )

        # For simple types, instantiate with any provided params
        return base_type_class(**final_params)

    def _parse_not_null_constraint(self, value: Any) -> NotNullConstraint:
        if not isinstance(value, bool):
            raise ValueError("The 'not_null' constraint expects a boolean value")
        return NotNullConstraint()

    def _parse_primary_key_constraint(self, value: Any) -> PrimaryKeyConstraint:
        if not isinstance(value, bool):
            raise ValueError("The 'primary_key' constraint expects a boolean value")
        return PrimaryKeyConstraint()

    def _parse_default_constraint(self, value: Any) -> DefaultConstraint:
        return DefaultConstraint(value=value)

    def _parse_foreign_key_constraint(self, value: Any) -> ForeignKeyConstraint:
        if not isinstance(value, dict):
            raise ValueError("The 'foreign_key' constraint expects a dictionary")
        if "references" not in value:
            raise ValueError("The 'foreign_key' constraint must specify 'references'")
        return ForeignKeyConstraint(
            name=value.get("name"),
            references=self._parse_references(value["references"]),
        )

    def _parse_identity_constraint(self, value: Any) -> IdentityConstraint:
        if not isinstance(value, dict):
            raise ValueError("The 'identity' constraint expects a dictionary")

        increment = value.get("increment")
        if increment == 0:
            raise ValueError("The 'increment' for an identity constraint cannot be 0")
        return IdentityConstraint(
            always=value.get("always", True),
            start=value.get("start"),
            increment=increment,
        )

    def _parse_constraints(
        self, constraints_def: dict[str, Any] | None
    ) -> list[ColumnConstraint]:
        constraints: list[ColumnConstraint] = []
        if not constraints_def:
            return constraints

        for key, value in constraints_def.items():
            if key in self._column_constraint_parsers:
                constraints.append(self._column_constraint_parsers[key](value))
            else:
                raise ValueError(f"Unknown column constraint: {key}")

        return constraints

    def _parse_generation_clause(
        self, gen_clause_def: dict[str, Any] | None
    ) -> GenerationClause | None:
        if not gen_clause_def:
            return None

        for required_field in ("column", "transform"):
            if required_field not in gen_clause_def:
                raise ValueError(
                    f"'{required_field}' is a required field in a generation clause"
                )

        return GenerationClause(
            column=gen_clause_def["column"],
            transform=gen_clause_def["transform"],
            transform_args=gen_clause_def.get("transform_args", []),
        )

    def _parse_column(self, col_def: dict[str, Any]) -> Field:
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
            type=self._parse_type(type_name, col_def),
            description=col_def.get("description"),
            constraints=self._parse_constraints(col_def.get("constraints")),
            metadata=col_def.get("metadata", {}),
            generated_as=self._parse_generation_clause(col_def.get("generated_as")),
        )

    def _parse_primary_key_table_constraint(
        self, const_def: dict[str, Any]
    ) -> PrimaryKeyTableConstraint:
        if "columns" not in const_def:
            raise ValueError("Primary key table constraint must specify 'columns'")
        return PrimaryKeyTableConstraint(
            columns=const_def["columns"], name=const_def.get("name")
        )

    def _parse_foreign_key_table_constraint(
        self, const_def: dict[str, Any]
    ) -> ForeignKeyTableConstraint:
        for required_field in ("columns", "references"):
            if required_field not in const_def:
                raise ValueError(
                    f"Foreign key table constraint must specify '{required_field}'"
                )
        return ForeignKeyTableConstraint(
            columns=const_def["columns"],
            name=const_def.get("name"),
            references=self._parse_references(const_def["references"]),
        )

    def _parse_references(self, references_def: dict[str, Any]) -> Reference:
        if "table" not in references_def:
            raise ValueError(
                "The 'references' of a foreign key must be a dictionary with a 'table' key"
            )
        return Reference(
            table=references_def["table"], columns=references_def.get("columns")
        )

    def _parse_table_constraints(
        self, table_constraints_def: list[dict[str, Any]] | None
    ) -> list[TableConstraint]:
        if not table_constraints_def:
            return []

        constraints: list[TableConstraint] = []
        for const_def in table_constraints_def:
            if not (constraint_type := const_def.get("type")):
                raise ValueError("Table constraint definition must have a 'type'")

            if parser := self._table_constraint_parsers.get(constraint_type):
                constraints.append(parser(const_def))
            else:
                raise ValueError(f"Unknown table constraint type: {constraint_type}")
        return constraints

    def _parse_options(self, options_def: dict[str, Any] | None) -> Options:
        if not options_def:
            return Options()
        return Options(**options_def)

    def _parse_storage(self, storage_def: dict[str, Any] | None) -> Storage | None:
        if not storage_def:
            return None

        return Storage(**storage_def)

    def _parse_partitioned_by(
        self, partitioned_by_def: list[dict[str, Any]] | None
    ) -> list[TransformedColumn]:
        if not partitioned_by_def:
            return []

        transformed_columns = []
        for pc in partitioned_by_def:
            if "column" not in pc:
                raise ValueError(
                    "Each item in 'partitioned_by' must have a 'column' key"
                )
            transformed_columns.append(
                TransformedColumn(
                    column=pc["column"],
                    transform=pc.get("transform"),
                    transform_args=pc.get("transform_args", []),
                )
            )
        return transformed_columns

    def _validate_spec(self) -> None:
        if not self.spec:
            return
        self._check_for_duplicate_constraint_definitions(self.spec)
        self._check_for_undefined_columns_in_table_constraints(self.spec)
        self._check_for_undefined_columns_in_partitioned_by(self.spec)
        self._check_for_undefined_columns_in_generated_as(self.spec)

    def _check_for_duplicate_constraint_definitions(self, spec: "SchemaSpec") -> None:
        for col_const_type, tbl_const_type in CONSTRAINT_EQUIVALENTS.items():
            constrained_cols = {
                c.name
                for c in spec.columns
                if any(isinstance(const, col_const_type) for const in c.constraints)
            }
            table_constrained_cols = set()
            for const in spec.table_constraints:
                if isinstance(const, tbl_const_type):
                    table_constrained_cols.update(const.get_constrained_columns())
            if duplicates := constrained_cols.intersection(table_constrained_cols):
                warnings.warn(
                    f"Columns {sorted(list(duplicates))} have a "
                    f"{col_const_type.__name__} defined at both the column and table "
                    "level.",
                    UserWarning,
                    stacklevel=2,
                )

    def _check_for_undefined_columns_in_table_constraints(
        self, spec: "SchemaSpec"
    ) -> None:
        for constraint in spec.table_constraints:
            constrained_columns = set(constraint.get_constrained_columns())
            if not_defined := constrained_columns - spec.column_names:
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

    def _check_for_undefined_columns_in_partitioned_by(
        self, spec: "SchemaSpec"
    ) -> None:
        if not_defined := spec.partition_column_names - spec.column_names:
            raise ValueError(
                f"Partition spec references undefined columns: {sorted(list(not_defined))}"
            )

    def _check_for_undefined_columns_in_generated_as(self, spec: "SchemaSpec") -> None:
        for gen_col, source_col in spec.generated_columns.items():
            if source_col not in spec.column_names:
                raise ValueError(
                    f"Generated column '{gen_col}' references undefined column: "
                    f"'{source_col}'"
                )


# Loader functions
def from_dict(data: dict[str, Any]) -> SchemaSpec:
    """Instantiates a SchemaSpec from a pre-parsed Python dictionary."""
    return SpecLoader(data).load()


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
