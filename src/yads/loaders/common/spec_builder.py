"""Build a `YadsSpec` from a normalized dictionary.

This module centralizes the logic to turn a parsed specification dictionary
into a validated `YadsSpec`. It contains parsing for complex types and
constraint handling that used to live inside the monolithic loader.
"""

from __future__ import annotations

import warnings
from typing import Any, Protocol

from ...constraints import (
    CONSTRAINT_EQUIVALENTS,
    ColumnConstraint,
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    ForeignKeyReference,
    TableConstraint,
)
from ...exceptions import (
    InvalidConstraintError,
    SpecParsingError,
    TypeDefinitionError,
    UnknownConstraintError,
    UnknownTypeError,
)
from ... import spec as yspec
from ... import types as ytypes


# %% ---- Protocols ------------------------------------------------------------------
class ConstraintParser(Protocol):
    def __call__(self, value: Any) -> ColumnConstraint: ...


class TableConstraintParser(Protocol):
    def __call__(self, const_def: dict[str, Any]) -> TableConstraint: ...


# %% ---- Spec builder ---------------------------------------------------------------
class SpecBuilder:
    """Builds and validates a `YadsSpec` from a dictionary.

    This is the single entry point to transform a normalized dictionary
    into a `YadsSpec`. It encapsulates type parsing, constraint parsing,
    storage and partition parsing, and spec-level validations.
    """

    _TYPE_PARSERS: dict[type, str] = {
        ytypes.Interval: "_parse_interval_type",
        ytypes.Array: "_parse_array_type",
        ytypes.Struct: "_parse_struct_type",
        ytypes.Map: "_parse_map_type",
        ytypes.Tensor: "_parse_tensor_type",
    }

    _COLUMN_CONSTRAINT_PARSERS: dict[str, str] = {
        "not_null": "_parse_not_null_constraint",
        "primary_key": "_parse_primary_key_constraint",
        "default": "_parse_default_constraint",
        "foreign_key": "_parse_foreign_key_constraint",
        "identity": "_parse_identity_constraint",
    }

    _TABLE_CONSTRAINT_PARSERS: dict[str, str] = {
        "primary_key": "_parse_primary_key_table_constraint",
        "foreign_key": "_parse_foreign_key_table_constraint",
    }

    def __init__(self, data: dict[str, Any]):
        self.data = data
        self._spec: yspec.YadsSpec | None = None

    def build(self) -> yspec.YadsSpec:
        """Build and validate the `YadsSpec` from provided data."""
        self._validate_keys(
            self.data,
            allowed_keys={
                "name",
                "version",
                "description",
                "external",
                "metadata",
                "storage",
                "partitioned_by",
                "table_constraints",
                "columns",
            },
            required_keys={"name", "version", "columns"},
            context="spec definition",
        )
        self._spec = yspec.YadsSpec(
            name=self.data["name"],
            version=self.data["version"],
            description=self.data.get("description"),
            external=self.data.get("external", False),
            storage=self._parse_storage(self.data.get("storage")),
            partitioned_by=self._parse_partitioned_by(self.data.get("partitioned_by")),
            table_constraints=self._parse_table_constraints(
                self.data.get("table_constraints")
            ),
            metadata=self.data.get("metadata", {}),
            columns=[self._parse_column(c) for c in self.data["columns"]],
        )
        self._validate_spec()
        return self._spec

    def _validate_keys(
        self,
        obj: dict[str, Any],
        *,
        allowed_keys: set[str],
        required_keys: set[str] | None = None,
        context: str,
    ) -> None:
        """Validate keys of an object against allowed/required sets."""
        unknown = set(obj.keys()) - allowed_keys
        if unknown:
            unknown_sorted = ", ".join(sorted(unknown))
            raise SpecParsingError(f"Unknown key(s) in {context}: {unknown_sorted}.")
        if required_keys:
            missing = required_keys - set(obj.keys())
            if missing:
                missing_sorted = ", ".join(sorted(missing))
                raise SpecParsingError(
                    f"Missing required key(s) in {context}: {missing_sorted}."
                )

    # %% ---- Type parsing ------------------------------------------------------------
    def _get_processed_type_params(
        self, type_name: str, type_def: dict[str, Any]
    ) -> dict[str, Any]:
        """Merge provided params with TYPE_ALIASES defaults for a type name."""
        type_params = type_def.get("params", {})
        default_params = ytypes.TYPE_ALIASES[type_name.lower()][1]
        return {**default_params, **type_params}

    def _parse_type(self, type_name: str, type_def: dict[str, Any]) -> ytypes.YadsType:
        type_name_lower = type_name.lower()

        if (alias := ytypes.TYPE_ALIASES.get(type_name_lower)) is None:
            raise UnknownTypeError(f"Unknown type: '{type_name}'.")

        base_type_class = alias[0]

        # Handle complex types with special parsing requirements using class-level
        # mapping
        if parser_method_name := self._TYPE_PARSERS.get(base_type_class):
            parser_method = getattr(self, parser_method_name)
            return parser_method(type_def)

        # For simple types, get processed params and instantiate
        final_params = self._get_processed_type_params(type_name, type_def)

        # Normalize temporal units to TimeUnit enum when present and applicable
        if "unit" in final_params and isinstance(final_params["unit"], str):
            final_params["unit"] = ytypes.TimeUnit(final_params["unit"])

        return base_type_class(**final_params)

    def _parse_interval_type(self, type_def: dict[str, Any]) -> ytypes.Interval:
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

    def _parse_array_type(self, type_def: dict[str, Any]) -> ytypes.Array:
        if "element" not in type_def:
            raise TypeDefinitionError("Array type definition must include 'element'.")

        element_def = type_def["element"]
        if not isinstance(element_def, dict) or "type" not in element_def:
            raise TypeDefinitionError(
                "The 'element' of an array must be a dictionary with a 'type' key."
            )

        element_type_name = element_def["type"]
        final_params = self._get_processed_type_params(type_def.get("type", ""), type_def)
        return ytypes.Array(
            element=self._parse_type(element_type_name, element_def),
            **final_params,
        )

    def _parse_struct_type(self, type_def: dict[str, Any]) -> ytypes.Struct:
        if "fields" not in type_def:
            raise TypeDefinitionError("Struct type definition must include 'fields'")

        return ytypes.Struct(fields=[self._parse_field(f) for f in type_def["fields"]])

    def _parse_map_type(self, type_def: dict[str, Any]) -> ytypes.Map:
        if "key" not in type_def or "value" not in type_def:
            raise TypeDefinitionError(
                "Map type definition must include 'key' and 'value'."
            )

        key_def = type_def["key"]
        value_def = type_def["value"]
        final_params = self._get_processed_type_params(type_def.get("type", ""), type_def)

        return ytypes.Map(
            key=self._parse_type(key_def["type"], key_def),
            value=self._parse_type(value_def["type"], value_def),
            **final_params,
        )

    def _parse_tensor_type(self, type_def: dict[str, Any]) -> ytypes.Tensor:
        if "element" not in type_def:
            raise TypeDefinitionError("Tensor type definition must include 'element'.")

        element_def = type_def["element"]
        if not isinstance(element_def, dict) or "type" not in element_def:
            raise TypeDefinitionError(
                "The 'element' of a tensor must be a dictionary with a 'type' key."
            )

        element_type_name = element_def["type"]
        final_params = self._get_processed_type_params(type_def.get("type", ""), type_def)

        if "shape" not in final_params:
            raise TypeDefinitionError("Tensor type definition must include 'shape'.")

        shape = final_params["shape"]
        if not isinstance(shape, (list, tuple)):
            raise TypeDefinitionError(
                f"Tensor 'shape' must be a list or tuple, got {type(shape).__name__}."
            )

        return ytypes.Tensor(
            element=self._parse_type(element_type_name, element_def),
            shape=tuple(shape),
        )

    # %% ---- Field/Column parsing ----------------------------------------------------
    def _parse_field(self, field_def: dict[str, Any]) -> yspec.Field:
        self._validate_field_definition(field_def, context="field")
        return yspec.Field(
            name=field_def["name"],
            type=self._parse_type(field_def["type"], field_def),
            description=field_def.get("description"),
            metadata=field_def.get("metadata", {}),
            constraints=self._parse_column_constraints(field_def.get("constraints")),
        )

    def _parse_column(self, col_def: dict[str, Any]) -> yspec.Column:
        self._validate_field_definition(col_def, context="column")
        return yspec.Column(
            name=col_def["name"],
            type=self._parse_type(col_def["type"], col_def),
            description=col_def.get("description"),
            metadata=col_def.get("metadata", {}),
            constraints=self._parse_column_constraints(col_def.get("constraints")),
            generated_as=self._parse_generation_clause(col_def.get("generated_as")),
        )

    def _validate_field_definition(
        self, field_def: dict[str, Any], context: str = "field"
    ) -> None:
        for required_field in ("name", "type"):
            if required_field not in field_def:
                raise SpecParsingError(
                    f"'{required_field}' is a required field in a {context} definition."
                )

        type_name = field_def["type"]
        if not isinstance(type_name, str):
            if type_name is None:
                raise TypeDefinitionError(
                    f"The 'type' of a {context} must be a string. Got None. "
                    f"Use quoted \"null\" or the synonym 'void' instead to specify a void type."
                )
            raise TypeDefinitionError(
                f"The 'type' of a {context} must be a string. Got {type_name!r}."
            )

        # Validate allowed keys based on context
        type_spec_keys = {"type", "params", "element", "fields", "key", "value"}
        common_field_keys = {"name", "description", "metadata", "constraints"}
        if context == "column":
            allowed = common_field_keys | type_spec_keys | {"generated_as"}
        else:  # context == "field"
            allowed = common_field_keys | type_spec_keys

        self._validate_keys(
            field_def,
            allowed_keys=allowed,
            required_keys={"name", "type"},
            context=f"{context} definition",
        )

    # %% ---- Column constraint parsing ----------------------------------------------
    def _parse_not_null_constraint(self, value: Any) -> NotNullConstraint:
        if not isinstance(value, bool):
            raise InvalidConstraintError(
                f"The 'not_null' constraint expects a boolean value. Got {value!r}."
            )
        return NotNullConstraint()

    def _parse_primary_key_constraint(self, value: Any) -> PrimaryKeyConstraint:
        if not isinstance(value, bool):
            raise InvalidConstraintError(
                f"The 'primary_key' constraint expects a boolean value. Got {value!r}."
            )
        return PrimaryKeyConstraint()

    def _parse_default_constraint(self, value: Any) -> DefaultConstraint:
        return DefaultConstraint(value=value)

    def _parse_foreign_key_constraint(self, value: Any) -> ForeignKeyConstraint:
        if not isinstance(value, dict):
            raise InvalidConstraintError(
                f"The 'foreign_key' constraint expects a dictionary. Got {value!r}."
            )
        if "references" not in value:
            raise InvalidConstraintError(
                "The 'foreign_key' constraint must specify 'references'."
            )
        return ForeignKeyConstraint(
            name=value.get("name"),
            references=self._parse_foreign_key_references(value["references"]),
        )

    def _parse_identity_constraint(self, value: Any) -> IdentityConstraint:
        if not isinstance(value, dict):
            raise InvalidConstraintError(
                f"The 'identity' constraint expects a dictionary. Got {value!r}."
            )

        increment = value.get("increment")
        return IdentityConstraint(
            always=value.get("always", True),
            start=value.get("start"),
            increment=increment,
        )

    def _parse_column_constraints(self, constraints_def: Any) -> list[ColumnConstraint]:
        constraints: list[ColumnConstraint] = []
        if constraints_def is None:
            return constraints
        # Ensure constraints are provided as a mapping even if empty
        if not isinstance(constraints_def, dict):
            raise SpecParsingError(
                f"The 'constraints' attribute of a column must be a dictionary. Got {constraints_def!r} of type {type(constraints_def)}."
            )

        for key, value in constraints_def.items():
            if parser_method_name := self._COLUMN_CONSTRAINT_PARSERS.get(key):
                parser_method = getattr(self, parser_method_name)
                # For boolean constraints, only add the constraint if the value is True
                if (
                    key in ("not_null", "primary_key")
                    and isinstance(value, bool)
                    and not value
                ):
                    continue  # Skip adding the constraint when value is False
                constraints.append(parser_method(value))
            else:
                raise UnknownConstraintError(f"Unknown column constraint: {key}.")

        return constraints

    # %% ---- Generation clauses & partitions ----------------------------------------
    def _parse_generation_clause(
        self, gen_clause_def: dict[str, Any] | None
    ) -> yspec.TransformedColumnReference | None:
        if not gen_clause_def:
            return None

        self._validate_keys(
            gen_clause_def,
            allowed_keys={"column", "transform", "transform_args"},
            required_keys={"column", "transform"},
            context="generation clause",
        )

        # For generated columns, transform is required
        if not gen_clause_def["transform"]:
            raise SpecParsingError("'transform' cannot be empty in a generation clause.")

        return yspec.TransformedColumnReference(
            column=gen_clause_def["column"],
            transform=gen_clause_def["transform"],
            transform_args=gen_clause_def.get("transform_args", []),
        )

    def _parse_partitioned_by(
        self, partitioned_by_def: list[dict[str, Any]] | None
    ) -> list[yspec.TransformedColumnReference]:
        if not partitioned_by_def:
            return []

        transformed_columns = []
        for pc in partitioned_by_def:
            self._validate_keys(
                pc,
                allowed_keys={"column", "transform", "transform_args"},
                required_keys={"column"},
                context="partitioned_by item",
            )
            transformed_columns.append(
                yspec.TransformedColumnReference(
                    column=pc["column"],
                    transform=pc.get("transform"),
                    transform_args=pc.get("transform_args", []),
                )
            )
        return transformed_columns

    # %% ---- Table constraint parsing -----------------------------------------------
    def _parse_primary_key_table_constraint(
        self, const_def: dict[str, Any]
    ) -> PrimaryKeyTableConstraint:
        for required_field in ("name", "columns"):
            if required_field not in const_def:
                raise InvalidConstraintError(
                    f"Primary key table constraint must specify '{required_field}'."
                )
        return PrimaryKeyTableConstraint(
            columns=const_def["columns"], name=const_def["name"]
        )

    def _parse_foreign_key_table_constraint(
        self, const_def: dict[str, Any]
    ) -> ForeignKeyTableConstraint:
        for required_field in ("name", "columns", "references"):
            if required_field not in const_def:
                raise InvalidConstraintError(
                    f"Foreign key table constraint must specify '{required_field}'."
                )
        return ForeignKeyTableConstraint(
            columns=const_def["columns"],
            name=const_def["name"],
            references=self._parse_foreign_key_references(const_def["references"]),
        )

    def _parse_foreign_key_references(
        self, references_def: dict[str, Any]
    ) -> ForeignKeyReference:
        if "table" not in references_def:
            raise InvalidConstraintError(
                "The 'references' of a foreign key must be a dictionary with a 'table' key."
            )
        return ForeignKeyReference(
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
                raise InvalidConstraintError(
                    "Table constraint definition must have a 'type'."
                )

            if parser_method_name := self._TABLE_CONSTRAINT_PARSERS.get(constraint_type):
                parser_method = getattr(self, parser_method_name)
                constraints.append(parser_method(const_def))
            else:
                raise UnknownConstraintError(
                    f"Unknown table constraint type: {constraint_type}."
                )
        return constraints

    # %% ---- Storage -----------------------------------------------------------------
    def _parse_storage(self, storage_def: dict[str, Any] | None) -> yspec.Storage | None:
        if not storage_def:
            return None
        self._validate_keys(
            storage_def,
            allowed_keys={"format", "location", "tbl_properties"},
            required_keys=set(),
            context="storage definition",
        )
        return yspec.Storage(**storage_def)

    # %% ---- Post-build validations --------------------------------------------------
    def _validate_spec(self) -> None:
        if not self._spec:
            return
        self._check_for_duplicate_constraint_definitions(self._spec)
        self._check_for_undefined_columns_in_table_constraints(self._spec)
        self._check_for_undefined_columns_in_partitioned_by(self._spec)
        self._check_for_undefined_columns_in_generated_as(self._spec)

    def _check_for_duplicate_constraint_definitions(self, spec: yspec.YadsSpec) -> None:
        for col_const_type, tbl_const_type in CONSTRAINT_EQUIVALENTS.items():
            constrained_cols = {
                c.name
                for c in spec.columns
                if any(isinstance(const, col_const_type) for const in c.constraints)
            }
            table_constrained_cols = set()
            for const in spec.table_constraints:
                if isinstance(const, tbl_const_type):
                    table_constrained_cols.update(const.constrained_columns)
            if duplicates := constrained_cols.intersection(table_constrained_cols):
                warnings.warn(
                    f"Columns {sorted(list(duplicates))} have a "
                    f"{col_const_type.__name__} defined at both the column and table "
                    "level.",
                    UserWarning,
                    stacklevel=2,
                )

    def _check_for_undefined_columns_in_table_constraints(
        self, spec: yspec.YadsSpec
    ) -> None:
        for constraint in spec.table_constraints:
            constrained_columns = set(constraint.constrained_columns)
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
        self, spec: yspec.YadsSpec
    ) -> None:
        if not_defined := spec.partition_column_names - spec.column_names:
            raise SpecParsingError(
                f"Partition spec references undefined columns: {sorted(list(not_defined))}."
            )

    def _check_for_undefined_columns_in_generated_as(self, spec: yspec.YadsSpec) -> None:
        for gen_col, source_col in spec.generated_columns.items():
            if source_col not in spec.column_names:
                raise SpecParsingError(
                    f"Generated column '{gen_col}' references undefined column: '{source_col}'."
                )
