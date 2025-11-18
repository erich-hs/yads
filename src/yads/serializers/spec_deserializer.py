"""Deserialize dictionaries into `YadsSpec` instances.

This module centralizes the current spec-building logic so it can evolve into
dedicated serializer/deserializer helpers that live alongside the core data
structures.
"""

from __future__ import annotations

import warnings
from typing import Any, Mapping, TypedDict

from ..constraints import CONSTRAINT_EQUIVALENTS
from ..exceptions import SpecParsingError, TypeDefinitionError
from .. import spec as yspec
from .constraint_deserializer import ConstraintDeserializer
from .type_deserializer import TypeDeserializer


# %% ---- Protocols ------------------------------------------------------------------
class _PartitioningOptionalDefinition(TypedDict, total=False):
    transform: str
    transform_args: list[Any]


class PartitioningDefinition(_PartitioningOptionalDefinition):
    column: str


# %% ---- Spec deserializer ----------------------------------------------------------
class SpecDeserializer:
    """Builds and validates a `YadsSpec` from a dictionary.

    This is the single entry point to transform a normalized dictionary
    into a `YadsSpec`. It encapsulates type parsing, constraint parsing,
    storage and partition parsing, and spec-level validations.
    """

    def __init__(
        self,
        *,
        type_deserializer: TypeDeserializer | None = None,
        constraint_deserializer: ConstraintDeserializer | None = None,
    ) -> None:
        self.data: dict[str, Any] | None = None
        self._spec: yspec.YadsSpec | None = None
        self._type_deserializer = type_deserializer or TypeDeserializer()
        self._constraint_deserializer = (
            constraint_deserializer or ConstraintDeserializer()
        )
        self._type_deserializer.set_field_factory(self._parse_field)

    def deserialize(self, data: Mapping[str, Any]) -> yspec.YadsSpec:
        """Build and validate the `YadsSpec` from provided data."""
        self.data = dict(data)
        self._validate_keys(
            self.data,
            allowed_keys={
                "name",
                "version",
                "yads_spec_version",
                "description",
                "external",
                "metadata",
                "storage",
                "partitioned_by",
                "table_constraints",
                "columns",
            },
            required_keys={"name", "columns"},
            context="spec definition",
        )

        # Extract version, default to 1 if not provided (for newly loaded specs)
        version = self.data.get("version", 1)
        # Ensure version is an integer
        if not isinstance(version, int):
            version = int(version)

        # Extract yads_spec_version, default to current spec version
        yads_spec_version = self.data.get("yads_spec_version", yspec.YADS_SPEC_VERSION)

        spec = yspec.YadsSpec(
            name=self.data["name"],
            version=version,
            yads_spec_version=yads_spec_version,
            description=self.data.get("description"),
            external=self.data.get("external", False),
            storage=self._parse_storage(self.data.get("storage")),
            partitioned_by=self._parse_partitioned_by(self.data.get("partitioned_by")),
            table_constraints=self._constraint_deserializer.parse_table_constraints(
                self.data.get("table_constraints")
            ),
            metadata=self.data.get("metadata", {}),
            columns=[self._parse_column(c) for c in self.data["columns"]],
        )
        self._spec = spec
        self._validate_spec()
        return spec

    def _validate_keys(
        self,
        obj: Mapping[str, Any],
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

    # (Type parsing now delegated to `TypeDeserializer`)

    # %% ---- Field/Column parsing ----------------------------------------------------
    def _parse_field(self, field_def: dict[str, Any]) -> yspec.Field:
        self._validate_field_definition(field_def, context="field")
        return yspec.Field(
            name=field_def["name"],
            type=self._type_deserializer.parse(field_def["type"], field_def),
            description=field_def.get("description"),
            metadata=field_def.get("metadata", {}),
            constraints=self._constraint_deserializer.parse_column_constraints(
                field_def.get("constraints")
            ),
        )

    def _parse_column(self, col_def: dict[str, Any]) -> yspec.Column:
        self._validate_field_definition(col_def, context="column")
        return yspec.Column(
            name=col_def["name"],
            type=self._type_deserializer.parse(col_def["type"], col_def),
            description=col_def.get("description"),
            metadata=col_def.get("metadata", {}),
            constraints=self._constraint_deserializer.parse_column_constraints(
                col_def.get("constraints")
            ),
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
        self, partitioned_by_def: list[PartitioningDefinition] | None
    ) -> list[yspec.TransformedColumnReference]:
        if not partitioned_by_def:
            return []

        transformed_columns: list[yspec.TransformedColumnReference] = []
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
            table_constrained_cols: set[str] = set()
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
