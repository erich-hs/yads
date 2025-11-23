"""Serialization and deserialization helpers for `YadsSpec` instances."""

from __future__ import annotations

import warnings
from collections.abc import Mapping, Sequence
from typing import Any, Literal, cast

from ..constraints import CONSTRAINT_EQUIVALENTS
from ..exceptions import SpecParsingError, TypeDefinitionError
from .. import spec as yspec
from .constraint_serializer import ConstraintDeserializer, ConstraintSerializer
from .type_serializer import TypeDeserializer, TypeSerializer


class SpecSerializer:
    """Serialize `YadsSpec` instances into normalized dictionaries."""

    def __init__(
        self,
        *,
        type_serializer: TypeSerializer | None = None,
        constraint_serializer: ConstraintSerializer | None = None,
    ) -> None:
        self._type_serializer = type_serializer or TypeSerializer()
        self._constraint_serializer = constraint_serializer or ConstraintSerializer()

    def serialize(self, spec: yspec.YadsSpec) -> dict[str, Any]:
        """Serialize the provided spec into a dictionary."""
        serialized: dict[str, Any] = {
            "name": spec.name,
            "version": spec.version,
            "yads_spec_version": spec.yads_spec_version,
            "columns": [self._serialize_column(column) for column in spec.columns],
        }
        if spec.description:
            serialized["description"] = spec.description
        if spec.external:
            serialized["external"] = True
        if spec.metadata:
            serialized["metadata"] = dict(spec.metadata)
        if spec.storage:
            storage_dict = self._serialize_storage(spec.storage)
            if storage_dict:
                serialized["storage"] = storage_dict
        if spec.partitioned_by:
            serialized["partitioned_by"] = [
                self._serialize_transformed_column_reference(ref)
                for ref in spec.partitioned_by
            ]
        table_constraints = self._constraint_serializer.serialize_table_constraints(
            spec.table_constraints
        )
        if table_constraints:
            serialized["table_constraints"] = table_constraints
        return serialized

    # ---- Column and Field serialization helpers ----------------------------------
    def _serialize_column(self, column: yspec.Column) -> dict[str, Any]:
        column_dict = self._serialize_field_definition(column)
        if column.generated_as:
            column_dict["generated_as"] = self._serialize_transformed_column_reference(
                column.generated_as
            )
        return column_dict

    def _serialize_field_definition(self, field: yspec.Field) -> dict[str, Any]:
        field_dict: dict[str, Any] = {"name": field.name}
        type_payload = self._type_serializer.serialize(
            field.type, field_serializer=self._serialize_field_definition
        )
        # Merge the type payload directly so callers share identical structure for
        # nested fields, arrays, and other recursive elements.
        field_dict.update(type_payload)
        if field.description:
            field_dict["description"] = field.description
        if field.metadata:
            field_dict["metadata"] = dict(field.metadata)
        constraints = self._constraint_serializer.serialize_column_constraints(
            field.constraints
        )
        if constraints:
            field_dict["constraints"] = constraints
        return field_dict

    # ---- Storage serialization helpers -------------------------------------------
    def _serialize_storage(self, storage: yspec.Storage) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if storage.format:
            payload["format"] = storage.format
        if storage.location:
            payload["location"] = storage.location
        if storage.tbl_properties:
            payload["tbl_properties"] = dict(storage.tbl_properties)
        return payload

    # ---- Transformed Column serialization helpers --------------------------------
    def _serialize_transformed_column_reference(
        self, reference: yspec.TransformedColumnReference
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"column": reference.column}
        if reference.transform:
            payload["transform"] = reference.transform
        if reference.transform_args:
            payload["transform_args"] = list(reference.transform_args)
        return payload


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
        self._type_deserializer = type_deserializer or TypeDeserializer()
        self._constraint_deserializer = (
            constraint_deserializer or ConstraintDeserializer()
        )

    def deserialize(self, data: Mapping[str, Any]) -> yspec.YadsSpec:
        """Build and validate the `YadsSpec` from provided data."""
        normalized_data = dict(data)
        self._validate_keys(
            normalized_data,
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

        name_value = normalized_data["name"]
        if not isinstance(name_value, str) or not name_value.strip():
            raise SpecParsingError("'name' must be a non-empty string.")

        # Extract version, default to 1 if not provided (for newly loaded specs)
        version_value = normalized_data.get("version", 1)
        try:
            version = int(version_value)
        except (TypeError, ValueError) as exc:
            raise SpecParsingError(
                "'version' must be an integer when specified."
            ) from exc
        # Guard against YAML booleans sneaking through since bool subclasses int.
        if isinstance(version_value, bool):
            raise SpecParsingError("'version' must be an integer when specified.")
        if version < 1:
            raise SpecParsingError("'version' must be a positive integer.")

        # Extract yads_spec_version, default to current spec version
        yads_spec_version = normalized_data.get(
            "yads_spec_version", yspec.YADS_SPEC_VERSION
        )
        if not isinstance(yads_spec_version, str) or not yads_spec_version.strip():
            raise SpecParsingError("'yads_spec_version' must be a non-empty string.")

        external_value = normalized_data.get("external", False)
        if not isinstance(external_value, bool):
            raise SpecParsingError("'external' must be a boolean when specified.")

        spec = yspec.YadsSpec(
            name=name_value,
            version=version,
            yads_spec_version=yads_spec_version,
            description=normalized_data.get("description"),
            external=external_value,
            storage=self._parse_storage(normalized_data.get("storage")),
            partitioned_by=self._parse_partitioned_by(
                normalized_data.get("partitioned_by")
            ),
            table_constraints=self._constraint_deserializer.parse_table_constraints(
                normalized_data.get("table_constraints")
            ),
            metadata=self._parse_metadata(
                normalized_data.get("metadata"), context="spec metadata"
            ),
            columns=self._parse_columns(normalized_data["columns"]),
        )
        self._validate_spec(spec)
        return spec

    # ---- Generic validation helpers ----------------------------------------------
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
            unknown_sorted = ", ".join(sorted(str(key) for key in unknown))
            raise SpecParsingError(f"Unknown key(s) in {context}: {unknown_sorted}.")
        if required_keys:
            missing = required_keys - set(obj.keys())
            if missing:
                missing_sorted = ", ".join(sorted(missing))
                raise SpecParsingError(
                    f"Missing required key(s) in {context}: {missing_sorted}."
                )

    # ---- Column and Field parsing helpers ------------------------------------------
    def _parse_columns(self, raw_columns: Any) -> list[yspec.Column]:
        normalized_columns = self._ensure_mapping_sequence(
            raw_columns, context="columns definition"
        )
        return [self._parse_column(col_def) for col_def in normalized_columns]

    def _parse_field(self, field_def: dict[str, Any]) -> yspec.Field:
        base_kwargs = self._build_field_arguments(field_def, context="field")
        return yspec.Field(**base_kwargs)

    def _parse_column(self, col_def: dict[str, Any]) -> yspec.Column:
        base_kwargs = self._build_field_arguments(col_def, context="column")
        return yspec.Column(
            **base_kwargs,
            generated_as=self._parse_generation_clause(col_def.get("generated_as")),
        )

    def _build_field_arguments(
        self, field_def: dict[str, Any], *, context: Literal["field", "column"]
    ) -> dict[str, Any]:
        self._validate_field_definition(field_def, context=context)
        metadata = self._parse_metadata(field_def.get("metadata"), context=context)
        return {
            "name": field_def["name"],
            # Feed the entire field definition to the type deserializer so nested
            # structures (arrays, structs, etc.) reuse the same recursion path.
            "type": self._type_deserializer.parse(
                field_def["type"],
                field_def,
                field_factory=self._parse_field,
            ),
            "description": field_def.get("description"),
            "metadata": metadata,
            "constraints": self._constraint_deserializer.parse_column_constraints(
                field_def.get("constraints")
            ),
        }

    def _validate_field_definition(
        self, field_def: dict[str, Any], context: str = "field"
    ) -> None:
        for required_field in ("name", "type"):
            if required_field not in field_def:
                raise SpecParsingError(
                    f"'{required_field}' is a required field in a {context} definition."
                )

        name_value = field_def["name"]
        if not isinstance(name_value, str) or not name_value:
            raise SpecParsingError(
                f"The 'name' of a {context} must be a non-empty string."
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

    # ---- Generation clause & partition helpers ------------------------------------
    def _parse_generation_clause(
        self, gen_clause_def: object | None
    ) -> yspec.TransformedColumnReference | None:
        if gen_clause_def is None:
            return None
        if not isinstance(gen_clause_def, Mapping):
            raise SpecParsingError(
                "Generated column definition must be a mapping when provided."
            )
        clause_mapping = cast(Mapping[str, Any], gen_clause_def)
        return self._parse_transformed_column_reference(
            clause_mapping,
            context="generation clause",
            transform_required=True,
        )

    def _parse_partitioned_by(
        self, partitioned_by_def: object | None
    ) -> list[yspec.TransformedColumnReference]:
        if partitioned_by_def is None:
            return []
        if isinstance(partitioned_by_def, (str, bytes)):
            raise SpecParsingError("'partitioned_by' must be a sequence of mappings.")
        if not isinstance(partitioned_by_def, Sequence):
            raise SpecParsingError("'partitioned_by' must be a sequence of mappings.")
        partition_sequence = cast(Sequence[object], partitioned_by_def)

        transformed_columns: list[yspec.TransformedColumnReference] = []
        for index, pc in enumerate(partition_sequence):
            if not isinstance(pc, Mapping):
                raise SpecParsingError(
                    f"Partition definition at index {index} must be a mapping."
                )
            partition_mapping = cast(Mapping[str, Any], pc)
            # Partition clauses mirror generated columns but permit passthrough
            # columns, so reuse the same helper with relaxed requirements.
            transformed_columns.append(
                self._parse_transformed_column_reference(
                    partition_mapping,
                    context="partitioned_by item",
                    transform_required=False,
                )
            )
        return transformed_columns

    def _parse_transformed_column_reference(
        self,
        clause_mapping: Mapping[str, Any],
        *,
        context: str,
        transform_required: bool,
    ) -> yspec.TransformedColumnReference:
        required_keys = {"column"}
        if transform_required:
            required_keys.add("transform")
        # Generated columns require explicit transforms, but partitions allow plain
        # column references; tweak validation accordingly.
        self._validate_keys(
            clause_mapping,
            allowed_keys={"column", "transform", "transform_args"},
            required_keys=required_keys,
            context=context,
        )

        column_value = clause_mapping["column"]
        if not isinstance(column_value, str):
            raise SpecParsingError(f"'column' in {context} must be a string.")

        transform_value = clause_mapping.get("transform")
        normalized_transform: str | None
        if transform_required:
            if not isinstance(transform_value, str):
                raise SpecParsingError(f"'transform' in {context} must be a string.")
            if not transform_value:
                raise SpecParsingError(
                    "'transform' cannot be empty in a generation clause."
                )
            normalized_transform = transform_value
        elif transform_value is not None and not isinstance(transform_value, str):
            raise SpecParsingError(
                f"'transform' in {context} must be a string when specified."
            )
        else:
            normalized_transform = (
                transform_value if isinstance(transform_value, str) else None
            )

        transform_args = self._parse_transform_args(
            clause_mapping.get("transform_args"), context=context
        )

        return yspec.TransformedColumnReference(
            column=column_value,
            transform=normalized_transform,
            transform_args=transform_args,
        )

    # ---- Storage & metadata helpers ------------------------------------------------
    def _parse_storage(self, storage_def: object | None) -> yspec.Storage | None:
        if storage_def is None:
            return None
        if not isinstance(storage_def, Mapping):
            raise SpecParsingError("Storage definition must be a mapping when provided.")
        storage_mapping = cast(Mapping[str, Any], storage_def)
        self._validate_keys(
            storage_mapping,
            allowed_keys={"format", "location", "tbl_properties"},
            required_keys=set(),
            context="storage definition",
        )
        normalized: dict[str, Any] = dict(storage_mapping)
        if "format" in normalized and not isinstance(normalized["format"], str):
            raise SpecParsingError("'storage.format' must be a string when specified.")
        if "location" in normalized and not isinstance(normalized["location"], str):
            raise SpecParsingError("'storage.location' must be a string when specified.")
        tbl_properties_value = normalized.get("tbl_properties")
        if tbl_properties_value is not None:
            if not isinstance(tbl_properties_value, Mapping):
                raise SpecParsingError(
                    "'storage.tbl_properties' must be a mapping of strings."
                )
            normalized["tbl_properties"] = self._parse_string_dict(
                cast(Mapping[Any, Any], tbl_properties_value),
                context="'storage.tbl_properties'",
            )
        return yspec.Storage(**normalized)

    def _parse_metadata(self, raw_metadata: Any, *, context: str) -> dict[str, Any]:
        if raw_metadata is None:
            return {}
        if not isinstance(raw_metadata, Mapping):
            raise SpecParsingError(
                f"Metadata for {context} must be a mapping of string keys."
            )
        metadata_mapping = cast(Mapping[Any, Any], raw_metadata)
        normalized: dict[str, Any] = {}
        for key, value in metadata_mapping.items():
            if not isinstance(key, str):
                raise SpecParsingError(
                    f"Metadata keys for {context} must be strings (got {key!r})."
                )
            normalized[key] = value
        return normalized

    # ---- Miscellaneous parsing helpers --------------------------------------------
    def _parse_transform_args(self, args: Any, *, context: str) -> list[Any]:
        if args is None:
            return []
        if not isinstance(args, Sequence) or isinstance(args, (str, bytes)):
            raise SpecParsingError(
                f"'transform_args' in {context} must be provided as a list."
            )
        sequence_args = cast(Sequence[Any], args)
        return list(sequence_args)

    def _parse_string_dict(
        self, value: Mapping[Any, Any], *, context: str
    ) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for key, val in value.items():
            if not isinstance(key, str) or not isinstance(val, str):
                raise SpecParsingError(
                    f"{context} must only contain string keys and values."
                )
            normalized[key] = val
        return normalized

    def _ensure_mapping_sequence(
        self, value: Any, *, context: str
    ) -> list[dict[str, Any]]:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise SpecParsingError(f"{context} must be a sequence of mappings.")
        sequence_value = cast(Sequence[object], value)
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(sequence_value):
            if not isinstance(item, Mapping):
                raise SpecParsingError(
                    f"Entry at index {index} in {context} must be a mapping."
                )
            normalized.append(dict(cast(Mapping[str, Any], item)))
        return normalized

    # ---- Post-build validations ----------------------------------------------------
    @staticmethod
    def _validate_spec(spec: yspec.YadsSpec) -> None:
        SpecDeserializer._check_for_duplicate_constraint_definitions(spec)
        SpecDeserializer._check_for_undefined_columns_in_table_constraints(spec)
        SpecDeserializer._check_for_undefined_columns_in_partitioned_by(spec)
        SpecDeserializer._check_for_undefined_columns_in_generated_as(spec)

    @staticmethod
    def _check_for_duplicate_constraint_definitions(spec: yspec.YadsSpec) -> None:
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

    @staticmethod
    def _check_for_undefined_columns_in_table_constraints(
        spec: yspec.YadsSpec,
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

    @staticmethod
    def _check_for_undefined_columns_in_partitioned_by(
        spec: yspec.YadsSpec,
    ) -> None:
        if not_defined := spec.partition_column_names - spec.column_names:
            raise SpecParsingError(
                f"Partition spec references undefined columns: {sorted(list(not_defined))}."
            )

    @staticmethod
    def _check_for_undefined_columns_in_generated_as(spec: yspec.YadsSpec) -> None:
        for gen_col, source_col in spec.generated_columns.items():
            if source_col not in spec.column_names:
                raise SpecParsingError(
                    f"Generated column '{gen_col}' references undefined column: '{source_col}'."
                )
