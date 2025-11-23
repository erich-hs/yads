"""Constraint serialization and deserialization helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Callable, ClassVar, Mapping as TypingMapping, cast

from ..constraints import (
    ColumnConstraint,
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyReference,
    ForeignKeyTableConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    TableConstraint,
)
from ..exceptions import (
    InvalidConstraintError,
    SpecParsingError,
    SpecSerializationError,
    UnknownConstraintError,
)

ColumnConstraintParser = Callable[[Any], ColumnConstraint]
TableConstraintParser = Callable[[Mapping[str, Any]], TableConstraint]


class ConstraintSerializer:
    """Serialize constraint objects into dictionary representations."""

    # ---- Column Constraints serialization helpers ---------------------------------
    def serialize_column_constraints(
        self, constraints: Sequence[ColumnConstraint]
    ) -> dict[str, Any]:
        serialized: dict[str, Any] = {}
        for constraint in constraints:
            if isinstance(constraint, NotNullConstraint):
                serialized["not_null"] = True
            elif isinstance(constraint, PrimaryKeyConstraint):
                serialized["primary_key"] = True
            elif isinstance(constraint, DefaultConstraint):
                serialized["default"] = constraint.value
            elif isinstance(constraint, ForeignKeyConstraint):
                serialized["foreign_key"] = self._serialize_foreign_key_constraint(
                    constraint
                )
            elif isinstance(constraint, IdentityConstraint):
                serialized["identity"] = self._serialize_identity_constraint(constraint)
            else:
                raise SpecSerializationError(
                    f"Unsupported column constraint {type(constraint).__name__!s}"
                )
        return serialized

    # ---- Table Constraints serialization helpers ----------------------------------
    def serialize_table_constraints(
        self, constraints: Sequence[TableConstraint]
    ) -> list[dict[str, Any]]:
        serialized: list[dict[str, Any]] = []
        for constraint in constraints:
            if isinstance(constraint, PrimaryKeyTableConstraint):
                serialized.append(
                    self._serialize_primary_key_table_constraint(constraint)
                )
            elif isinstance(constraint, ForeignKeyTableConstraint):
                serialized.append(
                    self._serialize_foreign_key_table_constraint(constraint)
                )
            else:
                raise SpecSerializationError(
                    f"Unsupported table constraint {type(constraint).__name__!s}"
                )
        return serialized

    # ---- Shared serialization helpers ---------------------------------------------
    def _serialize_foreign_key_constraint(
        self, constraint: ForeignKeyConstraint
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "references": self._serialize_foreign_key_reference(constraint.references)
        }
        if constraint.name:
            payload["name"] = constraint.name
        return payload

    def _serialize_identity_constraint(
        self, constraint: IdentityConstraint
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if constraint.always is not True:
            payload["always"] = constraint.always
        if constraint.start is not None:
            payload["start"] = constraint.start
        if constraint.increment is not None:
            payload["increment"] = constraint.increment
        return payload

    def _serialize_primary_key_table_constraint(
        self, constraint: PrimaryKeyTableConstraint
    ) -> dict[str, Any]:
        if not constraint.name:
            raise SpecSerializationError(
                "Primary key table constraints require a 'name' to serialize."
            )
        return {
            "type": "primary_key",
            "name": constraint.name,
            "columns": list(constraint.columns),
        }

    def _serialize_foreign_key_table_constraint(
        self, constraint: ForeignKeyTableConstraint
    ) -> dict[str, Any]:
        if not constraint.name:
            raise SpecSerializationError(
                "Foreign key table constraints require a 'name' to serialize."
            )
        payload: dict[str, Any] = {
            "type": "foreign_key",
            "name": constraint.name,
            "columns": list(constraint.columns),
            "references": self._serialize_foreign_key_reference(constraint.references),
        }
        return payload

    def _serialize_foreign_key_reference(
        self, reference: ForeignKeyReference
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"table": reference.table}
        if reference.columns:
            payload["columns"] = list(reference.columns)
        return payload


class ConstraintDeserializer:
    """Parse column and table constraint dictionaries."""

    def __init__(
        self,
        *,
        column_parsers: TypingMapping[str, ColumnConstraintParser] | None = None,
        table_parsers: TypingMapping[str, TableConstraintParser] | None = None,
    ) -> None:
        self._column_parsers: dict[str, ColumnConstraintParser] = (
            dict(column_parsers)
            if column_parsers is not None
            else dict(self._DEFAULT_COLUMN_CONSTRAINT_PARSERS)
        )
        self._table_parsers: dict[str, TableConstraintParser] = (
            dict(table_parsers)
            if table_parsers is not None
            else dict(self._DEFAULT_TABLE_CONSTRAINT_PARSERS)
        )

    # ---- Registration helpers -----------------------------------------------------
    def register_column_parser(self, name: str, parser: ColumnConstraintParser) -> None:
        """Register a parser for a named column constraint."""
        self._column_parsers[name] = parser

    def register_table_parser(self, name: str, parser: TableConstraintParser) -> None:
        """Register a parser for a named table constraint."""
        self._table_parsers[name] = parser

    # ---- Column Constraint parsing ------------------------------------------------
    def parse_column_constraints(
        self, constraints: object | None
    ) -> list[ColumnConstraint]:
        parsed: list[ColumnConstraint] = []
        if constraints is None:
            return parsed
        if not isinstance(constraints, Mapping):
            raise SpecParsingError(
                "The 'constraints' attribute of a column must be a dictionary."
            )
        typed_constraints = cast(Mapping[str, Any], constraints)
        for key, value in typed_constraints.items():
            parser = self._column_parsers.get(key)
            if not parser:
                raise UnknownConstraintError(f"Unknown column constraint: {key}.")
            if (
                key in {"not_null", "primary_key"}
                and isinstance(value, bool)
                and not value
            ):
                # Explicit `false` entries for boolean constraints simply mean
                # "constraint not present"; ignore them to match serialization output.
                continue
            parsed.append(parser(value))
        return parsed

    # ---- Table Constraint parsing -------------------------------------------------
    def parse_table_constraints(
        self, constraints: object | None
    ) -> list[TableConstraint]:
        if constraints is None:
            return []
        if isinstance(constraints, (str, bytes)):
            raise InvalidConstraintError(
                "Table constraints must be provided as a sequence of dictionaries."
            )
        parsed: list[TableConstraint] = []
        if not isinstance(constraints, Sequence):
            raise InvalidConstraintError("Table constraints must be a sequence.")
        typed_constraints = cast(Sequence[object], constraints)
        for index, constraint_def in enumerate(typed_constraints):
            if not isinstance(constraint_def, Mapping):
                raise InvalidConstraintError(
                    f"Table constraint at index {index} must be a dictionary."
                )
            typed_constraint = cast(Mapping[str, Any], constraint_def)
            constraint_type = typed_constraint.get("type")
            if not isinstance(constraint_type, str):
                raise InvalidConstraintError(
                    "Table constraint definition must have a 'type' string."
                )
            parser = self._table_parsers.get(constraint_type)
            if not parser:
                raise UnknownConstraintError(
                    f"Unknown table constraint type: {constraint_type}."
                )
            parsed.append(parser(dict(typed_constraint)))
        return parsed

    # ---- Column Constraint helpers -------------------------------------------------
    @staticmethod
    def _parse_not_null_constraint(value: Any) -> NotNullConstraint:
        if not isinstance(value, bool):
            raise InvalidConstraintError(
                f"The 'not_null' constraint expects a boolean value. Got {value!r}."
            )
        return NotNullConstraint()

    @staticmethod
    def _parse_primary_key_constraint(value: Any) -> PrimaryKeyConstraint:
        if not isinstance(value, bool):
            raise InvalidConstraintError(
                f"The 'primary_key' constraint expects a boolean value. Got {value!r}."
            )
        return PrimaryKeyConstraint()

    @staticmethod
    def _parse_default_constraint(value: Any) -> DefaultConstraint:
        return DefaultConstraint(value=value)

    @staticmethod
    def _parse_foreign_key_constraint(value: Any) -> ForeignKeyConstraint:
        if not isinstance(value, Mapping):
            raise InvalidConstraintError(
                f"The 'foreign_key' constraint expects a dictionary. Got {value!r}."
            )
        if "references" not in value:
            raise InvalidConstraintError(
                "The 'foreign_key' constraint must specify 'references'."
            )
        value_dict: dict[str, Any] = dict(cast(Mapping[str, Any], value))
        name = value_dict.get("name")
        if name is not None and not isinstance(name, str):
            raise InvalidConstraintError(
                "Foreign key constraint 'name' must be a string if provided."
            )

        references_value = value_dict["references"]
        if not isinstance(references_value, Mapping):
            raise InvalidConstraintError("Foreign key 'references' must be a dictionary.")

        # Delegate nested reference parsing so both serialization directions
        # validate column/table combinations consistently.
        return ForeignKeyConstraint(
            name=name,
            references=ConstraintDeserializer._parse_foreign_key_references(
                cast(Mapping[str, Any], references_value)
            ),
        )

    @staticmethod
    def _parse_identity_constraint(value: Any) -> IdentityConstraint:
        if not isinstance(value, Mapping):
            raise InvalidConstraintError(
                f"The 'identity' constraint expects a dictionary. Got {value!r}."
            )

        value_dict: dict[str, Any] = dict(cast(Mapping[str, Any], value))
        always_value = value_dict.get("always", True)
        if not isinstance(always_value, bool):
            raise InvalidConstraintError("'always' must be a boolean when specified.")

        start_value = value_dict.get("start")
        if start_value is not None and not isinstance(start_value, int):
            raise InvalidConstraintError("'start' must be an integer when specified.")

        increment_value = value_dict.get("increment")
        if increment_value is not None and not isinstance(increment_value, int):
            raise InvalidConstraintError("'increment' must be an integer when specified.")

        return IdentityConstraint(
            always=always_value,
            start=start_value,
            increment=increment_value,
        )

    # ---- Table Constraint helpers --------------------------------------------------
    @staticmethod
    def _parse_primary_key_table_constraint(
        const_def: Mapping[str, Any],
    ) -> PrimaryKeyTableConstraint:
        for required_field in ("name", "columns"):
            if required_field not in const_def:
                raise InvalidConstraintError(
                    f"Primary key table constraint must specify '{required_field}'."
                )
        name = const_def["name"]
        if not isinstance(name, str):
            raise InvalidConstraintError(
                "Primary key table constraint 'name' must be a string."
            )
        columns = ConstraintDeserializer._ensure_column_name_list(
            const_def["columns"], "primary key table constraint"
        )
        return PrimaryKeyTableConstraint(columns=columns, name=name)

    @staticmethod
    def _parse_foreign_key_table_constraint(
        const_def: Mapping[str, Any],
    ) -> ForeignKeyTableConstraint:
        for required_field in ("name", "columns", "references"):
            if required_field not in const_def:
                raise InvalidConstraintError(
                    f"Foreign key table constraint must specify '{required_field}'."
                )
        name = const_def["name"]
        if not isinstance(name, str):
            raise InvalidConstraintError(
                "Foreign key table constraint 'name' must be a string."
            )
        columns = ConstraintDeserializer._ensure_column_name_list(
            const_def["columns"], "foreign key table constraint"
        )
        references_value = const_def["references"]
        if not isinstance(references_value, Mapping):
            raise InvalidConstraintError(
                "Foreign key table constraint 'references' must be a dictionary."
            )
        return ForeignKeyTableConstraint(
            columns=columns,
            name=name,
            references=ConstraintDeserializer._parse_foreign_key_references(
                cast(Mapping[str, Any], references_value)
            ),
        )

    @staticmethod
    def _parse_foreign_key_references(
        references_def: Mapping[str, Any],
    ) -> ForeignKeyReference:
        if "table" not in references_def:
            raise InvalidConstraintError(
                "The 'references' of a foreign key must be a dictionary with a 'table' key."
            )
        table_value = references_def["table"]
        if not isinstance(table_value, str):
            raise InvalidConstraintError("'references.table' must be a string.")

        columns_value = references_def.get("columns")
        validated_columns: list[str] | None = None
        if columns_value is not None:
            if not isinstance(columns_value, list):
                raise InvalidConstraintError(
                    "'references.columns' must be a list of strings."
                )
            validated_columns = []
            columns_iterable = cast(list[object], columns_value)
            for column in columns_iterable:
                if not isinstance(column, str):
                    raise InvalidConstraintError(
                        "'references.columns' must be a list of strings."
                    )
                validated_columns.append(column)

        return ForeignKeyReference(
            table=table_value,
            columns=validated_columns,
        )

    @staticmethod
    def _ensure_column_name_list(value: Any, context: str) -> list[str]:
        if not isinstance(value, list):
            raise InvalidConstraintError(
                f"'{context}' columns must be a list of strings."
            )
        validated_columns: list[str] = []
        typed_columns = cast(list[object], value)
        for column in typed_columns:
            if not isinstance(column, str):
                raise InvalidConstraintError(
                    f"'{context}' columns must be a list of strings."
                )
            validated_columns.append(column)
        return validated_columns

    # ---- Built-in parser registries -----------------------------------------------
    _DEFAULT_COLUMN_CONSTRAINT_PARSERS: ClassVar[dict[str, ColumnConstraintParser]] = {
        "not_null": _parse_not_null_constraint,
        "primary_key": _parse_primary_key_constraint,
        "default": _parse_default_constraint,
        "foreign_key": _parse_foreign_key_constraint,
        "identity": _parse_identity_constraint,
    }

    _DEFAULT_TABLE_CONSTRAINT_PARSERS: ClassVar[dict[str, TableConstraintParser]] = {
        "primary_key": _parse_primary_key_table_constraint,
        "foreign_key": _parse_foreign_key_table_constraint,
    }
