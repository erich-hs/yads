"""Placeholder for constraint deserialization helpers."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from ..constraints import ColumnConstraint, TableConstraint


class ConstraintDeserializer:
    """Parse column and table constraint dictionaries."""

    def parse_column_constraints(
        self, constraints: Mapping[str, Any] | None
    ) -> list[ColumnConstraint]:
        raise NotImplementedError

    def parse_table_constraints(
        self, constraints: Sequence[Mapping[str, Any]] | None
    ) -> list[TableConstraint]:
        raise NotImplementedError
