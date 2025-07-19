from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .constraints import BaseConstraint
from .types import Type


@dataclass(frozen=True)
class Field:
    """Represents a named and typed data field."""

    name: str
    type: Type
    description: str | None = None
    constraints: list[BaseConstraint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Returns a string representation of the field."""
        return f"{self.name}: {self.type}"


@dataclass(frozen=True)
class Options:
    """Represents options for schema handling."""

    if_not_exists: bool = False
    or_replace: bool = False

    def __str__(self) -> str:
        """Returns a string representation of the options."""
        parts = []
        if self.if_not_exists:
            parts.append("if_not_exists=True")
        if self.or_replace:
            parts.append("or_replace=True")
        return f"Options({', '.join(parts)})"


@dataclass(frozen=True)
class PartitionColumn:
    """Represents a column used for partitioning."""

    column: str
    transform: str | None = None

    def __str__(self) -> str:
        """Returns a string representation of the partition column."""
        if self.transform:
            return f"{self.column} (transform: {self.transform})"
        return self.column


@dataclass(frozen=True)
class Properties:
    """Represents properties of the schema's underlying storage."""

    partitioned_by: list[PartitionColumn] = field(default_factory=list)
    location: str | None = None
    table_type: str | None = None
    format: str | None = None
    write_compression: str | None = None

    def __str__(self) -> str:
        """Returns a string representation of the properties."""
        parts = []
        if self.partitioned_by:
            parts.append(
                f"\n    partitioned_by=[{', '.join(map(str, self.partitioned_by))}]"
            )
        if self.location:
            parts.append(f"\n    location='{self.location}'")
        if self.table_type:
            parts.append(f"\n    table_type='{self.table_type}'")
        if self.format:
            parts.append(f"\n    format='{self.format}'")
        if self.write_compression:
            parts.append(f"\n    write_compression='{self.write_compression}'")
        return f"({','.join(parts)}\n)"


@dataclass(frozen=True)
class SchemaSpec:
    """A data class representing a full schema specification."""

    name: str
    version: str
    columns: list[Field]
    description: str | None = None
    options: Options = field(default_factory=Options)
    properties: Properties = field(default_factory=Properties)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Returns a pretty-printed string representation of the schema."""
        header = f"schema {self.name} (version: {self.version})"
        parts = [header]

        if self.options.if_not_exists or self.options.or_replace:
            parts.append(f"\noptions: {self.options}")

        if any(
            [
                self.properties.partitioned_by,
                self.properties.location,
                self.properties.table_type,
                self.properties.format,
                self.properties.write_compression,
            ]
        ):
            parts.append(f"\nproperties: {self.properties}")

        columns_str = "\n".join(f"  {column}" for column in self.columns)
        parts.append(f"\ncolumns:\n{columns_str}")

        return "\n".join(parts)
