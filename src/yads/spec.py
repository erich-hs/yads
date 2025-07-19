from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from typing import Any

from .constraints import BaseConstraint
from .types import Type


def _build_details_str(details: list[str]) -> str:
    """Builds a details string if there are any details."""
    if not details:
        return ""
    pretty_items = ",\n".join(details)
    return f"(\n{textwrap.indent(pretty_items, '  ')}\n)"


def _format_dict_as_kwargs(d: dict[str, Any]) -> str:
    """Formats a dictionary as a string of key-value pairs, like kwargs."""
    if not d:
        return "{}"
    items = [f"{k}={v!r}" for k, v in d.items()]
    return f"{{{', '.join(items)}}}"


def _format_multiline_dict_as_kwargs(d: dict[str, Any]) -> str:
    """Formats a dictionary as a multiline string of key-value pairs."""
    if not d:
        return "{}"
    items = [f"{k}={v!r}" for k, v in d.items()]
    pretty_items = ",\n".join(items)
    return f"{{\n{textwrap.indent(pretty_items, '  ')}\n}}"


@dataclass(frozen=True)
class Field:
    """Represents a named and typed data field."""

    name: str
    type: Type
    description: str | None = None
    constraints: list[BaseConstraint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def _get_details(self) -> list[str]:
        """Returns a list of details for the field."""
        details = []
        if self.description:
            details.append(f"description={self.description!r}")
        if self.constraints:
            constraints_str = ", ".join(map(str, self.constraints))
            details.append(f"constraints=[{constraints_str}]")
        if self.metadata:
            details.append(f"metadata={_format_dict_as_kwargs(self.metadata)}")
        return details

    def __str__(self) -> str:
        """Returns a string representation of the field."""
        details_str = _build_details_str(self._get_details())
        return f"{self.name}: {self.type}{details_str}"


@dataclass(frozen=True)
class Options:
    """Represents options for schema handling."""

    if_not_exists: bool = False
    or_replace: bool = False

    def is_defined(self) -> bool:
        """Checks if any option is defined."""
        return self.if_not_exists or self.or_replace

    def __str__(self) -> str:
        """Returns a string representation of the options."""
        if not self.is_defined():
            return "Options()"
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
            return f"{self.transform}({self.column})"
        return self.column


@dataclass(frozen=True)
class Properties:
    """Represents properties of the schema's underlying storage."""

    partitioned_by: list[PartitionColumn] = field(default_factory=list)
    location: str | None = None
    table_type: str | None = None
    format: str | None = None
    write_compression: str | None = None

    def is_defined(self) -> bool:
        """Checks if any property is defined."""
        return any(
            [
                self.partitioned_by,
                self.location,
                self.table_type,
                self.format,
                self.write_compression,
            ]
        )

    def __str__(self) -> str:
        """Returns a string representation of the properties."""
        if not self.is_defined():
            return "Properties()"
        parts = []
        if self.partitioned_by:
            p_cols = ", ".join(map(str, self.partitioned_by))
            parts.append(f"partitioned_by=[{p_cols}]")
        if self.location:
            parts.append(f"location={self.location!r}")
        if self.table_type:
            parts.append(f"table_type={self.table_type!r}")
        if self.format:
            parts.append(f"format={self.format!r}")
        if self.write_compression:
            parts.append(f"write_compression={self.write_compression!r}")
        pretty_parts = ",\n".join(parts)
        indented_parts = textwrap.indent(pretty_parts, "  ")
        return f"Properties(\n{indented_parts}\n)"


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

    def _build_header(self) -> str:
        """Builds the header section of the schema string representation."""
        return f"schema {self.name}(version={self.version!r})"

    def _build_body(self) -> str:
        """Builds the body section of the schema string representation."""
        parts = []
        if self.description:
            parts.append(f"description={self.description!r}")
        if self.metadata:
            parts.append(f"metadata={_format_multiline_dict_as_kwargs(self.metadata)}")
        if self.options.is_defined():
            parts.append(f"options={self.options}")
        if self.properties.is_defined():
            parts.append(f"properties={self.properties}")

        columns_str = "\n".join(f"{column}" for column in self.columns)
        indented_columns = textwrap.indent(columns_str, "  ")
        parts.append(f"columns=[\n{indented_columns}\n]")
        return "\n".join(parts)

    def __str__(self) -> str:
        """Returns a pretty-printed string representation of the schema."""
        body = textwrap.indent(self._build_body(), "  ")
        return f"{self._build_header()}(\n{body}\n)"
