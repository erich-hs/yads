from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from typing import Any

from .constraints import ColumnConstraint, TableConstraint
from .types import Type


def _format_dict_as_kwargs(d: dict[str, Any], multiline: bool = False) -> str:
    """Formats a dictionary as a string of key-value pairs, like kwargs."""
    if not d:
        return "{}"
    items = [f"{k}={v!r}" for k, v in d.items()]
    if multiline:
        pretty_items = ",\n".join(items)
        return f"{{\n{textwrap.indent(pretty_items, '  ')}\n}}"
    return f"{{{', '.join(items)}}}"


@dataclass(frozen=True)
class GenerationClause:
    """Defines a generation clause for columns that are generated from other columns in the table."""

    column: str
    transform: str
    transform_args: list[Any] = field(default_factory=list)

    def __str__(self) -> str:
        if self.transform_args:
            args_str = ", ".join(map(str, self.transform_args))
            return f"{self.transform}({self.column}, {args_str})"
        return f"{self.transform}({self.column})"


@dataclass(frozen=True)
class Field:
    """A named and typed data field, representing a column in a table or a field in a complex type."""

    name: str
    type: Type
    description: str | None = None
    constraints: list[ColumnConstraint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    generated_as: GenerationClause | None = None

    def _build_details_repr(self) -> str:
        """Builds the string representation of the field's details."""
        details = []
        if self.description:
            details.append(f"description={self.description!r}")
        if self.constraints:
            constraints_str = ", ".join(map(str, self.constraints))
            details.append(f"constraints=[{constraints_str}]")
        if self.metadata:
            details.append(f"metadata={_format_dict_as_kwargs(self.metadata)}")
        if self.generated_as:
            details.append(f"generated_as={self.generated_as}")

        if not details:
            return ""

        pretty_details = ",\n".join(details)
        return f"(\n{textwrap.indent(pretty_details, '  ')}\n)"

    def __str__(self) -> str:
        details_repr = self._build_details_repr()
        return f"{self.name}: {self.type}{details_repr}"


@dataclass(frozen=True)
class Options:
    """Represents high-level options for CREATE TABLE statements."""

    is_external: bool = False
    if_not_exists: bool = False
    or_replace: bool = False

    def is_defined(self) -> bool:
        """Checks if any option is defined."""
        return self.is_external or self.if_not_exists or self.or_replace

    def __str__(self) -> str:
        if not self.is_defined():
            return "Options()"
        parts = []
        if self.is_external:
            parts.append("is_external=True")
        if self.if_not_exists:
            parts.append("if_not_exists=True")
        if self.or_replace:
            parts.append("or_replace=True")
        return f"Options({', '.join(parts)})"


@dataclass(frozen=True)
class TransformedColumn:
    """A column that may have a transformation function applied."""

    column: str
    transform: str | None = None
    transform_args: list[Any] = field(default_factory=list)

    def __str__(self) -> str:
        if self.transform:
            if self.transform_args:
                args_str = ", ".join(map(str, self.transform_args))
                return f"{self.transform}({self.column}, {args_str})"
            return f"{self.transform}({self.column})"
        return self.column


@dataclass(frozen=True)
class Storage:
    """Defines the physical storage properties of a table."""

    format: str | None = None
    location: str | None = None
    tbl_properties: dict[str, str] = field(default_factory=dict)

    def __str__(self) -> str:
        parts = []
        if self.format:
            parts.append(f"format={self.format!r}")
        if self.location:
            parts.append(f"location={self.location!r}")
        if self.tbl_properties:
            tbl_props_str = _format_dict_as_kwargs(self.tbl_properties, multiline=True)
            parts.append(f"tbl_properties={tbl_props_str}")

        pretty_parts = ",\n".join(parts)
        indented_parts = textwrap.indent(pretty_parts, "  ")
        return f"Storage(\n{indented_parts}\n)"


@dataclass(frozen=True)
class SchemaSpec:
    """A full yads schema specification, representing a table and its properties."""

    name: str
    version: str
    columns: list[Field]
    description: str | None = None
    options: Options = field(default_factory=Options)
    storage: Storage | None = None
    partitioned_by: list[TransformedColumn] = field(default_factory=list)
    table_constraints: list[TableConstraint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def column_names(self) -> set[str]:
        """A set of all column names in the schema."""
        return {c.name for c in self.columns}

    @property
    def partition_column_names(self) -> set[str]:
        """A set of all partition column names in the schema."""
        return {p.column for p in self.partitioned_by}

    @property
    def generated_columns(self) -> dict[str, str]:
        """A dictionary of generated columns and their source columns."""
        return {
            c.name: c.generated_as.column
            for c in self.columns
            if c.generated_as is not None
        }

    def _build_header_str(self) -> str:
        """Builds the header section of the schema string representation."""
        return f"schema {self.name}(version={self.version!r})"

    def _build_body_str(self) -> str:
        """Builds the body section of the schema string representation."""
        parts = []
        if self.description:
            parts.append(f"description={self.description!r}")
        if self.metadata:
            parts.append(
                f"metadata={_format_dict_as_kwargs(self.metadata, multiline=True)}"
            )
        if self.options.is_defined():
            parts.append(f"options={self.options}")
        if self.storage:
            parts.append(f"storage={self.storage}")
        if self.partitioned_by:
            p_cols = ", ".join(map(str, self.partitioned_by))
            parts.append(f"partitioned_by=[{p_cols}]")
        if self.table_constraints:
            constraints_str = "\n".join(map(str, self.table_constraints))
            parts.append(
                f"table_constraints=[\n{textwrap.indent(constraints_str, '  ')}\n]"
            )

        columns_str = "\n".join(f"{column}" for column in self.columns)
        indented_columns = textwrap.indent(columns_str, "  ")
        parts.append(f"columns=[\n{indented_columns}\n]")
        return "\n".join(parts)

    def __str__(self) -> str:
        body = textwrap.indent(self._build_body_str(), "  ")
        return f"{self._build_header_str()}(\n{body}\n)"
