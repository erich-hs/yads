"""Core data structures for the canonical yads specification."""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Mapping, Type

from .constraints import ColumnConstraint, NotNullConstraint, TableConstraint
from .exceptions import SpecValidationError
from .types import YadsType

# Version of the yads specification format
YADS_SPEC_VERSION = "0.0.2"


def from_dict(data: Mapping[str, Any]) -> YadsSpec:
    """Build a `YadsSpec` from a normalized dictionary.

    Args:
        data: Canonical spec dictionary to deserialize.

    Returns:
        A fully validated `YadsSpec` instance.
    """
    from .serializers.spec_serializer import SpecDeserializer

    return SpecDeserializer().deserialize(data)


def _format_dict_as_kwargs(d: dict[str, Any], multiline: bool = False) -> str:
    if not d:
        return "{}"
    items = [f"{k}={v!r}" for k, v in d.items()]
    if multiline:
        pretty_items = ",\n".join(items)
        return f"{{\n{textwrap.indent(pretty_items, '  ')}\n}}"
    return f"{{{', '.join(items)}}}"


# Typed default factory helpers
def _empty_any_list() -> list[Any]:
    return []


def _empty_constraints() -> list[ColumnConstraint]:
    return []


def _empty_metadata() -> dict[str, Any]:
    return {}


def _empty_tbl_properties() -> dict[str, str]:
    return {}


def _empty_columns() -> list[Column]:
    return []


def _empty_partitions() -> list[TransformedColumnReference]:
    return []


def _empty_table_constraints() -> list[TableConstraint]:
    return []


@dataclass(frozen=True)
class TransformedColumnReference:
    """Reference to a column with an optional transformation.

    Args:
        column: Name of the referenced column.
        transform: Transformation function applied to the column, if any.
        transform_args: Arguments passed to the transformation.
    """

    column: str
    transform: str | None = None
    transform_args: list[Any] = field(default_factory=_empty_any_list)

    def __str__(self) -> str:
        if self.transform:
            if self.transform_args:
                args_str = ", ".join(map(str, self.transform_args))
                return f"{self.transform}({self.column}, {args_str})"
            return f"{self.transform}({self.column})"
        return self.column


@dataclass(frozen=True)
class Field:
    """A named, typed data field with optional constraints.

    Args:
        name: Field identifier.
        type: Logical yads type of the field.
        description: Optional human-friendly description.
        metadata: Arbitrary key-value metadata for consumers.
        constraints: Column-level constraints such as nullability or checks.
    """

    name: str
    type: YadsType
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=_empty_metadata)
    constraints: list[ColumnConstraint] = field(default_factory=_empty_constraints)

    @cached_property
    def has_metadata(self) -> bool:
        """True if the field has any metadata defined."""
        return bool(self.metadata)

    @cached_property
    def is_nullable(self) -> bool:
        """True if this field allows NULL values."""
        return not any(isinstance(c, NotNullConstraint) for c in self.constraints)

    @cached_property
    def has_constraints(self) -> bool:
        """True if this field has any constraints defined."""
        return bool(self.constraints)

    @cached_property
    def constraint_types(self) -> set[Type[ColumnConstraint]]:
        """Set of constraint types applied to this field."""
        return {type(constraint) for constraint in self.constraints}

    def _build_details_repr(self) -> str:
        details: list[str] = []
        if self.description:
            details.append(f"description={self.description!r}")
        if self.constraints:
            constraints_str = ", ".join(map(str, self.constraints))
            details.append(f"constraints=[{constraints_str}]")
        if self.metadata:
            details.append(f"metadata={_format_dict_as_kwargs(self.metadata)}")

        if not details:
            return ""

        pretty_details = ",\n".join(details)
        return f"(\n{textwrap.indent(pretty_details, '  ')}\n)"

    def __str__(self) -> str:
        details_repr = self._build_details_repr()
        return f"{self.name}: {self.type}{details_repr}"


@dataclass(frozen=True)
class Column(Field):
    """Table column extending `Field` with generation support.

    Args:
        name: Column name.
        type: Logical yads type of the column.
        description: Optional human-friendly description.
        metadata: Arbitrary key-value metadata for consumers.
        constraints: Column-level constraints such as nullability or checks.
        generated_as: Optional expression defining a generated/computed column.
    """

    generated_as: TransformedColumnReference | None = None

    @cached_property
    def is_generated(self) -> bool:
        """True if this column is a generated/computed column."""
        return self.generated_as is not None

    def _build_details_repr(self) -> str:
        details: list[str] = []
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


@dataclass(frozen=True)
class Storage:
    """Physical storage properties for a table.

    Args:
        format: Storage format (e.g., "parquet", "delta").
        location: Optional URI/path to the stored data.
        tbl_properties: Format-specific storage properties.
    """

    format: str | None = None
    location: str | None = None
    tbl_properties: dict[str, str] = field(default_factory=_empty_tbl_properties)

    def __str__(self) -> str:
        parts: list[str] = []
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
class YadsSpec:
    """Canonical yads specification.

    Represents a complete table definition including columns, constraints,
    storage properties, partitioning, and metadata. Instances are immutable.

    Args:
        name: Fully qualified table name (e.g., "catalog.database.table").
        version: Registry-assigned monotonic integer version for tracking changes.
        yads_spec_version: Version of the yads specification format itself.
        columns: List of Column objects defining the table structure.
        description: Optional human-readable description of the table.
        external: Whether to generate `CREATE EXTERNAL TABLE` statements.
        storage: Storage configuration including format and properties.
        partitioned_by: List of partition columns.
        table_constraints: List of table-level constraints (e.g., composite keys).
        metadata: Additional metadata as key-value pairs.

    Raises:
        SpecValidationError: If the spec contains validation errors such as
                             duplicate column names, undefined partition columns,
                             or invalid constraint references.
    """

    name: str
    version: int
    yads_spec_version: str = YADS_SPEC_VERSION
    columns: list[Column] = field(default_factory=_empty_columns)
    description: str | None = None
    external: bool = False
    storage: Storage | None = None
    partitioned_by: list[TransformedColumnReference] = field(
        default_factory=_empty_partitions
    )
    table_constraints: list[TableConstraint] = field(
        default_factory=_empty_table_constraints
    )
    metadata: dict[str, Any] = field(default_factory=_empty_metadata)

    def __post_init__(self):
        self._validate_columns()
        self._validate_partitions()
        self._validate_generated_columns()
        self._validate_table_constraints()

    def _validate_columns(self):
        names: set[str] = set()
        for c in self.columns:
            if c.name in names:
                raise SpecValidationError(f"Duplicate column name found: {c.name!r}.")
            names.add(c.name)

    def _validate_partitions(self):
        for p_col in self.partition_column_names:
            if p_col not in self.column_names:
                raise SpecValidationError(
                    f"Partition column {p_col!r} must be defined as a column in the schema."
                )

    def _validate_generated_columns(self):
        for gen_col, source_col in self.generated_columns.items():
            if source_col not in self.column_names:
                raise SpecValidationError(
                    f"Source column {source_col!r} for generated column {gen_col!r} "
                    "not found in schema."
                )

    def _validate_table_constraints(self):
        for constraint in self.table_constraints:
            for col in constraint.constrained_columns:
                if col not in self.column_names:
                    raise SpecValidationError(
                        f"Column {col!r} in constraint {constraint} not found in schema."
                    )

    def to_dict(self) -> dict[str, Any]:
        """Serialize this spec into the canonical dictionary format."""
        from .serializers.spec_serializer import SpecSerializer

        return SpecSerializer().serialize(self)

    @cached_property
    def column_names(self) -> set[str]:
        """Set of all column names defined in the spec."""
        return {c.name for c in self.columns}

    @cached_property
    def partition_column_names(self) -> set[str]:
        """Set of column names referenced as partition columns."""
        return {p.column for p in self.partitioned_by}

    @cached_property
    def generated_columns(self) -> dict[str, str]:
        """Mapping of generated column names to their source columns with format:
        `{generated_column_name: source_column_name}`.
        """
        return {
            c.name: c.generated_as.column
            for c in self.columns
            if c.generated_as is not None
        }

    @cached_property
    def nullable_columns(self) -> set[str]:
        """Set of column names that allow NULL values."""
        return {c.name for c in self.columns if c.is_nullable}

    @cached_property
    def constrained_columns(self) -> set[str]:
        """Set of column names that have any constraints defined."""
        return {c.name for c in self.columns if c.has_constraints}

    def _build_header_str(self) -> str:
        return f"spec {self.name}(version={self.version!r})"

    def _build_body_str(self) -> str:
        parts: list[str] = []
        if self.description:
            parts.append(f"description={self.description!r}")
        if self.metadata:
            parts.append(
                f"metadata={_format_dict_as_kwargs(self.metadata, multiline=True)}"
            )
        if self.external:
            parts.append("external=True")
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
