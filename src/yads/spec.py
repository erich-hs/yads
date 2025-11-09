"""Core data structures for the canonical yads specification.

Example:
    >>> from yads.constraints import NotNullConstraint
    >>> import yads.types as ytypes
    >>>
    >>> # Define table columns
    >>> columns = [
    ...     Column(name="id", type=ytypes.Integer(), constraints=[NotNullConstraint()]),
    ...     Column(name="name", type=ytypes.String(length=100))
    ... ]
    >>>
    >>> # Constraints may also be specified on nested fields (e.g., in Struct)
    >>> address_type = ytypes.Struct(fields=[
    ...     Field(name="street", type=ytypes.String(), constraints=[NotNullConstraint()]),
    ...     Field(name="city", type=ytypes.String()),
    ... ])
    >>>
    >>> # Create a complete spec
    >>> spec = YadsSpec(
    ...     name="users",
    ...     version="1.0.0",
    ...     columns=columns,
    ...     description="User information table"
    ... )
    >>>
    >>> # Use with converters
    >>> from yads.converters import SparkSQLConverter
    >>> converter = SparkSQLConverter()
    >>> ddl = converter.convert(spec)
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Type

from .constraints import ColumnConstraint, NotNullConstraint, TableConstraint
from .exceptions import SpecValidationError
from .types import YadsType


def _format_dict_as_kwargs(d: dict[str, Any], multiline: bool = False) -> str:
    if not d:
        return "{}"
    items = [f"{k}={v!r}" for k, v in d.items()]
    if multiline:
        pretty_items = ",\n".join(items)
        return f"{{\n{textwrap.indent(pretty_items, '  ')}\n}}"
    return f"{{{', '.join(items)}}}"


@dataclass(frozen=True)
class TransformedColumnReference:
    """A reference to a column with an optional transformation function.

    Used in:
    - Partitioning definitions to transform column values before partitioning.
    - Generated columns to define computed expressions.

    Common transformations include bucketing, truncating, and date part extraction.
    """

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
class Field:
    """A named and typed data field, representing a field in a complex type.

    Field is the base class for all named data elements in yads. It represents
    individual data fields with their types, constraints, and optional metadata.

    Example:
        >>> import yads.types as ytypes
        >>> from yads.constraints import NotNullConstraint
        >>>
        >>> # Simple field for use in complex types
        >>> field = Field(name="username", type=ytypes.String())
        >>> field.is_nullable
        True
        >>>
        >>> # Field with metadata and constraints
        >>> field = Field(
        ...     name="user_id",
        ...     type=ytypes.Integer(bits=64),
        ...     description="Unique identifier for the user",
        ...     metadata={"source": "user_service"},
        ...     constraints=[NotNullConstraint()],
        ... )
        >>> field.is_nullable
        False
    """

    name: str
    type: YadsType
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    constraints: list[ColumnConstraint] = field(default_factory=list)

    @cached_property
    def has_metadata(self) -> bool:
        """True if the field has any metadata defined."""
        return bool(self.metadata)

    @cached_property
    def is_nullable(self) -> bool:
        """True if this field allows NULL values (no NOT NULL constraint)."""
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
        details = []
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
    """A table column with optional generation logic.

    Column extends Field to represent database table columns specifically.
    It adds support for generated column definitions.

    Example:
        >>> from yads.constraints import NotNullConstraint, DefaultConstraint
        >>> import yads.types as ytypes
        >>>
        >>> # Simple column
        >>> column = Column(name="username", type=ytypes.String())
        >>>
        >>> # Column with constraints and metadata
        >>> column = Column(
        ...     name="user_id",
        ...     type=ytypes.Integer(bits=64),
        ...     constraints=[NotNullConstraint()],
        ...     description="Unique identifier for the user",
        ...     metadata={"source": "user_service"}
        ... )
        >>>
        >>> # Generated column
        >>> column = Column(
        ...     name="order_year",
        ...     type=ytypes.Integer(),
        ...     generated_as=TransformedColumnReference(column="order_date", transform="year")
        ... )
    """

    generated_as: TransformedColumnReference | None = None

    @cached_property
    def is_generated(self) -> bool:
        """True if this column is a generated/computed column."""
        return self.generated_as is not None

    def _build_details_repr(self) -> str:
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


@dataclass(frozen=True)
class Storage:
    """Defines the physical storage properties of a table.

    Includes file format, location, and format-specific properties.
    """

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
class YadsSpec:
    """Canonical yads specification.

    Represents a complete table definition including columns, constraints,
    storage properties, partitioning, and metadata. Instances are immutable.

    Args:
        name: Fully qualified table name (e.g., "catalog.database.table").
        version: spec version string for tracking changes.
        columns: List of Column objects defining the table structure.
        description: Optional human-readable description of the table.
        external: Whether to generate CREATE EXTERNAL TABLE statements.
        storage: Storage configuration including format and properties.
        partitioned_by: List of partition columns.
        table_constraints: List of table-level constraints (e.g., composite keys).
        metadata: Additional metadata as key-value pairs.

    Raises:
        SpecValidationError: If the spec contains validation errors such as
                             duplicate column names, undefined partition columns,
                             or invalid constraint references.

    Example:
        >>> from yads.constraints import NotNullConstraint, PrimaryKeyConstraint
        >>> import yads.types as ytypes
        >>>
        >>> # Create a simple spec
        >>> spec = YadsSpec(
        ...     name="users",
        ...     version="1.0.0",
        ...     description="User information table",
        ...     columns=[
        ...         Column(
        ...             name="id",
        ...             type=ytypes.Integer(),
        ...             constraints=[NotNullConstraint(), PrimaryKeyConstraint()]
        ...         ),
        ...         Column(
        ...             name="email",
        ...             type=ytypes.String(length=255),
        ...             constraints=[NotNullConstraint()]
        ...         ),
        ...         Column(name="name", type=ytypes.String())
        ...     ]
        ... )
        >>>
        >>> # Access spec properties
        >>> print(f"Spec: {spec.name} v{spec.version}")
        Spec: users v1.0.0
        >>> print(f"Columns: {len(spec.columns)}")
        Columns: 3
        >>> print(f"Column names: {spec.column_names}")
        Column names: {'id', 'email', 'name'}

        >>> # Use with converters
        >>> from yads.converters import SparkSQLConverter
        >>> converter = SparkSQLConverter()
        >>> ddl = converter.convert(spec)
    """

    name: str
    version: str
    columns: list[Column]
    description: str | None = None
    external: bool = False
    storage: Storage | None = None
    partitioned_by: list[TransformedColumnReference] = field(default_factory=list)
    table_constraints: list[TableConstraint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self._validate_columns()
        self._validate_partitions()
        self._validate_generated_columns()
        self._validate_table_constraints()

    def _validate_columns(self):
        names = set()
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
        parts = []
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
