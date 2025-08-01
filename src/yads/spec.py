"""Core schema specification data structures for yads.

This module defines the fundamental data structures that represent schema specifications
in yads. These immutable data classes form the canonical representation of the yads spec.

Example:
    >>> from yads.types import String, Integer
    >>> from yads.constraints import NotNullConstraint
    >>>
    >>> # Define table columns
    >>> columns = [
    ...     Field(name="id", type=Integer(), constraints=[NotNullConstraint()]),
    ...     Field(name="name", type=String(length=100))
    ... ]
    >>>
    >>> # Create complete schema specification
    >>> spec = SchemaSpec(
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
from typing import Any

from .constraints import ColumnConstraint, TableConstraint
from .exceptions import SchemaValidationError
from .types import Type


def _format_dict_as_kwargs(d: dict[str, Any], multiline: bool = False) -> str:
    if not d:
        return "{}"
    items = [f"{k}={v!r}" for k, v in d.items()]
    if multiline:
        pretty_items = ",\n".join(items)
        return f"{{\n{textwrap.indent(pretty_items, '  ')}\n}}"
    return f"{{{', '.join(items)}}}"


@dataclass(frozen=True)
class TransformedColumn:
    """A column that may have a transformation function applied.

    TransformedColumns are used in two contexts:
    1. Partitioning and clustering specifications - to define how column values
       should be transformed before partitioning/clustering
    2. Generated column specifications - to define computed columns derived from
       other columns (used in Field.generated_as)

    Common transformations include bucketing, truncating, and date part extraction.

    Example:
        >>> # Simple column reference for partitioning
        >>> col = TransformedColumn(column="status")
        >>> str(col)
        'status'

        >>> # Column with a transformation function applied
        >>> col = TransformedColumn(column="order_date", transform="month")
        >>> str(col)
        'month(order_date)'

        >>> col = TransformedColumn(column="user_id", transform="bucket", transform_args=[50])
        >>> str(col)
        'bucket(user_id, 50)'

        >>> # Generated column example (transform is required for generated columns)
        >>> col = TransformedColumn(column="order_date", transform="year")
        >>> # This would be used in: Field(name="order_year", type=Integer(), generated_as=col)
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
    """A named and typed data field, representing a column in a table or a field in a complex type.

    Fields are the building blocks of schemas, representing individual data columns
    with their types, constraints, and optional metadata. They can be used both
    for top-level table columns and nested fields within complex types like structs.

    Example:
        >>> from yads.types import String, Integer
        >>> from yads.constraints import NotNullConstraint, DefaultConstraint
        >>>
        >>> # Simple field
        >>> field = Field(name="username", type=String())
        >>>
        >>> # Field with constraints and metadata
        >>> field = Field(
        ...     name="user_id",
        ...     type=Integer(bits=64),
        ...     constraints=[NotNullConstraint()],
        ...     description="Unique identifier for the user",
        ...     metadata={"source": "user_service"}
        ... )
        >>>
        >>> # Generated field
        >>> field = Field(
        ...     name="order_year",
        ...     type=Integer(),
        ...     generated_as=TransformedColumn(column="order_date", transform="year")
        ... )
    """

    name: str
    type: Type
    description: str | None = None
    constraints: list[ColumnConstraint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    generated_as: TransformedColumn | None = None

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

    def __str__(self) -> str:
        details_repr = self._build_details_repr()
        return f"{self.name}: {self.type}{details_repr}"


@dataclass(frozen=True)
class Storage:
    """Defines the physical storage properties of a table.

    Storage configuration specifies how and where table data should be physically
    stored, including file format, location, and format-specific properties.

    Example:
        >>> # Basic Parquet storage
        >>> storage = Storage(format="parquet", location="/data/tables/users")
        >>>
        >>> # Iceberg table with properties
        >>> storage = Storage(
        ...     format="iceberg",
        ...     location="/warehouse/sales/orders",
        ...     tbl_properties={
        ...         "write.target-file-size-bytes": "536870912",
        ...         "read.split.target-size": "268435456"
        ...     }
        ... )
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
class SchemaSpec:
    """Complete yads schema specification.

    SchemaSpec is the central data structure in yads that represents a complete
    table definition including columns, constraints, storage properties, and
    metadata. It serves as the input for all converters and validation processes.

    The schema specification is immutable.

    Args:
        name: Fully qualified table name (e.g., "catalog.database.table").
        version: Schema version string for tracking changes.
        columns: List of Field objects defining the table structure.
        description: Optional human-readable description of the table.
        external: Whether to generate CREATE EXTERNAL TABLE statements.
        storage: Storage configuration including format and properties.
        partitioned_by: List of partitioning specifications.
        table_constraints: List of table-level constraints (e.g., composite keys).
        metadata: Additional metadata as key-value pairs.

    Raises:
        SchemaValidationError: If the schema contains validation errors such as
                             duplicate column names, undefined partition columns,
                             or invalid constraint references.

    Example:
        >>> from yads.types import String, Integer
        >>> from yads.constraints import NotNullConstraint, PrimaryKeyConstraint
        >>>
        >>> # Create a simple schema
        >>> spec = SchemaSpec(
        ...     name="users",
        ...     version="1.0.0",
        ...     description="User information table",
        ...     columns=[
        ...         Field(
        ...             name="id",
        ...             type=Integer(),
        ...             constraints=[NotNullConstraint(), PrimaryKeyConstraint()]
        ...         ),
        ...         Field(
        ...             name="email",
        ...             type=String(length=255),
        ...             constraints=[NotNullConstraint()]
        ...         ),
        ...         Field(name="name", type=String())
        ...     ]
        ... )
        >>>
        >>> # Access schema properties
        >>> print(f"Schema: {spec.name} v{spec.version}")
        Schema: users v1.0.0
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
    columns: list[Field]
    description: str | None = None
    external: bool = False
    storage: Storage | None = None
    partitioned_by: list[TransformedColumn] = field(default_factory=list)
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
                raise SchemaValidationError(f"Duplicate column name found: {c.name!r}.")
            names.add(c.name)

    def _validate_partitions(self):
        for p_col in self.partition_column_names:
            if p_col not in self.column_names:
                raise SchemaValidationError(
                    f"Partition column {p_col!r} must be defined as a column in the schema."
                )

    def _validate_generated_columns(self):
        for gen_col, source_col in self.generated_columns.items():
            if source_col not in self.column_names:
                raise SchemaValidationError(
                    f"Source column {source_col!r} for generated column {gen_col!r} "
                    "not found in schema."
                )

    def _validate_table_constraints(self):
        for constraint in self.table_constraints:
            for col in constraint.get_constrained_columns():
                if col not in self.column_names:
                    raise SchemaValidationError(
                        f"Column {col!r} in constraint {constraint} not found in schema."
                    )

    @property
    def column_names(self) -> set[str]:
        """Set of all column names defined in the schema."""
        return {c.name for c in self.columns}

    @property
    def partition_column_names(self) -> set[str]:
        """Set of column names referenced in partitioning specifications."""
        return {p.column for p in self.partitioned_by}

    @property
    def generated_columns(self) -> dict[str, str]:
        """Mapping of generated column names to their source columns.

        Identifies columns that are computed from other columns rather than
        stored directly. The keys are generated column names and values are
        the source column names they derive from.

        Returns:
            Dictionary mapping generated column names to source column names.
        """
        return {
            c.name: c.generated_as.column
            for c in self.columns
            if c.generated_as is not None
        }

    def _build_header_str(self) -> str:
        return f"schema {self.name}(version={self.version!r})"

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
