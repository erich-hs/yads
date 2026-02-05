"""Load a `YadsSpec` from a PostgreSQL table schema.

This loader queries PostgreSQL catalog tables (information_schema, pg_catalog)
to extract complete table schema information and convert it to a canonical
`YadsSpec` instance.

Supported features:
- Column names, types, and nullability
- Primary key constraints (column and table level)
- Foreign key constraints (column and table level)
- Default values (literal values only)
- Identity/serial columns
- Generated/computed columns
- Composite types (converted to Struct)
- Array types (converted to Array, nested for multi-dimensional)
- PostGIS geometry/geography types

Example:
    >>> import psycopg2
    >>> from yads.loaders.sql import PostgreSqlLoader
    >>> conn = psycopg2.connect("postgresql://localhost/mydb")
    >>> loader = PostgreSqlLoader(conn)
    >>> spec = loader.load("users", schema="public")
    >>> spec.name
    'mydb.public.users'
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Literal

from ... import spec as yspec
from ... import types as ytypes
from ...constraints import (
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
from ...exceptions import LoaderError, validation_warning
from .base import SqlLoader, SqlLoaderConfig

if TYPE_CHECKING:
    from ...spec import YadsSpec


class PostgreSqlLoader(SqlLoader):
    """Load a `YadsSpec` from a PostgreSQL database table.

    Queries PostgreSQL catalog tables to extract complete table schema including:
    - Column names, types, and nullability
    - Primary key, foreign key constraints
    - Default values (literal values converted to DefaultConstraint)
    - Identity/serial columns (converted to IdentityConstraint)
    - Generated columns (converted to generated_as)
    - Composite types (converted to Struct)
    - Array types (converted to Array, nested for multi-dimensional)
    - PostGIS geometry/geography types

    Unsupported PostgreSQL features:
    - UNIQUE constraints (not yet in yads constraint model)
    - CHECK constraints (not yet in yads constraint model)
    - Non-literal default expressions (functions, expressions)
    - Exclusion constraints
    - Partition information

    In "raise" mode, encountering unsupported types raises UnsupportedFeatureError.
    In "coerce" mode, unsupported types are converted to the fallback type with warnings.
    """

    def __init__(
        self,
        connection: Any,
        config: SqlLoaderConfig | None = None,
    ) -> None:
        """Initialize the PostgreSqlLoader.

        Args:
            connection: A DBAPI-compatible PostgreSQL connection (e.g., psycopg2,
                psycopg, asyncpg in sync mode). Must support parameterized queries
                with %s placeholders.
            config: Configuration object. If None, uses default SqlLoaderConfig.
        """
        super().__init__(connection, config or SqlLoaderConfig())
        self._current_schema: str = "public"

    def load(
        self,
        table_name: str,
        *,
        schema: str = "public",
        name: str | None = None,
        version: int = 1,
        description: str | None = None,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> YadsSpec:
        """Load a YadsSpec from a PostgreSQL table.

        Args:
            table_name: Name of the table to load.
            schema: PostgreSQL schema name. Defaults to "public".
            name: Spec name to assign. Defaults to "{catalog}.{schema}.{table_name}"
                where catalog is the current database name.
            version: Spec version integer. Defaults to 1.
            description: Optional human-readable description for the spec.
            mode: Optional override for the loading mode. When not provided, the
                loader's configured mode is used.

        Returns:
            A validated immutable `YadsSpec` instance.

        Raises:
            LoaderError: If the table does not exist or cannot be read.
            UnsupportedFeatureError: In "raise" mode when encountering unsupported types.
        """
        with self.load_context(mode=mode):
            # Store current schema for composite type lookups
            self._current_schema = schema

            # Get current database name for fully qualified spec name
            catalog = self._get_current_database()

            # Query column information
            columns_info = self._query_columns(schema, table_name)
            if not columns_info:
                raise LoaderError(
                    f"Table '{schema}.{table_name}' not found or has no columns."
                )

            # Query supplementary information
            constraints = self._query_constraints(schema, table_name)
            array_info = self._query_array_info(schema, table_name)
            serial_columns = self._query_serial_columns(schema, table_name)

            # Build columns
            columns: list[dict[str, Any]] = []
            for col_info in columns_info:
                with self.load_context(field=col_info["column_name"]):
                    column_def = self._build_column(
                        col_info,
                        constraints,
                        array_info,
                        serial_columns,
                    )
                    columns.append(column_def)

            # Build spec data
            spec_name = name or f"{catalog}.{schema}.{table_name}"
            data: dict[str, Any] = {
                "name": spec_name,
                "version": version,
                "columns": columns,
            }

            if description:
                data["description"] = description

            # Add table-level constraints
            table_constraints = self._build_table_constraints(constraints)
            if table_constraints:
                data["table_constraints"] = table_constraints

            return yspec.from_dict(data)

    # %% ---- Query methods --------------------------------------------------------
    def _get_current_database(self) -> str:
        """Get the name of the currently connected database."""
        rows = self._execute_query("SELECT current_database()")
        return rows[0]["current_database"]

    def _query_columns(
        self,
        schema: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Query information_schema.columns for column details."""
        query = """
        SELECT
            column_name,
            ordinal_position,
            data_type,
            udt_name,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            datetime_precision,
            interval_type,
            is_nullable,
            column_default,
            is_identity,
            identity_generation,
            identity_start,
            identity_increment,
            is_generated,
            generation_expression
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        return self._execute_query(query, (schema, table_name))

    def _query_constraints(
        self,
        schema: str,
        table_name: str,
    ) -> dict[str, Any]:
        """Query constraint information from catalog views.

        Returns a dictionary with:
        - primary_key: {"columns": list[str], "name": str | None}
        - foreign_keys: list of {"columns": list[str], "ref_table": str,
            "ref_schema": str, "ref_columns": list[str], "name": str}
        - unique_constraints: list of {"columns": list[str], "name": str}
        """
        result: dict[str, Any] = {
            "primary_key": None,
            "foreign_keys": [],
            "unique_constraints": [],
        }

        # Query all constraints with their columns
        query = """
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            kcu.ordinal_position,
            ccu.table_schema AS ref_schema,
            ccu.table_name AS ref_table,
            ccu.column_name AS ref_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.constraint_type = 'FOREIGN KEY'
        WHERE tc.table_schema = %s AND tc.table_name = %s
        ORDER BY tc.constraint_name, kcu.ordinal_position
        """
        rows = self._execute_query(query, (schema, table_name))

        # Group by constraint
        constraints_by_name: dict[str, dict[str, Any]] = {}
        for row in rows:
            cname = row["constraint_name"]
            if cname not in constraints_by_name:
                constraints_by_name[cname] = {
                    "type": row["constraint_type"],
                    "name": cname,
                    "columns": [],
                    "ref_schema": row.get("ref_schema"),
                    "ref_table": row.get("ref_table"),
                    "ref_columns": [],
                }
            constraints_by_name[cname]["columns"].append(row["column_name"])
            if row.get("ref_column"):
                constraints_by_name[cname]["ref_columns"].append(row["ref_column"])

        # Organize by constraint type
        for cdata in constraints_by_name.values():
            ctype = cdata["type"]
            if ctype == "PRIMARY KEY":
                result["primary_key"] = {
                    "columns": cdata["columns"],
                    "name": cdata["name"],
                }
            elif ctype == "FOREIGN KEY":
                result["foreign_keys"].append(
                    {
                        "columns": cdata["columns"],
                        "ref_schema": cdata["ref_schema"],
                        "ref_table": cdata["ref_table"],
                        "ref_columns": cdata["ref_columns"],
                        "name": cdata["name"],
                    }
                )
            elif ctype == "UNIQUE":
                result["unique_constraints"].append(
                    {
                        "columns": cdata["columns"],
                        "name": cdata["name"],
                    }
                )
                # Emit warning since UNIQUE is not yet supported
                validation_warning(
                    f"UNIQUE constraint '{cdata['name']}' on columns "
                    f"{cdata['columns']} is not yet supported in yads and will be ignored.",
                    filename=__name__,
                    module=__name__,
                )

        return result

    def _query_array_info(
        self,
        schema: str,
        table_name: str,
    ) -> dict[str, tuple[str, int]]:
        """Query pg_catalog for array element types and dimensions.

        Returns a dict mapping column_name -> (element_type_name, dimensions).
        """
        query = """
        SELECT
            a.attname AS column_name,
            et.typname AS element_type,
            a.attndims AS dimensions
        FROM pg_catalog.pg_attribute a
        JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
        LEFT JOIN pg_catalog.pg_type et ON t.typelem = et.oid
        WHERE n.nspname = %s
            AND c.relname = %s
            AND a.attnum > 0
            AND NOT a.attisdropped
            AND t.typcategory = 'A'
        """
        rows = self._execute_query(query, (schema, table_name))
        return {
            row["column_name"]: (row["element_type"], row["dimensions"] or 1)
            for row in rows
        }

    def _query_serial_columns(
        self,
        schema: str,
        table_name: str,
    ) -> dict[str, dict[str, Any]]:
        """Query for SERIAL/BIGSERIAL columns via sequence ownership.

        Serial columns in PostgreSQL are implemented as integer columns with
        a sequence default. We detect them by checking pg_depend for sequence
        ownership relationships.

        Returns a dict mapping column_name -> {"start": int, "increment": int}.
        """
        query = """
        SELECT
            a.attname AS column_name,
            s.seqstart AS start_value,
            s.seqincrement AS increment
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        JOIN pg_catalog.pg_depend d ON d.refobjid = c.oid AND d.refobjsubid = a.attnum
        JOIN pg_catalog.pg_class seq ON seq.oid = d.objid
        JOIN pg_catalog.pg_sequence s ON s.seqrelid = seq.oid
        WHERE n.nspname = %s
            AND c.relname = %s
            AND d.deptype = 'a'
            AND seq.relkind = 'S'
        """
        rows = self._execute_query(query, (schema, table_name))
        return {
            row["column_name"]: {
                "start": row["start_value"],
                "increment": row["increment"],
            }
            for row in rows
        }

    def _query_composite_type(
        self,
        type_name: str,
        type_schema: str = "public",
    ) -> list[yspec.Field] | None:
        """Query pg_catalog for composite type structure.

        Returns a list of Field objects if the type is a composite type,
        or None if it's not a composite type.
        """
        query = """
        SELECT
            a.attname AS field_name,
            a.attnum AS field_position,
            t.typname AS field_type,
            a.attnotnull AS not_null
        FROM pg_catalog.pg_type ct
        JOIN pg_catalog.pg_namespace n ON ct.typnamespace = n.oid
        JOIN pg_catalog.pg_attribute a ON a.attrelid = ct.typrelid
        JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
        WHERE n.nspname = %s
            AND ct.typname = %s
            AND ct.typtype = 'c'
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY a.attnum
        """
        rows = self._execute_query(query, (type_schema, type_name))

        if not rows:
            return None

        fields: list[yspec.Field] = []
        for row in rows:
            field_type = self._convert_simple_type(row["field_type"], {})
            if field_type is None:
                # Unsupported type in composite - raise or coerce
                field_type = self.raise_or_coerce(row["field_type"])

            field_constraints: list[ColumnConstraint] = []
            if row["not_null"]:
                field_constraints.append(NotNullConstraint())

            fields.append(
                yspec.Field(
                    name=row["field_name"],
                    type=field_type,
                    constraints=field_constraints,
                )
            )

        return fields

    # %% ---- Column building ------------------------------------------------------
    def _build_column(
        self,
        col_info: dict[str, Any],
        constraints: dict[str, Any],
        array_info: dict[str, tuple[str, int]],
        serial_columns: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Build a column definition dictionary from catalog information."""
        col_name = col_info["column_name"]

        # Convert type
        yads_type = self._convert_type(col_info, array_info)

        # Build constraints list
        col_constraints = self._build_column_constraints(
            col_info, constraints, serial_columns
        )

        # Build generated_as if applicable
        generated_as = self._build_generated_as(col_info)

        # Serialize
        payload: dict[str, Any] = {"name": col_name}
        payload.update(self._type_serializer.serialize(yads_type))

        if col_constraints:
            serialized_constraints = (
                self._constraint_serializer.serialize_column_constraints(col_constraints)
            )
            if serialized_constraints:
                payload["constraints"] = serialized_constraints

        if generated_as:
            generated_as_dict: dict[str, Any] = {
                "column": generated_as.column,
            }
            if generated_as.transform:
                generated_as_dict["transform"] = generated_as.transform
            if generated_as.transform_args:
                generated_as_dict["transform_args"] = generated_as.transform_args
            payload["generated_as"] = generated_as_dict

        return payload

    def _build_column_constraints(
        self,
        col_info: dict[str, Any],
        constraints: dict[str, Any],
        serial_columns: dict[str, dict[str, Any]],
    ) -> list[ColumnConstraint]:
        """Build column-level constraints from catalog information."""
        col_name = col_info["column_name"]
        result: list[ColumnConstraint] = []

        # NOT NULL
        if col_info["is_nullable"] == "NO":
            result.append(NotNullConstraint())

        # PRIMARY KEY (single column only - composite handled at table level)
        pk_info = constraints.get("primary_key")
        if pk_info and len(pk_info["columns"]) == 1 and col_name in pk_info["columns"]:
            result.append(PrimaryKeyConstraint())

        # FOREIGN KEY (single column only - composite handled at table level)
        for fk in constraints.get("foreign_keys", []):
            if len(fk["columns"]) == 1 and col_name in fk["columns"]:
                ref_table = fk["ref_table"]
                if fk.get("ref_schema") and fk["ref_schema"] != "public":
                    ref_table = f"{fk['ref_schema']}.{fk['ref_table']}"
                result.append(
                    ForeignKeyConstraint(
                        references=ForeignKeyReference(
                            table=ref_table,
                            columns=fk["ref_columns"] if fk["ref_columns"] else None,
                        ),
                        name=fk["name"],
                    )
                )

        # IDENTITY (explicit identity columns)
        if col_info["is_identity"] == "YES":
            result.append(
                IdentityConstraint(
                    always=(col_info["identity_generation"] == "ALWAYS"),
                    start=_safe_int(col_info.get("identity_start")),
                    increment=_safe_int(col_info.get("identity_increment")),
                )
            )
        # SERIAL (sequence-backed columns)
        elif col_name in serial_columns:
            serial_info = serial_columns[col_name]
            result.append(
                IdentityConstraint(
                    always=False,  # SERIAL allows manual values
                    start=serial_info.get("start"),
                    increment=serial_info.get("increment"),
                )
            )

        # DEFAULT (only for non-identity, non-serial columns)
        if (
            col_info["is_identity"] != "YES"
            and col_name not in serial_columns
            and col_info.get("column_default")
        ):
            default_constraint = self._parse_default_value(col_info["column_default"])
            if default_constraint:
                result.append(default_constraint)

        return result

    def _build_table_constraints(
        self,
        constraints: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Build table-level constraints from catalog information."""
        table_constraints: list[TableConstraint] = []

        # PRIMARY KEY (composite only)
        pk_info = constraints.get("primary_key")
        if pk_info and len(pk_info["columns"]) > 1:
            table_constraints.append(
                PrimaryKeyTableConstraint(
                    columns=pk_info["columns"],
                    name=pk_info.get("name"),
                )
            )

        # FOREIGN KEY (composite only)
        for fk in constraints.get("foreign_keys", []):
            if len(fk["columns"]) > 1:
                ref_table = fk["ref_table"]
                if fk.get("ref_schema") and fk["ref_schema"] != "public":
                    ref_table = f"{fk['ref_schema']}.{fk['ref_table']}"
                table_constraints.append(
                    ForeignKeyTableConstraint(
                        columns=fk["columns"],
                        references=ForeignKeyReference(
                            table=ref_table,
                            columns=fk["ref_columns"] if fk["ref_columns"] else None,
                        ),
                        name=fk["name"],
                    )
                )

        return self._constraint_serializer.serialize_table_constraints(table_constraints)

    def _build_generated_as(
        self,
        col_info: dict[str, Any],
    ) -> yspec.TransformedColumnReference | None:
        """Build generated_as for computed columns."""
        if col_info.get("is_generated") != "ALWAYS":
            return None

        expression = col_info.get("generation_expression")
        if not expression:
            return None

        # Try to parse the generation expression
        # PostgreSQL format varies: "column_name", "(expr)", etc.
        parsed = self._parse_generation_expression(expression)
        if parsed:
            return parsed

        # If we can't parse it, emit a warning
        validation_warning(
            f"Could not parse generation expression '{expression}' for column "
            f"'{col_info['column_name']}'. Generated column will not be represented.",
            filename=__name__,
            module=__name__,
        )
        return None

    # %% ---- Type conversion ------------------------------------------------------
    def _convert_type(
        self,
        col_info: dict[str, Any],
        array_info: dict[str, tuple[str, int]],
    ) -> ytypes.YadsType:
        """Convert PostgreSQL type to YadsType."""
        data_type = col_info["data_type"].lower()

        # Handle ARRAY types
        if data_type == "array":
            return self._convert_array_type(col_info, array_info)

        # Handle USER-DEFINED types (composites, domains, enums)
        if data_type == "user-defined":
            return self._convert_user_defined_type(col_info)

        # Handle standard types
        result = self._convert_simple_type(data_type, col_info)
        if result is not None:
            return result

        # Try udt_name as fallback
        udt_name = col_info.get("udt_name", "").lower()
        if udt_name and udt_name != data_type:
            result = self._convert_simple_type(udt_name, col_info)
            if result is not None:
                return result

        # Unknown type - raise or coerce
        return self.raise_or_coerce(data_type)

    def _convert_simple_type(
        self,
        type_name: str,
        col_info: dict[str, Any],
    ) -> ytypes.YadsType | None:
        """Convert a simple (non-array, non-composite) PostgreSQL type.

        Returns None if the type is not recognized.
        """
        type_name = type_name.lower()

        # Integers
        if type_name in ("smallint", "int2"):
            return ytypes.Integer(bits=16, signed=True)
        if type_name in ("integer", "int", "int4"):
            return ytypes.Integer(bits=32, signed=True)
        if type_name in ("bigint", "int8"):
            return ytypes.Integer(bits=64, signed=True)

        # Floats
        if type_name in ("real", "float4"):
            return ytypes.Float(bits=32)
        if type_name in ("double precision", "float8"):
            return ytypes.Float(bits=64)

        # Decimal/Numeric
        if type_name in ("numeric", "decimal"):
            precision = col_info.get("numeric_precision")
            scale = col_info.get("numeric_scale")
            if precision is not None and scale is not None:
                return ytypes.Decimal(precision=precision, scale=scale)
            return ytypes.Decimal()

        # Strings
        if type_name in ("character varying", "varchar"):
            length = col_info.get("character_maximum_length")
            return ytypes.String(length=length)
        if type_name in ("character", "char", "bpchar"):
            length = col_info.get("character_maximum_length")
            return ytypes.String(length=length)
        if type_name == "text":
            return ytypes.String()
        if type_name == "name":
            # PostgreSQL identifier type (63 chars max)
            return ytypes.String(length=63)

        # Binary
        if type_name == "bytea":
            return ytypes.Binary()

        # Boolean
        if type_name in ("boolean", "bool"):
            return ytypes.Boolean()

        # Date/Time
        if type_name == "date":
            return ytypes.Date(bits=32)

        if type_name in ("time", "time without time zone"):
            return ytypes.Time(unit=ytypes.TimeUnit.US)

        if type_name == "time with time zone":
            # yads Time doesn't have timezone - emit warning
            validation_warning(
                "PostgreSQL 'time with time zone' will be converted to Time without "
                "timezone information. Timezone data will be lost.",
                filename=__name__,
                module=__name__,
            )
            return ytypes.Time(unit=ytypes.TimeUnit.US)

        if type_name in ("timestamp", "timestamp without time zone"):
            return ytypes.TimestampNTZ(unit=ytypes.TimeUnit.US)

        if type_name == "timestamp with time zone":
            return ytypes.TimestampTZ(unit=ytypes.TimeUnit.US, tz="UTC")

        # Interval
        if type_name == "interval":
            return self._convert_interval_type(col_info)

        # UUID
        if type_name == "uuid":
            return ytypes.UUID()

        # JSON
        if type_name in ("json", "jsonb"):
            return ytypes.JSON()

        # PostGIS types
        if type_name == "geometry":
            return ytypes.Geometry()
        if type_name == "geography":
            return ytypes.Geography()

        return None

    def _convert_array_type(
        self,
        col_info: dict[str, Any],
        array_info: dict[str, tuple[str, int]],
    ) -> ytypes.YadsType:
        """Convert PostgreSQL array type to Array or nested Array."""
        col_name = col_info["column_name"]
        udt_name = col_info.get("udt_name", "").lower()

        # Get element type and dimensions from pg_catalog
        if col_name in array_info:
            element_type_name, dimensions = array_info[col_name]
        else:
            # Fallback: parse from udt_name (e.g., "_int4" -> "int4")
            element_type_name = (
                udt_name.lstrip("_") if udt_name.startswith("_") else udt_name
            )
            dimensions = 1

        # Convert element type
        element_type = self._convert_simple_type(element_type_name, {})
        if element_type is None:
            # Check if it's a composite type
            fields = self._query_composite_type(element_type_name, self._current_schema)
            if fields:
                element_type = ytypes.Struct(fields=fields)
            else:
                # Unknown element type
                element_type = self.raise_or_coerce(
                    element_type_name,
                    error_msg=f"Unknown array element type '{element_type_name}' for field '{col_name}'",
                )

        # Build nested arrays for multi-dimensional
        if dimensions > 1:
            validation_warning(
                f"Multi-dimensional array ({dimensions}D) for column '{col_name}' "
                f"will be represented as nested Arrays. Tensor type requires explicit shape.",
                filename=__name__,
                module=__name__,
            )
            result: ytypes.YadsType = element_type
            for _ in range(dimensions):
                result = ytypes.Array(element=result)
            return result

        return ytypes.Array(element=element_type)

    def _convert_user_defined_type(
        self,
        col_info: dict[str, Any],
    ) -> ytypes.YadsType:
        """Convert PostgreSQL USER-DEFINED type (composite, domain, enum)."""
        udt_name = col_info.get("udt_name", "")
        col_name = col_info["column_name"]

        # Check for PostGIS types first
        if udt_name.lower() == "geometry":
            return ytypes.Geometry()
        if udt_name.lower() == "geography":
            return ytypes.Geography()

        # Try to resolve as composite type
        fields = self._query_composite_type(udt_name, self._current_schema)
        if fields:
            return ytypes.Struct(fields=fields)

        # Try to resolve as domain type
        domain_base = self._resolve_domain_type(udt_name, self._current_schema)
        if domain_base:
            return domain_base

        # Unknown user-defined type
        return self.raise_or_coerce(
            udt_name,
            error_msg=f"Unknown user-defined type '{udt_name}' for field '{col_name}'",
        )

    def _resolve_domain_type(
        self,
        domain_name: str,
        schema: str = "public",
    ) -> ytypes.YadsType | None:
        """Resolve a domain type to its base type."""
        query = """
        SELECT
            t.typname AS base_type,
            t.typlen AS type_length
        FROM pg_catalog.pg_type d
        JOIN pg_catalog.pg_namespace n ON d.typnamespace = n.oid
        JOIN pg_catalog.pg_type t ON d.typbasetype = t.oid
        WHERE n.nspname = %s
            AND d.typname = %s
            AND d.typtype = 'd'
        """
        rows = self._execute_query(query, (schema, domain_name))

        if not rows:
            return None

        base_type_name = rows[0]["base_type"]
        result = self._convert_simple_type(base_type_name, {})

        if result is None:
            # Base type is also unsupported
            validation_warning(
                f"Domain type '{domain_name}' has unsupported base type '{base_type_name}'.",
                filename=__name__,
                module=__name__,
            )

        return result

    def _convert_interval_type(
        self,
        col_info: dict[str, Any],
    ) -> ytypes.Interval:
        """Convert PostgreSQL interval type with optional fields specification."""
        interval_type = col_info.get("interval_type")

        if not interval_type:
            # Default: DAY TO SECOND
            return ytypes.Interval(
                interval_start=ytypes.IntervalTimeUnit.DAY,
                interval_end=ytypes.IntervalTimeUnit.SECOND,
            )

        # Parse interval_type: "YEAR", "YEAR TO MONTH", "DAY TO SECOND", etc.
        interval_type = interval_type.upper()

        unit_map = {
            "YEAR": ytypes.IntervalTimeUnit.YEAR,
            "MONTH": ytypes.IntervalTimeUnit.MONTH,
            "DAY": ytypes.IntervalTimeUnit.DAY,
            "HOUR": ytypes.IntervalTimeUnit.HOUR,
            "MINUTE": ytypes.IntervalTimeUnit.MINUTE,
            "SECOND": ytypes.IntervalTimeUnit.SECOND,
        }

        if " TO " in interval_type:
            parts = interval_type.split(" TO ")
            start = unit_map.get(parts[0].strip())
            end = unit_map.get(parts[1].strip())
            if start and end:
                return ytypes.Interval(interval_start=start, interval_end=end)
        else:
            unit = unit_map.get(interval_type.strip())
            if unit:
                return ytypes.Interval(interval_start=unit)

        # Fallback to DAY TO SECOND
        return ytypes.Interval(
            interval_start=ytypes.IntervalTimeUnit.DAY,
            interval_end=ytypes.IntervalTimeUnit.SECOND,
        )

    # %% ---- Default value parsing --------------------------------------------
    def _parse_default_value(
        self,
        default_expr: str,
    ) -> DefaultConstraint | None:
        """Parse PostgreSQL default expression.

        Only returns DefaultConstraint for literal values.
        Emits a warning for function calls or complex expressions.
        """
        if not default_expr:
            return None

        expr = default_expr.strip()

        # Check for NULL
        if expr.upper() == "NULL":
            return DefaultConstraint(value=None)

        # Check for function calls (common patterns)
        function_patterns = [
            r"^\w+\(",  # function_name(
            r"^nextval\(",  # sequence
            r"^now\(",
            r"^current_",  # current_timestamp, current_date, etc.
            r"^gen_random_uuid\(",
            r"^uuid_generate_",
        ]
        for pattern in function_patterns:
            if re.match(pattern, expr, re.IGNORECASE):
                validation_warning(
                    f"Default expression '{expr}' is a function call. "
                    f"Non-literal defaults are not yet supported in yads.",
                    filename=__name__,
                    module=__name__,
                )
                return None

        # Try to parse as literal
        value = self._extract_literal_value(expr)
        if value is not None:
            return DefaultConstraint(value=value)

        # Complex expression - emit warning
        validation_warning(
            f"Default expression '{expr}' could not be parsed as a literal. "
            f"Non-literal defaults are not yet supported in yads.",
            filename=__name__,
            module=__name__,
        )
        return None

    def _extract_literal_value(self, expr: str) -> Any:
        """Extract literal value from PostgreSQL default expression.

        Handles:
        - String literals: 'value'::type or 'value'
        - Numeric literals: 42, 3.14, -17
        - Boolean literals: true, false
        - NULL
        """
        expr = expr.strip()

        # NULL
        if expr.upper() == "NULL":
            return None

        # Boolean
        if expr.upper() == "TRUE":
            return True
        if expr.upper() == "FALSE":
            return False

        # String literal: 'value'::type or just 'value'
        string_match = re.match(r"^'((?:[^']|'')*)'(?:::[\w\s]+)?$", expr)
        if string_match:
            # Unescape doubled single quotes
            return string_match.group(1).replace("''", "'")

        # Numeric literal (integer or float)
        # Handle optional type cast: 42::integer, 3.14::numeric
        numeric_match = re.match(r"^(-?\d+\.?\d*)(?:::[\w\s]+)?$", expr)
        if numeric_match:
            num_str = numeric_match.group(1)
            if "." in num_str:
                return float(num_str)
            return int(num_str)

        # Negative number with parentheses: (-42)
        neg_match = re.match(r"^\((-\d+\.?\d*)\)(?:::[\w\s]+)?$", expr)
        if neg_match:
            num_str = neg_match.group(1)
            if "." in num_str:
                return float(num_str)
            return int(num_str)

        return None

    # %% ---- Generation expression parsing --------------------------------------
    def _parse_generation_expression(
        self,
        expression: str,
    ) -> yspec.TransformedColumnReference | None:
        """Parse PostgreSQL generation expression to TransformedColumnReference.

        Handles simple cases:
        - Direct column reference: "column_name"
        - Simple expressions: "(column_a + column_b)"
        - Function calls: "upper(column_name)"
        """
        expr = expression.strip()

        # Remove outer parentheses if present
        if expr.startswith("(") and expr.endswith(")"):
            expr = expr[1:-1].strip()

        # Simple column reference (possibly quoted)
        if re.match(r'^"?\w+"?$', expr):
            col_name = expr.strip('"')
            return yspec.TransformedColumnReference(column=col_name)

        # Function call: func(column, args...)
        func_match = re.match(r"^(\w+)\((.+)\)$", expr)
        if func_match:
            func_name = func_match.group(1)
            args_str = func_match.group(2)

            # Try to extract first argument as column name
            # This is a simplified parser - complex expressions may not parse correctly
            args = [a.strip().strip('"') for a in args_str.split(",")]
            if args:
                return yspec.TransformedColumnReference(
                    column=args[0],
                    transform=func_name,
                    transform_args=args[1:] if len(args) > 1 else [],
                )

        # Binary operation: col_a + col_b, col_a * 2, etc.
        # Extract first identifier as the source column
        ident_match = re.match(r'^"?(\w+)"?', expr)
        if ident_match:
            return yspec.TransformedColumnReference(
                column=ident_match.group(1),
                transform="expression",
                transform_args=[expr],
            )

        return None


def _safe_int(value: Any) -> int | None:
    """Safely convert a value to int, returning None if not possible."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
