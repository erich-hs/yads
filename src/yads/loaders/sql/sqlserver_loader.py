"""Load a `YadsSpec` from a SQL Server table schema.

This loader queries SQL Server catalog views (INFORMATION_SCHEMA, sys.*) to
extract complete table schema information and convert it to a canonical
`YadsSpec` instance.

Supported features:
- Column names, types, and nullability
- Primary key constraints (column and table level)
- Foreign key constraints (column and table level)
- Default values (literal values only)
- Identity columns
- Computed columns (converted to generated_as)
- Spatial types (geometry, geography)

Example:
    >>> import pyodbc
    >>> from yads.loaders.sql import SqlServerLoader
    >>> conn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};...")
    >>> loader = SqlServerLoader(conn)
    >>> spec = loader.load("users", schema="dbo")
    >>> spec.name
    'mydb.dbo.users'
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


class SqlServerLoader(SqlLoader):
    """Load a `YadsSpec` from a SQL Server database table.

    Queries SQL Server catalog views to extract complete table schema including:
    - Column names, types, and nullability
    - Primary key, foreign key constraints
    - Default values (literal values converted to DefaultConstraint)
    - Identity columns (converted to IdentityConstraint)
    - Computed columns (converted to generated_as)
    - Spatial types (geometry, geography)

    Unsupported SQL Server features:
    - UNIQUE constraints (not yet in yads constraint model)
    - CHECK constraints (not yet in yads constraint model)
    - Non-literal default expressions (functions, expressions)
    - XML type
    - MONEY/SMALLMONEY types
    - SQL_VARIANT type
    - HIERARCHYID type
    - Filestream/Filetable columns
    - Temporal tables (system versioning)

    In "raise" mode, encountering unsupported types raises UnsupportedFeatureError.
    In "coerce" mode, unsupported types are converted to the fallback type with warnings.
    """

    def __init__(
        self,
        connection: Any,
        config: SqlLoaderConfig | None = None,
    ) -> None:
        """Initialize the SqlServerLoader.

        Args:
            connection: A DBAPI-compatible SQL Server connection (e.g., pyodbc,
                pymssql). Must support parameterized queries with ? placeholders.
            config: Configuration object. If None, uses default SqlLoaderConfig.
        """
        super().__init__(connection, config or SqlLoaderConfig())
        self._current_schema: str = "dbo"

    def load(
        self,
        table_name: str,
        *,
        schema: str = "dbo",
        name: str | None = None,
        version: int = 1,
        description: str | None = None,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> YadsSpec:
        """Load a YadsSpec from a SQL Server table.

        Args:
            table_name: Name of the table to load.
            schema: SQL Server schema name. Defaults to "dbo".
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
            # Store current schema for lookups
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
            identity_columns = self._query_identity_columns(schema, table_name)
            computed_columns = self._query_computed_columns(schema, table_name)

            # Build columns
            columns: list[dict[str, Any]] = []
            for col_info in columns_info:
                with self.load_context(field=col_info["column_name"]):
                    column_def = self._build_column(
                        col_info,
                        constraints,
                        identity_columns,
                        computed_columns,
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

    # ---- Query methods --------------------------------------------------------

    def _get_current_database(self) -> str:
        """Get the name of the currently connected database."""
        rows = self._execute_query("SELECT DB_NAME() AS current_database")
        return rows[0]["current_database"]

    def _query_columns(
        self,
        schema: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """Query INFORMATION_SCHEMA.COLUMNS for column details."""
        query = """
        SELECT
            c.COLUMN_NAME AS column_name,
            c.ORDINAL_POSITION AS ordinal_position,
            c.DATA_TYPE AS data_type,
            c.CHARACTER_MAXIMUM_LENGTH AS character_maximum_length,
            c.NUMERIC_PRECISION AS numeric_precision,
            c.NUMERIC_SCALE AS numeric_scale,
            c.DATETIME_PRECISION AS datetime_precision,
            c.IS_NULLABLE AS is_nullable,
            c.COLUMN_DEFAULT AS column_default
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
        ORDER BY c.ORDINAL_POSITION
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

        # Query all constraints with their columns using sys views
        # INFORMATION_SCHEMA doesn't properly expose FK referenced columns
        query = """
        SELECT
            kc.name AS constraint_name,
            CASE
                WHEN pk.object_id IS NOT NULL THEN 'PRIMARY KEY'
                WHEN fk.object_id IS NOT NULL THEN 'FOREIGN KEY'
                WHEN uq.object_id IS NOT NULL THEN 'UNIQUE'
            END AS constraint_type,
            c.name AS column_name,
            ic.key_ordinal AS ordinal_position,
            rs.name AS ref_schema,
            rt.name AS ref_table,
            rc.name AS ref_column
        FROM sys.key_constraints kc
        JOIN sys.tables t ON kc.parent_object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.index_columns ic ON kc.unique_index_id = ic.index_id
            AND kc.parent_object_id = ic.object_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        LEFT JOIN sys.key_constraints pk ON kc.object_id = pk.object_id
            AND pk.type = 'PK'
        LEFT JOIN sys.key_constraints uq ON kc.object_id = uq.object_id
            AND uq.type = 'UQ'
        LEFT JOIN sys.foreign_keys fk ON kc.object_id = fk.object_id
        LEFT JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
        LEFT JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
        LEFT JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            AND c.column_id = fkc.parent_column_id
        LEFT JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id
            AND fkc.referenced_column_id = rc.column_id
        WHERE s.name = ? AND t.name = ?

        UNION ALL

        SELECT
            fk.name AS constraint_name,
            'FOREIGN KEY' AS constraint_type,
            pc.name AS column_name,
            fkc.constraint_column_id AS ordinal_position,
            rs.name AS ref_schema,
            rt.name AS ref_table,
            rc.name AS ref_column
        FROM sys.foreign_keys fk
        JOIN sys.tables t ON fk.parent_object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
        JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id
            AND fkc.parent_column_id = pc.column_id
        JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
        JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
        JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id
            AND fkc.referenced_column_id = rc.column_id
        WHERE s.name = ? AND t.name = ?

        ORDER BY constraint_name, ordinal_position
        """
        rows = self._execute_query(query, (schema, table_name, schema, table_name))

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
            col_name = row["column_name"]
            if col_name and col_name not in constraints_by_name[cname]["columns"]:
                constraints_by_name[cname]["columns"].append(col_name)
            ref_col = row.get("ref_column")
            if ref_col and ref_col not in constraints_by_name[cname]["ref_columns"]:
                constraints_by_name[cname]["ref_columns"].append(ref_col)

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

    def _query_identity_columns(
        self,
        schema: str,
        table_name: str,
    ) -> dict[str, dict[str, Any]]:
        """Query for IDENTITY columns.

        Returns a dict mapping column_name -> {"seed": int, "increment": int}.
        """
        query = """
        SELECT
            c.name AS column_name,
            CAST(ic.seed_value AS BIGINT) AS seed_value,
            CAST(ic.increment_value AS BIGINT) AS increment_value
        FROM sys.identity_columns ic
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ?
        """
        rows = self._execute_query(query, (schema, table_name))
        return {
            row["column_name"]: {
                "seed": _safe_int(row["seed_value"]),
                "increment": _safe_int(row["increment_value"]),
            }
            for row in rows
        }

    def _query_computed_columns(
        self,
        schema: str,
        table_name: str,
    ) -> dict[str, dict[str, Any]]:
        """Query for computed columns.

        Returns a dict mapping column_name -> {"definition": str, "is_persisted": bool}.
        """
        query = """
        SELECT
            c.name AS column_name,
            cc.definition AS definition,
            cc.is_persisted AS is_persisted
        FROM sys.computed_columns cc
        JOIN sys.columns c ON cc.object_id = c.object_id AND cc.column_id = c.column_id
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ?
        """
        rows = self._execute_query(query, (schema, table_name))
        return {
            row["column_name"]: {
                "definition": row["definition"],
                "is_persisted": row["is_persisted"],
            }
            for row in rows
        }

    # ---- Column building ------------------------------------------------------

    def _build_column(
        self,
        col_info: dict[str, Any],
        constraints: dict[str, Any],
        identity_columns: dict[str, dict[str, Any]],
        computed_columns: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Build a column definition dictionary from catalog information."""
        col_name = col_info["column_name"]

        # Check if this is a computed column
        is_computed = col_name in computed_columns

        # Convert type
        yads_type = self._convert_type(col_info)

        # Build constraints list (skip for computed columns - they can't have constraints)
        col_constraints: list[ColumnConstraint] = []
        if not is_computed:
            col_constraints = self._build_column_constraints(
                col_info, constraints, identity_columns
            )

        # Build generated_as if this is a computed column
        generated_as = self._build_generated_as(col_name, computed_columns)

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
        identity_columns: dict[str, dict[str, Any]],
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
                if fk.get("ref_schema") and fk["ref_schema"] != "dbo":
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

        # IDENTITY
        if col_name in identity_columns:
            identity_info = identity_columns[col_name]
            result.append(
                IdentityConstraint(
                    always=False,  # SQL Server IDENTITY allows SET IDENTITY_INSERT ON
                    start=identity_info.get("seed"),
                    increment=identity_info.get("increment"),
                )
            )

        # DEFAULT (only for non-identity columns)
        if col_name not in identity_columns and col_info.get("column_default"):
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
                if fk.get("ref_schema") and fk["ref_schema"] != "dbo":
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
        col_name: str,
        computed_columns: dict[str, dict[str, Any]],
    ) -> yspec.TransformedColumnReference | None:
        """Build generated_as for computed columns."""
        if col_name not in computed_columns:
            return None

        expression = computed_columns[col_name].get("definition")
        if not expression:
            return None

        # Try to parse the computation expression
        parsed = self._parse_computation_expression(expression)
        if parsed:
            return parsed

        # If we can't parse it, emit a warning
        validation_warning(
            f"Could not parse computation expression '{expression}' for column "
            f"'{col_name}'. Computed column will not be represented.",
            filename=__name__,
            module=__name__,
        )
        return None

    # ---- Type conversion ------------------------------------------------------

    def _convert_type(
        self,
        col_info: dict[str, Any],
    ) -> ytypes.YadsType:
        """Convert SQL Server type to YadsType."""
        data_type = col_info["data_type"].lower()

        # Handle standard types
        result = self._convert_simple_type(data_type, col_info)
        if result is not None:
            return result

        # Unknown type - raise or coerce
        return self.raise_or_coerce(data_type)

    def _convert_simple_type(
        self,
        type_name: str,
        col_info: dict[str, Any],
    ) -> ytypes.YadsType | None:
        """Convert a simple SQL Server type.

        Returns None if the type is not recognized.
        """
        type_name = type_name.lower()

        # Integers
        if type_name == "tinyint":
            # SQL Server TINYINT is 0-255 (unsigned 8-bit)
            return ytypes.Integer(bits=8, signed=False)
        if type_name == "smallint":
            return ytypes.Integer(bits=16, signed=True)
        if type_name in ("int", "integer"):
            return ytypes.Integer(bits=32, signed=True)
        if type_name == "bigint":
            return ytypes.Integer(bits=64, signed=True)

        # Floats
        if type_name == "real":
            return ytypes.Float(bits=32)
        if type_name == "float":
            # SQL Server FLOAT can be FLOAT(n) where n determines precision
            # FLOAT(1-24) is 32-bit, FLOAT(25-53) is 64-bit
            # Default is 53 (64-bit)
            return ytypes.Float(bits=64)

        # Decimal/Numeric
        if type_name in ("numeric", "decimal"):
            precision = col_info.get("numeric_precision")
            scale = col_info.get("numeric_scale")
            if precision is not None and scale is not None:
                return ytypes.Decimal(precision=precision, scale=scale)
            return ytypes.Decimal()

        # Strings - ANSI
        if type_name == "char":
            length = col_info.get("character_maximum_length")
            return ytypes.String(length=length)
        if type_name == "varchar":
            length = col_info.get("character_maximum_length")
            # VARCHAR(MAX) has length -1
            if length == -1:
                return ytypes.String()
            return ytypes.String(length=length)
        if type_name == "text":
            return ytypes.String()

        # Strings - Unicode
        if type_name == "nchar":
            length = col_info.get("character_maximum_length")
            return ytypes.String(length=length)
        if type_name == "nvarchar":
            length = col_info.get("character_maximum_length")
            # NVARCHAR(MAX) has length -1
            if length == -1:
                return ytypes.String()
            return ytypes.String(length=length)
        if type_name == "ntext":
            return ytypes.String()

        # Binary
        if type_name == "binary":
            return ytypes.Binary()
        if type_name == "varbinary":
            # VARBINARY(MAX) has length -1
            return ytypes.Binary()
        if type_name == "image":
            return ytypes.Binary()

        # Boolean - SQL Server BIT
        if type_name == "bit":
            return ytypes.Boolean()

        # Date/Time
        if type_name == "date":
            return ytypes.Date(bits=32)

        if type_name == "time":
            return ytypes.Time(unit=ytypes.TimeUnit.US)

        if type_name == "smalldatetime":
            # SMALLDATETIME has minute precision
            return ytypes.TimestampNTZ(unit=ytypes.TimeUnit.S)

        if type_name == "datetime":
            # DATETIME has ~3.33ms precision (rounded to .000, .003, or .007)
            return ytypes.TimestampNTZ(unit=ytypes.TimeUnit.MS)

        if type_name == "datetime2":
            # DATETIME2 has up to 100ns precision
            return ytypes.TimestampNTZ(unit=ytypes.TimeUnit.US)

        if type_name == "datetimeoffset":
            # DATETIMEOFFSET includes timezone offset
            return ytypes.TimestampTZ(unit=ytypes.TimeUnit.US, tz="UTC")

        # UUID
        if type_name == "uniqueidentifier":
            return ytypes.UUID()

        # Spatial types
        if type_name == "geometry":
            return ytypes.Geometry()
        if type_name == "geography":
            return ytypes.Geography()

        return None

    # ---- Default value parsing ------------------------------------------------

    def _parse_default_value(
        self,
        default_expr: str,
    ) -> DefaultConstraint | None:
        """Parse SQL Server default expression.

        Only returns DefaultConstraint for literal values.
        Emits a warning for function calls or complex expressions.
        """
        if not default_expr:
            return None

        expr = default_expr.strip()

        # SQL Server wraps defaults in parentheses, often multiple layers
        # e.g., ((1)), (('value')), ((getdate()))
        while expr.startswith("(") and expr.endswith(")"):
            expr = expr[1:-1].strip()

        # Check for NULL
        if expr.upper() == "NULL":
            return DefaultConstraint(value=None)

        # Check for function calls (common patterns)
        function_patterns = [
            r"^\w+\(",  # function_name(
            r"^getdate\(",
            r"^getutcdate\(",
            r"^sysdatetime\(",
            r"^sysutcdatetime\(",
            r"^newid\(",
            r"^newsequentialid\(",
            r"^current_timestamp",
            r"^current_user",
            r"^system_user",
            r"^user_name\(",
        ]
        for pattern in function_patterns:
            if re.match(pattern, expr, re.IGNORECASE):
                validation_warning(
                    f"Default expression '{default_expr}' is a function call. "
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
            f"Default expression '{default_expr}' could not be parsed as a literal. "
            f"Non-literal defaults are not yet supported in yads.",
            filename=__name__,
            module=__name__,
        )
        return None

    def _extract_literal_value(self, expr: str) -> Any:
        """Extract literal value from SQL Server default expression.

        Handles:
        - String literals: 'value' or N'value'
        - Numeric literals: 42, 3.14, -17
        - Boolean literals: 1, 0 (SQL Server uses BIT)
        - NULL
        """
        expr = expr.strip()

        # NULL
        if expr.upper() == "NULL":
            return None

        # String literal: 'value' or N'value'
        string_match = re.match(r"^N?'((?:[^']|'')*)'$", expr)
        if string_match:
            # Unescape doubled single quotes
            return string_match.group(1).replace("''", "'")

        # Numeric literal (integer or float)
        numeric_match = re.match(r"^(-?\d+\.?\d*)$", expr)
        if numeric_match:
            num_str = numeric_match.group(1)
            if "." in num_str:
                return float(num_str)
            return int(num_str)

        return None

    # ---- Computation expression parsing ---------------------------------------

    def _parse_computation_expression(
        self,
        expression: str,
    ) -> yspec.TransformedColumnReference | None:
        """Parse SQL Server computed column expression to TransformedColumnReference.

        Handles simple cases:
        - Direct column reference: [column_name]
        - Simple expressions: ([column_a]+[column_b])
        - Function calls: upper([column_name])
        """
        expr = expression.strip()

        # Remove outer parentheses if present
        while expr.startswith("(") and expr.endswith(")"):
            expr = expr[1:-1].strip()

        # Simple column reference (possibly bracketed)
        bracket_match = re.match(r"^\[(\w+)\]$", expr)
        if bracket_match:
            return yspec.TransformedColumnReference(column=bracket_match.group(1))

        # Simple identifier
        if re.match(r"^\w+$", expr):
            return yspec.TransformedColumnReference(column=expr)

        # Function call: func([column], args...)
        func_match = re.match(r"^(\w+)\((.+)\)$", expr)
        if func_match:
            func_name = func_match.group(1).upper()
            args_str = func_match.group(2)

            # Try to extract first argument as column name
            # Handle bracketed column names: [col_name]
            args: list[str] = []
            for arg_part in args_str.split(","):
                arg_part = arg_part.strip()
                bracket_arg = re.match(r"^\[(\w+)\]$", arg_part)
                if bracket_arg:
                    args.append(bracket_arg.group(1))
                else:
                    args.append(arg_part)

            if args:
                return yspec.TransformedColumnReference(
                    column=args[0],
                    transform=func_name,
                    transform_args=args[1:] if len(args) > 1 else [],
                )

        # Binary operation: [col_a] + [col_b], [col_a] * 2, etc.
        # Extract first bracketed identifier as the source column
        bracket_ident = re.search(r"\[(\w+)\]", expr)
        if bracket_ident:
            return yspec.TransformedColumnReference(
                column=bracket_ident.group(1),
                transform="expression",
                transform_args=[expr],
            )

        # Try plain identifier
        ident_match = re.match(r"^(\w+)", expr)
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
