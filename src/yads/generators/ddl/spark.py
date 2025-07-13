"""
This module contains the generator for Spark DDL statements.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..base import SchemaGenerator

if TYPE_CHECKING:
    from ...specifications import TableSpecification


class SparkDDLGenerator(SchemaGenerator):
    """
    Generates a Spark DDL string from a TableSpecification.
    """

    def __init__(self, table_spec: "TableSpecification"):
        super().__init__(table_spec)

    def generate(self) -> str:
        """
        Generates a Data Definition Language (DDL) string for the table.

        Returns:
            A string containing the CREATE TABLE statement.
        """
        table_name = f"{self.table_spec.database}.{self.table_spec.table_name}"

        # Column definitions
        column_defs = []
        for col in self.table_spec.schema:
            col_type = col.type
            if col_type == "array":
                col_type = f"array<{col.element_type}>"
            nullable_str = "NOT NULL" if not col.get("nullable") else ""
            column_defs.append(
                "  " + f"`{col.name}` {col_type.upper()} {nullable_str}".strip()
            )

        columns_str = ",\n".join(column_defs)

        # Start DDL statement
        ddl = f"CREATE OR REPLACE TABLE {table_name} (\n{columns_str}\n)"

        # Add table format
        if (
            self.table_spec.get("properties", {}).get("table_type", "").lower()
            == "iceberg"
        ):
            ddl += "\nUSING ICEBERG"

        # Partitioning
        if self.table_spec.get("partitioning"):
            partition_defs = []
            for part in self.table_spec.partitioning:
                strategy = part.get("strategy")
                column = part.get("column")
                if column:
                    if strategy:
                        partition_defs.append(f"{strategy.lower()}(`{column}`)")
                    else:
                        partition_defs.append(f"`{column}`")
            if partition_defs:
                ddl += f"\nPARTITIONED BY ({', '.join(partition_defs)})"

        # Location
        if self.table_spec.get("location"):
            ddl += f"\nLOCATION '{self.table_spec.location}'"

        # Table Properties
        if self.table_spec.get("properties"):
            prop_defs = [
                f"'{k}' = '{v}'" for k, v in self.table_spec.properties.items()
            ]
            if prop_defs:
                props_str = ",\n  ".join(prop_defs)
                ddl += f"\nTBLPROPERTIES (\n  {props_str}\n)"

        return ddl + ";"
