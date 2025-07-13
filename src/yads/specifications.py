"""
This module contains the classes for different specification types.
"""

import yaml
from typing import Any, List, IO

from .base import BaseObject
from .resources import Column


class Specification(BaseObject):
    """
    Base class for all specifications. Provides a factory method to load from
    a YAML file.
    """

    @classmethod
    def from_yaml(cls, path: str) -> "Specification":
        """
        Loads a specification from a YAML file.

        Args:
            path: The path to the YAML file.

        Returns:
            An instance of the Specification class (or a subclass).
        """
        return cls(source=path)


class TableSpecification(Specification):
    """
    Represents a table specification, including its schema, properties,
    and other metadata.

    Can be initialized from a file path, a file-like object, or a dictionary.

    Attributes:
        schema (List[Column]): A list of Column objects representing the table's schema.
        table_name (str): The name of the table.
        database (str): The database the table belongs to.
        description (str | None): A description of the table.
        # ... and other dynamic attributes from the YAML file.
    """

    schema: List[Column]

    def __init__(self, source: str | IO[str] | None = None, **data: Any):
        """
        Initializes the TableSpecification. It can be initialized from a
        YAML file path, a file-like object, or directly from a dictionary.

        Args:
            source: A file path (str) or a file-like object to a YAML file.
            **data: A dictionary containing the specification data.
        """
        if source:
            if isinstance(source, str):
                with open(source, "r") as f:
                    loaded_data = yaml.safe_load(f)
            else:
                loaded_data = yaml.safe_load(source)
            super().__init__(**loaded_data)
        else:
            super().__init__(**data)
        self.schema = [Column(**col) for col in self._data.get("schema", [])]

    def to_ddl(self, dialect: str = "spark") -> str:
        """
        Generates a Data Definition Language (DDL) string for the table.

        Args:
            dialect: The SQL dialect to target. Currently, only "spark" is
                     supported for Iceberg tables.

        Returns:
            A string containing the CREATE TABLE statement.

        Raises:
            NotImplementedError: If the dialect is not supported.
        """
        if dialect.lower() != "spark":
            raise NotImplementedError(f"Dialect '{dialect}' is not yet supported.")

        table_name = f"{self.database}.{self.table_name}"

        # Column definitions
        column_defs = []
        for col in self.schema:
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
        if self.get("properties", {}).get("table_type", "").lower() == "iceberg":
            ddl += "\nUSING ICEBERG"

        # Partitioning
        if self.get("partitioning"):
            partition_defs = []
            for part in self.partitioning:
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
        if self.get("location"):
            ddl += f"\nLOCATION '{self.location}'"

        # Table Properties
        if self.get("properties"):
            prop_defs = [f"'{k}' = '{v}'" for k, v in self.properties.items()]
            if prop_defs:
                ddl += f"\nTBLPROPERTIES (\n  {',\n  '.join(prop_defs)}\n)"

        return ddl + ";"
