"""
This module contains the classes for different schema translators.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .resources import Column
    from .specifications import TableSpecification

    DataType = Any
    StructField = Any
    StructType = Any


class SchemaTranslator(ABC):
    """
    Base class for all schema translators.
    """

    def __init__(self, table_spec: "TableSpecification"):
        """
        Initializes the schema translator.

        Args:
            table_spec: The table specification to translate.
        """
        self.table_spec = table_spec

    @abstractmethod
    def translate(self) -> Any:
        """
        Translates the table specification into a specific format.
        """
        raise NotImplementedError


class SparkDDLTranslator(SchemaTranslator):
    """
    Translates a TableSpecification into a Spark DDL string.
    """

    def translate(self) -> str:
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


class PySparkSchemaTranslator(SchemaTranslator):
    """
    Translates a TableSpecification into a PySpark DataFrame schema.
    """

    def __init__(self, table_spec: "TableSpecification"):
        super().__init__(table_spec)
        self.logger = logging.getLogger(__name__)
        try:
            from pyspark.sql.types import (
                ArrayType,
                BooleanType,
                DateType,
                DecimalType,
                DoubleType,
                IntegerType,
                LongType,
                MapType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            self.pyspark_types = {
                "string": StringType(),
                "integer": IntegerType(),
                "long": LongType(),
                "double": DoubleType(),
                "boolean": BooleanType(),
                "date": DateType(),
                "timestamp": TimestampType(),
                "decimal": DecimalType(10, 2),  # Default precision and scale
            }
            self.StructType = StructType
            self.StructField = StructField
            self.ArrayType = ArrayType
            self.MapType = MapType

        except ImportError:
            self.logger.error(
                "pyspark is not installed. Please install it with `pip install 'yads[pyspark]'`"
            )
            raise

    def _get_pyspark_type(self, col: Column | dict[str, Any]) -> "DataType":
        """
        Maps a column type from the specification to a PySpark DataType.

        Args:
            col: A dictionary representing a column.

        Returns:
            A PySpark DataType.
        """
        col_type = col.get("type")
        if col_type in self.pyspark_types:
            return self.pyspark_types[col_type]
        elif col_type == "array":
            element_type = self._get_pyspark_type({"type": col.get("element_type")})
            return self.ArrayType(element_type, True)
        elif col_type == "map":
            key_type = self._get_pyspark_type({"type": col.get("key_type")})
            value_type = self._get_pyspark_type({"type": col.get("value_type")})
            return self.MapType(key_type, value_type, True)
        elif col_type == "struct":
            fields = [
                self._get_pyspark_field(sub_col) for sub_col in col.get("fields", [])
            ]
            return self.StructType(fields)
        else:
            self.logger.warning(
                f"Unsupported type '{col_type}'. Defaulting to StringType."
            )
            return self.pyspark_types["string"]

    def _get_pyspark_field(self, col: Column) -> "StructField":
        """
        Creates a StructField from a column specification.

        Args:
            col: A dictionary representing a column.

        Returns:
            A PySpark StructField.
        """
        col_name = col.get("name", "")
        col_type = self._get_pyspark_type(col)
        nullable = col.get("nullable", True)
        return self.StructField(col_name, col_type, nullable)

    def translate(self) -> "StructType":
        """
        Generates a PySpark StructType for the table.

        Returns:
            A PySpark StructType.
        """
        fields = [self._get_pyspark_field(col) for col in self.table_spec.schema]
        return self.StructType(fields)
