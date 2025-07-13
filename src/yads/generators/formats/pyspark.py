"""
This module contains the generator for PySpark DataFrame schemas.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ..base import SchemaGenerator
from ...models import NotNullConstraint

if TYPE_CHECKING:
    from ...models import Column
    from ...specifications import TableSpecification

    DataType = Any
    StructField = Any
    StructType = Any


class PySparkSchemaGenerator(SchemaGenerator):
    """
    Generates a PySpark DataFrame schema from a TableSpecification.
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

    def _get_pyspark_field(self, col: "Column") -> "StructField":
        """
        Creates a StructField from a column specification.

        Args:
            col: A dictionary representing a column.

        Returns:
            A PySpark StructField.
        """
        col_name = col.get("name", "")
        col_type = self._get_pyspark_type(col)
        is_not_null = any(isinstance(c, NotNullConstraint) for c in col.constraints)
        nullable = not is_not_null
        return self.StructField(col_name, col_type, nullable)

    def generate(self) -> "StructType":
        """
        Generates a PySpark StructType for the table.

        Returns:
            A PySpark StructType.
        """
        fields = [self._get_pyspark_field(col) for col in self.table_spec.schema]
        return self.StructType(fields)
