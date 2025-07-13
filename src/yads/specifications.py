"""
This module contains the classes for different specification types.
"""

import yaml
from typing import Any, List, IO

from .base import BaseObject
from .generators.ddl.spark import SparkDDLGenerator
from .generators.formats.pyspark import PySparkSchemaGenerator
from .models import Column


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
        elif data:
            super().__init__(**data)
        else:
            raise ValueError(
                "Either 'source' or a dictionary of data must be provided."
            )
        self.schema = [Column(**col) for col in self._data.get("table_schema", [])]

    def to_ddl(
        self,
        dialect: str = "spark",
        with_database: bool = True,
        with_schema: bool = True,
    ) -> str:
        """
        Generates a Data Definition Language (DDL) string for the table.

        Args:
            dialect: The SQL dialect to target. Currently, only "spark" is
                     supported for Iceberg tables.
            with_database: If True, prepends the database name.
            with_schema: If True, prepends the table schema name.

        Returns:
            A string containing the CREATE TABLE statement.

        Raises:
            NotImplementedError: If the dialect is not supported.
        """
        if dialect.lower() != "spark":
            raise NotImplementedError(f"Dialect '{dialect}' is not yet supported.")
        return SparkDDLGenerator(self).generate(
            with_database=with_database, with_schema=with_schema
        )

    def to_spark_schema(self) -> Any:
        """
        Generates a PySpark StructType for the table.

        Returns:
            A PySpark StructType.
        """
        return PySparkSchemaGenerator(self).generate()

    def to_spark_df_schema(self) -> Any:
        """
        Alias for to_spark_schema.
        """
        return self.to_spark_schema()
