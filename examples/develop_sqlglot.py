import yaml
from sqlglot import exp, parse_one
from typing import Dict, Any, List

DIALECT = "redshift"


class YadsSqlglotConverter:
    """
    Converts a YADS specification dictionary into a sqlglot CreateTable expression.
    """

    _YADS_TO_SQLGLOT_TYPE_MAP: Dict[str, exp.DataType.Type] = {
        "uuid": exp.DataType.Type.UUID,
        "integer": exp.DataType.Type.INT,
        "date": exp.DataType.Type.DATE,
        "decimal": exp.DataType.Type.DECIMAL,
        "string": exp.DataType.Type.VARCHAR,
        "timestamp_tz": exp.DataType.Type.TIMESTAMPTZ,
        "array": exp.DataType.Type.ARRAY,
        "struct": exp.DataType.Type.STRUCT,
        "map": exp.DataType.Type.MAP,
    }

    _YADS_TO_SQLGLOT_TRANSFORM_MAP: Dict[str, type[exp.Func]] = {
        "month": exp.Month,
        "year": exp.Year,
        "day": exp.Day,
    }

    def __init__(self, spec: Dict[str, Any]):
        """
        Initializes the converter with a YADS specification.
        Args:
            spec: A dictionary representing the parsed YADS YAML file.
        """
        if not isinstance(spec, dict):
            raise TypeError("spec must be a dictionary")
        self.spec = spec

    def convert_to_create_table_expression(self) -> exp.Create:
        """
        Performs the conversion from the YADS spec to a sqlglot Create expression.
        Returns:
            A sqlglot Create expression representing the table.
        """
        table_name = self.spec["name"]
        columns = self._build_column_definitions()
        options = self.spec.get("options", {})
        properties = self._build_properties()

        table = parse_one(table_name, into=exp.Table)

        return exp.Create(
            this=exp.Schema(this=table, expressions=columns),
            kind="TABLE",
            exists=options.get("if_not_exists", False),
            replace=options.get("or_replace", False),
            properties=exp.Properties(expressions=properties),
        )

    def _build_column_definitions(self) -> List[exp.ColumnDef]:
        """Builds a list of sqlglot ColumnDef expressions from the spec."""
        columns = []
        for col_spec in self.spec.get("columns", []):
            column_name = col_spec["name"]
            sqlglot_type = self._yads_to_sqlglot_type(col_spec)

            constraints = []
            if col_spec.get("constraints", {}).get("not_null"):
                constraints.append(exp.NotNullColumnConstraint())

            columns.append(
                exp.ColumnDef(
                    this=exp.Identifier(this=column_name),
                    kind=sqlglot_type,
                    constraints=constraints,
                )
            )
        return columns

    def _build_properties(self) -> List[exp.Property]:
        """Builds a list of sqlglot Property expressions from the spec."""
        properties: List[exp.Property] = []
        spec_properties = self.spec.get("properties", {})

        if "using" in spec_properties:
            properties.append(
                exp.FileFormatProperty(
                    this=exp.Identifier(this=spec_properties["using"])
                )
            )

        if "location" in spec_properties:
            properties.append(
                exp.LocationProperty(
                    this=exp.Literal.string(spec_properties["location"])
                )
            )

        if "partitioned_by" in spec_properties:
            partition_expressions = [
                self._build_partition_expression(p)
                for p in spec_properties["partitioned_by"]
            ]
            properties.append(
                exp.PartitionedByProperty(
                    this=exp.Tuple(expressions=partition_expressions)
                )
            )

        return properties

    def _build_partition_expression(self, partition_def: Any) -> exp.Expression:
        """
        Builds a sqlglot Expression for a partition definition.
        Args:
            partition_def: A dictionary representing the partition.
        Returns:
            A sqlglot Expression.
        """
        if not isinstance(partition_def, dict):
            raise TypeError(
                f"Unsupported partition definition type: {type(partition_def)}"
            )

        column = partition_def.get("column")
        if not column:
            raise ValueError("Partition definition must have a 'column' key.")

        transform = partition_def.get("transform")
        if not transform:
            return exp.Identifier(this=column)

        transform_class = self._YADS_TO_SQLGLOT_TRANSFORM_MAP.get(transform.lower())
        if not transform_class:
            raise ValueError(f"Unsupported partition transform: {transform}")

        return transform_class(this=exp.Identifier(this=column))

    def _yads_to_sqlglot_type(self, yads_type: Dict[str, Any]) -> exp.DataType:
        """Converts a YADS type definition to a sqlglot DataType expression."""
        type_name = yads_type["type"]
        sqlglot_type = self._YADS_TO_SQLGLOT_TYPE_MAP.get(type_name)

        if not sqlglot_type:
            raise ValueError(f"Unsupported YADS type: {type_name}")

        params = yads_type.get("params", {})
        expressions: List[exp.Expression] = []

        if type_name == "decimal":
            precision = params.get("precision")
            scale = params.get("scale")
            if precision is not None:
                expressions.append(exp.Literal.number(precision))
            if scale is not None:
                expressions.append(exp.Literal.number(scale))

        elif type_name == "string":
            length = params.get("length")
            if length is not None:
                expressions.append(exp.Literal.number(length))

        elif type_name == "array":
            element_type = self._yads_to_sqlglot_type(yads_type["element"])
            return exp.DataType(this=sqlglot_type, expressions=[element_type])

        elif type_name == "struct":
            fields = [
                exp.ColumnDef(
                    this=exp.Identifier(this=field["name"]),
                    kind=self._yads_to_sqlglot_type(field),
                )
                for field in yads_type.get("fields", [])
            ]
            return exp.DataType(this=sqlglot_type, expressions=fields)

        elif type_name == "map":
            key_type = self._yads_to_sqlglot_type(yads_type["key"])
            value_type = self._yads_to_sqlglot_type(yads_type["value"])
            return exp.DataType(this=sqlglot_type, expressions=[key_type, value_type])

        return exp.DataType(this=sqlglot_type, expressions=expressions)


def run_conversion(filepath: str, dialect: str, pretty: bool = False) -> None:
    """
    Loads a spec file, generates the DDL, and prints it to the console.
    Args:
        filepath: Path to the YADS YAML spec file.
        dialect: The target SQL dialect.
        pretty: Whether to format the output SQL.
    """
    with open(filepath, "r") as f:
        spec_data = yaml.safe_load(f)

    converter = YadsSqlglotConverter(spec_data)
    create_expression = converter.convert_to_create_table_expression()
    ddl = create_expression.sql(dialect=dialect, pretty=pretty)
    print(ddl)


def main() -> None:
    """
    Main entry point for the script.
    """
    print(f"Dialect: {DIALECT}")
    run_conversion("examples/specs/yads_spec.yaml", DIALECT, pretty=True)


if __name__ == "__main__":
    main()
