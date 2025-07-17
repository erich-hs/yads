import yaml
from sqlglot import exp
from typing import Dict, Any, List


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

        return exp.Create(
            this=exp.Schema(this=exp.Identifier(this=table_name), expressions=columns),
            kind="TABLE",
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
    run_conversion("examples/specs/yads_spec.yaml", "duckdb", pretty=True)


if __name__ == "__main__":
    main()
