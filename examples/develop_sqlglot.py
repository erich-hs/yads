import yaml
from sqlglot import exp

# A mapping from YADS types to sqlglot expression types.
# This is a simple example and can be expanded to cover more complex types.
YADS_TO_SQLGLOT_TYPE_MAP = {
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


def yads_to_sqlglot_type(yads_type: dict) -> exp.DataType:
    """Converts a YADS type definition to a sqlglot DataType expression."""
    type_name = yads_type["type"]
    sqlglot_type = YADS_TO_SQLGLOT_TYPE_MAP.get(type_name)

    if not sqlglot_type:
        raise ValueError(f"Unsupported YADS type: {type_name}")

    params = yads_type.get("params", {})
    expressions = []

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
        element_type = yads_to_sqlglot_type(yads_type["element"])
        return exp.DataType(this=sqlglot_type, expressions=[element_type])

    elif type_name == "struct":
        fields = [
            exp.ColumnDef(
                this=exp.Identifier(this=field["name"]),
                kind=yads_to_sqlglot_type(field),
            )
            for field in yads_type["fields"]
        ]
        return exp.DataType(this=sqlglot_type, expressions=fields)

    elif type_name == "map":
        key_type = yads_to_sqlglot_type(yads_type["key"])
        value_type = yads_to_sqlglot_type(yads_type["value"])
        return exp.DataType(this=sqlglot_type, expressions=[key_type, value_type])

    return exp.DataType(this=sqlglot_type, expressions=expressions)


def main():
    """
    Generates a CREATE TABLE DDL statement from a YADS spec file.
    """
    with open("examples/specs/yads_spec.yaml", "r") as f:
        spec = yaml.safe_load(f)

    table_name = spec["name"]
    columns = []
    for col_spec in spec["columns"]:
        column_name = col_spec["name"]
        sqlglot_type = yads_to_sqlglot_type(col_spec)
        constraints = []
        if col_spec.get("constraints"):
            if col_spec["constraints"].get("not_null"):
                constraints.append(exp.NotNullColumnConstraint())

        columns.append(
            exp.ColumnDef(
                this=exp.Identifier(this=column_name),
                kind=sqlglot_type,
                constraints=constraints,
            )
        )

    create_table_expression = exp.Create(
        this=exp.Schema(this=exp.Identifier(this=table_name), expressions=columns),
        kind="TABLE",
    )

    # Generate the DDL for the "spark" dialect
    ddl = create_table_expression.sql(dialect="spark")
    print(ddl)


if __name__ == "__main__":
    main()
