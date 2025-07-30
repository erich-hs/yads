import pytest
from sqlglot import parse_one
from yads.converters.sql import SqlglotConverter
from yads.loader import from_yaml


@pytest.mark.parametrize(
    "spec_path, expected_sql_path",
    [
        (
            "tests/fixtures/spec/valid/basic_spec.yaml",
            "tests/fixtures/sql/basic_spec.sql",
        ),
        (
            "tests/fixtures/spec/valid/constraints_spec.yaml",
            "tests/fixtures/sql/constraints_spec.sql",
        ),
        (
            "tests/fixtures/spec/valid/full_spec.yaml",
            "tests/fixtures/sql/full_spec.sql",
        ),
        (
            "tests/fixtures/spec/valid/interval_types_spec.yaml",
            "tests/fixtures/sql/interval_types_spec.sql",
        ),
        (
            "tests/fixtures/spec/valid/map_type_spec.yaml",
            "tests/fixtures/sql/map_type_spec.sql",
        ),
        (
            "tests/fixtures/spec/valid/nested_types_spec.yaml",
            "tests/fixtures/sql/nested_types_spec.sql",
        ),
        (
            "tests/fixtures/spec/valid/table_constraints_spec.yaml",
            "tests/fixtures/sql/table_constraints_spec.sql",
        ),
    ],
)
def test_converter(spec_path, expected_sql_path):
    """
    Tests that the converter generates the expected SQL AST.

    This test operates by:
    1. Loading a YAML specification from a file.
    2. Converting the specification to a sqlglot AST using the SqlglotConverter.
    3. Reading the expected SQL DDL from a corresponding .sql file.
    4. Parsing the expected SQL into a sqlglot AST.
    5. Comparing the generated AST with the expected AST.

    The comparison is done on the AST level, not on the raw SQL string, to ensure
    that the semantic structure is correct, regardless of formatting differences.
    """
    spec = from_yaml(spec_path)
    converter = SqlglotConverter()
    generated_ast = converter.convert(spec)

    with open(expected_sql_path) as f:
        expected_sql = f.read()
    expected_ast = parse_one(expected_sql)

    assert generated_ast.sql(dialect="spark") == expected_ast.sql(dialect="spark"), (
        f"SQL DDL from YAML fixture and SQL DDL from SQL fixture are not equal.\n\n"
        f"YAML DDL:\n{generated_ast.sql(dialect='spark', pretty=True)}\n\n"
        f"SQL DDL:\n{expected_ast.sql(dialect='spark', pretty=True)}"
    )
