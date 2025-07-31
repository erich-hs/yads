import pytest
from sqlglot import parse_one, exp
from yads.converters.sql import SQLGlotConverter
from yads.loader import from_yaml
from yads.types import (
    String,
    Integer,
    Float,
    Boolean,
    Decimal,
    Date,
    Timestamp,
    Binary,
    UUID,
    Interval,
    IntervalTimeUnit,
    Array,
    Struct,
    Map,
)
from yads.spec import Field


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
    2. Converting the specification to a sqlglot AST using the SQLGlotConverter.
    3. Reading the expected SQL DDL from a corresponding .sql file.
    4. Parsing the expected SQL into a sqlglot AST.
    5. Comparing the generated AST with the expected AST.

    The comparison is done on the AST level, not on the raw SQL string, to ensure
    that the semantic structure is correct, regardless of formatting differences.
    """
    spec = from_yaml(spec_path)
    converter = SQLGlotConverter()
    generated_ast = converter.convert(spec)

    with open(expected_sql_path) as f:
        expected_sql = f.read()
    expected_ast = parse_one(expected_sql)

    assert generated_ast == expected_ast, (
        "Generated AST does not match expected AST.\n\n"
        f"YAML AST: {repr(generated_ast)}\n\n"
        f"SQL AST:  {repr(expected_ast)}"
    )


class TestTypeConversion:
    """Tests that SQLGlotConverter correctly converts Type objects to sqlglot DataType expressions."""

    def setUp(self):
        self.converter = SQLGlotConverter()

    @pytest.mark.parametrize(
        "yads_type, expected_datatype",
        [
            # String types
            (String(), exp.DataType(this=exp.DataType.Type.TEXT)),
            (
                String(length=255),
                exp.DataType(
                    this=exp.DataType.Type.TEXT,
                    expressions=[exp.DataTypeParam(this=exp.Literal.number("255"))],
                ),
            ),
            # Integer types - handled by type handler
            (Integer(), exp.DataType(this=exp.DataType.Type.INT)),
            (Integer(bits=8), exp.DataType(this=exp.DataType.Type.TINYINT)),
            (Integer(bits=16), exp.DataType(this=exp.DataType.Type.SMALLINT)),
            (Integer(bits=32), exp.DataType(this=exp.DataType.Type.INT)),
            (Integer(bits=64), exp.DataType(this=exp.DataType.Type.BIGINT)),
            # Float types - handled by type handler
            (Float(), exp.DataType(this=exp.DataType.Type.FLOAT)),
            (Float(bits=32), exp.DataType(this=exp.DataType.Type.FLOAT)),
            (Float(bits=64), exp.DataType(this=exp.DataType.Type.DOUBLE)),
            # Decimal types - handled by type handler
            (Decimal(), exp.DataType(this=exp.DataType.Type.DECIMAL)),
            (
                Decimal(precision=10, scale=2),
                exp.DataType(
                    this=exp.DataType.Type.DECIMAL,
                    expressions=[
                        exp.DataTypeParam(this=exp.Literal.number("10")),
                        exp.DataTypeParam(this=exp.Literal.number("2")),
                    ],
                ),
            ),
            # Boolean type - fallback to build
            (Boolean(), exp.DataType.build("boolean")),
            # Temporal types - fallback to build
            (Date(), exp.DataType.build("date")),
            (Timestamp(), exp.DataType.build("timestamp")),
            # Binary types - fallback to build
            (Binary(), exp.DataType.build("binary")),
            # Other types - fallback to build
            (UUID(), exp.DataType.build("uuid")),
        ],
    )
    def test_simple_type_conversion(self, yads_type, expected_datatype):
        """Test conversion of simple (non-complex) types to sqlglot DataType expressions."""
        converter = SQLGlotConverter()
        result = converter._convert_type(yads_type)
        assert result == expected_datatype

    @pytest.mark.parametrize(
        "yads_type, expected_datatype",
        [
            # Interval types - handled by type handler
            (
                Interval(interval_start=IntervalTimeUnit.YEAR),
                exp.DataType(this=exp.Interval(unit=exp.Var(this="YEAR"))),
            ),
            (
                Interval(interval_start=IntervalTimeUnit.MONTH),
                exp.DataType(this=exp.Interval(unit=exp.Var(this="MONTH"))),
            ),
            (
                Interval(interval_start=IntervalTimeUnit.DAY),
                exp.DataType(this=exp.Interval(unit=exp.Var(this="DAY"))),
            ),
            (
                Interval(interval_start=IntervalTimeUnit.HOUR),
                exp.DataType(this=exp.Interval(unit=exp.Var(this="HOUR"))),
            ),
            (
                Interval(interval_start=IntervalTimeUnit.MINUTE),
                exp.DataType(this=exp.Interval(unit=exp.Var(this="MINUTE"))),
            ),
            (
                Interval(interval_start=IntervalTimeUnit.SECOND),
                exp.DataType(this=exp.Interval(unit=exp.Var(this="SECOND"))),
            ),
            # Interval ranges
            (
                Interval(
                    interval_start=IntervalTimeUnit.YEAR,
                    interval_end=IntervalTimeUnit.MONTH,
                ),
                exp.DataType(
                    this=exp.Interval(
                        unit=exp.IntervalSpan(
                            this=exp.Var(this="YEAR"), expression=exp.Var(this="MONTH")
                        )
                    )
                ),
            ),
            (
                Interval(
                    interval_start=IntervalTimeUnit.DAY,
                    interval_end=IntervalTimeUnit.SECOND,
                ),
                exp.DataType(
                    this=exp.Interval(
                        unit=exp.IntervalSpan(
                            this=exp.Var(this="DAY"), expression=exp.Var(this="SECOND")
                        )
                    )
                ),
            ),
        ],
    )
    def test_interval_type_conversion(self, yads_type, expected_datatype):
        """Test conversion of interval types to sqlglot DataType expressions."""
        converter = SQLGlotConverter()
        result = converter._convert_type(yads_type)
        assert result == expected_datatype

    @pytest.mark.parametrize(
        "yads_type",
        [
            # Array types
            Array(element=String()),
            Array(element=Integer(bits=32)),
            Array(element=Boolean()),
            Array(element=Decimal(precision=10, scale=2)),
            # Nested arrays
            Array(element=Array(element=String())),
        ],
    )
    def test_array_type_conversion(self, yads_type):
        """Test conversion of array types to sqlglot DataType expressions."""
        converter = SQLGlotConverter()
        result = converter._convert_type(yads_type)

        assert isinstance(result, exp.DataType)
        assert result.this == exp.DataType.Type.ARRAY
        assert len(result.expressions) == 1

        # Verify the element type is correctly converted
        element_datatype = result.expressions[0]
        expected_element = converter._convert_type(yads_type.element)
        assert element_datatype == expected_element

    @pytest.mark.parametrize(
        "yads_type",
        [
            # Map types
            Map(key=String(), value=Integer(bits=32)),
            Map(key=UUID(), value=Float(bits=64)),
            Map(key=Integer(bits=32), value=Array(element=String())),
        ],
    )
    def test_map_type_conversion(self, yads_type):
        """Test conversion of map types to sqlglot DataType expressions."""
        converter = SQLGlotConverter()
        result = converter._convert_type(yads_type)

        assert isinstance(result, exp.DataType)
        assert result.this == exp.DataType.Type.MAP
        assert len(result.expressions) == 2

        # Verify key and value types are correctly converted
        key_datatype, value_datatype = result.expressions
        expected_key = converter._convert_type(yads_type.key)
        expected_value = converter._convert_type(yads_type.value)
        assert key_datatype == expected_key
        assert value_datatype == expected_value

    def test_struct_type_conversion(self):
        """Test conversion of struct types to sqlglot DataType expressions."""
        # Create a struct with multiple fields
        struct_fields = [
            Field(name="field1", type=String()),
            Field(name="field2", type=Integer(bits=32)),
            Field(name="field3", type=Boolean()),
        ]
        yads_type = Struct(fields=struct_fields)

        converter = SQLGlotConverter()
        result = converter._convert_type(yads_type)

        assert isinstance(result, exp.DataType)
        assert result.this == exp.DataType.Type.STRUCT
        assert len(result.expressions) == 3

        # Verify each field is correctly converted
        for i, field_def in enumerate(result.expressions):
            assert isinstance(field_def, exp.ColumnDef)
            assert field_def.this.this == struct_fields[i].name

            expected_field_type = converter._convert_type(struct_fields[i].type)
            assert field_def.kind == expected_field_type

    def test_nested_struct_type_conversion(self):
        """Test conversion of nested struct types to sqlglot DataType expressions."""
        # Create a nested struct
        inner_fields = [Field(name="inner_field", type=Integer(bits=32))]
        inner_struct = Struct(fields=inner_fields)

        outer_fields = [
            Field(name="simple_field", type=String()),
            Field(name="nested_struct", type=inner_struct),
        ]
        yads_type = Struct(fields=outer_fields)

        converter = SQLGlotConverter()
        result = converter._convert_type(yads_type)

        assert isinstance(result, exp.DataType)
        assert result.this == exp.DataType.Type.STRUCT
        assert len(result.expressions) == 2

        # Check simple field
        simple_field_def = result.expressions[0]
        assert simple_field_def.this.this == "simple_field"
        assert simple_field_def.kind == converter._convert_type(String())

        # Check nested struct field
        nested_field_def = result.expressions[1]
        assert nested_field_def.this.this == "nested_struct"
        assert isinstance(nested_field_def.kind, exp.DataType)
        assert nested_field_def.kind.this == exp.DataType.Type.STRUCT
