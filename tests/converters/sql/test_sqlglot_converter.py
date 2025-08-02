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
from yads.constraints import (
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    ForeignKeyReference,
    DefaultConstraint,
    IdentityConstraint,
)
from yads.exceptions import ConversionError, UnsupportedFeatureError


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


class TestConstraintConversion:
    def test_not_null_constraint_conversion(self):
        converter = SQLGlotConverter()
        constraint = NotNullConstraint()
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())
        assert result == expected

    def test_primary_key_constraint_conversion(self):
        converter = SQLGlotConverter()
        constraint = PrimaryKeyConstraint()
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())
        assert result == expected

    def test_default_constraint_conversion(self):
        converter = SQLGlotConverter()
        constraint = DefaultConstraint(value="test_value")
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(
            kind=exp.DefaultColumnConstraint(this=exp.Literal.string("test_value"))
        )
        assert result == expected

    def test_identity_constraint_positive_values_conversion(self):
        converter = SQLGlotConverter()
        constraint = IdentityConstraint(always=True, start=1, increment=1)
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(
            kind=exp.GeneratedAsIdentityColumnConstraint(
                this=True,
                start=exp.Literal.number("1"),
                increment=exp.Literal.number("1"),
            )
        )
        assert result == expected

    def test_identity_constraint_negative_increment_conversion(self):
        converter = SQLGlotConverter()
        constraint = IdentityConstraint(always=False, start=10, increment=-1)
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(
            kind=exp.GeneratedAsIdentityColumnConstraint(
                this=False,
                start=exp.Literal.number("10"),
                increment=exp.Neg(this=exp.Literal.number("1")),
            )
        )
        assert result == expected

    def test_identity_constraint_negative_start_conversion(self):
        converter = SQLGlotConverter()
        constraint = IdentityConstraint(always=True, start=-5, increment=2)
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(
            kind=exp.GeneratedAsIdentityColumnConstraint(
                this=True,
                start=exp.Neg(this=exp.Literal.number("5")),
                increment=exp.Literal.number("2"),
            )
        )
        assert result == expected

    def test_foreign_key_constraint_with_name_conversion(self):
        converter = SQLGlotConverter()
        constraint = ForeignKeyConstraint(
            name="fk_test",
            references=ForeignKeyReference(table="other_table", columns=["id"]),
        )
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(
            this=exp.Identifier(this="fk_test"),
            kind=exp.Reference(
                this=exp.Schema(
                    this=exp.Table(
                        this=exp.Identifier(this="other_table"), db=None, catalog=None
                    ),
                    expressions=[exp.Identifier(this="id")],
                )
            ),
        )
        assert result == expected

    def test_foreign_key_constraint_no_name_conversion(self):
        converter = SQLGlotConverter()
        constraint = ForeignKeyConstraint(
            references=ForeignKeyReference(table="other_table", columns=["id"])
        )
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(
            kind=exp.Reference(
                this=exp.Schema(
                    this=exp.Table(
                        this=exp.Identifier(this="other_table"), db=None, catalog=None
                    ),
                    expressions=[exp.Identifier(this="id")],
                )
            )
        )
        assert result == expected

    def test_primary_key_table_constraint_with_name_conversion(self):
        converter = SQLGlotConverter()
        constraint = PrimaryKeyTableConstraint(name="pk_test", columns=["col1", "col2"])
        result = converter._convert_table_constraint(constraint)

        expected = exp.Constraint(
            this=exp.Identifier(this="pk_test"),
            expressions=[
                exp.PrimaryKey(
                    expressions=[
                        exp.Ordered(
                            this=exp.Column(this=exp.Identifier(this="col1")),
                            nulls_first=True,
                        ),
                        exp.Ordered(
                            this=exp.Column(this=exp.Identifier(this="col2")),
                            nulls_first=True,
                        ),
                    ]
                )
            ],
        )
        assert result == expected

    def test_primary_key_table_constraint_no_name_raises_error(self):
        converter = SQLGlotConverter()
        constraint = PrimaryKeyTableConstraint(columns=["col1"])

        with pytest.raises(
            ConversionError, match="Primary key constraint must have a name"
        ):
            converter._convert_table_constraint(constraint)

    def test_foreign_key_table_constraint_with_name_conversion(self):
        converter = SQLGlotConverter()
        constraint = ForeignKeyTableConstraint(
            name="fk_test",
            columns=["col1"],
            references=ForeignKeyReference(table="other_table", columns=["id"]),
        )
        result = converter._convert_table_constraint(constraint)

        expected = exp.Constraint(
            this=exp.Identifier(this="fk_test"),
            expressions=[
                exp.ForeignKey(
                    expressions=[exp.Identifier(this="col1")],
                    reference=exp.Reference(
                        this=exp.Schema(
                            this=exp.Table(
                                this=exp.Identifier(this="other_table"),
                                db=None,
                                catalog=None,
                            ),
                            expressions=[exp.Identifier(this="id")],
                        )
                    ),
                )
            ],
        )
        assert result == expected

    def test_foreign_key_table_constraint_no_name_raises_error(self):
        converter = SQLGlotConverter()
        constraint = ForeignKeyTableConstraint(
            columns=["col1"],
            references=ForeignKeyReference(table="other_table", columns=["id"]),
        )

        with pytest.raises(
            ConversionError, match="Foreign key constraint must have a name"
        ):
            converter._convert_table_constraint(constraint)

    def test_unsupported_column_constraint_raises_error(self):
        converter = SQLGlotConverter()

        class UnsupportedConstraint:
            pass

        constraint = UnsupportedConstraint()

        with pytest.raises(
            UnsupportedFeatureError,
            match="SQLGlotConverter does not support constraint",
        ):
            converter._convert_column_constraint(constraint)

    def test_unsupported_table_constraint_raises_error(self):
        converter = SQLGlotConverter()

        class UnsupportedTableConstraint:
            pass

        constraint = UnsupportedTableConstraint()

        with pytest.raises(
            UnsupportedFeatureError,
            match="SQLGlotConverter does not support table constraint",
        ):
            converter._convert_table_constraint(constraint)


class TestTransformConversion:
    def test_bucket_transform_conversion(self):
        converter = SQLGlotConverter()
        result = converter._handle_bucket_transform("col1", [10])

        expected = exp.PartitionedByBucket(
            this=exp.column("col1"),
            expression=exp.Literal.number("10"),
        )
        assert result == expected

    def test_bucket_transform_wrong_args_raises_error(self):
        converter = SQLGlotConverter()

        with pytest.raises(
            ConversionError,
            match="The 'bucket' transform requires exactly one argument",
        ):
            converter._handle_bucket_transform("col1", [10, 20])

    def test_truncate_transform_conversion(self):
        converter = SQLGlotConverter()
        result = converter._handle_truncate_transform("col1", [5])

        expected = exp.PartitionByTruncate(
            this=exp.column("col1"),
            expression=exp.Literal.number("5"),
        )
        assert result == expected

    def test_truncate_transform_wrong_args_raises_error(self):
        converter = SQLGlotConverter()

        with pytest.raises(
            ConversionError,
            match="The 'truncate' transform requires exactly one argument",
        ):
            converter._handle_truncate_transform("col1", [])

    def test_cast_transform_conversion(self):
        converter = SQLGlotConverter()
        result = converter._handle_cast_transform("col1", ["TEXT"])

        expected = exp.Cast(
            this=exp.column("col1"),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        )
        assert result == expected

    def test_cast_transform_wrong_args_raises_error(self):
        converter = SQLGlotConverter()

        with pytest.raises(
            ConversionError, match="The 'cast' transform requires exactly one argument"
        ):
            converter._handle_cast_transform("col1", ["TEXT", "INT"])

    def test_unknown_transform_fallback(self):
        converter = SQLGlotConverter()
        result = converter._handle_transformation(
            "col1", "custom_func", ["arg1", "arg2"]
        )

        expected = exp.func(
            "custom_func",
            exp.column("col1"),
            exp.Literal.string("arg1"),
            exp.Literal.string("arg2"),
        )
        assert result == expected

    def test_known_transform_handling(self):
        converter = SQLGlotConverter()
        result = converter._handle_transformation("col1", "bucket", [10])

        expected = exp.PartitionedByBucket(
            this=exp.column("col1"),
            expression=exp.Literal.number("10"),
        )
        assert result == expected


class TestGeneratedColumnConversion:
    def test_generated_column_conversion(self):
        converter = SQLGlotConverter()
        from yads.spec import TransformedColumn

        field = Field(
            name="generated_col",
            type=String(),
            generated_as=TransformedColumn(
                column="source_col", transform="upper", transform_args=[]
            ),
        )
        result = converter._convert_field(field)

        assert result.this.this == "generated_col"
        assert isinstance(result.kind, exp.DataType)
        assert result.constraints is not None
        assert len(result.constraints) == 1
        constraint = result.constraints[0]
        assert isinstance(constraint, exp.ColumnConstraint)
        assert isinstance(constraint.kind, exp.GeneratedAsIdentityColumnConstraint)
        assert constraint.kind.this is True

    def test_generated_column_with_transform_args(self):
        converter = SQLGlotConverter()
        from yads.spec import TransformedColumn

        field = Field(
            name="generated_col",
            type=String(),
            generated_as=TransformedColumn(
                column="source_col", transform="substring", transform_args=[1, 10]
            ),
        )
        result = converter._convert_field(field)

        assert result.this.this == "generated_col"
        assert result.constraints is not None
        assert len(result.constraints) == 1

        constraint = result.constraints[0]
        assert isinstance(constraint.kind, exp.GeneratedAsIdentityColumnConstraint)
        assert constraint.kind.this is True
        # The expression should be a function call with the arguments
        assert constraint.kind.expression is not None

    def test_field_without_generated_clause(self):
        converter = SQLGlotConverter()

        field = Field(
            name="regular_col", type=String(), constraints=[NotNullConstraint()]
        )
        result = converter._convert_field(field)

        assert result.this.this == "regular_col"
        assert result.constraints is not None
        assert len(result.constraints) == 1

        # Should only have the NotNull constraint, no generated constraint
        constraint = result.constraints[0]
        assert isinstance(constraint.kind, exp.NotNullColumnConstraint)

    def test_field_with_both_constraints_and_generated(self):
        converter = SQLGlotConverter()
        from yads.spec import TransformedColumn

        field = Field(
            name="complex_col",
            type=String(),
            constraints=[NotNullConstraint()],
            generated_as=TransformedColumn(
                column="source_col", transform="upper", transform_args=[]
            ),
        )
        result = converter._convert_field(field)

        # Check that the field has both constraints
        assert result.this.this == "complex_col"
        assert result.constraints is not None
        assert len(result.constraints) == 2

        # Should have both generated and not null constraints
        constraint_types = [type(c.kind) for c in result.constraints]
        assert exp.GeneratedAsIdentityColumnConstraint in constraint_types
        assert exp.NotNullColumnConstraint in constraint_types


class TestTypeConversion:
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


class TestTableNameParsing:
    def test_parse_full_table_name_with_catalog_and_database(self):
        converter = SQLGlotConverter()
        result = converter._parse_full_table_name("prod.sales.orders")

        expected = exp.Table(
            this=exp.Identifier(this="orders"),
            db=exp.Identifier(this="sales"),
            catalog=exp.Identifier(this="prod"),
        )
        assert result == expected

    def test_parse_full_table_name_with_database_only(self):
        converter = SQLGlotConverter()
        result = converter._parse_full_table_name("sales.orders")

        expected = exp.Table(
            this=exp.Identifier(this="orders"),
            db=exp.Identifier(this="sales"),
            catalog=None,
        )
        assert result == expected

    def test_parse_full_table_name_with_table_only(self):
        converter = SQLGlotConverter()
        result = converter._parse_full_table_name("orders")

        expected = exp.Table(
            this=exp.Identifier(this="orders"),
            db=None,
            catalog=None,
        )
        assert result == expected

    def test_parse_full_table_name_ignore_catalog(self):
        converter = SQLGlotConverter()
        result = converter._parse_full_table_name(
            "prod.sales.orders", ignore_catalog=True
        )

        expected = exp.Table(
            this=exp.Identifier(this="orders"),
            db=exp.Identifier(this="sales"),
            catalog=None,
        )
        assert result == expected

    def test_parse_full_table_name_ignore_database(self):
        converter = SQLGlotConverter()
        result = converter._parse_full_table_name(
            "prod.sales.orders", ignore_database=True
        )

        expected = exp.Table(
            this=exp.Identifier(this="orders"),
            db=None,
            catalog=exp.Identifier(this="prod"),
        )
        assert result == expected

    def test_parse_full_table_name_ignore_both(self):
        converter = SQLGlotConverter()
        result = converter._parse_full_table_name(
            "prod.sales.orders", ignore_catalog=True, ignore_database=True
        )

        expected = exp.Table(
            this=exp.Identifier(this="orders"),
            db=None,
            catalog=None,
        )
        assert result == expected

    def test_parse_full_table_name_ignore_catalog_partial_qualified(self):
        converter = SQLGlotConverter()
        result = converter._parse_full_table_name("sales.orders", ignore_catalog=True)

        expected = exp.Table(
            this=exp.Identifier(this="orders"),
            db=exp.Identifier(this="sales"),
            catalog=None,
        )
        assert result == expected

    def test_parse_full_table_name_ignore_database_partial_qualified(self):
        converter = SQLGlotConverter()
        result = converter._parse_full_table_name("prod.orders", ignore_database=True)

        expected = exp.Table(
            this=exp.Identifier(this="orders"),
            db=None,
            catalog=None,
        )
        assert result == expected


class TestConvertWithIgnoreArguments:
    def test_convert_with_ignore_catalog(self):
        from yads.loader import from_yaml

        spec = from_yaml("tests/fixtures/spec/valid/basic_spec.yaml")
        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_catalog=True)

        table_expression = result.this.this
        assert table_expression.this.this == "test_schema"
        assert table_expression.db == "db"
        assert table_expression.catalog == ""

    def test_convert_with_ignore_database(self):
        from yads.loader import from_yaml

        spec = from_yaml("tests/fixtures/spec/valid/basic_spec.yaml")
        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_database=True)

        table_expression = result.this.this
        assert table_expression.this.this == "test_schema"
        assert table_expression.db == ""
        assert table_expression.catalog == "catalog"

    def test_convert_with_ignore_both(self):
        from yads.loader import from_yaml

        spec = from_yaml("tests/fixtures/spec/valid/basic_spec.yaml")
        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_catalog=True, ignore_database=True)

        table_expression = result.this.this
        assert table_expression.this.this == "test_schema"
        assert table_expression.db == ""
        assert table_expression.catalog == ""

    def test_convert_with_ignore_arguments_and_other_kwargs(self):
        from yads.loader import from_yaml

        spec = from_yaml("tests/fixtures/spec/valid/basic_spec.yaml")
        converter = SQLGlotConverter()
        result = converter.convert(
            spec, ignore_catalog=True, ignore_database=True, if_not_exists=True
        )

        table_expression = result.this.this
        assert table_expression.this.this == "test_schema"
        assert table_expression.db == ""
        assert table_expression.catalog == ""

        assert result.args["exists"] is True

    def test_convert_with_partial_qualified_name_ignore_catalog(self):
        from yads.spec import SchemaSpec, Field
        from yads.types import String

        spec = SchemaSpec(
            name="sales.orders",
            version="1.0.0",
            columns=[Field(name="id", type=String())],
        )

        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_catalog=True)

        table_expression = result.this.this
        assert table_expression.this.this == "orders"
        assert table_expression.db == "sales"
        assert table_expression.catalog == ""

    def test_convert_with_partial_qualified_name_ignore_database(self):
        from yads.spec import SchemaSpec, Field
        from yads.types import String

        spec = SchemaSpec(
            name="prod.orders",
            version="1.0.0",
            columns=[Field(name="id", type=String())],
        )

        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_database=True)

        table_expression = result.this.this
        assert table_expression.this.this == "orders"
        assert table_expression.db == ""
        assert table_expression.catalog == ""
