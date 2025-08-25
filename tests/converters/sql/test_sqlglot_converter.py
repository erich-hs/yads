import pytest
from sqlglot import parse_one, exp
from yads.converters.sql import SQLGlotConverter
from yads.loaders import from_yaml_path
from yads.types import (
    String,
    Integer,
    Float,
    Decimal,
    Boolean,
    Binary,
    Date,
    Time,
    TimeUnit,
    Timestamp,
    TimestampTZ,
    TimestampLTZ,
    TimestampNTZ,
    Duration,
    IntervalTimeUnit,
    Interval,
    Array,
    Struct,
    Map,
    JSON,
    Geometry,
    Geography,
    UUID,
    Void,
    Variant,
)
from yads.spec import Column, Field
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
from yads.loaders import from_yaml_string


# ======================================================================
# SQLGlotConverter tests
# Scope: conversion to sqlglot AST, types, constraints, transforms, names
# ======================================================================


# %% Integration tests
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
def test_convert_matches_expected_ast_from_fixtures(spec_path, expected_sql_path):
    spec = from_yaml_path(spec_path)
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


# %% Constraint conversion
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
        converter = SQLGlotConverter(mode="raise")

        class UnsupportedConstraint:
            pass

        constraint = UnsupportedConstraint()

        with pytest.raises(
            UnsupportedFeatureError,
            match="SQLGlotConverter does not support constraint",
        ):
            converter._convert_column_constraint(constraint)

    def test_unsupported_table_constraint_raises_error(self):
        converter = SQLGlotConverter(mode="raise")

        class UnsupportedTableConstraint:
            pass

        constraint = UnsupportedTableConstraint()

        with pytest.raises(
            UnsupportedFeatureError,
            match="SQLGlotConverter does not support table constraint",
        ):
            converter._convert_table_constraint(constraint)


# %% Transform handling
class TestTransformConversion:
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
            ConversionError, match="The 'cast' transform requires exactly 1 argument"
        ):
            converter._handle_cast_transform("col1", ["TEXT", "INT"])

    def test_cast_transform_unknown_type_raises_error(self):
        converter = SQLGlotConverter(mode="raise")
        with pytest.raises(
            UnsupportedFeatureError,
            match="Transform type 'NOT_A_TYPE' is not a valid sqlglot Type",
        ):
            converter._handle_cast_transform("col1", ["not_a_type"])

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
            match="The 'bucket' transform requires exactly 1 argument",
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
            match="The 'truncate' transform requires exactly 1 argument",
        ):
            converter._handle_truncate_transform("col1", [])

    def test_date_trunc_transform_conversion(self):
        converter = SQLGlotConverter()
        result = converter._handle_date_trunc_transform("col1", ["month"])

        expected = exp.DateTrunc(
            unit=exp.Literal.string("month"),
            this=exp.column("col1"),
        )
        assert result == expected

    def test_date_trunc_transform_wrong_args_raises_error(self):
        converter = SQLGlotConverter()

        with pytest.raises(
            ConversionError,
            match="The 'date_trunc' transform requires exactly 1 argument",
        ):
            converter._handle_date_trunc_transform("col1", [])

    def test_unknown_transform_fallback(self):
        converter = SQLGlotConverter()
        result = converter._handle_transformation("col1", "custom_func", ["arg1", "arg2"])

        expected = exp.func(
            "custom_func",
            exp.column("col1"),
            exp.Literal.string("arg1"),
            exp.Literal.string("arg2"),
        )
        assert result == expected

    def test_handle_transformation_known_transform_bucket(self):
        converter = SQLGlotConverter()
        result = converter._handle_transformation("col1", "bucket", [10])

        expected = exp.PartitionedByBucket(
            this=exp.column("col1"),
            expression=exp.Literal.number("10"),
        )
        assert result == expected


# %% Generated column conversion
class TestGeneratedColumnConversion:
    def test_generated_column_conversion(self):
        converter = SQLGlotConverter()
        from yads.spec import TransformedColumnReference

        column = Column(
            name="generated_col",
            type=String(),
            generated_as=TransformedColumnReference(
                column="source_col", transform="upper", transform_args=[]
            ),
        )
        result = converter._convert_column(column)

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
        from yads.spec import TransformedColumnReference

        column = Column(
            name="generated_col",
            type=String(),
            generated_as=TransformedColumnReference(
                column="source_col", transform="substring", transform_args=[1, 10]
            ),
        )
        result = converter._convert_column(column)

        assert result.this.this == "generated_col"
        assert result.constraints is not None
        assert len(result.constraints) == 1

        constraint = result.constraints[0]
        assert isinstance(constraint.kind, exp.GeneratedAsIdentityColumnConstraint)
        assert constraint.kind.this is True
        # The expression should be a function call with the arguments
        assert constraint.kind.expression is not None

    def test_column_without_generated_clause(self):
        converter = SQLGlotConverter()

        column = Column(
            name="regular_col", type=String(), constraints=[NotNullConstraint()]
        )
        result = converter._convert_column(column)

        assert result.this.this == "regular_col"
        assert result.constraints is not None
        assert len(result.constraints) == 1

        # Should only have the NotNull constraint, no generated constraint
        constraint = result.constraints[0]
        assert isinstance(constraint.kind, exp.NotNullColumnConstraint)

    def test_column_with_both_constraints_and_generated(self):
        converter = SQLGlotConverter()
        from yads.spec import TransformedColumnReference

        column = Column(
            name="complex_col",
            type=String(),
            constraints=[NotNullConstraint()],
            generated_as=TransformedColumnReference(
                column="source_col", transform="upper", transform_args=[]
            ),
        )
        result = converter._convert_column(column)

        # Check that the field has both constraints
        assert result.this.this == "complex_col"
        assert result.constraints is not None
        assert len(result.constraints) == 2

        # Should have both generated and not null constraints
        constraint_types = [type(c.kind) for c in result.constraints]
        assert exp.GeneratedAsIdentityColumnConstraint in constraint_types
        assert exp.NotNullColumnConstraint in constraint_types


# %% Mode hierarchy for SQLGlotConverter
class TestSQLGlotConverterModeHierarchy:
    def test_instance_mode_raise_used_by_default(self):
        yaml_string = """
        name: t
        version: 1
        columns:
          - name: c
            type: duration
        """
        spec = from_yaml_string(yaml_string)

        converter = SQLGlotConverter(mode="raise")
        with pytest.raises(
            UnsupportedFeatureError, match="does not support type: duration"
        ):
            converter.convert(spec)

    def test_call_override_to_coerce_does_not_persist(self):
        yaml_string = """
        name: t
        version: 1
        columns:
          - name: c
            type: duration
        """
        spec = from_yaml_string(yaml_string)

        converter = SQLGlotConverter(mode="raise")
        with pytest.warns(
            UserWarning,
            match="SQLGlotConverter does not support type: duration",
        ):
            ast = converter.convert(spec, mode="coerce")
        # Coerce should succeed and produce an AST
        assert ast is not None

        # Instance remains raise
        with pytest.raises(
            UnsupportedFeatureError, match="does not support type: duration"
        ):
            converter.convert(spec)


# fmt: off
# %% Type conversion
class TestTypeConversion:
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
            (Integer(signed=False), exp.DataType(this=exp.DataType.Type.UINT)),
            (Integer(bits=8, signed=False), exp.DataType(this=exp.DataType.Type.UTINYINT)),
            (Integer(bits=16, signed=False), exp.DataType(this=exp.DataType.Type.USMALLINT)),
            (Integer(bits=32, signed=False), exp.DataType(this=exp.DataType.Type.UINT)),
            (Integer(bits=64, signed=False), exp.DataType(this=exp.DataType.Type.UBIGINT)),
            # Float types - handled by type handler
            (Float(), exp.DataType(this=exp.DataType.Type.FLOAT)),
            (Float(bits=16), exp.DataType(this=exp.DataType.Type.FLOAT)),
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
            (
                Decimal(precision=10, scale=2, bits=128),
                exp.DataType(
                    this=exp.DataType.Type.DECIMAL,
                    expressions=[
                        # Bits are currently ignored
                        exp.DataTypeParam(this=exp.Literal.number("10")),
                        exp.DataTypeParam(this=exp.Literal.number("2")),
                    ],
                ),
            ),
            # Boolean type - fallback to build
            (Boolean(), exp.DataType(this=exp.DataType.Type.BOOLEAN)),
            # Binary types - fallback to build
            (Binary(), exp.DataType(this=exp.DataType.Type.BINARY)),
            (
                Binary(length=8),
                exp.DataType(
                    this=exp.DataType.Type.BINARY,
                    expressions=[exp.DataTypeParam(this=exp.Literal.number("8"))],
                ),
            ),
            # Temporal types
            (Date(), exp.DataType(this=exp.DataType.Type.DATE)),
            # Date bits are currently ignored
            (Date(bits=32), exp.DataType(this=exp.DataType.Type.DATE)),
            (Date(bits=64), exp.DataType(this=exp.DataType.Type.DATE)),
            (Time(), exp.DataType(this=exp.DataType.Type.TIME)),
            (Time(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIME)),
            (Time(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIME)),
            (Time(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIME)),
            (Time(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIME)),
            # Time bits are currently ignored
            (Time(bits=32), exp.DataType(this=exp.DataType.Type.TIME)),
            (Time(bits=64), exp.DataType(this=exp.DataType.Type.TIME)),
            (Timestamp(), exp.DataType(this=exp.DataType.Type.TIMESTAMP)),
            # Timestamp unit and tz are currently ignored
            (Timestamp(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIMESTAMP)),
            (Timestamp(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIMESTAMP)),
            (Timestamp(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIMESTAMP)),
            (Timestamp(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIMESTAMP)),
            (TimestampTZ(), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)),
            (TimestampTZ(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)),
            (TimestampTZ(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)),
            (TimestampTZ(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)),
            (TimestampTZ(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)),
            (TimestampTZ(tz="UTC"), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ)),
            (TimestampLTZ(), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ)),
            (TimestampLTZ(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ)),
            (TimestampLTZ(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ)),
            (TimestampLTZ(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ)),
            (TimestampLTZ(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ)),
            (TimestampNTZ(), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ)),
            (TimestampNTZ(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ)),
            (TimestampNTZ(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ)),
            (TimestampNTZ(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ)),
            (TimestampNTZ(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ)),
            # JSON type - fallback to build
            (JSON(), exp.DataType(this=exp.DataType.Type.JSON)),
            # Spatial types - fallback to build
            (Geometry(), exp.DataType(this=exp.DataType.Type.GEOMETRY)),
            (Geometry(srid=4326), exp.DataType(this=exp.DataType.Type.GEOMETRY, expressions=[exp.DataTypeParam(this=exp.Literal.number("4326"))])),
            (Geography(), exp.DataType(this=exp.DataType.Type.GEOGRAPHY)),
            (Geography(srid=4326), exp.DataType(this=exp.DataType.Type.GEOGRAPHY, expressions=[exp.DataTypeParam(this=exp.Literal.number("4326"))])),
            # Void type - handled by type handler
            (Void(), exp.DataType(this=exp.DataType.Type.USERDEFINED, kind="VOID")),
            # Other types - fallback to build
            (UUID(), exp.DataType(this=exp.DataType.Type.UUID)),
            (Variant(), exp.DataType(this=exp.DataType.Type.VARIANT)),
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
            Array(element=String(), size=2),
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
        
        # Array size is currently ignored
        if hasattr(yads_type, 'size') and yads_type.size is not None:
            assert len(result.expressions) == 1

    @pytest.mark.parametrize(
        "yads_type",
        [
            # Map types
            Map(key=String(), value=Integer(bits=32)),
            Map(key=UUID(), value=Float(bits=64)),
            Map(key=Integer(bits=32), value=Array(element=String())),
            Map(key=String(), value=Integer(), keys_sorted=True),
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
        
        # Map keys_sorted is currently ignored
        if hasattr(yads_type, 'keys_sorted') and yads_type.keys_sorted is not None:
            assert len(result.expressions) == 2

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

    @pytest.mark.parametrize("yads_type", [Duration()])
    def test_unsupported_types(self, yads_type):
        converter = SQLGlotConverter(mode="raise")
        with pytest.raises(
            UnsupportedFeatureError, match="SQLGlotConverter does not support type:"
        ):
            converter._convert_type(yads_type)
# fmt: on


# %% Table name parsing
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


# %% Storage properties
class TestStoragePropertiesHandling:
    def test_storage_properties_order_format_before_location(self):
        from yads.spec import Storage

        converter = SQLGlotConverter()
        storage = Storage(
            format="parquet",
            location="/data/tables/test",
            tbl_properties={"key1": "value1", "key2": "value2"},
        )

        properties = converter._handle_storage_properties(storage)

        # Should have 4 properties total: format + location + 2 table properties
        assert len(properties) == 4

        # Verify format property comes first
        format_property = properties[0]
        assert isinstance(format_property, exp.FileFormatProperty)
        assert format_property.this.this == "parquet"

        # Verify location property comes second
        location_property = properties[1]
        assert isinstance(location_property, exp.LocationProperty)
        assert location_property.this.this == "/data/tables/test"

        # Verify table properties come after format and location
        tbl_prop1 = properties[2]
        tbl_prop2 = properties[3]
        assert isinstance(tbl_prop1, exp.Property)
        assert isinstance(tbl_prop2, exp.Property)

        # Check that the properties are the expected ones (order may vary for tbl_properties)
        prop_keys = {prop.this.this for prop in [tbl_prop1, tbl_prop2]}
        assert prop_keys == {"key1", "key2"}

    def test_storage_properties_partial_storage(self):
        from yads.spec import Storage

        converter = SQLGlotConverter()

        # Test with only format
        storage_format_only = Storage(format="delta")
        properties = converter._handle_storage_properties(storage_format_only)
        assert len(properties) == 1
        assert isinstance(properties[0], exp.FileFormatProperty)
        assert properties[0].this.this == "delta"

        # Test with only location
        storage_location_only = Storage(location="/path/to/data")
        properties = converter._handle_storage_properties(storage_location_only)
        assert len(properties) == 1
        assert isinstance(properties[0], exp.LocationProperty)
        assert properties[0].this.this == "/path/to/data"

        # Test with only table properties
        storage_props_only = Storage(tbl_properties={"prop": "value"})
        properties = converter._handle_storage_properties(storage_props_only)
        assert len(properties) == 1
        assert isinstance(properties[0], exp.Property)
        assert properties[0].this.this == "prop"

    def test_storage_properties_none_storage(self):
        converter = SQLGlotConverter()
        properties = converter._handle_storage_properties(None)
        assert properties == []


# %% Convert arguments
class TestConvertWithIgnoreArguments:
    def test_convert_with_ignore_catalog(self):
        from yads.loaders import from_yaml_path

        spec = from_yaml_path("tests/fixtures/spec/valid/basic_spec.yaml")
        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_catalog=True)

        table_expression = result.this.this
        assert table_expression.this.this == "test_spec"
        assert table_expression.db == "db"
        assert table_expression.catalog == ""

    def test_convert_with_ignore_database(self):
        from yads.loaders import from_yaml_path

        spec = from_yaml_path("tests/fixtures/spec/valid/basic_spec.yaml")
        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_database=True)

        table_expression = result.this.this
        assert table_expression.this.this == "test_spec"
        assert table_expression.db == ""
        assert table_expression.catalog == "catalog"

    def test_convert_with_ignore_both(self):
        from yads.loaders import from_yaml_path

        spec = from_yaml_path("tests/fixtures/spec/valid/basic_spec.yaml")
        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_catalog=True, ignore_database=True)

        table_expression = result.this.this
        assert table_expression.this.this == "test_spec"
        assert table_expression.db == ""
        assert table_expression.catalog == ""

    def test_convert_with_ignore_arguments_and_other_kwargs(self):
        from yads.loaders import from_yaml_path

        spec = from_yaml_path("tests/fixtures/spec/valid/basic_spec.yaml")
        converter = SQLGlotConverter()
        result = converter.convert(
            spec, ignore_catalog=True, ignore_database=True, if_not_exists=True
        )

        table_expression = result.this.this
        assert table_expression.this.this == "test_spec"
        assert table_expression.db == ""
        assert table_expression.catalog == ""

        assert result.args["exists"] is True

    def test_convert_with_partial_qualified_name_ignore_catalog(self):
        from yads.spec import YadsSpec, Column
        from yads.types import String

        spec = YadsSpec(
            name="sales.orders",
            version="1.0.0",
            columns=[Column(name="id", type=String())],
        )

        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_catalog=True)

        table_expression = result.this.this
        assert table_expression.this.this == "orders"
        assert table_expression.db == "sales"
        assert table_expression.catalog == ""

    def test_convert_with_partial_qualified_name_ignore_database(self):
        from yads.spec import YadsSpec, Column
        from yads.types import String

        spec = YadsSpec(
            name="prod.orders",
            version="1.0.0",
            columns=[Column(name="id", type=String())],
        )

        converter = SQLGlotConverter()
        result = converter.convert(spec, ignore_database=True)

        table_expression = result.this.this
        assert table_expression.this.this == "orders"
        assert table_expression.db == ""
        assert table_expression.catalog == ""
