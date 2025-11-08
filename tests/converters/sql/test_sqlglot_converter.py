import pytest
import warnings
from sqlglot import parse_one, exp
from sqlglot.expressions import convert
from yads.converters.sql import SQLGlotConverter, SQLGlotConverterConfig
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
    Tensor,
)
from yads.spec import Column, Field, YadsSpec, TransformedColumnReference
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
from yads.exceptions import (
    ConversionError,
    UnsupportedFeatureError,
    ValidationWarning,
    ConverterConfigError,
)
from yads.loaders import from_yaml_string


# fmt: off
# %% Types
class TestSQLGlotConverterTypes:
    @pytest.mark.parametrize(
        "yads_type, expected_datatype, expected_warning",
        [
            # String types
            (String(), exp.DataType(this=exp.DataType.Type.TEXT), None),
            (
                String(length=255),
                exp.DataType(
                    this=exp.DataType.Type.TEXT,
                    expressions=[exp.DataTypeParam(this=exp.Literal.number("255"))],
                ),
                None,
            ),
            # Integer types - handled by type handler
            (Integer(), exp.DataType(this=exp.DataType.Type.INT), None),
            (Integer(bits=8), exp.DataType(this=exp.DataType.Type.TINYINT), None),
            (Integer(bits=16), exp.DataType(this=exp.DataType.Type.SMALLINT), None),
            (Integer(bits=32), exp.DataType(this=exp.DataType.Type.INT), None),
            (Integer(bits=64), exp.DataType(this=exp.DataType.Type.BIGINT), None),
            (Integer(signed=False), exp.DataType(this=exp.DataType.Type.UINT), None),
            (Integer(bits=8, signed=False), exp.DataType(this=exp.DataType.Type.UTINYINT), None),
            (Integer(bits=16, signed=False), exp.DataType(this=exp.DataType.Type.USMALLINT), None),
            (Integer(bits=32, signed=False), exp.DataType(this=exp.DataType.Type.UINT), None),
            (Integer(bits=64, signed=False), exp.DataType(this=exp.DataType.Type.UBIGINT), None),
            # Float types - handled by type handler
            (Float(), exp.DataType(this=exp.DataType.Type.FLOAT), None),
            (
                Float(bits=16),
                exp.DataType(this=exp.DataType.Type.FLOAT),
                "SQLGlotConverter does not support half-precision Float (bits=16).",
            ),
            (Float(bits=32), exp.DataType(this=exp.DataType.Type.FLOAT), None),
            (Float(bits=64), exp.DataType(this=exp.DataType.Type.DOUBLE), None),
            # Decimal types - handled by type handler
            (Decimal(), exp.DataType(this=exp.DataType.Type.DECIMAL), None),
            (
                Decimal(precision=10, scale=2),
                exp.DataType(
                    this=exp.DataType.Type.DECIMAL,
                    expressions=[
                        exp.DataTypeParam(this=exp.Literal.number("10")),
                        exp.DataTypeParam(this=exp.Literal.number("2")),
                    ],
                ),
                None,
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
                None,
            ),
            # Boolean type - fallback to build
            (Boolean(), exp.DataType(this=exp.DataType.Type.BOOLEAN), None),
            # Binary types - fallback to build
            (Binary(), exp.DataType(this=exp.DataType.Type.BINARY), None),
            (
                Binary(length=8),
                exp.DataType(
                    this=exp.DataType.Type.BINARY,
                    expressions=[exp.DataTypeParam(this=exp.Literal.number("8"))],
                ),
                None,
            ),
            # Temporal types
            (Date(), exp.DataType(this=exp.DataType.Type.DATE), None),
            # Date bits are currently ignored
            (Date(bits=32), exp.DataType(this=exp.DataType.Type.DATE), None),
            (Date(bits=64), exp.DataType(this=exp.DataType.Type.DATE), None),
            (Time(), exp.DataType(this=exp.DataType.Type.TIME), None),
            (Time(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIME), None),
            (Time(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIME), None),
            (Time(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIME), None),
            (Time(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIME), None),
            # Time bits are currently ignored
            (Time(bits=32), exp.DataType(this=exp.DataType.Type.TIME), None),
            (Time(bits=64), exp.DataType(this=exp.DataType.Type.TIME), None),
            (Timestamp(), exp.DataType(this=exp.DataType.Type.TIMESTAMP), None),
            # Timestamp unit and tz are currently ignored
            (Timestamp(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIMESTAMP), None),
            (Timestamp(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIMESTAMP), None),
            (Timestamp(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIMESTAMP), None),
            (Timestamp(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIMESTAMP), None),
            (TimestampTZ(), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ), None),
            (TimestampTZ(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ), None),
            (TimestampTZ(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ), None),
            (TimestampTZ(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ), None),
            (TimestampTZ(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ), None),
            (TimestampTZ(tz="UTC"), exp.DataType(this=exp.DataType.Type.TIMESTAMPTZ), None),
            (TimestampLTZ(), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ), None),
            (TimestampLTZ(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ), None),
            (TimestampLTZ(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ), None),
            (TimestampLTZ(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ), None),
            (TimestampLTZ(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIMESTAMPLTZ), None),
            (TimestampNTZ(), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ), None),
            (TimestampNTZ(unit=TimeUnit.S), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ), None),
            (TimestampNTZ(unit=TimeUnit.MS), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ), None),
            (TimestampNTZ(unit=TimeUnit.US), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ), None),
            (TimestampNTZ(unit=TimeUnit.NS), exp.DataType(this=exp.DataType.Type.TIMESTAMPNTZ), None),
            # Duration - warning and coerced to TEXT
            (
                Duration(),
                exp.DataType(this=exp.DataType.Type.TEXT),
                "SQLGlotConverter does not support type: duration",
            ),
            # JSON type - fallback to build
            (JSON(), exp.DataType(this=exp.DataType.Type.JSON), None),
            # Spatial types - fallback to build
            (Geometry(), exp.DataType(this=exp.DataType.Type.GEOMETRY), None),
            (
                Geometry(srid=4326),
                exp.DataType(
                    this=exp.DataType.Type.GEOMETRY,
                    expressions=[exp.DataTypeParam(this=exp.Literal.number("4326"))]
                ),
                None,
            ),
            (Geography(), exp.DataType(this=exp.DataType.Type.GEOGRAPHY), None),
            (
                Geography(srid=4326),
                exp.DataType(
                    this=exp.DataType.Type.GEOGRAPHY,
                    expressions=[exp.DataTypeParam(this=exp.Literal.number("4326"))],
                ),
                None,
            ),
            # Void type - handled by type handler
            (Void(), exp.DataType(this=exp.DataType.Type.USERDEFINED, kind="VOID"), None),
            # Other types - fallback to build
            (UUID(), exp.DataType(this=exp.DataType.Type.UUID), None),
            (Variant(), exp.DataType(this=exp.DataType.Type.VARIANT), None),
            # Unsupported types
            (
                Tensor(element=Integer(bits=32), shape=(10, 20)),
                exp.DataType(this=exp.DataType.Type.TEXT),
                "SQLGlotConverter does not support type: tensor<integer(bits=32), shape=[10, 20]>"
            ),
        ],
    )
    def test_convert_type(self, yads_type, expected_datatype, expected_warning):
        converter = SQLGlotConverter(
            config=SQLGlotConverterConfig(fallback_type=exp.DataType.Type.TEXT)
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_type(yads_type)

        assert result == expected_datatype

        if expected_warning is not None:
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert expected_warning in str(w[0].message)
        else:
            assert len(w) == 0

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
    def test_convert_interval_type(self, yads_type, expected_datatype):
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
    def test_convert_array_type(self, yads_type):
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
    def test_convert_map_type(self, yads_type):
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

    def test_convert_struct_type(self):
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

    def test_convert_nested_struct_type(self):
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
        converter = SQLGlotConverter(SQLGlotConverterConfig(mode="raise"))
        with pytest.raises(
            UnsupportedFeatureError, match="SQLGlotConverter does not support type:"
        ):
            converter._convert_type(yads_type)
# fmt: on


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

    # Normalize both ASTs to be version-agnostic by removing IndexParameters from PrimaryKey
    # Added to support sqlglot 27.0.0, after IndexParameters was added as a required `include` argument in 27.2.0
    def normalize_ast(ast):
        """Remove IndexParameters from PrimaryKey expressions to make comparison version-agnostic."""
        if hasattr(ast, "find_all"):
            for pk in ast.find_all(exp.PrimaryKey):
                if hasattr(pk, "args") and "include" in pk.args:
                    pk.set("include", None)
        return ast

    normalized_generated = normalize_ast(generated_ast)
    normalized_expected = normalize_ast(expected_ast)

    assert normalized_generated == normalized_expected, (
        "Generated AST does not match expected AST.\n\n"
        f"YAML AST: {repr(generated_ast)}\n\n"
        f"SQL AST:  {repr(expected_ast)}"
    )


# %% Constraint conversion
class TestConstraintConversion:
    def test_convert_not_null_constraint(self):
        converter = SQLGlotConverter()
        constraint = NotNullConstraint()
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())
        assert result == expected

    def test_convert_primary_key_constraint(self):
        converter = SQLGlotConverter()
        constraint = PrimaryKeyConstraint()
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(kind=exp.PrimaryKeyColumnConstraint())
        assert result == expected

    def test_convert_default_constraint(self):
        converter = SQLGlotConverter()
        constraint = DefaultConstraint(value="test_value")
        result = converter._convert_column_constraint(constraint)

        expected = exp.ColumnConstraint(
            kind=exp.DefaultColumnConstraint(this=exp.Literal.string("test_value"))
        )
        assert result == expected

    def test_convert_identity_constraint_positive_values(self):
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

    def test_convert_identity_constraint_negative_increment(self):
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

    def test_convert_identity_constraint_negative_start(self):
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

    def test_convert_foreign_key_constraint_with_name(self):
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

    def test_convert_foreign_key_constraint_no_name(self):
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

    def test_convert_primary_key_table_constraint_with_name(self):
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
                    ],
                    include=exp.IndexParameters(),
                )
            ],
        )
        assert result == expected

    def test_convert_primary_key_table_constraint_no_name_raises_error(self):
        converter = SQLGlotConverter()
        constraint = PrimaryKeyTableConstraint(columns=["col1"])

        with pytest.raises(
            ConversionError, match="Primary key constraint must have a name"
        ):
            converter._convert_table_constraint(constraint)

    def test_convert_foreign_key_table_constraint_with_name(self):
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

    def test_convert_foreign_key_table_constraint_no_name_raises_error(self):
        converter = SQLGlotConverter()
        constraint = ForeignKeyTableConstraint(
            columns=["col1"],
            references=ForeignKeyReference(table="other_table", columns=["id"]),
        )

        with pytest.raises(
            ConversionError, match="Foreign key constraint must have a name"
        ):
            converter._convert_table_constraint(constraint)

    def test_convert_unsupported_column_constraint_raises_error(self):
        converter = SQLGlotConverter(SQLGlotConverterConfig(mode="raise"))

        class UnsupportedConstraint:
            pass

        constraint = UnsupportedConstraint()

        with pytest.raises(
            UnsupportedFeatureError,
            match="SQLGlotConverter does not support constraint",
        ):
            converter._convert_column_constraint(constraint)

    def test_convert_unsupported_column_constraint_coerce_omits_and_warns(self):
        converter = SQLGlotConverter(SQLGlotConverterConfig(mode="coerce"))

        class UnsupportedConstraint:
            pass

        constraint = UnsupportedConstraint()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_column_constraint(constraint)

        assert result is None
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "does not support constraint" in str(w[0].message)

    def test_convert_unsupported_table_constraint_raises_error(self):
        converter = SQLGlotConverter(SQLGlotConverterConfig(mode="raise"))

        class UnsupportedTableConstraint:
            pass

        constraint = UnsupportedTableConstraint()

        with pytest.raises(
            UnsupportedFeatureError,
            match="SQLGlotConverter does not support table constraint",
        ):
            converter._convert_table_constraint(constraint)

    def test_convert_unsupported_table_constraint_coerce_omits_and_warns(self):
        converter = SQLGlotConverter(SQLGlotConverterConfig(mode="coerce"))

        class UnsupportedTableConstraint:
            pass

        constraint = UnsupportedTableConstraint()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_table_constraint(constraint)

        assert result is None
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "does not support table constraint" in str(w[0].message)


# %% Transform handling
class TestTransformConversion:
    def test_convert_cast_transform(self):
        converter = SQLGlotConverter()
        result = converter._handle_cast_transform("col1", ["TEXT"])

        expected = exp.Cast(
            this=exp.column("col1"),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        )
        assert result == expected

    def test_convert_cast_transform_wrong_args_raises_error(self):
        converter = SQLGlotConverter()

        with pytest.raises(
            ConversionError, match="The 'cast' transform requires exactly 1 argument"
        ):
            converter._handle_cast_transform("col1", ["TEXT", "INT"])

    def test_convert_cast_transform_unknown_type_raises_error(self):
        converter = SQLGlotConverter(SQLGlotConverterConfig(mode="raise"))
        with pytest.raises(
            UnsupportedFeatureError,
            match="Transform type 'NOT_A_TYPE' is not a valid sqlglot Type",
        ):
            converter._handle_cast_transform("col1", ["not_a_type"])

    def test_convert_cast_transform_unknown_type_coerce_warns_and_coerces_to_text(self):
        converter = SQLGlotConverter(
            SQLGlotConverterConfig(mode="coerce", fallback_type=exp.DataType.Type.TEXT)
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._handle_cast_transform("col1", ["not_a_type"])

        expected = exp.Cast(
            this=exp.column("col1"),
            to=exp.DataType(this=exp.DataType.Type.TEXT),
        )
        assert result == expected
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "is not a valid sqlglot Type" in str(w[0].message)

    def test_convert_bucket_transform(self):
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
    def test_convert_generated_column(self):
        converter = SQLGlotConverter()

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

    def test_convert_generated_column_with_transform_args(self):
        converter = SQLGlotConverter()

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

    def test_convert_column_without_generated_clause(self):
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

    def test_convert_column_with_both_constraints_and_generated(self):
        converter = SQLGlotConverter()

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

        converter = SQLGlotConverter(SQLGlotConverterConfig(mode="raise"))
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

        converter = SQLGlotConverter(SQLGlotConverterConfig(mode="raise"))
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
        config = SQLGlotConverterConfig(ignore_catalog=True)
        converter = SQLGlotConverter(config)
        result = converter.convert(spec)

        table_expression = result.this.this
        assert table_expression.this.this == "test_spec"
        assert table_expression.db == "db"
        assert table_expression.catalog == ""

    def test_convert_with_ignore_database(self):
        from yads.loaders import from_yaml_path

        spec = from_yaml_path("tests/fixtures/spec/valid/basic_spec.yaml")
        config = SQLGlotConverterConfig(ignore_database=True)
        converter = SQLGlotConverter(config)
        result = converter.convert(spec)

        table_expression = result.this.this
        assert table_expression.this.this == "test_spec"
        assert table_expression.db == ""
        assert table_expression.catalog == "catalog"

    def test_convert_with_ignore_both(self):
        from yads.loaders import from_yaml_path

        spec = from_yaml_path("tests/fixtures/spec/valid/basic_spec.yaml")
        config = SQLGlotConverterConfig(ignore_catalog=True, ignore_database=True)
        converter = SQLGlotConverter(config)
        result = converter.convert(spec)

        table_expression = result.this.this
        assert table_expression.this.this == "test_spec"
        assert table_expression.db == ""
        assert table_expression.catalog == ""

    def test_convert_with_ignore_arguments_and_other_kwargs(self):
        from yads.loaders import from_yaml_path

        spec = from_yaml_path("tests/fixtures/spec/valid/basic_spec.yaml")
        config = SQLGlotConverterConfig(
            ignore_catalog=True, ignore_database=True, if_not_exists=True
        )
        converter = SQLGlotConverter(config)
        result = converter.convert(spec)

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

        config = SQLGlotConverterConfig(ignore_catalog=True)
        converter = SQLGlotConverter(config)
        result = converter.convert(spec)

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

        config = SQLGlotConverterConfig(ignore_database=True)
        converter = SQLGlotConverter(config)
        result = converter.convert(spec)

        table_expression = result.this.this
        assert table_expression.this.this == "orders"
        assert table_expression.db == ""
        assert table_expression.catalog == ""


# %% SQLGlotConverter column filtering and customization
class TestSQLGlotConverterCustomization:
    def test_ignore_columns(self):
        """Test that ignore_columns excludes specified columns from the AST."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
                Column(name="secret", type=String()),
            ],
        )
        config = SQLGlotConverterConfig(ignore_columns={"secret"})
        converter = SQLGlotConverter(config)
        ast = converter.convert(spec)

        # Extract column names from AST
        column_names = {
            expr.this.this
            for expr in ast.this.expressions
            if isinstance(expr, exp.ColumnDef)
        }

        assert "id" in column_names
        assert "name" in column_names
        assert "secret" not in column_names

    def test_include_columns(self):
        """Test that include_columns only includes specified columns in the AST."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
                Column(name="internal", type=String()),
            ],
        )
        config = SQLGlotConverterConfig(include_columns={"id", "name"})
        converter = SQLGlotConverter(config)
        ast = converter.convert(spec)

        # Extract column names from AST
        column_names = {
            expr.this.this
            for expr in ast.this.expressions
            if isinstance(expr, exp.ColumnDef)
        }

        assert "id" in column_names
        assert "name" in column_names
        assert "internal" not in column_names

    def test_column_override_basic(self):
        """Test basic column override functionality."""

        def custom_name_override(field, converter):
            # Override name field to be BIGINT with custom constraint
            return exp.ColumnDef(
                this=exp.Identifier(this=field.name),
                kind=exp.DataType(this=exp.DataType.Type.BIGINT),
                constraints=[exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())],
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
            ],
        )
        config = SQLGlotConverterConfig(column_overrides={"name": custom_name_override})
        converter = SQLGlotConverter(config)
        ast = converter.convert(spec)

        # Find the overridden column
        name_column = None
        id_column = None
        for expr in ast.this.expressions:
            if isinstance(expr, exp.ColumnDef):
                if expr.this.this == "name":
                    name_column = expr
                elif expr.this.this == "id":
                    id_column = expr

        # Check that override was applied to name column
        assert name_column is not None
        assert name_column.kind.this == exp.DataType.Type.BIGINT
        assert len(name_column.constraints) == 1
        assert isinstance(name_column.constraints[0].kind, exp.NotNullColumnConstraint)

        # Check that other columns use default conversion
        assert id_column is not None
        assert id_column.kind.this == exp.DataType.Type.INT

    def test_column_override_with_complex_type(self):
        """Test column override with complex custom column definition."""

        def custom_metadata_override(field, converter):
            # Create a custom struct column for metadata
            struct_type = exp.DataType(
                this=exp.DataType.Type.STRUCT,
                expressions=[
                    exp.ColumnDef(
                        this=exp.Identifier(this="version"),
                        kind=exp.DataType(this=exp.DataType.Type.TEXT),
                    ),
                    exp.ColumnDef(
                        this=exp.Identifier(this="tags"),
                        kind=exp.DataType(
                            this=exp.DataType.Type.ARRAY,
                            expressions=[exp.DataType(this=exp.DataType.Type.TEXT)],
                        ),
                    ),
                ],
            )
            return exp.ColumnDef(
                this=exp.Identifier(this=field.name), kind=struct_type, constraints=None
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="metadata", type=JSON()),
            ],
        )
        config = SQLGlotConverterConfig(
            column_overrides={"metadata": custom_metadata_override}
        )
        converter = SQLGlotConverter(config)
        ast = converter.convert(spec)

        # Find the overridden column
        metadata_column = None
        for expr in ast.this.expressions:
            if isinstance(expr, exp.ColumnDef) and expr.this.this == "metadata":
                metadata_column = expr
                break

        # Check that override was applied
        assert metadata_column is not None
        assert metadata_column.kind.this == exp.DataType.Type.STRUCT
        assert len(metadata_column.kind.expressions) == 2
        assert metadata_column.kind.expressions[0].this.this == "version"
        assert metadata_column.kind.expressions[1].this.this == "tags"

        # Find the version and tags fields in the complex column
        version_field = None
        tags_field = None
        for expr in metadata_column.kind.expressions:
            if isinstance(expr.this, exp.Identifier) and expr.this.this == "version":
                version_field = expr
            elif isinstance(expr.this, exp.Identifier) and expr.this.this == "tags":
                tags_field = expr

        assert version_field is not None
        assert tags_field is not None
        assert version_field.kind.this == exp.DataType.Type.TEXT
        assert tags_field.kind.this == exp.DataType.Type.ARRAY
        assert tags_field.kind.expressions[0].this == exp.DataType.Type.TEXT

    @pytest.mark.parametrize(
        "fallback_type",
        [exp.DataType.Type.TEXT, exp.DataType.Type.BINARY, exp.DataType.Type.BLOB],
    )
    def test_valid_fallback_types(self, fallback_type: exp.DataType.Type):
        """Test fallback_type for unsupported types."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="unsupported", type=Duration()),
            ],
        )
        config = SQLGlotConverterConfig(fallback_type=fallback_type)
        converter = SQLGlotConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ast = converter.convert(spec, mode="coerce")

        # Find the fallback column
        unsupported_column = None
        for expr in ast.this.expressions:
            if isinstance(expr, exp.ColumnDef) and expr.this.this == "unsupported":
                unsupported_column = expr
                break

        # Check fallback was applied
        assert unsupported_column is not None
        assert unsupported_column.kind is not None
        assert unsupported_column.kind.this == fallback_type

        # Check warning was emitted
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "does not support type: duration" in str(w[0].message)
        assert fallback_type.name in str(w[0].message)

    def test_invalid_fallback_type_raises_error(self):
        """Test that invalid fallback_type raises UnsupportedFeatureError."""
        with pytest.raises(
            UnsupportedFeatureError,
            match="fallback_type must be one of: exp.DataType.Type.TEXT, exp.DataType.Type.BINARY, exp.DataType.Type.BLOB",
        ):
            SQLGlotConverterConfig(fallback_type=exp.DataType.Type.INT)

    def test_precedence_ignore_over_override(self):
        """Test that ignore_columns takes precedence over column_overrides."""

        def should_not_be_called(field, converter):
            pytest.fail("Override should not be called for ignored column")

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="ignored_col", type=String()),
            ],
        )
        config = SQLGlotConverterConfig(
            ignore_columns={"ignored_col"},
            column_overrides={"ignored_col": should_not_be_called},
        )
        converter = SQLGlotConverter(config)
        ast = converter.convert(spec)

        # Extract column names from AST
        column_names = {
            expr.this.this
            for expr in ast.this.expressions
            if isinstance(expr, exp.ColumnDef)
        }

        assert len(column_names) == 1
        assert "id" in column_names
        assert "ignored_col" not in column_names

    def test_precedence_override_over_default_conversion(self):
        """Test that column_overrides takes precedence over default conversion."""

        def integer_as_text_override(field, converter):
            return exp.ColumnDef(
                this=exp.Identifier(this=field.name),
                kind=exp.DataType(this=exp.DataType.Type.TEXT),
                constraints=None,
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="normal_int", type=Integer()),
                Column(name="text_int", type=Integer()),
            ],
        )
        config = SQLGlotConverterConfig(
            column_overrides={"text_int": integer_as_text_override}
        )
        converter = SQLGlotConverter(config)
        ast = converter.convert(spec)

        # Find both columns
        normal_column = None
        text_column = None
        for expr in ast.this.expressions:
            if isinstance(expr, exp.ColumnDef):
                if expr.this.this == "normal_int":
                    normal_column = expr
                elif expr.this.this == "text_int":
                    text_column = expr

        # Normal conversion
        assert normal_column is not None
        assert normal_column.kind.this == exp.DataType.Type.INT

        # Override conversion
        assert text_column is not None
        assert text_column.kind.this == exp.DataType.Type.TEXT

    def test_precedence_override_over_fallback(self):
        """Test that column_overrides takes precedence over fallback_type."""

        def custom_duration_override(field, converter):
            return exp.ColumnDef(
                this=exp.Identifier(this=field.name),
                kind=exp.DataType(this=exp.DataType.Type.BIGINT),
                constraints=[exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())],
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="fallback_duration", type=Duration()),
                Column(name="override_duration", type=Duration()),
            ],
        )
        config = SQLGlotConverterConfig(
            fallback_type=exp.DataType.Type.BINARY,
            column_overrides={"override_duration": custom_duration_override},
        )
        converter = SQLGlotConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ast = converter.convert(spec, mode="coerce")

        # Find both columns
        fallback_column = None
        override_column = None
        for expr in ast.this.expressions:
            if isinstance(expr, exp.ColumnDef):
                if expr.this.this == "fallback_duration":
                    fallback_column = expr
                elif expr.this.this == "override_duration":
                    override_column = expr

        # Fallback applied to fallback_duration
        assert fallback_column is not None
        assert fallback_column.kind.this == exp.DataType.Type.BINARY

        # Override applied to override_duration
        assert override_column is not None
        assert override_column.kind.this == exp.DataType.Type.BIGINT
        assert len(override_column.constraints) == 1

        # Only one warning for the fallback field
        assert len(w) == 1
        assert "fallback_duration" in str(w[0].message)

    def test_field_metadata_preservation_with_fallback(self):
        """SQLGlotConverter does not convert field metadata or field description."""
        pass

    def test_unknown_column_in_filters_raises_error(self):
        """Test that unknown columns in filters raise validation errors."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="col1", type=String())],
        )

        # Test unknown ignore_columns
        config1 = SQLGlotConverterConfig(ignore_columns={"nonexistent"})
        converter1 = SQLGlotConverter(config1)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in ignore_columns: nonexistent"
        ):
            converter1.convert(spec)

        # Test unknown include_columns
        config2 = SQLGlotConverterConfig(include_columns={"nonexistent"})
        converter2 = SQLGlotConverter(config2)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in include_columns: nonexistent"
        ):
            converter2.convert(spec)

    def test_conflicting_ignore_and_include_raises_error(self):
        """Test that overlapping ignore_columns and include_columns raises error."""
        with pytest.raises(
            ConverterConfigError,
            match="Columns cannot be both ignored and included: \\['col1'\\]",
        ):
            SQLGlotConverterConfig(
                ignore_columns={"col1", "col2"}, include_columns={"col1", "col3"}
            )

    def test_column_override_preserves_field_context(self):
        """Test that column overrides have access to original field properties."""

        def field_inspector_override(field, converter):
            # Override that inspects the original field and creates a column based on it
            constraints = []

            # Add NOT NULL if original field has NotNullConstraint
            if any(isinstance(c, NotNullConstraint) for c in field.constraints):
                constraints.append(
                    exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())
                )

            # Add DEFAULT if original field has DefaultConstraint
            for constraint in field.constraints:
                if isinstance(constraint, DefaultConstraint):
                    constraints.append(
                        exp.ColumnConstraint(
                            kind=exp.DefaultColumnConstraint(
                                this=convert(constraint.value)
                            )
                        )
                    )

            # Choose type based on original field properties
            if hasattr(field.type, "length") and field.type.length is not None:
                data_type = exp.DataType(
                    this=exp.DataType.Type.TEXT,
                    expressions=[exp.DataTypeParam(this=convert(field.type.length))],
                )
            else:
                data_type = exp.DataType(this=exp.DataType.Type.TEXT)

            return exp.ColumnDef(
                this=exp.Identifier(this=field.name),
                kind=data_type,
                constraints=constraints if constraints else None,
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(
                    name="inspected_col",
                    type=String(length=100),
                    description="A test column",
                    constraints=[NotNullConstraint(), DefaultConstraint(value="test")],
                ),
            ],
        )
        config = SQLGlotConverterConfig(
            column_overrides={"inspected_col": field_inspector_override}
        )
        converter = SQLGlotConverter(config)
        ast = converter.convert(spec)

        # Find the inspected column
        inspected_column = None
        for expr in ast.this.expressions:
            if isinstance(expr, exp.ColumnDef) and expr.this.this == "inspected_col":
                inspected_column = expr
                break

        # Check that override had access to original field properties
        assert inspected_column is not None
        assert inspected_column.kind.this == exp.DataType.Type.TEXT
        # Check length parameter was preserved
        assert len(inspected_column.kind.expressions) == 1
        assert inspected_column.kind.expressions[0].this.this == "100"
        # Check constraints were preserved
        assert len(inspected_column.constraints) == 2
        constraint_types = {type(c.kind) for c in inspected_column.constraints}
        assert exp.NotNullColumnConstraint in constraint_types
        assert exp.DefaultColumnConstraint in constraint_types

    def test_column_override_with_generated_column(self):
        """Test that column overrides work correctly with generated columns."""

        def generated_override(field, converter):
            # Create a custom generated column override
            return exp.ColumnDef(
                this=exp.Identifier(this=field.name),
                kind=exp.DataType(this=exp.DataType.Type.TEXT),
                constraints=[
                    exp.ColumnConstraint(
                        kind=exp.GeneratedAsIdentityColumnConstraint(
                            this=True,  # ALWAYS
                            expression=exp.func("UPPER", exp.column("source_col")),
                        )
                    )
                ],
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="source_col", type=String()),
                Column(name="generated_col", type=String()),
            ],
        )
        config = SQLGlotConverterConfig(
            column_overrides={"generated_col": generated_override}
        )
        converter = SQLGlotConverter(config)
        ast = converter.convert(spec)

        # Find the generated column
        generated_column = None
        for expr in ast.this.expressions:
            if isinstance(expr, exp.ColumnDef) and expr.this.this == "generated_col":
                generated_column = expr
                break

        # Check that override was applied
        assert generated_column is not None
        assert generated_column.kind.this == exp.DataType.Type.TEXT
        assert len(generated_column.constraints) == 1
        assert isinstance(
            generated_column.constraints[0].kind, exp.GeneratedAsIdentityColumnConstraint
        )
        assert generated_column.constraints[0].kind.this is True

    def test_multiple_overrides_and_filters_combined(self):
        """Test complex scenario with multiple overrides and filters combined."""

        def text_override(field, converter):
            return exp.ColumnDef(
                this=exp.Identifier(this=field.name),
                kind=exp.DataType(this=exp.DataType.Type.TEXT),
                constraints=None,
            )

        def bigint_override(field, converter):
            return exp.ColumnDef(
                this=exp.Identifier(this=field.name),
                kind=exp.DataType(this=exp.DataType.Type.BIGINT),
                constraints=[exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())],
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="keep_default", type=Integer()),
                Column(name="override_to_text", type=Integer()),
                Column(name="override_to_bigint", type=Float()),
                Column(name="ignored_col", type=String()),
                Column(name="excluded_col", type=Boolean()),
                Column(name="fallback_col", type=Duration()),
            ],
        )
        config = SQLGlotConverterConfig(
            ignore_columns={"ignored_col"},
            include_columns={
                "keep_default",
                "override_to_text",
                "override_to_bigint",
                "fallback_col",
            },
            column_overrides={
                "override_to_text": text_override,
                "override_to_bigint": bigint_override,
            },
            fallback_type=exp.DataType.Type.BLOB,
        )
        converter = SQLGlotConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ast = converter.convert(spec, mode="coerce")

        # Extract all columns
        columns = {}
        for expr in ast.this.expressions:
            if isinstance(expr, exp.ColumnDef):
                columns[expr.this.this] = expr

        # Check expected columns are present
        expected_columns = {
            "keep_default",
            "override_to_text",
            "override_to_bigint",
            "fallback_col",
        }
        assert set(columns.keys()) == expected_columns

        # Check individual column conversions
        assert (
            columns["keep_default"].kind.this == exp.DataType.Type.INT
        )  # Default conversion
        assert columns["override_to_text"].kind.this == exp.DataType.Type.TEXT  # Override
        assert (
            columns["override_to_bigint"].kind.this == exp.DataType.Type.BIGINT
        )  # Override
        assert columns["fallback_col"].kind.this == exp.DataType.Type.BLOB  # Fallback

        # Check constraints from overrides
        assert len(columns["override_to_bigint"].constraints) == 1
        assert isinstance(
            columns["override_to_bigint"].constraints[0].kind, exp.NotNullColumnConstraint
        )

        # Check warning for fallback
        assert len(w) == 1
        assert "fallback_col" in str(w[0].message)
