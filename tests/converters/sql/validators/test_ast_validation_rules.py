from __future__ import annotations

import pytest
from sqlglot import parse_one
from sqlglot.expressions import ColumnDef, DataType

from yads.converters.sql.validators.ast_validation_rules import (
    DisallowType,
    DisallowUserDefinedType,
    DisallowFixedLengthString,
    DisallowFixedLengthBinary,
    DisallowNegativeScaleDecimal,
    DisallowParameterizedGeometry,
    DisallowColumnConstraintGeneratedIdentity,
    DisallowTableConstraintPrimaryKeyNullsFirst,
)


# %% DisallowType
class TestDisallowType:
    @pytest.fixture
    def rule(self) -> DisallowType:
        return DisallowType(disallow_type=DataType.Type.JSON)

    @pytest.mark.parametrize(
        "sql, expected",
        [
            (
                "JSON",
                "Data type 'JSON' is not supported for column 'col'.",
            ),
            ("INT", None),
            ("STRING", None),
        ],
    )
    def test_validate_disallowed_type_json(
        self, rule: DisallowType, sql: str, expected: str | None
    ):
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust_replaces_disallowed_type_with_default(self, rule: DisallowType):
        ast = parse_one("CREATE TABLE t (col JSON)")
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted_node = rule.adjust(data_type)

        assert isinstance(adjusted_node, DataType)
        assert adjusted_node.this == DataType.Type.TEXT
        assert not adjusted_node.expressions

    def test_adjustment_description(self, rule: DisallowType):
        assert (
            rule.adjustment_description == "The data type will be replaced with 'TEXT'."
        )

    def test_adjust_with_custom_fallback(self):
        rule = DisallowType(
            disallow_type=DataType.Type.JSON, fallback_type=DataType.Type.VARCHAR
        )
        ast = parse_one("CREATE TABLE t (col JSON)")
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted_node = rule.adjust(data_type)

        assert isinstance(adjusted_node, DataType)
        assert adjusted_node.this == DataType.Type.VARCHAR
        assert not adjusted_node.expressions
        assert (
            rule.adjustment_description
            == "The data type will be replaced with 'VARCHAR'."
        )


# %% DisallowUserDefinedType
class TestDisallowUserDefinedType:
    @pytest.mark.parametrize(
        "rule, sql, expected",
        [
            (
                DisallowUserDefinedType(disallow_type="VOID"),
                "VOID",
                "Data type 'VOID' is not supported for column 'col'.",
            ),
            (DisallowUserDefinedType(disallow_type="VOID"), "GEOMETRY", None),
            (DisallowUserDefinedType(disallow_type="VOID"), "TEXT", None),
        ],
    )
    def test_validate_user_defined_type(
        self, rule: DisallowUserDefinedType, sql: str, expected: str | None
    ):
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust_replaces_user_defined_type_with_fallback(self):
        rule = DisallowUserDefinedType(
            disallow_type="VOID", fallback_type=DataType.Type.TEXT
        )
        ast = parse_one("CREATE TABLE t (col VOID)")
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted = rule.adjust(data_type)

        assert isinstance(adjusted, DataType)
        assert adjusted.this == DataType.Type.TEXT
        assert not adjusted.expressions

    def test_adjustment_description(self):
        rule = DisallowUserDefinedType(
            disallow_type="VOID", fallback_type=DataType.Type.TEXT
        )
        assert (
            rule.adjustment_description == "The data type will be replaced with 'TEXT'."
        )


# %% DisallowFixedLengthString
class TestDisallowFixedLengthString:
    @pytest.fixture
    def rule(self) -> DisallowFixedLengthString:
        return DisallowFixedLengthString()

    @pytest.mark.parametrize(
        "sql, expected",
        [
            ("VARCHAR(50)", "Fixed-length strings are not supported for column 'col'."),
            (
                "CHAR(10)",
                "Fixed-length strings are not supported for column 'col'.",
            ),
            (
                "STRING",
                None,
            ),
            ("INT", None),
        ],
    )
    def test_validate_fixed_length_strings(
        self, rule: DisallowFixedLengthString, sql: str, expected: str | None
    ):
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust_removes_length_and_normalizes_type(
        self, rule: DisallowFixedLengthString
    ):
        sql = "CREATE TABLE t (col VARCHAR(50))"
        ast = parse_one(sql)
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted_node = rule.adjust(data_type)

        assert isinstance(adjusted_node, DataType)
        assert adjusted_node.this == DataType.Type.VARCHAR
        assert not adjusted_node.expressions

    def test_adjustment_description(self, rule: DisallowFixedLengthString):
        assert rule.adjustment_description == "The length parameter will be removed."


# %% DisallowFixedLengthBinary
class TestDisallowFixedLengthBinary:
    @pytest.fixture
    def rule(self) -> DisallowFixedLengthBinary:
        return DisallowFixedLengthBinary()

    @pytest.mark.parametrize(
        "sql, expected",
        [
            ("BINARY(50)", "Fixed-length binary is not supported for column 'col'."),
            (
                "BINARY",
                None,
            ),
            ("INT", None),
        ],
    )
    def test_validate_fixed_length_binary(
        self, rule: DisallowFixedLengthBinary, sql: str, expected: str | None
    ):
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust_removes_length_and_normalizes_type(
        self, rule: DisallowFixedLengthBinary
    ):
        sql = "CREATE TABLE t (col BINARY(50))"
        ast = parse_one(sql)
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted_node = rule.adjust(data_type)

        assert isinstance(adjusted_node, DataType)
        assert adjusted_node.this == DataType.Type.BINARY
        assert not adjusted_node.expressions

    def test_adjustment_description(self, rule: DisallowFixedLengthBinary):
        assert rule.adjustment_description == "The length parameter will be removed."


# %% DisallowParameterizedGeometry
class TestDisallowParameterizedGeometry:
    @pytest.fixture
    def rule(self) -> DisallowParameterizedGeometry:
        return DisallowParameterizedGeometry()

    @pytest.mark.parametrize(
        "sql, expected",
        [
            (
                "GEOMETRY(4326)",
                "Parameterized 'GEOMETRY' is not supported for column 'col'.",
            ),
            ("GEOMETRY", None),
            ("INT", None),
        ],
    )
    def test_validate_parameterized_geometry(
        self, rule: DisallowParameterizedGeometry, sql: str, expected: str | None
    ):
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust_removes_geometry_parameters(
        self, rule: DisallowParameterizedGeometry
    ):
        ast = parse_one("CREATE TABLE t (col GEOMETRY(4326))")
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted = rule.adjust(data_type)

        assert isinstance(adjusted, DataType)
        assert adjusted.this == DataType.Type.GEOMETRY
        assert not adjusted.expressions

    def test_adjustment_description(self, rule: DisallowParameterizedGeometry):
        assert rule.adjustment_description == "The parameters will be removed."


# %% DisallowNegativeScaleDecimal
class TestDisallowNegativeScaleDecimal:
    @pytest.fixture
    def rule(self) -> DisallowNegativeScaleDecimal:
        return DisallowNegativeScaleDecimal()

    @pytest.fixture
    def decimal_datatype_in_create(self):
        # Build a full CREATE TABLE AST and return the nested DECIMAL DataType node
        # to ensure ancestor context (column name) is available during validation.
        from sqlglot.expressions import (
            Create,
            Schema,
            Table,
            Identifier,
            ColumnDef,
            DataType,
            DataTypeParam,
            Literal,
        )

        def _build(precision: int, scale: int) -> DataType:
            dtype = DataType(
                this=DataType.Type.DECIMAL,
                expressions=[
                    DataTypeParam(this=Literal(this=precision)),
                    DataTypeParam(this=Literal(this=scale)),
                ],
            )
            coldef = ColumnDef(this=Identifier(this="col"), kind=dtype)
            _ = Create(
                this=Schema(this=Table(this=Identifier(this="t")), expressions=[coldef]),
                kind="TABLE",
            )
            # Returning dtype keeps it attached in the AST so ancestors are discoverable
            return dtype

        return _build

    def test_validate_detects_negative_scale_decimal(
        self, rule: DisallowNegativeScaleDecimal, decimal_datatype_in_create
    ):
        data_type = decimal_datatype_in_create(10, -2)
        result = rule.validate(data_type)
        assert (
            result
            == "Negative scale decimals are not supported for column 'col'. Found DECIMAL(10, -2)."
        )

    def test_validate_ignores_positive_scale_decimal(
        self, rule: DisallowNegativeScaleDecimal, decimal_datatype_in_create
    ):
        data_type = decimal_datatype_in_create(10, 2)
        result = rule.validate(data_type)
        assert result is None

    def test_validate_ignores_zero_scale_decimal(
        self, rule: DisallowNegativeScaleDecimal, decimal_datatype_in_create
    ):
        data_type = decimal_datatype_in_create(10, 0)
        result = rule.validate(data_type)
        assert result is None

    def test_validate_ignores_non_decimal_types(self, rule: DisallowNegativeScaleDecimal):
        from sqlglot.expressions import DataType

        # Test INT - should not trigger validation
        data_type = DataType(this=DataType.Type.INT, expressions=[])

        result = rule.validate(data_type)
        assert result is None

    def test_adjust_coerces_negative_scale_to_positive_scale(
        self, rule: DisallowNegativeScaleDecimal, decimal_datatype_in_create
    ):
        # Test DECIMAL(10, -2) -> DECIMAL(12, 0)
        data_type = decimal_datatype_in_create(10, -2)
        adjusted_node = rule.adjust(data_type)

        assert isinstance(adjusted_node, DataType)
        assert adjusted_node.this == DataType.Type.DECIMAL
        assert len(adjusted_node.expressions) == 2
        assert adjusted_node.expressions[0].this.this == 12  # 10 + abs(-2)
        assert adjusted_node.expressions[1].this.this == 0  # scale set to 0

    def test_adjust_coerces_large_negative_scale(
        self, rule: DisallowNegativeScaleDecimal, decimal_datatype_in_create
    ):
        # Test DECIMAL(10, -6) -> DECIMAL(16, 0)
        data_type = decimal_datatype_in_create(10, -6)
        adjusted_node = rule.adjust(data_type)

        assert isinstance(adjusted_node, DataType)
        assert adjusted_node.this == DataType.Type.DECIMAL
        assert len(adjusted_node.expressions) == 2
        assert adjusted_node.expressions[0].this.this == 16  # 10 + abs(-6)
        assert adjusted_node.expressions[1].this.this == 0  # scale set to 0

    def test_adjust_does_not_modify_valid_decimal(
        self, rule: DisallowNegativeScaleDecimal, decimal_datatype_in_create
    ):
        # Test that valid decimals are not modified
        data_type = decimal_datatype_in_create(10, 2)
        adjusted_node = rule.adjust(data_type)

        # Should return the same node unchanged
        assert adjusted_node is data_type

    def test_adjustment_description(self, rule: DisallowNegativeScaleDecimal):
        assert (
            rule.adjustment_description
            == "The precision will be increased by the absolute value of the negative scale, and the scale will be set to 0."
        )


# %% DisallowColumnConstraintGeneratedIdentity
class TestDisallowColumnConstraintGeneratedIdentity:
    @pytest.fixture
    def rule(self) -> DisallowColumnConstraintGeneratedIdentity:
        return DisallowColumnConstraintGeneratedIdentity()

    def test_validate_detects_identity_constraint(
        self, rule: DisallowColumnConstraintGeneratedIdentity
    ):
        sql = """
        CREATE TABLE t (
          c1 INT GENERATED ALWAYS AS IDENTITY(1, 1),
          c2 TEXT
        )
        """
        ast = parse_one(sql)
        coldef = ast.find(ColumnDef)
        assert coldef
        assert (
            rule.validate(coldef)
            == "GENERATED ALWAYS AS IDENTITY is not supported for column 'c1'."
        )

    def test_adjust_removes_identity_constraint(
        self, rule: DisallowColumnConstraintGeneratedIdentity
    ):
        sql = "CREATE TABLE t (c1 INT GENERATED ALWAYS AS IDENTITY(1, 1))"
        ast = parse_one(sql)
        coldef = ast.find(ColumnDef)
        assert coldef
        adjusted = rule.adjust(coldef)
        assert isinstance(adjusted, DataType) or isinstance(adjusted, ColumnDef)
        # Ensure no GeneratedAsIdentityColumnConstraint remains
        if isinstance(adjusted, ColumnDef):
            constraints = adjusted.args.get("constraints") or []
            assert all(
                type(c.kind).__name__ != "GeneratedAsIdentityColumnConstraint"
                for c in constraints
            )


# %% DisallowTableConstraintPrimaryKeyNullsFirst
class TestDisallowTableConstraintPrimaryKeyNullsFirst:
    @pytest.fixture
    def rule(self) -> DisallowTableConstraintPrimaryKeyNullsFirst:
        return DisallowTableConstraintPrimaryKeyNullsFirst()

    def test_validate_detects_nulls_first_in_pk(
        self, rule: DisallowTableConstraintPrimaryKeyNullsFirst
    ):
        sql = "CREATE TABLE t (c1 INT, CONSTRAINT pk PRIMARY KEY (c1 NULLS FIRST))"
        ast = parse_one(sql)
        from sqlglot.expressions import PrimaryKey

        node = ast.find(PrimaryKey)
        assert node is not None
        assert (
            rule.validate(node)
            == "NULLS FIRST is not supported in PRIMARY KEY constraints."
        )

    def test_adjust_removes_nulls_first_in_pk(
        self, rule: DisallowTableConstraintPrimaryKeyNullsFirst
    ):
        sql = "CREATE TABLE t (c1 INT, CONSTRAINT pk PRIMARY KEY (c1 NULLS FIRST))"
        ast = parse_one(sql)
        from sqlglot.expressions import PrimaryKey, Ordered

        node = ast.find(PrimaryKey)
        assert node is not None
        adjusted = rule.adjust(node)
        for expr in adjusted.expressions:
            if isinstance(expr, Ordered):
                assert not expr.args.get("nulls_first")
