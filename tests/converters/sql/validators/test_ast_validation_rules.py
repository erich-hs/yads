from __future__ import annotations

import pytest
from sqlglot import parse_one
from sqlglot.expressions import ColumnDef, DataType

from yads.converters.sql.validators.ast_validation_rules import (
    DisallowFixedLengthString,
    DisallowType,
    DisallowParameterizedGeometryType,
    DisallowVoidType,
    DisallowColumnConstraintGeneratedIdentity,
    DisallowTableConstraintPrimaryKeyNullsFirst,
)


# ==========================================================
# AST validation rules tests
# Scope: unit rules: DisallowFixedLengthString, DisallowType
# ==========================================================


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


# %% DisallowParameterizedGeometryType
class TestDisallowParameterizedGeometryType:
    @pytest.fixture
    def rule(self) -> DisallowParameterizedGeometryType:
        return DisallowParameterizedGeometryType()

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
        self, rule: DisallowParameterizedGeometryType, sql: str, expected: str | None
    ):
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust_removes_geometry_parameters(
        self, rule: DisallowParameterizedGeometryType
    ):
        ast = parse_one("CREATE TABLE t (col GEOMETRY(4326))")
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted = rule.adjust(data_type)

        assert isinstance(adjusted, DataType)
        assert adjusted.this == DataType.Type.GEOMETRY
        assert not adjusted.expressions

    def test_adjustment_description(self, rule: DisallowParameterizedGeometryType):
        assert rule.adjustment_description == "The parameters will be removed."


# %% DisallowVoidType
class TestDisallowVoidType:
    @pytest.fixture
    def rule(self) -> DisallowVoidType:
        return DisallowVoidType()

    @pytest.mark.parametrize(
        "sql, expected",
        [
            ("VOID", "Data type 'VOID' is not supported for column 'col'."),
            ("GEOMETRY", None),
            ("TEXT", None),
        ],
    )
    def test_validate_void(self, rule: DisallowVoidType, sql: str, expected: str | None):
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust_replaces_void_with_text(self, rule: DisallowVoidType):
        ast = parse_one("CREATE TABLE t (col VOID)")
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted = rule.adjust(data_type)

        assert isinstance(adjusted, DataType)
        assert adjusted.this == DataType.Type.TEXT
        assert not adjusted.expressions

    def test_adjustment_description(self, rule: DisallowVoidType):
        assert (
            rule.adjustment_description == "The data type will be replaced with 'TEXT'."
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
