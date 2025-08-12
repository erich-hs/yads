from __future__ import annotations

import pytest
from sqlglot import parse_one
from sqlglot.expressions import ColumnDef, DataType

from yads.converters.sql.validators.ast_validation_rules import (
    DisallowFixedLengthString,
    DisallowType,
)


# ==========================================================
# AST validation rules tests
# Scope: unit rules: DisallowFixedLengthString, DisallowType
# ==========================================================


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
