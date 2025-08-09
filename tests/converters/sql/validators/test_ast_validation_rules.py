from __future__ import annotations

import pytest
from sqlglot import parse_one
from sqlglot.expressions import ColumnDef, DataType

from yads.converters.sql.validators.ast_validation_rules import (
    DisallowFixedLengthString,
    DisallowType,
)


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
    def test_validate(
        self, rule: DisallowFixedLengthString, sql: str, expected: str | None
    ):
        """Tests that the rule correctly identifies fixed-length strings."""
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust(self, rule: DisallowFixedLengthString):
        """Tests that the rule correctly removes the length from a fixed-length string."""
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
        """Tests that the adjustment description is correct."""
        assert rule.adjustment_description == "The length parameter will be removed."


class TestDisallowType:
    @pytest.fixture
    def rule(self) -> DisallowType:
        return DisallowType(disallowed_types=[DataType.Type.JSON])

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
    def test_validate(self, rule: DisallowType, sql: str, expected: str | None):
        ast = parse_one(f"CREATE TABLE t (col {sql})")
        assert ast
        column_def = ast.find(ColumnDef)
        assert column_def
        data_type = column_def.find(DataType)
        assert data_type

        assert rule.validate(data_type) == expected

    def test_adjust(self, rule: DisallowType):
        ast = parse_one("CREATE TABLE t (col JSON)")
        assert ast
        data_type = ast.find(DataType)
        assert data_type

        adjusted_node = rule.adjust(data_type)

        assert isinstance(adjusted_node, DataType)
        assert adjusted_node.this == DataType.Type.TEXT
        assert not adjusted_node.expressions

    def test_adjustment_description(self, rule: DisallowType):
        assert rule.adjustment_description == "The data type will be replaced with TEXT."
