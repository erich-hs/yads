from __future__ import annotations

import pytest
import warnings
from sqlglot import exp, parse_one

from yads.exceptions import ValidationRuleError
from yads.validator.core import AstValidator, Rule


class MockRule(Rule):
    def validate(self, node: exp.Expression) -> str | None:
        if isinstance(node, exp.DataType) and node.this == exp.DataType.Type.TEXT:
            return "TEXT type is not allowed."
        return None

    def adjust(self, node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.DataType) and node.this == exp.DataType.Type.TEXT:
            node.set("this", exp.DataType.Type.VARCHAR)
        return node

    @property
    def adjustment_description(self) -> str:
        return "It will be converted to VARCHAR."


@pytest.fixture
def ast_validator() -> AstValidator:
    return AstValidator(rules=[MockRule()])


@pytest.fixture
def create_table_ast() -> exp.Create:
    sql = "CREATE TABLE my_table (col_a INT, col_b TEXT)"
    ast = parse_one(sql)
    assert isinstance(ast, exp.Create)
    return ast


class TestAstValidator:
    def test_validate_strict_mode_raises_error(
        self, ast_validator: AstValidator, create_table_ast: exp.Create
    ):
        with pytest.raises(ValidationRuleError) as excinfo:
            ast_validator.validate(create_table_ast, mode="strict")
        assert "TEXT type is not allowed." in str(excinfo.value)

    def test_validate_fix_mode_adjusts_ast_and_warns(
        self, ast_validator: AstValidator, create_table_ast: exp.Create
    ):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processed_ast = ast_validator.validate(create_table_ast, mode="fix")

            assert len(w) == 1
            assert issubclass(w[-1].category, UserWarning)
            assert "TEXT type is not allowed." in str(w[-1].message)
            assert "It will be converted to VARCHAR." in str(w[-1].message)

        text_nodes = [
            node
            for node in processed_ast.find_all(exp.DataType)
            if node.this == exp.DataType.Type.TEXT
        ]
        assert not text_nodes

    def test_validate_warn_mode_warns_without_adjusting(
        self, ast_validator: AstValidator, create_table_ast: exp.Create
    ):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processed_ast = ast_validator.validate(create_table_ast, mode="warn")

            assert len(w) == 1
            assert issubclass(w[-1].category, UserWarning)
            assert "TEXT type is not allowed." in str(w[-1].message)
            assert "Set mode to 'fix' to automatically adjust the AST." in str(
                w[-1].message
            )

        text_nodes = [
            node
            for node in processed_ast.find_all(exp.DataType)
            if node.this == exp.DataType.Type.TEXT
        ]
        assert text_nodes

    def test_validate_invalid_mode_raises_error(
        self, ast_validator: AstValidator, create_table_ast: exp.Create
    ):
        with pytest.raises(ValidationRuleError) as excinfo:
            ast_validator.validate(create_table_ast, mode="invalid_mode")  # type: ignore
        assert "Invalid mode: invalid_mode" in str(excinfo.value)

    def test_validate_with_no_errors(self, ast_validator: AstValidator):
        sql = "CREATE TABLE my_table (col_a INT, col_b VARCHAR)"
        ast = parse_one(sql)
        assert isinstance(ast, exp.Create)

        processed_ast = ast_validator.validate(ast, mode="strict")
        assert processed_ast.sql() == ast.sql()
