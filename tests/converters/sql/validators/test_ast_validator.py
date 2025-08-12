from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import warnings
from sqlglot import parse_one
from sqlglot.expressions import DataType, Create

from yads.exceptions import AstValidationError
from yads.converters.sql.validators.ast_validator import AstValidator
from yads.converters.sql.validators.ast_validation_rules import AstValidationRule

if TYPE_CHECKING:
    from sqlglot.expressions import Expression, Create


# ==========================================================
# AstValidator tests
# Scope: rule application across modes: raise, warn, ignore
# ==========================================================


# %% Mocks
class MockRule(AstValidationRule):
    def validate(self, node: Expression) -> str | None:
        if isinstance(node, DataType) and node.this == DataType.Type.TEXT:
            return "TEXT type is not allowed."
        return None

    def adjust(self, node: Expression) -> Expression:
        if isinstance(node, DataType) and node.this == DataType.Type.TEXT:
            node.set("this", DataType.Type.VARCHAR)
        return node

    @property
    def adjustment_description(self) -> str:
        return "It will be converted to VARCHAR."


# %% Fixtures
@pytest.fixture
def ast_validator() -> AstValidator:
    return AstValidator(rules=[MockRule()])


@pytest.fixture
def create_table_ast() -> Create:
    sql = "CREATE TABLE my_table (col_a INT, col_b TEXT)"
    ast = parse_one(sql)
    assert isinstance(ast, Create)
    return ast


# %% Validation modes
class TestAstValidator:
    def test_validate_raise_mode_raises_error(
        self, ast_validator: AstValidator, create_table_ast: Create
    ):
        with pytest.raises(AstValidationError) as excinfo:
            ast_validator.validate(create_table_ast, mode="raise")
        assert "TEXT type is not allowed." in str(excinfo.value)

    def test_validate_warn_mode_adjusts_ast_and_warns(
        self, ast_validator: AstValidator, create_table_ast: Create
    ):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processed_ast = ast_validator.validate(create_table_ast, mode="warn")

            assert len(w) == 1
            assert issubclass(w[-1].category, UserWarning)
            assert "TEXT type is not allowed." in str(w[-1].message)
            assert "It will be converted to VARCHAR." in str(w[-1].message)

        text_nodes = [
            node
            for node in processed_ast.find_all(DataType)
            if node.this == DataType.Type.TEXT
        ]
        assert not text_nodes

    def test_validate_ignore_mode_does_nothing(
        self, ast_validator: AstValidator, create_table_ast: Create
    ):
        processed_ast = ast_validator.validate(create_table_ast, mode="ignore")
        text_nodes = [
            node
            for node in processed_ast.find_all(DataType)
            if node.this == DataType.Type.TEXT
        ]
        assert text_nodes

    def test_validate_invalid_mode_raises_error(
        self, ast_validator: AstValidator, create_table_ast: Create
    ):
        with pytest.raises(AstValidationError) as excinfo:
            ast_validator.validate(create_table_ast, mode="invalid_mode")  # type: ignore
        assert "Invalid mode: invalid_mode" in str(excinfo.value)

    def test_validate_with_no_errors(self, ast_validator: AstValidator):
        sql = "CREATE TABLE my_table (col_a INT, col_b VARCHAR)"
        ast = parse_one(sql)
        assert isinstance(ast, Create)

        processed_ast = ast_validator.validate(ast, mode="raise")
        assert processed_ast.sql() == ast.sql()
