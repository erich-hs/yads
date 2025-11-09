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


# %% Mocks
class DisallowTextTypeRule(AstValidationRule):
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


class DisallowIntTypeRule(AstValidationRule):
    def validate(self, node: Expression) -> str | None:
        if isinstance(node, DataType) and node.this == DataType.Type.INT:
            return "INT type is not allowed."
        return None

    def adjust(self, node: Expression) -> Expression:
        if isinstance(node, DataType) and node.this == DataType.Type.INT:
            node.set("this", DataType.Type.BIGINT)
        return node

    @property
    def adjustment_description(self) -> str:
        return "It will be converted to BIGINT."


# %% Fixtures
@pytest.fixture
def ast_validator() -> AstValidator:
    return AstValidator(rules=[DisallowTextTypeRule()])


@pytest.fixture
def ast_validator_two_rules() -> AstValidator:
    return AstValidator(rules=[DisallowTextTypeRule(), DisallowIntTypeRule()])


@pytest.fixture
def parse_sql():
    def _parse(sql: str) -> Create:
        ast = parse_one(sql)
        assert isinstance(ast, Create)
        return ast

    return _parse


@pytest.fixture
def sql_one_text_violation() -> str:
    return "CREATE TABLE my_table (col_a INT, col_b TEXT)"


@pytest.fixture
def sql_two_text_violations() -> str:
    return "CREATE TABLE my_table (a TEXT, b TEXT, c INT)"


@pytest.fixture
def sql_int_and_text_violations() -> str:
    return "CREATE TABLE my_table (a INT, b TEXT, c VARCHAR)"


@pytest.fixture
def sql_no_violations() -> str:
    return "CREATE TABLE my_table (col_a INT, col_b VARCHAR)"


# %% Validation modes
class TestAstValidator:
    def test_validate_raise_mode_raises_error(
        self, ast_validator: AstValidator, parse_sql, sql_one_text_violation: str
    ):
        create_table_ast = parse_sql(sql_one_text_violation)
        with pytest.raises(AstValidationError) as excinfo:
            ast_validator.validate(create_table_ast, mode="raise")
        assert "TEXT type is not allowed." in str(excinfo.value)

    def test_validate_coerce_mode_adjusts_ast_and_warns(
        self, ast_validator: AstValidator, parse_sql, sql_one_text_violation: str
    ):
        create_table_ast = parse_sql(sql_one_text_violation)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processed_ast = ast_validator.validate(create_table_ast, mode="coerce")

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

    def test_validate_invalid_mode_raises_error(
        self, ast_validator: AstValidator, parse_sql, sql_one_text_violation: str
    ):
        create_table_ast = parse_sql(sql_one_text_violation)
        with pytest.raises(AstValidationError) as excinfo:
            ast_validator.validate(create_table_ast, mode="invalid_mode")  # type: ignore
        assert "Invalid mode: invalid_mode" in str(excinfo.value)

    def test_validate_with_no_errors(
        self, ast_validator: AstValidator, parse_sql, sql_no_violations: str
    ):
        ast = parse_sql(sql_no_violations)
        processed_ast = ast_validator.validate(ast, mode="raise")
        assert processed_ast.sql() == ast.sql()

    def test_coerce_mode_multiple_same_rule_occurrences(
        self, ast_validator: AstValidator, parse_sql, sql_two_text_violations: str
    ):
        ast = parse_sql(sql_two_text_violations)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processed_ast = ast_validator.validate(ast, mode="coerce")

            assert len(w) == 2
            for warn in w:
                assert issubclass(warn.category, UserWarning)
                assert "TEXT type is not allowed." in str(warn.message)
                assert "It will be converted to VARCHAR." in str(warn.message)

        text_nodes = [
            node
            for node in processed_ast.find_all(DataType)
            if node.this == DataType.Type.TEXT
        ]
        assert not text_nodes

    def test_coerce_mode_multiple_distinct_rules(
        self,
        ast_validator_two_rules: AstValidator,
        parse_sql,
        sql_int_and_text_violations: str,
    ):
        validator = ast_validator_two_rules
        ast = parse_sql(sql_int_and_text_violations)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processed_ast = validator.validate(ast, mode="coerce")

            assert len(w) == 2
            messages = [str(warn.message) for warn in w]
            assert any("TEXT type is not allowed." in m for m in messages)
            assert any("INT type is not allowed." in m for m in messages)
            assert any("It will be converted to VARCHAR." in m for m in messages)
            assert any("It will be converted to BIGINT." in m for m in messages)

        # Ensure both violations were adjusted
        int_nodes = [
            node
            for node in processed_ast.find_all(DataType)
            if node.this == DataType.Type.INT
        ]
        text_nodes = [
            node
            for node in processed_ast.find_all(DataType)
            if node.this == DataType.Type.TEXT
        ]
        assert not int_nodes
        assert not text_nodes

    def test_no_violations_no_raise_or_warning(
        self, ast_validator: AstValidator, parse_sql, sql_no_violations: str
    ):
        # raise mode: no exception
        ast1 = parse_sql(sql_no_violations)
        processed_raise = ast_validator.validate(ast1, mode="raise")
        assert processed_raise.sql() == ast1.sql()

        # coerce mode: no warnings and unchanged AST
        ast2 = parse_sql(sql_no_violations)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processed_warn = ast_validator.validate(ast2, mode="coerce")
            assert len(w) == 0
        assert processed_warn.sql() == ast2.sql()
