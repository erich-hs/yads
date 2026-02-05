from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .sql_converter import (
        SqlConverter,
        SqlConverterConfig,
        SparkSqlConverter,
        DuckdbSqlConverter,
    )
    from .ast_converter import AstConverter, SqlglotConverter, SqlglotConverterConfig
    from .validators.ast_validator import AstValidator
    from .validators.ast_validation_rules import (
        AstValidationRule,
        DisallowType,
        DisallowUserDefinedType,
        DisallowFixedLengthString,
        DisallowFixedLengthBinary,
        DisallowNegativeScaleDecimal,
        DisallowParameterizedGeometry,
        DisallowColumnConstraintGeneratedIdentity,
        DisallowTableConstraintPrimaryKeyNullsFirst,
    )

__all__ = [
    "AstConverter",
    "SqlConverter",
    "SqlConverterConfig",
    "SparkSqlConverter",
    "DuckdbSqlConverter",
    "SqlglotConverter",
    "SqlglotConverterConfig",
    "AstValidator",
    "AstValidationRule",
    "DisallowType",
    "DisallowUserDefinedType",
    "DisallowFixedLengthString",
    "DisallowFixedLengthBinary",
    "DisallowNegativeScaleDecimal",
    "DisallowParameterizedGeometry",
    "DisallowColumnConstraintGeneratedIdentity",
    "DisallowTableConstraintPrimaryKeyNullsFirst",
]


def __getattr__(name: str) -> Any:
    """Lazy import SQL converters to avoid eager sqlglot dependency."""
    if name in (
        "SqlConverter",
        "SqlConverterConfig",
        "SparkSqlConverter",
        "DuckdbSqlConverter",
    ):
        from . import sql_converter

        return getattr(sql_converter, name)
    if name in ("AstConverter", "SqlglotConverter", "SqlglotConverterConfig"):
        from . import ast_converter

        return getattr(ast_converter, name)
    if name == "AstValidator":
        from .validators import ast_validator

        return getattr(ast_validator, name)
    if name in (
        "AstValidationRule",
        "DisallowType",
        "DisallowUserDefinedType",
        "DisallowFixedLengthString",
        "DisallowFixedLengthBinary",
        "DisallowNegativeScaleDecimal",
        "DisallowParameterizedGeometry",
        "DisallowColumnConstraintGeneratedIdentity",
        "DisallowTableConstraintPrimaryKeyNullsFirst",
    ):
        from .validators import ast_validation_rules

        return getattr(ast_validation_rules, name)
    raise AttributeError(name)
