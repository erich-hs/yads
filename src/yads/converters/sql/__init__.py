from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .sql_converter import (
        SQLConverter,
        SQLConverterConfig,
        SparkSQLConverter,
        DuckdbSQLConverter,
    )
    from .ast_converter import AstConverter, SQLGlotConverter, SQLGlotConverterConfig
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
    "SQLConverter",
    "SQLConverterConfig",
    "SparkSQLConverter",
    "DuckdbSQLConverter",
    "SQLGlotConverter",
    "SQLGlotConverterConfig",
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
        "SQLConverter",
        "SQLConverterConfig",
        "SparkSQLConverter",
        "DuckdbSQLConverter",
    ):
        from . import sql_converter

        return getattr(sql_converter, name)
    if name in ("AstConverter", "SQLGlotConverter", "SQLGlotConverterConfig"):
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
