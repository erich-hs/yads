from __future__ import annotations

from typing import Any, Type

import pytest

from yads.constraints import (
    BaseConstraint,
    DefaultConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)


@pytest.mark.parametrize(
    ("constraint", "expected_repr"),
    [
        (NotNullConstraint(), "NotNullConstraint()"),
        (NotNullConstraint(name="pk_col"), "NotNullConstraint(name='pk_col')"),
        (PrimaryKeyConstraint(), "PrimaryKeyConstraint()"),
        (
            PrimaryKeyConstraint(name="pk_col"),
            "PrimaryKeyConstraint(name='pk_col')",
        ),
        (DefaultConstraint(value=1), "DefaultConstraint(value=1)"),
        (
            DefaultConstraint(value=1, name="default_one"),
            "DefaultConstraint(value=1, name='default_one')",
        ),
    ],
)
def test_constraint_repr(constraint: BaseConstraint, expected_repr: str) -> None:
    """Tests the __repr__ of constraint objects."""
    assert repr(constraint) == expected_repr


@pytest.mark.parametrize(
    ("constraint_class", "init_args", "expected_attrs"),
    [
        (NotNullConstraint, {}, {"name": None}),
        (NotNullConstraint, {"name": "not_null_con"}, {"name": "not_null_con"}),
        (PrimaryKeyConstraint, {}, {"name": None}),
        (PrimaryKeyConstraint, {"name": "pk_con"}, {"name": "pk_con"}),
        (DefaultConstraint, {"value": 123}, {"value": 123, "name": None}),
        (
            DefaultConstraint,
            {"value": "abc", "name": "default_con"},
            {"value": "abc", "name": "default_con"},
        ),
    ],
)
def test_constraint_attributes(
    constraint_class: Type[BaseConstraint],
    init_args: dict[str, Any],
    expected_attrs: dict[str, Any],
) -> None:
    """Tests that constraint attributes are correctly set on initialization."""
    constraint = constraint_class(**init_args)
    for attr_name, expected_value in expected_attrs.items():
        assert getattr(constraint, attr_name) == expected_value
