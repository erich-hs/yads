from __future__ import annotations

from typing import Any, Type

import pytest

from yads.constraints import (
    BaseConstraint,
    DefaultConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    TableConstraint,
)


@pytest.mark.parametrize(
    ("constraint", "expected_repr"),
    [
        (NotNullConstraint(), "NotNullConstraint()"),
        (PrimaryKeyConstraint(), "PrimaryKeyConstraint()"),
        (DefaultConstraint(value=1), "DefaultConstraint(value=1)"),
        (
            PrimaryKeyTableConstraint(columns=["id", "name"], name="pk_table"),
            "PrimaryKeyTableConstraint(columns=['id', 'name'], name='pk_table')",
        ),
        (
            PrimaryKeyTableConstraint(columns=["id"]),
            "PrimaryKeyTableConstraint(columns=['id'], name=None)",
        ),
    ],
)
def test_constraint_repr(
    constraint: BaseConstraint | TableConstraint, expected_repr: str
) -> None:
    """Tests the __repr__ of constraint objects."""
    assert repr(constraint) == expected_repr


@pytest.mark.parametrize(
    ("constraint_class", "init_args", "expected_attrs"),
    [
        (DefaultConstraint, {"value": 123}, {"value": 123}),
        (DefaultConstraint, {"value": "abc"}, {"value": "abc"}),
        (
            PrimaryKeyTableConstraint,
            {"columns": ["id", "name"], "name": "pk_con"},
            {"columns": ["id", "name"], "name": "pk_con"},
        ),
    ],
)
def test_constraint_attributes(
    constraint_class: Type[BaseConstraint | TableConstraint],
    init_args: dict[str, Any],
    expected_attrs: dict[str, Any],
) -> None:
    """Tests that constraint attributes are correctly set on initialization."""
    constraint = constraint_class(**init_args)
    for attr_name, expected_value in expected_attrs.items():
        assert getattr(constraint, attr_name) == expected_value
