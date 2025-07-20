from __future__ import annotations

from typing import Any, Type

import pytest

from yads.constraints import (
    CONSTRAINT_EQUIVALENTS,
    ColumnConstraint,
    DefaultConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    TableConstraint,
)


@pytest.mark.parametrize(
    ("constraint", "expected_str"),
    [
        (NotNullConstraint(), "NotNullConstraint()"),
        (PrimaryKeyConstraint(), "PrimaryKeyConstraint()"),
        (DefaultConstraint(value=1), "DefaultConstraint(value=1)"),
        (
            PrimaryKeyTableConstraint(columns=["id", "name"], name="pk_table"),
            "PrimaryKeyTableConstraint(\n  name='pk_table',\n  columns=[\n    'id',\n    'name'\n  ]\n)",
        ),
        (
            PrimaryKeyTableConstraint(columns=["id"]),
            "PrimaryKeyTableConstraint(\n  columns=[\n    'id'\n  ]\n)",
        ),
    ],
)
def test_constraint_str(
    constraint: ColumnConstraint | TableConstraint, expected_str: str
) -> None:
    """Tests the __str__ of constraint objects."""
    assert str(constraint) == expected_str


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
    constraint_class: Type[ColumnConstraint | TableConstraint],
    init_args: dict[str, Any],
    expected_attrs: dict[str, Any],
) -> None:
    """Tests that constraint attributes are correctly set on initialization."""
    constraint = constraint_class(**init_args)
    for attr_name, expected_value in expected_attrs.items():
        assert getattr(constraint, attr_name) == expected_value


def test_get_constrained_columns():
    """Tests that get_constrained_columns returns the correct columns."""
    constraint = PrimaryKeyTableConstraint(columns=["id", "name"], name="pk")
    assert constraint.get_constrained_columns() == ["id", "name"]


def test_constraint_equivalents():
    """Tests the constraint equivalents mapping."""
    assert CONSTRAINT_EQUIVALENTS == {PrimaryKeyConstraint: PrimaryKeyTableConstraint}
