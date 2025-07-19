from __future__ import annotations

import pytest

from yads.constraints import (
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
def test_constraint_repr(constraint, expected_repr):
    assert repr(constraint) == expected_repr
