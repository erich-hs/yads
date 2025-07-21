from __future__ import annotations


from yads.constraints import (
    CONSTRAINT_EQUIVALENTS,
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    Reference,
)


def test_not_null_constraint_str():
    """Tests the __str__ of NotNullConstraint."""
    assert str(NotNullConstraint()) == "NotNullConstraint()"


def test_primary_key_constraint_str():
    """Tests the __str__ of PrimaryKeyConstraint."""
    assert str(PrimaryKeyConstraint()) == "PrimaryKeyConstraint()"


def test_default_constraint_str():
    """Tests the __str__ of DefaultConstraint."""
    assert str(DefaultConstraint(value=1)) == "DefaultConstraint(value=1)"


def test_foreign_key_constraint_str():
    """Tests the __str__ of ForeignKeyConstraint."""
    constraint = ForeignKeyConstraint(
        name="fk_col", references=Reference(table="ref_table", columns=["id"])
    )
    assert (
        str(constraint)
        == "ForeignKeyConstraint(name='fk_col', references=ref_table(id))"
    )


def test_primary_key_table_constraint_str():
    """Tests the __str__ of PrimaryKeyTableConstraint."""
    constraint = PrimaryKeyTableConstraint(columns=["id", "name"], name="pk_table")
    expected_str = "PrimaryKeyTableConstraint(\n  name='pk_table',\n  columns=[\n    'id',\n    'name'\n  ]\n)"
    assert str(constraint) == expected_str

    constraint_no_name = PrimaryKeyTableConstraint(columns=["id"])
    expected_str_no_name = "PrimaryKeyTableConstraint(\n  columns=[\n    'id'\n  ]\n)"
    assert str(constraint_no_name) == expected_str_no_name


def test_foreign_key_table_constraint_str():
    """Tests the __str__ of ForeignKeyTableConstraint."""
    constraint = ForeignKeyTableConstraint(
        columns=["col1", "col2"],
        name="fk_table",
        references=Reference(table="ref_table", columns=["ref1", "ref2"]),
    )
    expected_str = "ForeignKeyTableConstraint(\n  name='fk_table',\n  columns=[\n    'col1',\n    'col2'\n  ],\n  references=ref_table(ref1, ref2)\n)"
    assert str(constraint) == expected_str


def test_default_constraint_attributes():
    """Tests that DefaultConstraint attributes are correctly set."""
    constraint = DefaultConstraint(value=123)
    assert constraint.value == 123

    constraint = DefaultConstraint(value="abc")
    assert constraint.value == "abc"


def test_primary_key_table_constraint_attributes():
    """Tests that PrimaryKeyTableConstraint attributes are correctly set."""
    constraint = PrimaryKeyTableConstraint(columns=["id", "name"], name="pk_con")
    assert constraint.columns == ["id", "name"]
    assert constraint.name == "pk_con"


def test_foreign_key_constraint_attributes():
    """Tests that ForeignKeyConstraint attributes are correctly set."""
    references = Reference(table="ref_table", columns=["id"])
    constraint = ForeignKeyConstraint(name="fk_col", references=references)
    assert constraint.name == "fk_col"
    assert constraint.references == references


def test_foreign_key_table_constraint_attributes():
    """Tests that ForeignKeyTableConstraint attributes are correctly set."""
    references = Reference(table="ref_table", columns=["ref1", "ref2"])
    constraint = ForeignKeyTableConstraint(
        name="fk_table", columns=["col1", "col2"], references=references
    )
    assert constraint.name == "fk_table"
    assert constraint.columns == ["col1", "col2"]
    assert constraint.references == references


def test_pk_table_constraint_get_constrained_columns():
    """Tests that get_constrained_columns returns the correct columns for a PK."""
    pk_constraint = PrimaryKeyTableConstraint(columns=["id", "name"], name="pk")
    assert pk_constraint.get_constrained_columns() == ["id", "name"]


def test_fk_table_constraint_get_constrained_columns():
    """Tests that get_constrained_columns returns the correct columns for a FK."""
    fk_constraint = ForeignKeyTableConstraint(
        columns=["col1", "col2"], name="fk", references=Reference(table="ref")
    )
    assert fk_constraint.get_constrained_columns() == ["col1", "col2"]


def test_constraint_equivalents():
    """Tests the constraint equivalents mapping."""
    assert CONSTRAINT_EQUIVALENTS == {
        PrimaryKeyConstraint: PrimaryKeyTableConstraint,
        ForeignKeyConstraint: ForeignKeyTableConstraint,
    }
