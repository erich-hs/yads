import pytest
from yads.constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    ForeignKeyReference,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
)
from yads.exceptions import InvalidConstraintError


class TestForeignKeyReference:
    def test_reference_with_columns(self):
        ref = ForeignKeyReference(table="other_table", columns=["id1", "id2"])
        assert ref.table == "other_table"
        assert ref.columns == ["id1", "id2"]
        assert str(ref) == "other_table(id1, id2)"

    def test_reference_without_columns(self):
        ref = ForeignKeyReference(table="other_table")
        assert ref.table == "other_table"
        assert ref.columns is None
        assert str(ref) == "other_table"

    def test_reference_with_empty_columns_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="ForeignKeyReference 'columns' cannot be an empty list.",
        ):
            ForeignKeyReference(table="other_table", columns=[])


class TestColumnConstraints:
    def test_not_null_constraint(self):
        constraint = NotNullConstraint()
        assert str(constraint) == "NotNullConstraint()"

    def test_primary_key_constraint(self):
        constraint = PrimaryKeyConstraint()
        assert str(constraint) == "PrimaryKeyConstraint()"

    def test_default_constraint(self):
        constraint = DefaultConstraint(value="default_value")
        assert constraint.value == "default_value"
        assert str(constraint) == "DefaultConstraint(value='default_value')"

    def test_foreign_key_constraint(self):
        ref = ForeignKeyReference(table="other_table", columns=["id"])
        constraint = ForeignKeyConstraint(references=ref, name="fk_name")
        assert constraint.references == ref
        assert constraint.name == "fk_name"
        assert (
            str(constraint)
            == "ForeignKeyConstraint(name='fk_name', references=other_table(id))"
        )

    def test_foreign_key_constraint_without_name(self):
        ref = ForeignKeyReference(table="other_table", columns=["id"])
        constraint = ForeignKeyConstraint(references=ref)
        assert constraint.name is None
        assert str(constraint) == "ForeignKeyConstraint(references=other_table(id))"

    def test_identity_constraint_default(self):
        constraint = IdentityConstraint()
        assert constraint.always is True
        assert constraint.start is None
        assert constraint.increment is None

    def test_identity_constraint_custom(self):
        constraint = IdentityConstraint(always=False, start=100, increment=5)
        assert constraint.always is False
        assert constraint.start == 100
        assert constraint.increment == 5

    def test_identity_constraint_with_zero_increment_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="Identity 'increment' must be a non-zero integer",
        ):
            IdentityConstraint(increment=0)


class TestTableConstraints:
    def test_primary_key_table_constraint(self):
        constraint = PrimaryKeyTableConstraint(columns=["id1", "id2"], name="pk_name")
        assert constraint.columns == ["id1", "id2"]
        assert constraint.name == "pk_name"
        assert constraint.constrained_columns == ["id1", "id2"]
        expected_str = (
            "PrimaryKeyTableConstraint(\n"
            "  name='pk_name',\n"
            "  columns=[\n"
            "    'id1',\n"
            "    'id2'\n"
            "  ]\n"
            ")"
        )
        assert str(constraint) == expected_str

    def test_primary_key_table_constraint_without_name(self):
        constraint = PrimaryKeyTableConstraint(columns=["id"])
        assert constraint.name is None
        assert constraint.constrained_columns == ["id"]
        expected_str = "PrimaryKeyTableConstraint(\n  columns=[\n    'id'\n  ]\n)"
        assert str(constraint) == expected_str

    def test_primary_key_table_constraint_with_empty_columns_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="PrimaryKeyTableConstraint 'columns' cannot be empty.",
        ):
            PrimaryKeyTableConstraint(columns=[])

    def test_foreign_key_table_constraint(self):
        ref = ForeignKeyReference(table="other_table", columns=["ref_id1", "ref_id2"])
        constraint = ForeignKeyTableConstraint(
            columns=["id1", "id2"], references=ref, name="fk_name"
        )
        assert constraint.columns == ["id1", "id2"]
        assert constraint.references == ref
        assert constraint.name == "fk_name"
        assert constraint.constrained_columns == ["id1", "id2"]
        expected_str = (
            "ForeignKeyTableConstraint(\n"
            "  name='fk_name',\n"
            "  columns=[\n"
            "    'id1',\n"
            "    'id2'\n"
            "  ],\n"
            "  references=other_table(ref_id1, ref_id2)\n"
            ")"
        )
        assert str(constraint) == expected_str

    def test_foreign_key_table_constraint_without_name(self):
        ref = ForeignKeyReference(table="other_table", columns=["ref_id"])
        constraint = ForeignKeyTableConstraint(columns=["id"], references=ref)
        assert constraint.name is None
        assert constraint.constrained_columns == ["id"]
        expected_str = (
            "ForeignKeyTableConstraint(\n"
            "  columns=[\n"
            "    'id'\n"
            "  ],\n"
            "  references=other_table(ref_id)\n"
            ")"
        )
        assert str(constraint) == expected_str

    def test_foreign_key_table_constraint_with_empty_columns_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="ForeignKeyTableConstraint 'columns' cannot be empty.",
        ):
            ForeignKeyTableConstraint(
                columns=[], references=ForeignKeyReference(table="t")
            )

    def test_foreign_key_table_constraint_with_mismatched_columns_raises_error(self):
        with pytest.raises(
            InvalidConstraintError, match="must match the number of referenced columns"
        ):
            ForeignKeyTableConstraint(
                columns=["a"],
                references=ForeignKeyReference(table="t", columns=["b", "c"]),
            )


def test_constraint_equivalents():
    """Tests that the constraint equivalents are correctly mapped."""
    from yads.constraints import (
        CONSTRAINT_EQUIVALENTS,
        ForeignKeyConstraint,
        ForeignKeyTableConstraint,
        PrimaryKeyConstraint,
        PrimaryKeyTableConstraint,
    )

    assert CONSTRAINT_EQUIVALENTS == {
        PrimaryKeyConstraint: PrimaryKeyTableConstraint,
        ForeignKeyConstraint: ForeignKeyTableConstraint,
    }
