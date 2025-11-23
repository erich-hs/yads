import pytest

from yads.constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyReference,
    ForeignKeyTableConstraint,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
)
from yads.exceptions import InvalidConstraintError, SpecParsingError
from yads.serializers import ConstraintDeserializer, ConstraintSerializer


class TestConstraintSerializer:
    def test_column_constraints(self):
        serializer = ConstraintSerializer()
        constraints = [
            NotNullConstraint(),
            PrimaryKeyConstraint(),
            DefaultConstraint(value="guest"),
            IdentityConstraint(always=False, start=10, increment=5),
            ForeignKeyConstraint(
                name="fk_user_profile",
                references=ForeignKeyReference(table="profiles", columns=["id"]),
            ),
        ]

        payload = serializer.serialize_column_constraints(constraints)

        assert payload["not_null"] is True
        assert payload["primary_key"] is True
        assert payload["default"] == "guest"
        assert payload["identity"] == {"always": False, "start": 10, "increment": 5}
        assert payload["foreign_key"] == {
            "name": "fk_user_profile",
            "references": {"table": "profiles", "columns": ["id"]},
        }

    def test_table_constraints(self):
        serializer = ConstraintSerializer()
        constraints = [
            PrimaryKeyTableConstraint(columns=["id"], name="pk_users"),
            ForeignKeyTableConstraint(
                columns=["profile_id"],
                name="fk_profile",
                references=ForeignKeyReference(table="profiles", columns=["id"]),
            ),
        ]

        payload = serializer.serialize_table_constraints(constraints)

        assert payload == [
            {"type": "primary_key", "name": "pk_users", "columns": ["id"]},
            {
                "type": "foreign_key",
                "name": "fk_profile",
                "columns": ["profile_id"],
                "references": {"table": "profiles", "columns": ["id"]},
            },
        ]


class TestConstraintDeserializerColumnConstraints:
    def setup_method(self) -> None:
        self.deserializer = ConstraintDeserializer()

    def _parse(self, data: object) -> list[object]:
        return self.deserializer.parse_column_constraints(data)

    def test_not_null_constraint_with_non_boolean_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="The 'not_null' constraint expects a boolean",
        ):
            self._parse({"not_null": "true"})

    def test_primary_key_constraint_with_non_boolean_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="The 'primary_key' constraint expects a boolean",
        ):
            self._parse({"primary_key": "true"})

    def test_foreign_key_constraint_with_non_dict_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="The 'foreign_key' constraint expects a dictionary",
        ):
            self._parse({"foreign_key": "table"})

    def test_foreign_key_constraint_missing_references_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="The 'foreign_key' constraint must specify 'references'",
        ):
            self._parse({"foreign_key": {"name": "fk_test"}})

    def test_identity_constraint_with_non_dict_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="The 'identity' constraint expects a dictionary",
        ):
            self._parse({"identity": True})

    def test_default_constraint_deserialization(self):
        constraints = self._parse({"default": "test_value"})
        default_constraints = [c for c in constraints if isinstance(c, DefaultConstraint)]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == "test_value"

    def test_identity_constraint_deserialization(self):
        constraints = self._parse(
            {"identity": {"always": False, "start": 10, "increment": 2}}
        )
        identity_constraints = [
            c for c in constraints if isinstance(c, IdentityConstraint)
        ]
        assert len(identity_constraints) == 1
        identity = identity_constraints[0]
        assert identity.always is False
        assert identity.start == 10
        assert identity.increment == 2

    def test_identity_constraint_with_negative_increment(self):
        constraints = self._parse(
            {"identity": {"always": False, "start": 10, "increment": -2}}
        )
        identity = next(c for c in constraints if isinstance(c, IdentityConstraint))
        assert identity.always is False
        assert identity.start == 10
        assert identity.increment == -2

    def test_foreign_key_constraint_deserialization(self):
        constraints = self._parse(
            {
                "foreign_key": {
                    "name": "fk_test",
                    "references": {"table": "other_table", "columns": ["id"]},
                }
            }
        )
        fk_constraint = next(
            c for c in constraints if isinstance(c, ForeignKeyConstraint)
        )
        assert fk_constraint.name == "fk_test"
        assert fk_constraint.references.table == "other_table"
        assert fk_constraint.references.columns == ["id"]

    def test_constraints_attribute_as_list_raises_error(self):
        with pytest.raises(
            SpecParsingError,
            match=r"The 'constraints' attribute of a column must be a dictionary",
        ):
            self._parse([{"primary_key": True}])

    def test_not_null_constraint_false_creates_no_constraint(self):
        constraints = self._parse({"not_null": False})
        assert all(not isinstance(c, NotNullConstraint) for c in constraints)

    def test_not_null_constraint_true_creates_constraint(self):
        constraints = self._parse({"not_null": True})
        not_null_constraints = [
            c for c in constraints if isinstance(c, NotNullConstraint)
        ]
        assert len(not_null_constraints) == 1

    def test_primary_key_constraint_false_creates_no_constraint(self):
        constraints = self._parse({"primary_key": False})
        assert all(not isinstance(c, PrimaryKeyConstraint) for c in constraints)

    def test_primary_key_constraint_true_creates_constraint(self):
        constraints = self._parse({"primary_key": True})
        pk_constraints = [c for c in constraints if isinstance(c, PrimaryKeyConstraint)]
        assert len(pk_constraints) == 1

    def test_multiple_boolean_constraints_mixed_values(self):
        constraints = self._parse({"not_null": True, "primary_key": False})
        assert any(isinstance(c, NotNullConstraint) for c in constraints)
        assert all(not isinstance(c, PrimaryKeyConstraint) for c in constraints)

    def test_boolean_constraints_with_other_constraints(self):
        constraints = self._parse(
            {"not_null": False, "default": "test_value", "primary_key": True}
        )
        assert all(not isinstance(c, NotNullConstraint) for c in constraints)
        assert any(isinstance(c, PrimaryKeyConstraint) for c in constraints)
        default_constraint = next(
            c for c in constraints if isinstance(c, DefaultConstraint)
        )
        assert default_constraint.value == "test_value"


class TestConstraintDeserializerTableConstraints:
    def setup_method(self) -> None:
        self.deserializer = ConstraintDeserializer()

    def _parse(self, data: object) -> list[object]:
        return self.deserializer.parse_table_constraints(data)

    def test_primary_key_table_constraint_missing_columns_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="Primary key table constraint must specify 'columns'",
        ):
            self._parse([{"type": "primary_key", "name": "pk_test"}])

    def test_primary_key_table_constraint_with_no_name_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="Primary key table constraint must specify 'name'",
        ):
            self._parse([{"type": "primary_key", "columns": ["col1"]}])

    def test_foreign_key_table_constraint_missing_columns_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="Foreign key table constraint must specify 'columns'",
        ):
            self._parse(
                [
                    {
                        "type": "foreign_key",
                        "name": "fk_test",
                        "references": {"table": "other_table"},
                    }
                ]
            )

    def test_foreign_key_table_constraint_with_no_name_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="Foreign key table constraint must specify 'name'",
        ):
            self._parse(
                [
                    {
                        "type": "foreign_key",
                        "columns": ["col1"],
                        "references": {"table": "other_table"},
                    }
                ]
            )

    def test_foreign_key_table_constraint_missing_references_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="Foreign key table constraint must specify 'references'",
        ):
            self._parse([{"type": "foreign_key", "name": "fk_test", "columns": ["col1"]}])

    def test_table_constraint_missing_type_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match="Table constraint definition must have a 'type'",
        ):
            self._parse([{"name": "test_constraint", "columns": ["col1"]}])

    def test_foreign_key_references_missing_table_raises_error(self):
        with pytest.raises(
            InvalidConstraintError,
            match=(
                "The 'references' of a foreign key must be a dictionary with a 'table' key"
            ),
        ):
            self._parse(
                [
                    {
                        "type": "foreign_key",
                        "name": "fk_test",
                        "columns": ["col1"],
                        "references": {"columns": ["id"]},
                    }
                ]
            )

    def test_valid_primary_key_table_constraint_parsing(self):
        constraints = self._parse(
            [
                {"type": "primary_key", "name": "pk_test", "columns": ["col1", "col2"]},
            ]
        )
        assert len(constraints) == 1
        pk_constraint = constraints[0]
        assert isinstance(pk_constraint, PrimaryKeyTableConstraint)
        assert pk_constraint.name == "pk_test"
        assert pk_constraint.columns == ["col1", "col2"]

    def test_valid_foreign_key_table_constraint_parsing(self):
        constraints = self._parse(
            [
                {
                    "type": "foreign_key",
                    "name": "fk_test",
                    "columns": ["col1"],
                    "references": {"table": "other_table", "columns": ["id"]},
                }
            ]
        )
        assert len(constraints) == 1
        fk_constraint = constraints[0]
        assert isinstance(fk_constraint, ForeignKeyTableConstraint)
        assert fk_constraint.name == "fk_test"
        assert fk_constraint.columns == ["col1"]
        assert fk_constraint.references.table == "other_table"
        assert fk_constraint.references.columns == ["id"]
