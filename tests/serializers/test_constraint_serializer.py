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
from yads.serializers import ConstraintSerializer


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
