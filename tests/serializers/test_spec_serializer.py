from yads import spec
from yads.serializers import SpecSerializer


def _build_complex_spec_dict() -> dict:
    return {
        "name": "catalog.db.users",
        "version": 2,
        "yads_spec_version": "0.0.2",
        "description": "Serialized spec",
        "metadata": {"team": "data-eng"},
        "storage": {
            "format": "delta",
            "location": "s3://bucket/users",
            "tbl_properties": {"delta.appendOnly": "true"},
        },
        "columns": [
            {
                "name": "id",
                "type": "integer",
                "constraints": {"not_null": True},
            },
            {
                "name": "profile",
                "type": "struct",
                "fields": [
                    {
                        "name": "username",
                        "type": "string",
                        "constraints": {"not_null": True},
                    },
                    {
                        "name": "attributes",
                        "type": "map",
                        "key": {"type": "string"},
                        "value": {"type": "string"},
                    },
                ],
            },
            {"name": "created_at", "type": "timestamp"},
            {
                "name": "created_date",
                "type": "date",
                "generated_as": {"column": "created_at", "transform": "date"},
            },
        ],
        "partitioned_by": [{"column": "created_date"}],
        "table_constraints": [
            {"type": "primary_key", "name": "pk_users", "columns": ["id"]}
        ],
    }


class TestSpecSerializer:
    def test_roundtrip_dict(self):
        original = _build_complex_spec_dict()
        parsed_spec = spec.from_dict(original)

        serializer = SpecSerializer()
        serialized = serializer.serialize(parsed_spec)

        assert serialized == original
        assert spec.from_dict(serialized) == parsed_spec

    def test_yads_spec_to_dict(self):
        spec_dict = _build_complex_spec_dict()
        parsed_spec = spec.from_dict(spec_dict)

        assert parsed_spec.to_dict() == spec_dict
