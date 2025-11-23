from yads import spec, types
from yads.serializers import TypeSerializer


def _make_type_serializer() -> TypeSerializer:
    serializer = TypeSerializer()

    def _field_serializer(field: spec.Field) -> dict:
        payload: dict[str, object] = {"name": field.name}
        payload.update(serializer.serialize(field.type))
        if field.description:
            payload["description"] = field.description
        if field.metadata:
            payload["metadata"] = dict(field.metadata)
        return payload

    serializer.bind_field_serializer(_field_serializer)
    return serializer


class TestTypeSerializer:
    def test_integer_alias_without_params(self):
        serializer = _make_type_serializer()
        payload = serializer.serialize(types.Integer())

        assert payload == {"type": "integer"}

    def test_integer_with_non_default_params(self):
        serializer = _make_type_serializer()
        payload = serializer.serialize(types.Integer(bits=16, signed=False))

        assert payload["type"] == "integer"
        assert payload["params"] == {"bits": 16, "signed": False}

    def test_struct_fields_use_bound_serializer(self):
        serializer = _make_type_serializer()
        struct_type = types.Struct(
            fields=[
                spec.Field(
                    name="inner",
                    type=types.String(length=32),
                    description="nested",
                    metadata={"source": "test"},
                )
            ]
        )

        payload = serializer.serialize(struct_type)

        assert payload["type"] == "struct"
        assert payload["fields"][0]["name"] == "inner"
        assert payload["fields"][0]["description"] == "nested"
        assert payload["fields"][0]["metadata"] == {"source": "test"}
        assert payload["fields"][0]["type"] == "string"
        assert payload["fields"][0]["params"] == {"length": 32}

    def test_map_and_tensor_types(self):
        serializer = _make_type_serializer()
        tensor_type = types.Tensor(element=types.Float(bits=32), shape=(2, 2))
        map_type = types.Map(
            key=types.String(),
            value=types.Array(element=tensor_type, size=5),
            keys_sorted=True,
        )

        payload = serializer.serialize(map_type)

        assert payload["type"] == "map"
        assert payload["params"] == {"keys_sorted": True}
        assert payload["key"] == {"type": "string"}
        assert payload["value"]["type"] == "array"
        assert payload["value"]["params"] == {"size": 5}
        assert payload["value"]["element"]["type"] == "tensor"
        assert payload["value"]["element"]["params"]["shape"] == [2, 2]
