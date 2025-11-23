from dataclasses import dataclass, field

import pytest

from yads import spec, types
from yads.exceptions import SpecSerializationError, TypeDefinitionError
from yads.loaders import from_yaml_string
from yads.serializers import TypeDeserializer, TypeSerializer
from yads.types import YadsType


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

    def test_struct_serialization_requires_field_serializer(self):
        serializer = TypeSerializer(type_aliases={"struct": (types.Struct, {})})
        struct_type = types.Struct(fields=[spec.Field(name="a", type=types.String())])

        with pytest.raises(
            SpecSerializationError,
            match="Struct serialization requires a bound field serializer",
        ):
            serializer.serialize(struct_type)

    def test_collect_params_requires_dataclass_type(self):
        class CustomType(YadsType):
            pass

        serializer = TypeSerializer(type_aliases={"custom": (CustomType, {})})
        with pytest.raises(
            SpecSerializationError,
            match="Type CustomType must be a dataclass to be serialized",
        ):
            serializer.serialize(CustomType())

    def test_collect_params_respects_default_factory(self):
        @dataclass(frozen=True)
        class CustomDefaults(YadsType):
            values: tuple[int, ...] = field(default_factory=lambda: (1, 2))

        serializer = TypeSerializer(type_aliases={"custom": (CustomDefaults, {})})
        payload = serializer.serialize(CustomDefaults(values=(3, 4)))

        assert payload == {"type": "custom", "params": {"values": [3, 4]}}

    def test_alias_fallback_uses_first_alias_when_no_canonical(self):
        @dataclass(frozen=True)
        class WeirdName(YadsType):
            scale: int = 1

        serializer = TypeSerializer(type_aliases={"alias_one": (WeirdName, {})})
        payload = serializer.serialize(WeirdName(scale=2))

        assert payload["type"] == "alias_one"
        assert payload["params"] == {"scale": 2}


def test_unquoted_null_type_gives_helpful_error():
    content = """
name: test_spec
version: 1
columns:
  - name: col1
    type: null  # This will parse as None, not "null"
"""
    with pytest.raises(
        TypeDefinitionError,
        match=r"Use quoted \"null\" or the synonym 'void' instead to specify a void type",
    ):
        from_yaml_string(content)


class TestTypeDeserialization:
    def setup_method(self) -> None:
        self.deserializer = TypeDeserializer()

    def _parse(self, type_def: dict) -> types.YadsType:
        return self.deserializer.parse(
            type_def["type"],
            type_def,
            field_factory=self._field_factory,
        )

    def _field_factory(self, field_def: dict) -> spec.Field:
        return spec.Field(
            name=field_def["name"],
            type=self.deserializer.parse(
                field_def["type"], field_def, field_factory=self._field_factory
            ),
            description=field_def.get("description"),
            metadata=field_def.get("metadata") or {},
        )

    @pytest.mark.parametrize(
        "type_def, expected_type, expected_str",
        [
            # String types
            ({"type": "string"}, types.String(), "string"),
            (
                {"type": "string", "params": {"length": 255}},
                types.String(length=255),
                "string(length=255)",
            ),
            # Integer types
            ({"type": "int8"}, types.Integer(bits=8), "integer(bits=8)"),
            ({"type": "int16"}, types.Integer(bits=16), "integer(bits=16)"),
            ({"type": "int32"}, types.Integer(bits=32), "integer(bits=32)"),
            ({"type": "int64"}, types.Integer(bits=64), "integer(bits=64)"),
            # Integer unsigned via params
            (
                {"type": "int32", "params": {"signed": False}},
                types.Integer(bits=32, signed=False),
                "integer(bits=32, signed=False)",
            ),
            # Float types
            ({"type": "float16"}, types.Float(bits=16), "float(bits=16)"),
            ({"type": "float32"}, types.Float(bits=32), "float(bits=32)"),
            ({"type": "float64"}, types.Float(bits=64), "float(bits=64)"),
            # Decimal types
            ({"type": "decimal"}, types.Decimal(), "decimal"),
            (
                {"type": "decimal", "params": {"precision": 10, "scale": 2}},
                types.Decimal(precision=10, scale=2),
                "decimal(precision=10, scale=2)",
            ),
            (
                {"type": "decimal", "params": {"precision": 12, "scale": -3}},
                types.Decimal(precision=12, scale=-3),
                "decimal(precision=12, scale=-3)",
            ),
            (
                {"type": "decimal", "params": {"bits": 256}},
                types.Decimal(bits=256),
                "decimal(bits=256)",
            ),
            # Boolean types
            ({"type": "boolean"}, types.Boolean(), "boolean"),
            # Binary types
            ({"type": "binary"}, types.Binary(), "binary"),
            ({"type": "blob"}, types.Binary(), "binary"),
            ({"type": "bytes"}, types.Binary(), "binary"),
            # Temporal types
            ({"type": "date"}, types.Date(), "date"),
            ({"type": "date32"}, types.Date(bits=32), "date(bits=32)"),
            ({"type": "date64"}, types.Date(bits=64), "date(bits=64)"),
            ({"type": "time"}, types.Time(), "time(unit=ms)"),
            ({"type": "time32"}, types.Time(bits=32), "time(unit=ms, bits=32)"),
            (
                {"type": "time64"},
                types.Time(bits=64, unit=types.TimeUnit.NS),
                "time(unit=ns, bits=64)",
            ),
            (
                {"type": "time", "params": {"unit": "s"}},
                types.Time(unit=types.TimeUnit.S),
                "time(unit=s)",
            ),
            ({"type": "timestamp"}, types.Timestamp(), "timestamp(unit=ns)"),
            (
                {"type": "timestamp", "params": {"unit": "s"}},
                types.Timestamp(unit=types.TimeUnit.S),
                "timestamp(unit=s)",
            ),
            (
                {"type": "timestamptz"},
                types.TimestampTZ(),
                "timestamptz(unit=ns, tz=UTC)",
            ),
            (
                {"type": "timestamptz", "params": {"unit": "s"}},
                types.TimestampTZ(unit=types.TimeUnit.S),
                "timestamptz(unit=s, tz=UTC)",
            ),
            (
                {"type": "timestamp_tz"},
                types.TimestampTZ(),
                "timestamptz(unit=ns, tz=UTC)",
            ),
            ({"type": "timestampltz"}, types.TimestampLTZ(), "timestampltz(unit=ns)"),
            (
                {"type": "timestampltz", "params": {"unit": "s"}},
                types.TimestampLTZ(unit=types.TimeUnit.S),
                "timestampltz(unit=s)",
            ),
            (
                {"type": "timestamp_ltz"},
                types.TimestampLTZ(),
                "timestampltz(unit=ns)",
            ),
            ({"type": "timestampntz"}, types.TimestampNTZ(), "timestampntz(unit=ns)"),
            (
                {"type": "timestampntz", "params": {"unit": "s"}},
                types.TimestampNTZ(unit=types.TimeUnit.S),
                "timestampntz(unit=s)",
            ),
            (
                {"type": "timestamp_ntz"},
                types.TimestampNTZ(),
                "timestampntz(unit=ns)",
            ),
            ({"type": "duration"}, types.Duration(), "duration(unit=ns)"),
            (
                {"type": "duration", "params": {"unit": "s"}},
                types.Duration(unit=types.TimeUnit.S),
                "duration(unit=s)",
            ),
            # Interval types
            (
                {"type": "interval", "params": {"interval_start": "YEAR"}},
                types.Interval(interval_start=types.IntervalTimeUnit.YEAR),
                "interval(interval_start=YEAR)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "MONTH"}},
                types.Interval(interval_start=types.IntervalTimeUnit.MONTH),
                "interval(interval_start=MONTH)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "DAY"}},
                types.Interval(interval_start=types.IntervalTimeUnit.DAY),
                "interval(interval_start=DAY)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "HOUR"}},
                types.Interval(interval_start=types.IntervalTimeUnit.HOUR),
                "interval(interval_start=HOUR)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "MINUTE"}},
                types.Interval(interval_start=types.IntervalTimeUnit.MINUTE),
                "interval(interval_start=MINUTE)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "SECOND"}},
                types.Interval(interval_start=types.IntervalTimeUnit.SECOND),
                "interval(interval_start=SECOND)",
            ),
            (
                {
                    "type": "interval",
                    "params": {"interval_start": "YEAR", "interval_end": "MONTH"},
                },
                types.Interval(
                    interval_start=types.IntervalTimeUnit.YEAR,
                    interval_end=types.IntervalTimeUnit.MONTH,
                ),
                "interval(interval_start=YEAR, interval_end=MONTH)",
            ),
            (
                {
                    "type": "interval",
                    "params": {"interval_start": "DAY", "interval_end": "SECOND"},
                },
                types.Interval(
                    interval_start=types.IntervalTimeUnit.DAY,
                    interval_end=types.IntervalTimeUnit.SECOND,
                ),
                "interval(interval_start=DAY, interval_end=SECOND)",
            ),
            # Complex types
            ({"type": "json"}, types.JSON(), "json"),
            # Other complex types have dedicated tests below
            # Spatial types
            ({"type": "geometry"}, types.Geometry(), "geometry"),
            ({"type": "geography"}, types.Geography(), "geography"),
            (
                {"type": "geometry", "params": {"srid": 4326}},
                types.Geometry(srid=4326),
                "geometry(srid=4326)",
            ),
            (
                {"type": "geography", "params": {"srid": 4326}},
                types.Geography(srid=4326),
                "geography(srid=4326)",
            ),
            # Other types
            ({"type": "uuid"}, types.UUID(), "uuid"),
            ({"type": "void"}, types.Void(), "void"),
            ({"type": "variant"}, types.Variant(), "variant"),
        ],
    )
    def test_simple_type_deserialization(self, type_def, expected_type, expected_str):
        parsed_type = self._parse(type_def)
        assert parsed_type == expected_type
        assert str(parsed_type) == expected_str

    @pytest.mark.parametrize(
        "type_def, expected_element_type",
        [
            # Array types
            ({"type": "array", "element": {"type": "string"}}, types.String()),
            ({"type": "list", "element": {"type": "int"}}, types.Integer(bits=32)),
            (
                {
                    "type": "array",
                    "element": {
                        "type": "decimal",
                        "params": {"precision": 10, "scale": 2},
                    },
                },
                types.Decimal(precision=10, scale=2),
            ),
            # Nested arrays
            (
                {
                    "type": "array",
                    "element": {"type": "array", "element": {"type": "boolean"}},
                },
                types.Array(element=types.Boolean()),
            ),
        ],
    )
    def test_array_type_deserialization(self, type_def, expected_element_type):
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Array)
        assert parsed_type.element == expected_element_type

    def test_array_type_with_size_parameter(self):
        type_def = {
            "type": "array",
            "element": {"type": "string"},
            "params": {"size": 10},
        }
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Array)
        assert parsed_type.element == types.String()
        assert parsed_type.size == 10
        assert str(parsed_type) == "array<string, size=10>"

    @pytest.mark.parametrize(
        "type_def, expected_key_type, expected_value_type",
        [
            # Map types
            (
                {"type": "map", "key": {"type": "string"}, "value": {"type": "int"}},
                types.String(),
                types.Integer(bits=32),
            ),
            (
                {
                    "type": "dictionary",
                    "key": {"type": "uuid"},
                    "value": {"type": "double"},
                },
                types.UUID(),
                types.Float(bits=64),
            ),
            (
                {
                    "type": "map",
                    "key": {"type": "int"},
                    "value": {"type": "array", "element": {"type": "string"}},
                },
                types.Integer(bits=32),
                types.Array(element=types.String()),
            ),
        ],
    )
    def test_map_type_deserialization(
        self, type_def, expected_key_type, expected_value_type
    ):
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Map)
        assert parsed_type.key == expected_key_type
        assert parsed_type.value == expected_value_type

    def test_map_type_with_keys_sorted_parameter_true(self):
        """Test that Map type correctly handles keys_sorted parameter."""
        type_def = {
            "type": "map",
            "key": {"type": "integer"},
            "value": {"type": "string"},
            "params": {"keys_sorted": True},
        }
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Map)
        assert parsed_type.key == types.Integer(bits=32)
        assert parsed_type.value == types.String()
        assert parsed_type.keys_sorted is True
        assert str(parsed_type) == "map<integer(bits=32), string, keys_sorted=True>"

    def test_map_type_with_keys_sorted_parameter_false(self):
        type_def = {
            "type": "map",
            "key": {"type": "integer"},
            "value": {"type": "string"},
            "params": {"keys_sorted": False},
        }
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Map)
        assert parsed_type.key == types.Integer(bits=32)
        assert parsed_type.value == types.String()
        assert parsed_type.keys_sorted is False
        assert str(parsed_type) == "map<integer(bits=32), string>"

    def test_struct_type_deserialization(self):
        type_def = {
            "type": "struct",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"},
                {"name": "field3", "type": "boolean"},
            ],
        }
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Struct)
        assert len(parsed_type.fields) == 3

        field1, field2, field3 = parsed_type.fields
        assert field1.name == "field1"
        assert field1.type == types.String()
        assert field2.name == "field2"
        assert field2.type == types.Integer(bits=32)
        assert field3.name == "field3"
        assert field3.type == types.Boolean()

    def test_nested_struct_type_deserialization(self):
        type_def = {
            "type": "struct",
            "fields": [
                {"name": "simple_field", "type": "string"},
                {
                    "name": "nested_struct",
                    "type": "struct",
                    "fields": [{"name": "inner_field", "type": "int"}],
                },
            ],
        }
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Struct)
        assert len(parsed_type.fields) == 2

        simple_field, nested_field = parsed_type.fields
        assert simple_field.name == "simple_field"
        assert simple_field.type == types.String()

        assert nested_field.name == "nested_struct"
        assert isinstance(nested_field.type, types.Struct)
        assert len(nested_field.type.fields) == 1
        assert nested_field.type.fields[0].name == "inner_field"
        assert nested_field.type.fields[0].type == types.Integer(bits=32)

    def test_tensor_type_deserialization(self):
        """Tensor type loading with element and shape."""
        type_def = {
            "type": "tensor",
            "element": {"type": "int32"},
            "params": {"shape": [10, 20]},
        }
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Tensor)
        assert parsed_type.element == types.Integer(bits=32)
        assert parsed_type.shape == (10, 20)
        assert str(parsed_type) == "tensor<integer(bits=32), shape=[10, 20]>"

    def test_tensor_type_deserialization_float_elements(self):
        """Tensor type loading with float elements."""
        type_def = {
            "type": "tensor",
            "element": {"type": "float64"},
            "params": {"shape": [5, 10, 15]},
        }
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Tensor)
        assert parsed_type.element == types.Float(bits=64)
        assert parsed_type.shape == (5, 10, 15)
        assert str(parsed_type) == "tensor<float(bits=64), shape=[5, 10, 15]>"

    def test_tensor_type_deserialization_complex_element(self):
        """Tensor type loading with complex element type."""
        type_def = {
            "type": "tensor",
            "element": {
                "type": "struct",
                "fields": [{"name": "value", "type": "string"}],
            },
            "params": {"shape": [2, 3]},
        }
        parsed_type = self._parse(type_def)

        assert isinstance(parsed_type, types.Tensor)
        assert isinstance(parsed_type.element, types.Struct)
        assert parsed_type.shape == (2, 3)
        assert len(parsed_type.element.fields) == 1
        assert parsed_type.element.fields[0].name == "value"
        assert parsed_type.element.fields[0].type == types.String()

    def test_params_must_be_mapping_with_string_keys(self):
        with pytest.raises(
            TypeDefinitionError, match="'params' must be a mapping of parameter names"
        ):
            self._parse({"type": "integer", "params": "bits=32"})

        with pytest.raises(
            TypeDefinitionError, match="'params' must be a mapping of string keys"
        ):
            self._parse({"type": "integer", "params": {1: 2}})

    def test_array_element_validation(self):
        with pytest.raises(
            TypeDefinitionError,
            match="The 'element' of an array must be a dictionary with a 'type' key",
        ):
            self._parse({"type": "array", "element": "string"})

        with pytest.raises(
            TypeDefinitionError,
            match="The 'element' definition must include a string 'type' value",
        ):
            self._parse({"type": "array", "element": {}})

    def test_struct_field_shape_validation(self):
        with pytest.raises(
            TypeDefinitionError, match="Struct 'fields' must be a sequence of objects"
        ):
            self._parse({"type": "struct", "fields": "oops"})

        with pytest.raises(
            TypeDefinitionError, match="Struct field at index 0 must be a dictionary"
        ):
            self._parse({"type": "struct", "fields": [1]})

    def test_map_key_value_validation(self):
        with pytest.raises(
            TypeDefinitionError,
            match="Map key definition must be a dictionary that includes 'type'",
        ):
            self._parse({"type": "map", "key": "string", "value": {"type": "int"}})

        with pytest.raises(
            TypeDefinitionError,
            match="Map value definition must be a dictionary that includes 'type'",
        ):
            self._parse({"type": "map", "key": {"type": "string"}, "value": "int"})

        with pytest.raises(
            TypeDefinitionError, match="Map key definition must include a string 'type'"
        ):
            self._parse({"type": "map", "key": {}, "value": {"type": "int"}})

        with pytest.raises(
            TypeDefinitionError, match="Map value definition must include a string 'type'"
        ):
            self._parse({"type": "map", "key": {"type": "string"}, "value": {}})

    def test_tensor_type_missing_element_raises_error(self):
        """Tensor type without element raises error."""
        type_def = {
            "type": "tensor",
            "params": {"shape": [10, 20]},
        }
        with pytest.raises(
            TypeDefinitionError, match="Tensor type definition must include 'element'"
        ):
            self._parse(type_def)

    def test_tensor_type_missing_shape_raises_error(self):
        """Tensor type without shape raises error."""
        type_def = {
            "type": "tensor",
            "element": {"type": "int32"},
        }
        with pytest.raises(
            TypeDefinitionError, match="Tensor type definition must include 'shape'"
        ):
            self._parse(type_def)

    def test_tensor_type_invalid_shape_raises_error(self):
        """Tensor type with invalid shape raises error."""
        type_def = {
            "type": "tensor",
            "element": {"type": "int32"},
            "params": {"shape": [10, 0, 20]},
        }
        with pytest.raises(
            TypeDefinitionError,
            match="Tensor 'shape' must contain only positive integers",
        ):
            self._parse(type_def)

    def test_tensor_validation_branches(self):
        with pytest.raises(
            TypeDefinitionError,
            match="The 'element' of a tensor must be a dictionary with a 'type' key",
        ):
            self._parse({"type": "tensor", "element": "int32", "params": {"shape": [1]}})

        with pytest.raises(
            TypeDefinitionError,
            match="Tensor element definition must include a string 'type'",
        ):
            self._parse({"type": "tensor", "element": {}, "params": {"shape": [1]}})

        with pytest.raises(
            TypeDefinitionError, match="Tensor 'shape' must be a list or tuple of ints"
        ):
            self._parse(
                {
                    "type": "tensor",
                    "element": {"type": "int32"},
                    "params": {"shape": "bad"},
                }
            )

        with pytest.raises(
            TypeDefinitionError,
            match="Tensor 'shape' elements must be integers \\(failed at index 1\\)",
        ):
            self._parse(
                {
                    "type": "tensor",
                    "element": {"type": "int32"},
                    "params": {"shape": [1, "two"]},
                }
            )
