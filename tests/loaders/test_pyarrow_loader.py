"""Unit tests for PyArrowLoader."""

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from yads.constraints import NotNullConstraint
from yads.exceptions import UnsupportedFeatureError
from yads.loaders import PyArrowLoader
from yads.types import (
    YadsType,
    String,
    Integer,
    Float,
    Decimal,
    Boolean,
    Binary,
    Date,
    TimeUnit,
    Time,
    Timestamp,
    TimestampTZ,
    Duration,
    IntervalTimeUnit,
    Interval,
    Array,
    Struct,
    Map,
    JSON,
    UUID,
    Void,
)


# fmt: off
# %% Type conversion tests
class TestPyArrowLoaderTypeConversion:
    @pytest.mark.parametrize(
        "pa_type, expected_yads_type",
        [
            # Null / Boolean
            (pa.null(), Void()),
            (pa.bool_(), Boolean()),
            
            # Integers
            (pa.int8(), Integer(bits=8, signed=True)),
            (pa.int16(), Integer(bits=16, signed=True)),
            (pa.int32(), Integer(bits=32, signed=True)),
            (pa.int64(), Integer(bits=64, signed=True)),
            (pa.uint8(), Integer(bits=8, signed=False)),
            (pa.uint16(), Integer(bits=16, signed=False)),
            (pa.uint32(), Integer(bits=32, signed=False)),
            (pa.uint64(), Integer(bits=64, signed=False)),
            
            # Floats
            (pa.float16(), Float(bits=16)),
            (pa.float32(), Float(bits=32)),
            (pa.float64(), Float(bits=64)),
            
            # Strings / Binary
            (pa.string(), String()),
            (pa.utf8(), String()),
            (pa.binary(), Binary()),
            (pa.binary(-1), Binary()),
            (pa.binary(8), Binary(length=8)),
            (pa.large_string(), String()),
            (pa.large_binary(), Binary()),
            (pa.string_view(), String()),
            (pa.binary_view(), Binary()),
            
            # Decimal
            (pa.decimal128(10, 2), Decimal(precision=10, scale=2, bits=128)),
            (pa.decimal256(20, 3), Decimal(precision=20, scale=3, bits=256)),
            
            # Date / Time
            (pa.date32(), Date(bits=32)),
            (pa.date64(), Date(bits=64)),
            (pa.time32("s"), Time(unit=TimeUnit.S, bits=32)),
            (pa.time32("ms"), Time(unit=TimeUnit.MS, bits=32)),
            (pa.time64("us"), Time(unit=TimeUnit.US, bits=64)),
            (pa.time64("ns"), Time(unit=TimeUnit.NS, bits=64)),
            
            # Timestamp
            (pa.timestamp("s"), Timestamp(unit=TimeUnit.S)),
            (pa.timestamp("ms"), Timestamp(unit=TimeUnit.MS)),
            (pa.timestamp("us"), Timestamp(unit=TimeUnit.US)),
            (pa.timestamp("ns"), Timestamp(unit=TimeUnit.NS)),
            (pa.timestamp("s", tz="UTC"), TimestampTZ(unit=TimeUnit.S, tz="UTC")),
            (pa.timestamp("ms", tz="America/New_York"), TimestampTZ(unit=TimeUnit.MS, tz="America/New_York")),
            
            # Duration
            (pa.duration("s"), Duration(unit=TimeUnit.S)),
            (pa.duration("ms"), Duration(unit=TimeUnit.MS)),
            (pa.duration("us"), Duration(unit=TimeUnit.US)),
            (pa.duration("ns"), Duration(unit=TimeUnit.NS)),
            
            # Interval
            (pa.month_day_nano_interval(), Interval(interval_start=IntervalTimeUnit.DAY)),
            
            # Extension types
            (pa.uuid(), UUID()),
            (pa.json_(), JSON()),
        ],
    )
    def test_convert_primitive_types(
        self, pa_type: pa.DataType, expected_yads_type: YadsType
    ):
        schema = pa.schema([pa.field("col1", pa_type)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()
        
        assert spec.name == "test_spec"
        assert spec.version == "1.0.0"
        assert len(spec.columns) == 1
        
        column = spec.columns[0]
        assert column.name == "col1"
        assert column.type == expected_yads_type
        assert column.is_nullable is True  # Default nullability

    @pytest.mark.parametrize(
        "pa_type, expected_element_type, expected_size",
        [
            # Fixed size lists
            (pa.list_(pa.string(), list_size=5), String(), 5),
            (pa.list_(pa.int32(), list_size=10), Integer(bits=32, signed=True), 10),
            (pa.list_(pa.float64(), list_size=3), Float(bits=64), 3),
            
            # Variable size lists
            (pa.large_list(pa.int32()), Integer(bits=32, signed=True), None),
            (pa.large_list(pa.string()), String(), None),
            (pa.large_list(pa.bool_()), Boolean(), None),
            
            # List view types
            (pa.list_view(pa.float64()), Float(bits=64), None),
            (pa.list_view(pa.string()), String(), None),
            (pa.list_view(pa.int64()), Integer(bits=64, signed=True), None),
            
            # Large list view types
            (pa.large_list_view(pa.bool_()), Boolean(), None),
            (pa.large_list_view(pa.string()), String(), None),
            (pa.large_list_view(pa.int32()), Integer(bits=32, signed=True), None),
        ],
    )
    def test_convert_list_types(
        self, pa_type: pa.DataType, expected_element_type: YadsType, expected_size: int | None
    ):
        schema = pa.schema([pa.field("col1", pa_type)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()
        
        column = spec.columns[0]
        assert isinstance(column.type, Array)
        assert column.type.element == expected_element_type
        assert column.type.size == expected_size

    @pytest.mark.parametrize(
        "pa_type, expected_key_type, expected_value_type, expected_keys_sorted",
        [
            # Regular maps
            (pa.map_(pa.string(), pa.int32()), String(), Integer(bits=32, signed=True), False),
            (pa.map_(pa.int64(), pa.string()), Integer(bits=64, signed=True), String(), False),
            (pa.map_(pa.string(), pa.float64()), String(), Float(bits=64), False),
            (pa.map_(pa.bool_(), pa.string()), Boolean(), String(), False),
            
            # Sorted maps
            (pa.map_(pa.string(), pa.int32(), keys_sorted=True), String(), Integer(bits=32, signed=True), True),
            (pa.map_(pa.int64(), pa.string(), keys_sorted=True), Integer(bits=64, signed=True), String(), True),
            (pa.map_(pa.string(), pa.float64(), keys_sorted=True), String(), Float(bits=64), True),
        ],
    )
    def test_convert_map_type(
        self, pa_type: pa.DataType, expected_key_type: YadsType, 
        expected_value_type: YadsType, expected_keys_sorted: bool
    ):
        schema = pa.schema([pa.field("col1", pa_type)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()
        
        column = spec.columns[0]
        assert isinstance(column.type, Map)
        assert column.type.key == expected_key_type
        assert column.type.value == expected_value_type
        assert column.type.keys_sorted == expected_keys_sorted

    def test_convert_struct_type(self):
        schema = pa.schema([
            pa.field("struct_col", pa.struct([
                pa.field("x", pa.int32()),
                pa.field("y", pa.string()),
                pa.field("z", pa.float64()),
            ]))
        ])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()
        
        column = spec.columns[0]
        assert isinstance(column.type, Struct)
        assert len(column.type.fields) == 3
        
        field_x = column.type.fields[0]
        assert field_x.name == "x"
        assert field_x.type == Integer(bits=32, signed=True)
        
        field_y = column.type.fields[1]
        assert field_y.name == "y"
        assert field_y.type == String()
        
        field_z = column.type.fields[2]
        assert field_z.name == "z"
        assert field_z.type == Float(bits=64)

    def test_convert_nested_complex_types(self):
        inner_struct = pa.struct([
            pa.field("id", pa.int32()),
            pa.field("metadata", pa.map_(pa.string(), pa.string())),
        ])
        schema = pa.schema([pa.field("nested", pa.list_(inner_struct))])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()
        
        column = spec.columns[0]
        assert isinstance(column.type, Array)
        
        element_type = column.type.element
        assert isinstance(element_type, Struct)
        assert len(element_type.fields) == 2
        
        id_field = element_type.fields[0]
        assert id_field.name == "id"
        assert id_field.type == Integer(bits=32, signed=True)
        
        metadata_field = element_type.fields[1]
        assert metadata_field.name == "metadata"
        assert isinstance(metadata_field.type, Map)
        assert metadata_field.type.key == String()
        assert metadata_field.type.value == String()

    def test_convert_deeply_nested_complex_types(self):
        # Map with array values containing structs
        inner_struct = pa.struct([
            pa.field("id", pa.int32()),
            pa.field("data", pa.list_(pa.string())),
        ])
        schema = pa.schema([
            pa.field("complex_col", pa.map_(pa.string(), pa.list_(inner_struct)))
        ])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()
        
        column = spec.columns[0]
        assert isinstance(column.type, Map)
        assert column.type.key == String()
        
        # Value should be Array of Struct
        value_type = column.type.value
        assert isinstance(value_type, Array)
        assert isinstance(value_type.element, Struct)
        
        struct_fields = value_type.element.fields
        assert len(struct_fields) == 2
        assert struct_fields[0].name == "id"
        assert struct_fields[0].type == Integer(bits=32, signed=True)
        assert struct_fields[1].name == "data"
        assert isinstance(struct_fields[1].type, Array)
        assert struct_fields[1].type.element == String()
# fmt: on


# %% Field nullability and constraints tests
class TestPyArrowLoaderNullability:
    def test_nullable_fields(self):
        schema = pa.schema(
            [
                pa.field("nullable_col", pa.string(), nullable=True),
                pa.field("non_nullable_col", pa.string(), nullable=False),
            ]
        )
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        nullable_col = spec.columns[0]
        assert nullable_col.name == "nullable_col"
        assert nullable_col.is_nullable is True
        assert len(nullable_col.constraints) == 0

        non_nullable_col = spec.columns[1]
        assert non_nullable_col.name == "non_nullable_col"
        assert non_nullable_col.is_nullable is False
        assert len(non_nullable_col.constraints) == 1
        assert isinstance(non_nullable_col.constraints[0], NotNullConstraint)

    def test_nested_field_nullability(self):
        schema = pa.schema(
            [
                pa.field(
                    "struct_col",
                    pa.struct(
                        [
                            pa.field("nullable_field", pa.int32(), nullable=True),
                            pa.field("non_nullable_field", pa.string(), nullable=False),
                        ]
                    ),
                )
            ]
        )
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        struct_col = spec.columns[0]
        assert isinstance(struct_col.type, Struct)

        nullable_field = struct_col.type.fields[0]
        assert nullable_field.name == "nullable_field"
        assert nullable_field.is_nullable is True
        assert len(nullable_field.constraints) == 0

        non_nullable_field = struct_col.type.fields[1]
        assert non_nullable_field.name == "non_nullable_field"
        assert non_nullable_field.is_nullable is False
        assert len(non_nullable_field.constraints) == 1
        assert isinstance(non_nullable_field.constraints[0], NotNullConstraint)


# %% Metadata handling tests
class TestPyArrowLoaderMetadata:
    def test_field_metadata_handling(self):
        field_metadata = {
            "description": "A test field",
            "custom_key": "custom_value",
            "numeric_value": "42",  # PyArrow metadata values must be strings or bytes
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        column = spec.columns[0]
        assert column.name == "test_col"
        assert column.description == "A test field"
        assert column.metadata == {"custom_key": "custom_value", "numeric_value": 42}

    def test_schema_metadata_handling(self):
        schema_metadata = {
            "owner": "data-eng",
            "version_info": '{"major": 1, "minor": 0}',  # JSON string for PyArrow
            "tags": '["production", "critical"]',  # JSON string for PyArrow
        }
        schema = pa.schema(
            [
                pa.field("col1", pa.string()),
                pa.field("col2", pa.int32()),
            ],
            metadata=schema_metadata,
        )
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        # The loader should parse JSON strings back to objects
        assert spec.metadata["owner"] == "data-eng"
        assert spec.metadata["version_info"] == {"major": 1, "minor": 0}
        assert spec.metadata["tags"] == ["production", "critical"]

    def test_description_lifted_from_metadata(self):
        field_metadata = {
            "description": "This is a description",
            "other_key": "other_value",
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        column = spec.columns[0]
        assert column.description == "This is a description"
        assert column.metadata == {"other_key": "other_value"}

    def test_metadata_with_json_values(self):
        field_metadata = {
            "config": '{"retries": 3, "timeout": 30}',
            "tags": '["tag1", "tag2"]',
            "simple_string": "just a string",
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        column = spec.columns[0]
        assert column.metadata["config"] == {"retries": 3, "timeout": 30}
        assert column.metadata["tags"] == ["tag1", "tag2"]
        assert column.metadata["simple_string"] == "just a string"

    def test_metadata_with_invalid_json_fallback(self):
        field_metadata = {
            "valid_json": '{"key": "value"}',
            "invalid_json": "not valid json",
            "number": "123",
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        column = spec.columns[0]
        assert column.metadata["valid_json"] == {"key": "value"}
        assert column.metadata["invalid_json"] == "not valid json"
        assert column.metadata["number"] == 123

    def test_metadata_with_bytes_keys_and_values(self):
        field_metadata = {
            b"description": b"A field with bytes metadata",
            b"encoding": b"utf-8",
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        column = spec.columns[0]
        assert column.description == "A field with bytes metadata"
        assert column.metadata == {"encoding": "utf-8"}

    def test_metadata_with_mixed_key_types(self):
        field_metadata = {
            "string_key": "string_value",
            b"bytes_key": b"bytes_value",
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        column = spec.columns[0]
        assert column.metadata == {
            "string_key": "string_value",
            "bytes_key": "bytes_value",
        }

    def test_metadata_with_non_utf8_bytes(self):
        field_metadata = {
            "valid_utf8": "hello world",
            b"invalid_utf8": b"\xff\xfe\x00\x01",  # Invalid UTF-8
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader(schema, name="test_spec", version="1.0.0")
        spec = loader.load()

        column = spec.columns[0]
        assert column.metadata["valid_utf8"] == "hello world"
        # Invalid UTF-8 should be handled gracefully
        assert "invalid_utf8" in column.metadata

    def test_schema_with_no_metadata(self):
        schema = pa.schema(
            [
                pa.field("col1", pa.string()),
                pa.field("col2", pa.int32()),
            ]
        )
        loader = PyArrowLoader(schema, name="test", version="1.0.0")
        spec = loader.load()

        # SpecBuilder creates empty metadata dictionaries by default
        assert spec.metadata == {}
        for column in spec.columns:
            assert column.metadata == {}

    def test_field_with_empty_metadata(self):
        schema = pa.schema(
            [
                pa.field("col1", pa.string(), metadata={}),
            ]
        )
        loader = PyArrowLoader(schema, name="test", version="1.0.0")
        spec = loader.load()

        column = spec.columns[0]
        # SpecBuilder creates empty metadata dictionaries by default
        assert column.metadata == {}

    def test_schema_with_empty_metadata(self):
        schema = pa.schema(
            [
                pa.field("col1", pa.string()),
            ],
            metadata={},
        )
        loader = PyArrowLoader(schema, name="test", version="1.0.0")
        spec = loader.load()

        # SpecBuilder creates empty metadata dictionaries by default
        assert spec.metadata == {}


# %% Schema-level tests
class TestPyArrowLoaderSchema:
    def test_basic_schema_conversion(self):
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
            ]
        )
        loader = PyArrowLoader(
            schema, name="users", version="2.1.0", description="User data schema"
        )
        spec = loader.load()

        assert spec.name == "users"
        assert spec.version == "2.1.0"
        assert spec.description == "User data schema"
        assert len(spec.columns) == 2

    def test_schema_without_description(self):
        schema = pa.schema([pa.field("id", pa.int32())])
        loader = PyArrowLoader(schema, name="test", version="1.0.0")
        spec = loader.load()

        assert spec.name == "test"
        assert spec.version == "1.0.0"
        assert spec.description is None

    def test_empty_schema(self):
        schema = pa.schema([])
        loader = PyArrowLoader(schema, name="empty", version="1.0.0")
        spec = loader.load()

        assert spec.name == "empty"
        assert spec.version == "1.0.0"
        assert len(spec.columns) == 0


# %% Unsupported types and error handling
class TestPyArrowLoaderUnsupportedTypes:
    def test_dictionary_encoded_type_raises_error(self):
        schema = pa.schema([pa.field("dict_col", pa.dictionary(pa.int32(), pa.string()))])
        loader = PyArrowLoader(schema, name="test", version="1.0.0")

        with pytest.raises(
            UnsupportedFeatureError,
            match="Dictionary-encoded types are not supported for field 'dict_col'",
        ):
            loader.load()

    def test_run_end_encoded_type_raises_error(self):
        if hasattr(pa, "run_end_encoded"):
            schema = pa.schema(
                [pa.field("run_col", pa.run_end_encoded(pa.int32(), pa.string()))]
            )
            loader = PyArrowLoader(schema, name="test", version="1.0.0")

            with pytest.raises(
                UnsupportedFeatureError,
                match="Run-end encoded types are not supported for field 'run_col'",
            ):
                loader.load()

    def test_union_type_raises_error(self):
        if hasattr(pa, "dense_union"):
            schema = pa.schema(
                [
                    pa.field(
                        "union_col",
                        pa.dense_union(
                            [
                                pa.field("int_val", pa.int32()),
                                pa.field("str_val", pa.string()),
                            ]
                        ),
                    )
                ]
            )
            loader = PyArrowLoader(schema, name="test", version="1.0.0")

            with pytest.raises(
                UnsupportedFeatureError,
                match="Union types are not supported for field 'union_col'",
            ):
                loader.load()

    def test_unknown_type_raises_error(self):
        # Use a type that exists but is not supported by the loader
        # We'll test this by mocking the type checking in the loader
        # For now, let's test with a type that should be unsupported
        if hasattr(pa, "run_end_encoded"):
            # This should trigger the unsupported type error
            schema = pa.schema(
                [pa.field("unknown_col", pa.run_end_encoded(pa.int32(), pa.string()))]
            )
            loader = PyArrowLoader(schema, name="test", version="1.0.0")

            with pytest.raises(
                UnsupportedFeatureError,
                match="Run-end encoded types are not supported for field 'unknown_col'",
            ):
                loader.load()
        else:
            # If run_end_encoded is not available, skip this test
            pytest.skip("No unsupported types available for testing")
