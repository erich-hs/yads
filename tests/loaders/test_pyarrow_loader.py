"""Unit tests for PyArrowLoader."""

import warnings
import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from yads.constraints import NotNullConstraint
from yads.exceptions import LoaderConfigError, UnsupportedFeatureError, ValidationWarning
from yads.loaders import PyArrowLoader, PyArrowLoaderConfig
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
    Tensor,
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
        ] + (
            # Version-gated view types (added in PyArrow 16.0.0)
            [(pa.string_view(), String()), (pa.binary_view(), Binary())] if hasattr(pa, 'string_view') else []
        ) + [
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
        ] + (
            # Extension types (UUID added in PyArrow 18.0.0)
            [(pa.uuid(), UUID())] if hasattr(pa, 'uuid') else []
        ) + (
            # Extension types (JSON added in PyArrow 19.0.0)
            [(pa.json_(), JSON())] if hasattr(pa, 'json_') else []
        ) + [
            (pa.fixed_shape_tensor(pa.int32(), [10, 20]), Tensor(element=Integer(bits=32, signed=True), shape=(10, 20))),
            (pa.fixed_shape_tensor(pa.float64(), [5, 10, 15]), Tensor(element=Float(bits=64), shape=(5, 10, 15))),
            (pa.fixed_shape_tensor(pa.string(), [100]), Tensor(element=String(), shape=(100,))),
        ],
    )
    def test_convert_primitive_types(
        self, pa_type: pa.DataType, expected_yads_type: YadsType
    ):
        schema = pa.schema([pa.field("col1", pa_type)])
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
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
        ] + (
            # List view types (added in PyArrow 16.0.0)
            [
                (pa.list_view(pa.float64()), Float(bits=64), None),
                (pa.list_view(pa.string()), String(), None),
                (pa.list_view(pa.int64()), Integer(bits=64, signed=True), None),
            ] if hasattr(pa, 'list_view') else []
        ) + (
            # Large list view types (added in PyArrow 16.0.0)
            [
                (pa.large_list_view(pa.bool_()), Boolean(), None),
                (pa.large_list_view(pa.string()), String(), None),
                (pa.large_list_view(pa.int32()), Integer(bits=32, signed=True), None),
            ] if hasattr(pa, 'large_list_view') else []
        ) + [
        ],
    )
    def test_convert_list_types(
        self, pa_type: pa.DataType, expected_element_type: YadsType, expected_size: int | None
    ):
        schema = pa.schema([pa.field("col1", pa_type)])
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
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
        inner_struct = pa.struct([
            pa.field("id", pa.int32()),
            pa.field("data", pa.list_(pa.string())),
        ])
        schema = pa.schema([
            pa.field("complex_col", pa.map_(pa.string(), pa.list_(inner_struct)))
        ])
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.name == "test_col"
        assert column.description == "A test field"
        assert column.metadata == {"custom_key": "custom_value", "numeric_value": 42}

    def test_schema_metadata_handling(self):
        schema_metadata = {
            "owner": "data-eng",
            "version_info": '{"major": 1, "minor": 0}',
            "tags": '["production", "critical"]',
        }
        schema = pa.schema(
            [
                pa.field("col1", pa.string()),
                pa.field("col2", pa.int32()),
            ],
            metadata=schema_metadata,
        )
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        assert spec.metadata["owner"] == "data-eng"
        assert spec.metadata["version_info"] == {"major": 1, "minor": 0}
        assert spec.metadata["tags"] == ["production", "critical"]

    def test_description_lifted_from_metadata(self):
        field_metadata = {
            "description": "This is a description",
            "other_key": "other_value",
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

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
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.description == "A field with bytes metadata"
        assert column.metadata == {"encoding": "utf-8"}

    def test_metadata_with_mixed_key_types(self):
        field_metadata = {
            "string_key": "string_value",
            b"bytes_key": b"bytes_value",
        }
        schema = pa.schema([pa.field("test_col", pa.string(), metadata=field_metadata)])
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.metadata == {
            "string_key": "string_value",
            "bytes_key": "bytes_value",
        }

    def test_schema_with_no_metadata(self):
        schema = pa.schema(
            [
                pa.field("col1", pa.string()),
                pa.field("col2", pa.int32()),
            ]
        )
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test", version="1.0.0")

        assert spec.metadata == {}
        for column in spec.columns:
            assert column.metadata == {}

    def test_field_with_empty_metadata(self):
        schema = pa.schema(
            [
                pa.field("col1", pa.string(), metadata={}),
            ]
        )
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test", version="1.0.0")

        column = spec.columns[0]
        assert column.metadata == {}

    def test_schema_with_empty_metadata(self):
        schema = pa.schema(
            [
                pa.field("col1", pa.string()),
            ],
            metadata={},
        )
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test", version="1.0.0")

        assert spec.metadata == {}


# %% Schema-level tests
class TestPyArrowLoaderSchema:
    def test_schema_without_description(self):
        schema = pa.schema([pa.field("id", pa.int32())])
        loader = PyArrowLoader()
        spec = loader.load(schema, name="test", version="1.0.0")

        assert spec.name == "test"
        assert spec.version == "1.0.0"
        assert spec.description is None

    def test_empty_schema(self):
        schema = pa.schema([])
        loader = PyArrowLoader()
        spec = loader.load(schema, name="empty", version="1.0.0")

        assert spec.name == "empty"
        assert spec.version == "1.0.0"
        assert len(spec.columns) == 0


# %% Unsupported types and error handling
class TestPyArrowLoaderUnsupportedTypes:
    def test_dictionary_encoded_type_raises_error(self):
        schema = pa.schema([pa.field("dict_col", pa.dictionary(pa.int32(), pa.string()))])
        config = PyArrowLoaderConfig(mode="raise")
        loader = PyArrowLoader(config)

        with pytest.raises(
            UnsupportedFeatureError,
            match="PyArrowLoader does not support PyArrow type.*for 'dict_col'",
        ):
            loader.load(schema, name="test", version="1.0.0")

    def test_run_end_encoded_type_raises_error(self):
        if hasattr(pa, "run_end_encoded"):
            schema = pa.schema(
                [pa.field("run_col", pa.run_end_encoded(pa.int32(), pa.string()))]
            )
            config = PyArrowLoaderConfig(mode="raise")
            loader = PyArrowLoader(config)

            with pytest.raises(
                UnsupportedFeatureError,
                match="PyArrowLoader does not support PyArrow type.*for 'run_col'",
            ):
                loader.load(schema, name="test", version="1.0.0")

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
            config = PyArrowLoaderConfig(mode="raise")
            loader = PyArrowLoader(config)

            with pytest.raises(
                UnsupportedFeatureError,
                match="PyArrowLoader does not support PyArrow type.*for 'union_col'",
            ):
                loader.load(schema, name="test", version="1.0.0")


# %% Configuration tests
class TestPyArrowLoaderConfig:
    def test_default_config(self):
        config = PyArrowLoaderConfig()
        assert config.mode == "coerce"
        assert config.fallback_type is None

    def test_custom_config(self):
        config = PyArrowLoaderConfig(mode="raise", fallback_type=Binary())
        assert config.mode == "raise"
        assert config.fallback_type == Binary()

    def test_invalid_mode_raises_error(self):
        with pytest.raises(
            LoaderConfigError, match="mode must be one of 'raise' or 'coerce'"
        ):
            PyArrowLoaderConfig(mode="invalid")

    def test_invalid_fallback_type_raises_error(self):
        with pytest.raises(
            LoaderConfigError, match="fallback_type must be either String or Binary"
        ):
            PyArrowLoaderConfig(fallback_type=Integer())

    def test_config_is_immutable(self):
        config = PyArrowLoaderConfig(mode="raise")
        # Config should be frozen
        with pytest.raises(AttributeError):
            config.mode = "coerce"


class TestPyArrowLoaderWithConfig:
    def test_loader_with_default_config(self):
        loader = PyArrowLoader()
        assert loader.config.mode == "coerce"
        assert loader.config.fallback_type is None

    def test_loader_with_custom_config(self):
        config = PyArrowLoaderConfig(mode="raise", fallback_type=Binary())
        loader = PyArrowLoader(config)
        assert loader.config.mode == "raise"
        assert loader.config.fallback_type == Binary()

    def test_mode_override_in_load_method(self):
        config = PyArrowLoaderConfig(mode="coerce")
        loader = PyArrowLoader(config)
        schema = pa.schema([pa.field("dict_col", pa.dictionary(pa.int32(), pa.string()))])

        with pytest.raises(UnsupportedFeatureError):
            loader.load(schema, name="test", version="1.0.0", mode="raise")

    def test_coercion_mode_without_fallback_raises(self):
        """Test that coerce mode raises when fallback_type is None."""
        config = PyArrowLoaderConfig(mode="coerce")  # fallback_type=None
        loader = PyArrowLoader(config)
        schema = pa.schema([pa.field("dict_col", pa.dictionary(pa.int32(), pa.string()))])

        with pytest.raises(
            UnsupportedFeatureError,
            match="Specify a fallback_type to enable coercion",
        ):
            loader.load(schema, name="test", version="1.0.0")

    def test_coercion_mode_with_custom_fallback(self):
        config = PyArrowLoaderConfig(mode="coerce", fallback_type=Binary(length=10))
        loader = PyArrowLoader(config)
        schema = pa.schema([pa.field("dict_col", pa.dictionary(pa.int32(), pa.string()))])

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PyArrowLoader does not support PyArrow type" in str(w[0].message)
            assert "for 'dict_col'" in str(w[0].message)
            assert "The data type will be coerced to binary(length=10)" in str(
                w[0].message
            )

        column = spec.columns[0]
        assert column.name == "dict_col"
        assert column.type == Binary(length=10)

    def test_raise_mode_with_unsupported_types(self):
        config = PyArrowLoaderConfig(mode="raise")
        loader = PyArrowLoader(config)

        schema = pa.schema([pa.field("dict_col", pa.dictionary(pa.int32(), pa.string()))])
        with pytest.raises(
            UnsupportedFeatureError, match="PyArrowLoader does not support PyArrow type"
        ):
            loader.load(schema, name="test", version="1.0.0")

    def test_multiple_unsupported_types_coercion(self):
        config = PyArrowLoaderConfig(mode="coerce", fallback_type=String())
        loader = PyArrowLoader(config)

        schema = pa.schema(
            [
                pa.field("dict_col", pa.dictionary(pa.int32(), pa.string())),
                pa.field("normal_col", pa.string()),
                pa.field("unknown_col", pa.dictionary(pa.int64(), pa.float64())),
            ]
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 2
            assert all(
                "PyArrowLoader does not support PyArrow type" in str(warning.message)
                and "for '" in str(warning.message)
                for warning in w
            )

        assert len(spec.columns) == 3
        assert spec.columns[0].name == "dict_col"
        assert spec.columns[0].type == String()  # coerced
        assert spec.columns[1].name == "normal_col"
        assert spec.columns[1].type == String()  # normal conversion
        assert spec.columns[2].name == "unknown_col"
        assert spec.columns[2].type == String()  # coerced

    def test_field_context_in_error_messages(self):
        config = PyArrowLoaderConfig(mode="raise")
        loader = PyArrowLoader(config)

        schema = pa.schema([pa.field("my_field", pa.dictionary(pa.int32(), pa.string()))])

        with pytest.raises(UnsupportedFeatureError) as exc_info:
            loader.load(schema, name="test", version="1.0.0")

        assert "for 'my_field'" in str(exc_info.value)

    def test_nested_unsupported_types_coercion(self):
        config = PyArrowLoaderConfig(mode="coerce", fallback_type=String())
        loader = PyArrowLoader(config)

        # Create a struct with an unsupported field
        inner_struct = pa.struct(
            [
                pa.field("id", pa.int32()),
                pa.field("dict_field", pa.dictionary(pa.int32(), pa.string())),
            ]
        )
        schema = pa.schema([pa.field("nested", inner_struct)])

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PyArrowLoader does not support PyArrow type" in str(w[0].message)
            assert "for 'dict_field'" in str(w[0].message)

        # Check that only the unsupported field within the struct was coerced
        column = spec.columns[0]
        assert column.name == "nested"
        assert isinstance(column.type, Struct)  # struct preserved
        assert len(column.type.fields) == 2

        # Check individual fields
        id_field = next(f for f in column.type.fields if f.name == "id")
        assert isinstance(id_field.type, Integer)  # normal field preserved

        dict_field = next(f for f in column.type.fields if f.name == "dict_field")
        assert isinstance(dict_field.type, String)  # unsupported field coerced

    def test_fallback_preserves_field_metadata(self):
        """Test that field metadata and description are preserved during fallback."""
        field_metadata = {
            "description": "This is a description",
            "custom_key": "custom_value",
            "numeric_value": "42",
        }
        schema = pa.schema(
            [
                pa.field(
                    "dict_col",
                    pa.dictionary(pa.int32(), pa.string()),
                    nullable=False,
                    metadata=field_metadata,
                )
            ]
        )
        config = PyArrowLoaderConfig(mode="coerce", fallback_type=String())
        loader = PyArrowLoader(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PyArrowLoader does not support PyArrow type" in str(w[0].message)
            assert "for 'dict_col'" in str(w[0].message)

        column = spec.columns[0]
        assert column.name == "dict_col"
        assert column.type == String()  # coerced
        assert column.description == "This is a description"  # preserved
        assert column.metadata == {
            "custom_key": "custom_value",
            "numeric_value": 42,
        }  # preserved
        assert column.is_nullable is False  # preserved
        assert len(column.constraints) == 1
        assert isinstance(column.constraints[0], NotNullConstraint)

    def test_fallback_preserves_field_metadata_with_binary_fallback(self):
        """Test that field metadata and description are preserved with binary fallback."""
        field_metadata = {
            "description": "Binary fallback test",
            "encoding": "utf-8",
        }
        schema = pa.schema(
            [
                pa.field(
                    "union_col",
                    pa.dictionary(pa.int32(), pa.string()),
                    nullable=True,
                    metadata=field_metadata,
                )
            ]
        )
        config = PyArrowLoaderConfig(mode="coerce", fallback_type=Binary(length=10))
        loader = PyArrowLoader(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PyArrowLoader does not support PyArrow type" in str(w[0].message)
            assert "for 'union_col'" in str(w[0].message)

        column = spec.columns[0]
        assert column.name == "union_col"
        assert column.type == Binary(length=10)  # coerced
        assert column.description == "Binary fallback test"  # preserved
        assert column.metadata == {"encoding": "utf-8"}  # preserved
        assert column.is_nullable is True  # preserved
        assert len(column.constraints) == 0

    def test_fallback_preserves_schema_metadata(self):
        """Test that schema metadata is preserved during fallback."""
        schema_metadata = {
            "owner": "data-eng",
            "version_info": '{"major": 1, "minor": 0}',
        }
        field_metadata = {
            "description": "Field description",
        }
        schema = pa.schema(
            [
                pa.field(
                    "dict_col",
                    pa.dictionary(pa.int32(), pa.string()),
                    metadata=field_metadata,
                )
            ],
            metadata=schema_metadata,
        )
        config = PyArrowLoaderConfig(mode="coerce", fallback_type=String())
        loader = PyArrowLoader(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PyArrowLoader does not support PyArrow type" in str(w[0].message)
            assert "for 'dict_col'" in str(w[0].message)

        # Check schema metadata is preserved
        assert spec.metadata["owner"] == "data-eng"
        assert spec.metadata["version_info"] == {"major": 1, "minor": 0}

        # Check field metadata is preserved
        column = spec.columns[0]
        assert column.description == "Field description"
        assert column.type == String()  # coerced

    def test_complex_nested_fallback_behavior(self):
        inner_struct = pa.struct(
            [
                pa.field("id", pa.int32()),
                pa.field("unsupported_field", pa.dictionary(pa.int32(), pa.string())),
                pa.field("normal_string", pa.string()),
            ]
        )

        array_of_structs = pa.list_(inner_struct)

        map_with_unsupported = pa.map_(
            pa.dictionary(pa.int32(), pa.string()),  # unsupported key
            array_of_structs,
        )

        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("complex_data", map_with_unsupported),
            ]
        )

        config = PyArrowLoaderConfig(mode="coerce", fallback_type=String())
        loader = PyArrowLoader(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test_complex", version="1.0.0")

        # Should have warnings for all unsupported types
        assert len(w) == 2
        assert all(issubclass(warning.category, ValidationWarning) for warning in w)

        warning_messages = [str(warning.message) for warning in w]
        assert any(
            "PyArrowLoader does not support PyArrow type" in msg and "<map_key>" in msg
            for msg in warning_messages
        )
        assert any(
            "PyArrowLoader does not support PyArrow type" in msg
            and "unsupported_field" in msg
            for msg in warning_messages
        )

        # Check that the structure is preserved
        assert len(spec.columns) == 2

        # Normal field should be preserved
        id_col = spec.columns[0]
        assert id_col.name == "id"
        assert isinstance(id_col.type, Integer)

        # Complex field should still be a map
        complex_col = spec.columns[1]
        assert complex_col.name == "complex_data"
        assert isinstance(complex_col.type, Map)

        # Map key should be coerced to fallback
        assert isinstance(complex_col.type.key, String)

        # Map value should still be an array
        assert isinstance(complex_col.type.value, Array)

        # Array element should still be a struct
        assert isinstance(complex_col.type.value.element, Struct)
        assert len(complex_col.type.value.element.fields) == 3

        # Check struct fields
        struct_fields = complex_col.type.value.element.fields
        id_field = next(f for f in struct_fields if f.name == "id")
        assert isinstance(id_field.type, Integer)  # preserved

        unsupported_field = next(
            f for f in struct_fields if f.name == "unsupported_field"
        )
        assert isinstance(unsupported_field.type, String)  # coerced

        normal_string_field = next(f for f in struct_fields if f.name == "normal_string")
        assert isinstance(normal_string_field.type, String)  # preserved
