"""Unit tests for PolarsLoader."""

import warnings
import polars as pl  # type: ignore[import-untyped]
import pytest

from yads.exceptions import LoaderConfigError, UnsupportedFeatureError, ValidationWarning
from yads.loaders import PolarsLoader, PolarsLoaderConfig
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
    Array,
    Struct,
    Void,
    Variant,
    Tensor,
)


# fmt: off
# %% Type conversion tests
class TestPolarsLoaderTypeConversion:
    @pytest.mark.parametrize(
        "pl_type, expected_yads_type",
        [
            # Null / Boolean
            (pl.Null, Void()),
            (pl.Boolean, Boolean()),
            
            # Integers
            (pl.Int8, Integer(bits=8, signed=True)),
            (pl.Int16, Integer(bits=16, signed=True)),
            (pl.Int32, Integer(bits=32, signed=True)),
            (pl.Int64, Integer(bits=64, signed=True)),
            (pl.UInt8, Integer(bits=8, signed=False)),
            (pl.UInt16, Integer(bits=16, signed=False)),
            (pl.UInt32, Integer(bits=32, signed=False)),
            (pl.UInt64, Integer(bits=64, signed=False)),
            
            # Floats
            (pl.Float32, Float(bits=32)),
            (pl.Float64, Float(bits=64)),
            
            # Strings / Binary
            (pl.String, String()),
            (pl.Utf8, String()),  # Alias for String
            (pl.Binary, Binary()),
            
            # Date / Time
            (pl.Date, Date(bits=32)),
            (pl.Time, Time(unit=TimeUnit.NS, bits=64)),
            
            # Decimal
            (pl.Decimal(precision=10, scale=2), Decimal(precision=10, scale=2)),
            (pl.Decimal(precision=20, scale=3), Decimal(precision=20, scale=3)),
        ],
    )
    def test_convert_primitive_types(
        self, pl_type: pl.DataType, expected_yads_type: YadsType
    ):
        schema = pl.Schema({"col1": pl_type})
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        assert spec.name == "test_spec"
        assert spec.version == "1.0.0"
        assert len(spec.columns) == 1
        
        column = spec.columns[0]
        assert column.name == "col1"
        assert column.type == expected_yads_type
        assert column.is_nullable is True  # Polars Schema doesn't track nullability

    @pytest.mark.parametrize(
        "pl_type, expected_yads_type",
        [
            # Timestamp without timezone
            (pl.Datetime(time_unit="ms", time_zone=None), Timestamp(unit=TimeUnit.MS)),
            (pl.Datetime(time_unit="us", time_zone=None), Timestamp(unit=TimeUnit.US)),
            (pl.Datetime(time_unit="ns", time_zone=None), Timestamp(unit=TimeUnit.NS)),
            
            # Timestamp with timezone
            (pl.Datetime(time_unit="ms", time_zone="UTC"), TimestampTZ(unit=TimeUnit.MS, tz="UTC")),
            (pl.Datetime(time_unit="us", time_zone="America/New_York"), TimestampTZ(unit=TimeUnit.US, tz="America/New_York")),
            (pl.Datetime(time_unit="ns", time_zone="Europe/London"), TimestampTZ(unit=TimeUnit.NS, tz="Europe/London")),
            
            # Duration
            (pl.Duration(time_unit="ms"), Duration(unit=TimeUnit.MS)),
            (pl.Duration(time_unit="us"), Duration(unit=TimeUnit.US)),
            (pl.Duration(time_unit="ns"), Duration(unit=TimeUnit.NS)),
        ],
    )
    def test_convert_temporal_types(
        self, pl_type: pl.DataType, expected_yads_type: YadsType
    ):
        schema = pl.Schema({"col1": pl_type})
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert column.name == "col1"
        assert column.type == expected_yads_type
        assert column.is_nullable is True

    @pytest.mark.parametrize(
        "pl_type, expected_element_type, expected_size",
        [
            # Variable-length lists
            (pl.List(pl.String), String(), None),
            (pl.List(pl.Int32), Integer(bits=32, signed=True), None),
            (pl.List(pl.Float64), Float(bits=64), None),
            (pl.List(pl.Boolean), Boolean(), None),
            
            # Fixed-size arrays (1D)
            (pl.Array(pl.String, shape=5), String(), 5),
            (pl.Array(pl.Int32, shape=10), Integer(bits=32, signed=True), 10),
            (pl.Array(pl.Float64, shape=3), Float(bits=64), 3),
        ],
    )
    def test_convert_list_and_array_types(
        self, pl_type: pl.DataType, expected_element_type: YadsType, expected_size: int | None
    ):
        schema = pl.Schema({"col1": pl_type})
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Array)
        assert column.type.element == expected_element_type
        assert column.type.size == expected_size

    @pytest.mark.parametrize(
        "pl_type, expected_element_type, expected_shape",
        [
            # Multi-dimensional arrays -> Tensor
            (pl.Array(pl.Int32, shape=(10, 20)), Integer(bits=32, signed=True), (10, 20)),
            (pl.Array(pl.Float64, shape=(5, 10, 15)), Float(bits=64), (5, 10, 15)),
            (pl.Array(pl.String, shape=(100, 50)), String(), (100, 50)),
            (pl.Array(pl.Boolean, shape=(3, 4, 5, 6)), Boolean(), (3, 4, 5, 6)),
        ],
    )
    def test_convert_multidimensional_array_to_tensor(
        self, pl_type: pl.DataType, expected_element_type: YadsType, expected_shape: tuple[int, ...]
    ):
        schema = pl.Schema({"col1": pl_type})
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Tensor)
        assert column.type.element == expected_element_type
        assert column.type.shape == expected_shape

    def test_convert_struct_type(self):
        schema = pl.Schema({
            "struct_col": pl.Struct([
                pl.Field("x", pl.Int32),
                pl.Field("y", pl.String),
                pl.Field("z", pl.Float64),
            ])
        })
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Struct)
        assert len(column.type.fields) == 3
        
        field_x = column.type.fields[0]
        assert field_x.name == "x"
        assert field_x.type == Integer(bits=32, signed=True)
        assert field_x.is_nullable is True
        
        field_y = column.type.fields[1]
        assert field_y.name == "y"
        assert field_y.type == String()
        assert field_y.is_nullable is True
        
        field_z = column.type.fields[2]
        assert field_z.name == "z"
        assert field_z.type == Float(bits=64)
        assert field_z.is_nullable is True

    def test_convert_nested_complex_types(self):
        inner_struct = pl.Struct([
            pl.Field("id", pl.Int32),
            pl.Field("data", pl.List(pl.String)),
        ])
        schema = pl.Schema({"nested": pl.List(inner_struct)})
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Array)
        
        element_type = column.type.element
        assert isinstance(element_type, Struct)
        assert len(element_type.fields) == 2
        
        id_field = element_type.fields[0]
        assert id_field.name == "id"
        assert id_field.type == Integer(bits=32, signed=True)
        
        data_field = element_type.fields[1]
        assert data_field.name == "data"
        assert isinstance(data_field.type, Array)
        assert data_field.type.element == String()

    def test_convert_deeply_nested_complex_types(self):
        inner_struct = pl.Struct([
            pl.Field("id", pl.Int32),
            pl.Field("values", pl.Array(pl.Float64, shape=5)),
        ])
        schema = pl.Schema({
            "complex_col": pl.List(inner_struct)
        })
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Array)
        
        # Element should be Struct
        element_type = column.type.element
        assert isinstance(element_type, Struct)
        
        struct_fields = element_type.fields
        assert len(struct_fields) == 2
        assert struct_fields[0].name == "id"
        assert struct_fields[0].type == Integer(bits=32, signed=True)
        assert struct_fields[1].name == "values"
        assert isinstance(struct_fields[1].type, Array)
        assert struct_fields[1].type.element == Float(bits=64)
        assert struct_fields[1].type.size == 5

    @pytest.mark.skipif(
        not hasattr(pl, "Object"),
        reason="pl.Object not available in this polars version"
    )
    def test_convert_object_type(self):
        """Test that pl.Object maps to Variant."""
        schema = pl.Schema({"obj_col": pl.Object})
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert column.name == "obj_col"
        assert isinstance(column.type, Variant)

    def test_convert_nested_array_with_struct(self):
        """Test nested structure with arrays containing structs with arrays."""
        inner_struct = pl.Struct([
            pl.Field("name", pl.String),
            pl.Field("tags", pl.List(pl.String)),
            pl.Field("matrix", pl.Array(pl.Int32, shape=(3, 3))),
        ])
        schema = pl.Schema({
            "records": pl.List(inner_struct)
        })
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Array)
        assert isinstance(column.type.element, Struct)
        
        fields = column.type.element.fields
        assert len(fields) == 3
        
        # Check name field
        assert fields[0].name == "name"
        assert fields[0].type == String()
        
        # Check tags field (variable-length list)
        assert fields[1].name == "tags"
        assert isinstance(fields[1].type, Array)
        assert fields[1].type.element == String()
        assert fields[1].type.size is None
        
        # Check matrix field (2D fixed array -> Tensor)
        assert fields[2].name == "matrix"
        assert isinstance(fields[2].type, Tensor)
        assert fields[2].type.element == Integer(bits=32, signed=True)
        assert fields[2].type.shape == (3, 3)
# fmt: on


# %% Field nullability tests
class TestPolarsLoaderNullability:
    def test_all_fields_nullable_by_default(self):
        """Polars Schema doesn't track nullability, so all fields are nullable."""
        schema = pl.Schema(
            {
                "col1": pl.String,
                "col2": pl.Int32,
            }
        )
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        for column in spec.columns:
            assert column.is_nullable is True
            assert len(column.constraints) == 0

    def test_nested_fields_nullable(self):
        """Nested struct fields should also be nullable by default."""
        schema = pl.Schema(
            {
                "struct_col": pl.Struct(
                    [
                        pl.Field("x", pl.Int32),
                        pl.Field("y", pl.String),
                    ]
                )
            }
        )
        loader = PolarsLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        struct_col = spec.columns[0]
        assert isinstance(struct_col.type, Struct)

        for field in struct_col.type.fields:
            assert field.is_nullable is True
            assert len(field.constraints) == 0


# %% Schema-level tests
class TestPolarsLoaderSchema:
    def test_schema_without_description(self):
        schema = pl.Schema({"id": pl.Int32})
        loader = PolarsLoader()
        spec = loader.load(schema, name="test", version="1.0.0")

        assert spec.name == "test"
        assert spec.version == "1.0.0"
        assert spec.description is None

    def test_schema_with_description(self):
        schema = pl.Schema({"id": pl.Int32})
        loader = PolarsLoader()
        spec = loader.load(
            schema, name="test", version="1.0.0", description="Test description"
        )

        assert spec.name == "test"
        assert spec.version == "1.0.0"
        assert spec.description == "Test description"

    def test_empty_schema(self):
        schema = pl.Schema({})
        loader = PolarsLoader()
        spec = loader.load(schema, name="empty", version="1.0.0")

        assert spec.name == "empty"
        assert spec.version == "1.0.0"
        assert len(spec.columns) == 0

    def test_multiple_columns(self):
        schema = pl.Schema(
            {
                "id": pl.Int64,
                "name": pl.String,
                "age": pl.Int32,
                "score": pl.Float64,
            }
        )
        loader = PolarsLoader()
        spec = loader.load(schema, name="test", version="1.0.0")

        assert len(spec.columns) == 4
        assert spec.columns[0].name == "id"
        assert spec.columns[1].name == "name"
        assert spec.columns[2].name == "age"
        assert spec.columns[3].name == "score"


# %% Unsupported types and error handling
class TestPolarsLoaderUnsupportedTypes:
    def test_categorical_type_raises_error(self):
        """Test that Categorical type raises error in raise mode."""
        # Create a DataFrame with categorical data to get the schema
        df = pl.DataFrame({"cat_col": pl.Series(["a", "b", "c"], dtype=pl.Categorical)})
        schema = df.schema

        config = PolarsLoaderConfig(mode="raise")
        loader = PolarsLoader(config)

        with pytest.raises(
            UnsupportedFeatureError,
            match="PolarsLoader does not support Polars type.*for 'cat_col'",
        ):
            loader.load(schema, name="test", version="1.0.0")

    def test_enum_type_raises_error(self):
        """Test that Enum type raises error in raise mode."""
        if hasattr(pl, "Enum"):
            schema = pl.Schema({"enum_col": pl.Enum(["a", "b", "c"])})
            config = PolarsLoaderConfig(mode="raise")
            loader = PolarsLoader(config)

            with pytest.raises(
                UnsupportedFeatureError,
                match="PolarsLoader does not support Polars type.*for 'enum_col'",
            ):
                loader.load(schema, name="test", version="1.0.0")


# %% Configuration tests
class TestPolarsLoaderConfig:
    def test_default_config(self):
        config = PolarsLoaderConfig()
        assert config.mode == "coerce"
        assert config.fallback_type is None

    def test_custom_config(self):
        config = PolarsLoaderConfig(mode="raise", fallback_type=Binary())
        assert config.mode == "raise"
        assert config.fallback_type == Binary()

    def test_invalid_mode_raises_error(self):
        with pytest.raises(
            LoaderConfigError, match="mode must be one of 'raise' or 'coerce'"
        ):
            PolarsLoaderConfig(mode="invalid")  # type: ignore[arg-type]

    def test_invalid_fallback_type_raises_error(self):
        with pytest.raises(
            LoaderConfigError, match="fallback_type must be either String or Binary"
        ):
            PolarsLoaderConfig(fallback_type=Integer())  # type: ignore[arg-type]

    def test_config_is_immutable(self):
        config = PolarsLoaderConfig(mode="raise")
        # Config should be frozen
        with pytest.raises(AttributeError):
            config.mode = "coerce"  # type: ignore[misc]


class TestPolarsLoaderWithConfig:
    def test_loader_with_default_config(self):
        loader = PolarsLoader()
        assert loader.config.mode == "coerce"
        assert loader.config.fallback_type is None

    def test_loader_with_custom_config(self):
        config = PolarsLoaderConfig(mode="raise", fallback_type=Binary())
        loader = PolarsLoader(config)
        assert loader.config.mode == "raise"
        assert loader.config.fallback_type == Binary()

    def test_mode_override_in_load_method(self):
        config = PolarsLoaderConfig(mode="coerce", fallback_type=String())
        loader = PolarsLoader(config)

        # Create a DataFrame with categorical data to get the schema
        df = pl.DataFrame({"cat_col": pl.Series(["a", "b", "c"], dtype=pl.Categorical)})
        schema = df.schema

        with pytest.raises(UnsupportedFeatureError):
            loader.load(schema, name="test", version="1.0.0", mode="raise")

    def test_coercion_mode_without_fallback_raises(self):
        """Test that coerce mode raises when fallback_type is None."""
        config = PolarsLoaderConfig(mode="coerce")  # fallback_type=None
        loader = PolarsLoader(config)

        # Create a DataFrame with categorical data to get the schema
        df = pl.DataFrame({"cat_col": pl.Series(["a", "b", "c"], dtype=pl.Categorical)})
        schema = df.schema

        with pytest.raises(
            UnsupportedFeatureError,
            match="Specify a fallback_type to enable coercion",
        ):
            loader.load(schema, name="test", version="1.0.0")

    def test_coercion_mode_with_string_fallback(self):
        config = PolarsLoaderConfig(mode="coerce", fallback_type=String())
        loader = PolarsLoader(config)

        # Create a DataFrame with categorical data to get the schema
        df = pl.DataFrame({"cat_col": pl.Series(["a", "b", "c"], dtype=pl.Categorical)})
        schema = df.schema

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PolarsLoader does not support Polars type" in str(w[0].message)
            assert "for 'cat_col'" in str(w[0].message)
            assert "The data type will be coerced to string" in str(w[0].message)

        column = spec.columns[0]
        assert column.name == "cat_col"
        assert column.type == String()

    def test_coercion_mode_with_custom_binary_fallback(self):
        config = PolarsLoaderConfig(mode="coerce", fallback_type=Binary(length=10))
        loader = PolarsLoader(config)

        # Create a DataFrame with categorical data to get the schema
        df = pl.DataFrame({"cat_col": pl.Series(["a", "b", "c"], dtype=pl.Categorical)})
        schema = df.schema

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PolarsLoader does not support Polars type" in str(w[0].message)
            assert "for 'cat_col'" in str(w[0].message)
            assert "The data type will be coerced to binary(length=10)" in str(
                w[0].message
            )

        column = spec.columns[0]
        assert column.name == "cat_col"
        assert column.type == Binary(length=10)

    def test_raise_mode_with_unsupported_types(self):
        config = PolarsLoaderConfig(mode="raise")
        loader = PolarsLoader(config)

        # Create a DataFrame with categorical data to get the schema
        df = pl.DataFrame({"cat_col": pl.Series(["a", "b", "c"], dtype=pl.Categorical)})
        schema = df.schema

        with pytest.raises(
            UnsupportedFeatureError, match="PolarsLoader does not support Polars type"
        ):
            loader.load(schema, name="test", version="1.0.0")

    def test_multiple_unsupported_types_coercion(self):
        config = PolarsLoaderConfig(mode="coerce", fallback_type=String())
        loader = PolarsLoader(config)

        # Create a DataFrame with categorical data to get the schema
        df = pl.DataFrame(
            {
                "cat_col": pl.Series(["a", "b", "c"], dtype=pl.Categorical),
                "normal_col": pl.Series(["x", "y", "z"], dtype=pl.String),
            }
        )
        schema = df.schema

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PolarsLoader does not support Polars type" in str(w[0].message)
            assert "for 'cat_col'" in str(w[0].message)

        assert len(spec.columns) == 2
        assert spec.columns[0].name == "cat_col"
        assert spec.columns[0].type == String()  # coerced
        assert spec.columns[1].name == "normal_col"
        assert spec.columns[1].type == String()  # normal conversion

    def test_field_context_in_error_messages(self):
        config = PolarsLoaderConfig(mode="raise")
        loader = PolarsLoader(config)

        # Create a DataFrame with categorical data to get the schema
        df = pl.DataFrame({"my_field": pl.Series(["a", "b", "c"], dtype=pl.Categorical)})
        schema = df.schema

        with pytest.raises(UnsupportedFeatureError) as exc_info:
            loader.load(schema, name="test", version="1.0.0")

        assert "for 'my_field'" in str(exc_info.value)

    def test_nested_unsupported_types_coercion(self):
        config = PolarsLoaderConfig(mode="coerce", fallback_type=String())
        loader = PolarsLoader(config)

        df = pl.DataFrame(
            {
                "id": [1, 2],
                "cat_field": pl.Series(["a", "b"], dtype=pl.Categorical),
            }
        ).select(pl.struct(["id", "cat_field"]).alias("nested"))
        schema = df.schema

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PolarsLoader does not support Polars type" in str(w[0].message)
            assert "for 'cat_field'" in str(w[0].message)

        # Check that only the unsupported field within the struct was coerced
        column = spec.columns[0]
        assert column.name == "nested"
        assert isinstance(column.type, Struct)  # struct preserved
        assert len(column.type.fields) == 2

        # Check individual fields
        id_field = next(f for f in column.type.fields if f.name == "id")
        assert isinstance(id_field.type, Integer)  # normal field preserved

        cat_field = next(f for f in column.type.fields if f.name == "cat_field")
        assert isinstance(cat_field.type, String)  # unsupported field coerced

    def test_complex_nested_fallback_behavior(self):
        config = PolarsLoaderConfig(mode="coerce", fallback_type=String())
        loader = PolarsLoader(config)

        inner_struct = pl.Struct(
            [
                pl.Field("id", pl.Int32),
                pl.Field("unsupported_field", pl.Categorical),
                pl.Field("normal_string", pl.String),
            ]
        )

        array_of_structs = pl.List(inner_struct)

        schema = pl.Schema(
            {
                "id": pl.Int32,
                "complex_data": array_of_structs,
            }
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test_complex", version="1.0.0")

        # Should have warnings for the unsupported type
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)

        warning_message = str(w[0].message)
        assert "PolarsLoader does not support Polars type" in warning_message
        assert "unsupported_field" in warning_message

        # Check that the structure is preserved
        assert len(spec.columns) == 2

        # Normal field should be preserved
        id_col = spec.columns[0]
        assert id_col.name == "id"
        assert isinstance(id_col.type, Integer)

        # Complex field should still be an array
        complex_col = spec.columns[1]
        assert complex_col.name == "complex_data"
        assert isinstance(complex_col.type, Array)

        # Array element should still be a struct
        assert isinstance(complex_col.type.element, Struct)
        assert len(complex_col.type.element.fields) == 3

        # Check struct fields
        struct_fields = complex_col.type.element.fields
        id_field = next(f for f in struct_fields if f.name == "id")
        assert isinstance(id_field.type, Integer)  # preserved

        unsupported_field = next(
            f for f in struct_fields if f.name == "unsupported_field"
        )
        assert isinstance(unsupported_field.type, String)  # coerced

        normal_string_field = next(f for f in struct_fields if f.name == "normal_string")
        assert isinstance(normal_string_field.type, String)  # preserved
