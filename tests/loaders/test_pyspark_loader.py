"""Unit tests for PySparkLoader."""

import warnings
import pytest

from pyspark.sql.types import (  # type: ignore[import-untyped]
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Optional types that may not be available in older PySpark versions
try:
    from pyspark.sql.types import CalendarIntervalType  # type: ignore[attr-defined]

    HAS_CALENDAR_INTERVAL_TYPE = True
except ImportError:
    CalendarIntervalType = None  # type: ignore[assignment, misc]
    HAS_CALENDAR_INTERVAL_TYPE = False

try:
    from pyspark.sql.types import DayTimeIntervalType  # type: ignore[attr-defined]

    HAS_DAY_TIME_INTERVAL_TYPE = True
except ImportError:
    DayTimeIntervalType = None  # type: ignore[assignment, misc]
    HAS_DAY_TIME_INTERVAL_TYPE = False

try:
    from pyspark.sql.types import CharType  # type: ignore[attr-defined]

    HAS_CHAR_TYPE = True
except ImportError:
    CharType = None  # type: ignore[assignment, misc]
    HAS_CHAR_TYPE = False

try:
    from pyspark.sql.types import VarcharType  # type: ignore[attr-defined]

    HAS_VARCHAR_TYPE = True
except ImportError:
    VarcharType = None  # type: ignore[assignment, misc]
    HAS_VARCHAR_TYPE = False

try:
    from pyspark.sql.types import TimestampNTZType  # type: ignore[attr-defined]

    HAS_TIMESTAMP_NTZ_TYPE = True
except ImportError:
    TimestampNTZType = None  # type: ignore[assignment, misc]
    HAS_TIMESTAMP_NTZ_TYPE = False

try:
    from pyspark.sql.types import YearMonthIntervalType  # type: ignore[attr-defined]

    HAS_YEAR_MONTH_INTERVAL_TYPE = True
except ImportError:
    YearMonthIntervalType = None  # type: ignore[assignment, misc]
    HAS_YEAR_MONTH_INTERVAL_TYPE = False

try:
    from pyspark.sql.types import VariantType  # type: ignore[attr-defined]

    HAS_VARIANT_TYPE = True
except ImportError:
    VariantType = None  # type: ignore[assignment, misc]
    HAS_VARIANT_TYPE = False

from yads.constraints import NotNullConstraint
from yads.exceptions import LoaderConfigError, UnsupportedFeatureError, ValidationWarning
from yads.loaders.pyspark_loader import PySparkLoader, PySparkLoaderConfig
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
    TimestampLTZ,
    TimestampNTZ,
    Interval,
    Array,
    Struct,
    Map,
    Void,
    Variant,
)


# fmt: off
# %% Type conversion tests
class TestPySparkLoaderTypeConversion:
    @pytest.mark.parametrize(
        "pyspark_type, expected_yads_type",
        [
            # Null / Boolean
            (NullType(), Void()),
            (BooleanType(), Boolean()),
            
            # Integers
            (ByteType(), Integer(bits=8, signed=True)),
            (ShortType(), Integer(bits=16, signed=True)),
            (IntegerType(), Integer(bits=32, signed=True)),
            (LongType(), Integer(bits=64, signed=True)),
            
            # Floats
            (FloatType(), Float(bits=32)),
            (DoubleType(), Float(bits=64)),
            
            # Strings / Binary
            (StringType(), String()),
            pytest.param(
                CharType(10) if HAS_CHAR_TYPE else None,
                String(length=10),
                marks=pytest.mark.skipif(not HAS_CHAR_TYPE, reason="CharType not available"),
                id="char_type"
            ),
            pytest.param(
                VarcharType(255) if HAS_VARCHAR_TYPE else None,
                String(length=255),
                marks=pytest.mark.skipif(not HAS_VARCHAR_TYPE, reason="VarcharType not available"),
                id="varchar_type"
            ),
            (BinaryType(), Binary()),
            
            # Decimal
            (DecimalType(10, 2), Decimal(precision=10, scale=2)),
            (DecimalType(20, 3), Decimal(precision=20, scale=3)),
            (DecimalType(38, 18), Decimal(precision=38, scale=18)),
            
            # Temporal types
            (DateType(), Date(bits=32)),
            (TimestampType(), TimestampLTZ(unit=TimeUnit.NS)),
            pytest.param(
                TimestampNTZType() if HAS_TIMESTAMP_NTZ_TYPE else None,
                TimestampNTZ(unit=TimeUnit.NS),
                marks=pytest.mark.skipif(not HAS_TIMESTAMP_NTZ_TYPE, reason="TimestampNTZType not available"),
                id="timestamp_ntz"
            ),
            
            # Special types
            pytest.param(
                VariantType() if HAS_VARIANT_TYPE else None,
                Variant(),
                marks=pytest.mark.skipif(not HAS_VARIANT_TYPE, reason="VariantType not available"),
                id="variant_type"
            ),
        ],
    )
    def test_convert_primitive_types(
        self, pyspark_type: DataType, expected_yads_type: YadsType
    ):
        schema = StructType([StructField("col1", pyspark_type, nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        assert spec.name == "test_spec"
        assert spec.version == "1.0.0"
        assert len(spec.columns) == 1
        
        column = spec.columns[0]
        assert column.name == "col1"
        assert column.type == expected_yads_type
        assert column.is_nullable is True  # Default nullability

    @pytest.mark.skipif(
        not HAS_YEAR_MONTH_INTERVAL_TYPE,
        reason="YearMonthIntervalType not available"
    )
    @pytest.mark.parametrize(
        "pyspark_type, expected_interval_start, expected_interval_end",
        [
            # Year-Month intervals
            (YearMonthIntervalType(0, 0), "YEAR", None),  # YEAR to YEAR
            (YearMonthIntervalType(0, 1), "YEAR", "MONTH"),  # YEAR to MONTH
            (YearMonthIntervalType(1, 1), "MONTH", None),  # MONTH to MONTH
        ] if HAS_YEAR_MONTH_INTERVAL_TYPE else [],
    )
    def test_convert_year_month_interval_types(
        self, pyspark_type: DataType, expected_interval_start: str, expected_interval_end: str | None
    ):
        schema = StructType([StructField("col1", pyspark_type, nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Interval)
        assert column.type.interval_start.value == expected_interval_start
        if expected_interval_end:
            assert column.type.interval_end is not None
            assert column.type.interval_end.value == expected_interval_end
        else:
            assert column.type.interval_end is None

    @pytest.mark.skipif(
        not HAS_DAY_TIME_INTERVAL_TYPE,
        reason="DayTimeIntervalType not available"
    )
    @pytest.mark.parametrize(
        "pyspark_type, expected_interval_start, expected_interval_end",
        [
            # Day-Time intervals
            (DayTimeIntervalType(0, 0), "DAY", None),  # DAY to DAY
            (DayTimeIntervalType(0, 1), "DAY", "HOUR"),  # DAY to HOUR
            (DayTimeIntervalType(0, 2), "DAY", "MINUTE"),  # DAY to MINUTE
            (DayTimeIntervalType(0, 3), "DAY", "SECOND"),  # DAY to SECOND
            (DayTimeIntervalType(1, 1), "HOUR", None),  # HOUR to HOUR
            (DayTimeIntervalType(1, 2), "HOUR", "MINUTE"),  # HOUR to MINUTE
            (DayTimeIntervalType(1, 3), "HOUR", "SECOND"),  # HOUR to SECOND
            (DayTimeIntervalType(2, 2), "MINUTE", None),  # MINUTE to MINUTE
            (DayTimeIntervalType(2, 3), "MINUTE", "SECOND"),  # MINUTE to SECOND
            (DayTimeIntervalType(3, 3), "SECOND", None),  # SECOND to SECOND
        ] if HAS_DAY_TIME_INTERVAL_TYPE else [],
    )
    def test_convert_day_time_interval_types(
        self, pyspark_type: DataType, expected_interval_start: str, expected_interval_end: str | None
    ):
        schema = StructType([StructField("col1", pyspark_type, nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Interval)
        assert column.type.interval_start.value == expected_interval_start
        if expected_interval_end:
            assert column.type.interval_end is not None
            assert column.type.interval_end.value == expected_interval_end
        else:
            assert column.type.interval_end is None

    @pytest.mark.parametrize(
        "pyspark_type, expected_element_type",
        [
            # Array types
            (ArrayType(StringType(), containsNull=True), String()),
            (ArrayType(IntegerType(), containsNull=True), Integer(bits=32, signed=True)),
            (ArrayType(FloatType(), containsNull=True), Float(bits=32)),
            (ArrayType(DoubleType(), containsNull=True), Float(bits=64)),
            (ArrayType(BooleanType(), containsNull=True), Boolean()),
            (ArrayType(BinaryType(), containsNull=False), Binary()),
        ],
    )
    def test_convert_array_types(
        self, pyspark_type: DataType, expected_element_type: YadsType
    ):
        schema = StructType([StructField("col1", pyspark_type, nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Array)
        assert column.type.element == expected_element_type
        assert column.type.size is None  # PySpark arrays don't have fixed size

    @pytest.mark.parametrize(
        "pyspark_type, expected_key_type, expected_value_type",
        [
            # Map types
            (MapType(StringType(), IntegerType(), valueContainsNull=True), String(), Integer(bits=32, signed=True)),
            (MapType(IntegerType(), StringType(), valueContainsNull=True), Integer(bits=32, signed=True), String()),
            (MapType(StringType(), FloatType(), valueContainsNull=False), String(), Float(bits=32)),
            (MapType(BooleanType(), StringType(), valueContainsNull=True), Boolean(), String()),
        ],
    )
    def test_convert_map_type(
        self, pyspark_type: DataType, expected_key_type: YadsType, expected_value_type: YadsType
    ):
        schema = StructType([StructField("col1", pyspark_type, nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Map)
        assert column.type.key == expected_key_type
        assert column.type.value == expected_value_type
        assert column.type.keys_sorted is False  # PySpark maps are not sorted by default

    def test_convert_struct_type(self):
        schema = StructType([
            StructField("struct_col", StructType([
                StructField("x", IntegerType(), nullable=True),
                StructField("y", StringType(), nullable=True),
                StructField("z", DoubleType(), nullable=False),
            ]), nullable=True)
        ])
        loader = PySparkLoader()
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
        assert field_z.is_nullable is False

    def test_convert_nested_complex_types(self):
        inner_struct = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("metadata", MapType(StringType(), StringType(), valueContainsNull=True), nullable=True),
        ])
        schema = StructType([StructField("nested", ArrayType(inner_struct, containsNull=True), nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")
        
        column = spec.columns[0]
        assert isinstance(column.type, Array)
        
        element_type = column.type.element
        assert isinstance(element_type, Struct)
        assert len(element_type.fields) == 2
        
        id_field = element_type.fields[0]
        assert id_field.name == "id"
        assert id_field.type == Integer(bits=32, signed=True)
        assert id_field.is_nullable is False
        
        metadata_field = element_type.fields[1]
        assert metadata_field.name == "metadata"
        assert isinstance(metadata_field.type, Map)
        assert metadata_field.type.key == String()
        assert metadata_field.type.value == String()
        assert metadata_field.is_nullable is True

    def test_convert_deeply_nested_complex_types(self):
        inner_struct = StructType([
            StructField("id", IntegerType(), nullable=False),
            StructField("data", ArrayType(StringType(), containsNull=True), nullable=True),
        ])
        schema = StructType([
            StructField("complex_col", MapType(
                StringType(), 
                ArrayType(inner_struct, containsNull=True), 
                valueContainsNull=True
            ), nullable=True)
        ])
        loader = PySparkLoader()
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
        assert struct_fields[0].is_nullable is False
        assert struct_fields[1].name == "data"
        assert isinstance(struct_fields[1].type, Array)
        assert struct_fields[1].type.element == String()
        assert struct_fields[1].is_nullable is True
# fmt: on


# %% Field nullability and constraints tests
class TestPySparkLoaderNullability:
    def test_nullable_fields(self):
        schema = StructType(
            [
                StructField("nullable_col", StringType(), nullable=True),
                StructField("non_nullable_col", StringType(), nullable=False),
            ]
        )
        loader = PySparkLoader()
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
        schema = StructType(
            [
                StructField(
                    "struct_col",
                    StructType(
                        [
                            StructField("nullable_field", IntegerType(), nullable=True),
                            StructField(
                                "non_nullable_field", StringType(), nullable=False
                            ),
                        ]
                    ),
                    nullable=True,
                )
            ]
        )
        loader = PySparkLoader()
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
class TestPySparkLoaderMetadata:
    def test_field_metadata_handling(self):
        field_metadata = {
            "description": "A test field",
            "custom_key": "custom_value",
            "numeric_value": 42,
        }
        schema = StructType(
            [
                StructField(
                    "test_col", StringType(), nullable=True, metadata=field_metadata
                )
            ]
        )
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.name == "test_col"
        assert column.description == "A test field"
        assert column.metadata == {"custom_key": "custom_value", "numeric_value": 42}

    def test_description_lifted_from_metadata(self):
        field_metadata = {
            "description": "This is a description",
            "other_key": "other_value",
        }
        schema = StructType(
            [
                StructField(
                    "test_col", StringType(), nullable=True, metadata=field_metadata
                )
            ]
        )
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.description == "This is a description"
        assert column.metadata == {"other_key": "other_value"}

    def test_metadata_with_various_values(self):
        field_metadata = {
            "config_dict": {"retries": 3, "timeout": 30},
            "tags_list": ["tag1", "tag2"],
            "simple_string": "just a string",
            "number": 123,
            "json_string": '{"retries": 3, "timeout": 30}',  # Remains as string
        }
        schema = StructType(
            [
                StructField(
                    "test_col", StringType(), nullable=True, metadata=field_metadata
                )
            ]
        )
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.metadata["config_dict"] == {"retries": 3, "timeout": 30}
        assert column.metadata["tags_list"] == ["tag1", "tag2"]
        assert column.metadata["simple_string"] == "just a string"
        assert column.metadata["number"] == 123
        assert (
            column.metadata["json_string"] == '{"retries": 3, "timeout": 30}'
        )  # No JSON parsing

    def test_metadata_with_string_values(self):
        field_metadata = {
            "json_like_string": '{"key": "value"}',
            "regular_string": "not valid json",
            "number": 123,
        }
        schema = StructType(
            [
                StructField(
                    "test_col", StringType(), nullable=True, metadata=field_metadata
                )
            ]
        )
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert (
            column.metadata["json_like_string"] == '{"key": "value"}'
        )  # Remains as string
        assert column.metadata["regular_string"] == "not valid json"
        assert column.metadata["number"] == 123

    def test_field_with_no_metadata(self):
        schema = StructType([StructField("test_col", StringType(), nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.metadata == {}
        assert column.description is None

    def test_field_with_empty_metadata(self):
        schema = StructType(
            [StructField("test_col", StringType(), nullable=True, metadata={})]
        )
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.metadata == {}
        assert column.description is None

    def test_nested_field_metadata_handling(self):
        inner_struct = StructType(
            [
                StructField(
                    "id",
                    IntegerType(),
                    nullable=False,
                    metadata={"description": "ID field", "primary_key": True},
                ),
                StructField(
                    "name",
                    StringType(),
                    nullable=True,
                    metadata={"description": "Name field", "max_length": 255},
                ),
            ]
        )
        schema = StructType([StructField("nested", inner_struct, nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        nested_col = spec.columns[0]
        assert isinstance(nested_col.type, Struct)

        id_field = nested_col.type.fields[0]
        assert id_field.name == "id"
        assert id_field.description == "ID field"
        assert id_field.metadata == {"primary_key": True}

        name_field = nested_col.type.fields[1]
        assert name_field.name == "name"
        assert name_field.description == "Name field"
        assert name_field.metadata == {"max_length": 255}

    def test_complex_metadata_with_multiple_types(self):
        field_metadata = {
            "description": "Complex metadata field",
            "string_val": "test",
            "int_val": 42,
            "float_val": 3.14,
            "bool_val": True,
            "list_val": [1, 2, 3],
            "dict_val": {"nested": "value"},
            "json_string": '{"parsed": true}',
        }
        schema = StructType(
            [
                StructField(
                    "complex_col", StringType(), nullable=True, metadata=field_metadata
                )
            ]
        )
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.description == "Complex metadata field"
        assert column.metadata == {
            "string_val": "test",
            "int_val": 42,
            "float_val": 3.14,
            "bool_val": True,
            "list_val": [1, 2, 3],
            "dict_val": {"nested": "value"},
            "json_string": '{"parsed": true}',  # Remains as string
        }

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_metadata_preserved_with_fallback_types(self):
        """Test that metadata is preserved when using fallback types."""
        field_metadata = {
            "description": "Unsupported type field",
            "source": "external_system",
            "version": "v1.0",
        }
        schema = StructType(
            [
                StructField(
                    "unsupported_col",
                    CalendarIntervalType(),
                    nullable=True,
                    metadata=field_metadata,
                )
            ]
        )
        config = PySparkLoaderConfig(mode="coerce", fallback_type=String())
        loader = PySparkLoader(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test_spec", version="1.0.0")

            assert len(w) == 1
            assert "PySparkLoader does not support PySpark type" in str(w[0].message)

        column = spec.columns[0]
        assert column.name == "unsupported_col"
        assert column.type == String()  # coerced
        assert column.description == "Unsupported type field"  # preserved
        assert column.metadata == {
            "source": "external_system",
            "version": "v1.0",
        }  # preserved

    def test_metadata_with_nullability_constraints(self):
        """Test that metadata is preserved along with nullability constraints."""
        field_metadata = {
            "description": "Non-nullable field with metadata",
            "required": True,
            "validation": {"min_length": 1},  # Already a dict
        }
        schema = StructType(
            [
                StructField(
                    "required_col", StringType(), nullable=False, metadata=field_metadata
                )
            ]
        )
        loader = PySparkLoader()
        spec = loader.load(schema, name="test_spec", version="1.0.0")

        column = spec.columns[0]
        assert column.name == "required_col"
        assert column.is_nullable is False
        assert len(column.constraints) == 1
        assert isinstance(column.constraints[0], NotNullConstraint)
        assert column.description == "Non-nullable field with metadata"
        assert column.metadata == {
            "required": True,
            "validation": {"min_length": 1},
        }


# %% Schema-level tests
class TestPySparkLoaderSchema:
    def test_schema_without_description(self):
        schema = StructType([StructField("id", IntegerType(), nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(schema, name="test", version="1.0.0")

        assert spec.name == "test"
        assert spec.version == "1.0.0"
        assert spec.description is None

    def test_empty_schema(self):
        schema = StructType([])
        loader = PySparkLoader()
        spec = loader.load(schema, name="empty", version="1.0.0")

        assert spec.name == "empty"
        assert spec.version == "1.0.0"
        assert len(spec.columns) == 0

    def test_schema_with_description(self):
        schema = StructType([StructField("id", IntegerType(), nullable=True)])
        loader = PySparkLoader()
        spec = loader.load(
            schema, name="test", version="1.0.0", description="Test description"
        )

        assert spec.name == "test"
        assert spec.version == "1.0.0"
        assert spec.description == "Test description"


# %% Unsupported types and error handling
class TestPySparkLoaderUnsupportedTypes:
    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_calendar_interval_type_raises_error(self):
        schema = StructType(
            [StructField("interval_col", CalendarIntervalType(), nullable=True)]
        )
        config = PySparkLoaderConfig(mode="raise")
        loader = PySparkLoader(config)

        with pytest.raises(
            UnsupportedFeatureError,
            match="PySparkLoader does not support PySpark type.*for 'interval_col'",
        ):
            loader.load(schema, name="test", version="1.0.0")


# %% Configuration tests
class TestPySparkLoaderConfig:
    def test_default_config(self):
        config = PySparkLoaderConfig()
        assert config.mode == "coerce"
        assert config.fallback_type is None

    def test_custom_config(self):
        config = PySparkLoaderConfig(mode="raise", fallback_type=Binary())
        assert config.mode == "raise"
        assert config.fallback_type == Binary()

    def test_invalid_mode_raises_error(self):
        with pytest.raises(
            LoaderConfigError, match="mode must be one of 'raise' or 'coerce'"
        ):
            PySparkLoaderConfig(mode="invalid")

    def test_invalid_fallback_type_raises_error(self):
        with pytest.raises(
            LoaderConfigError, match="fallback_type must be either String or Binary"
        ):
            PySparkLoaderConfig(fallback_type=Integer())

    def test_config_is_immutable(self):
        config = PySparkLoaderConfig(mode="raise")
        # Config should be frozen
        with pytest.raises(AttributeError):
            config.mode = "coerce"


class TestPySparkLoaderWithConfig:
    def test_loader_with_default_config(self):
        loader = PySparkLoader()
        assert loader.config.mode == "coerce"
        assert loader.config.fallback_type is None

    def test_loader_with_custom_config(self):
        config = PySparkLoaderConfig(mode="raise", fallback_type=Binary())
        loader = PySparkLoader(config)
        assert loader.config.mode == "raise"
        assert loader.config.fallback_type == Binary()

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_mode_override_in_load_method(self):
        config = PySparkLoaderConfig(mode="coerce")
        loader = PySparkLoader(config)
        schema = StructType(
            [StructField("interval_col", CalendarIntervalType(), nullable=True)]
        )

        with pytest.raises(UnsupportedFeatureError):
            loader.load(schema, name="test", version="1.0.0", mode="raise")

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_coercion_mode_with_default_fallback(self):
        config = PySparkLoaderConfig(mode="coerce", fallback_type=String())
        loader = PySparkLoader(config)
        schema = StructType(
            [StructField("interval_col", CalendarIntervalType(), nullable=True)]
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PySparkLoader does not support PySpark type" in str(w[0].message)
            assert "for 'interval_col'" in str(w[0].message)
            assert "The data type will be coerced to string" in str(w[0].message)

        column = spec.columns[0]
        assert column.name == "interval_col"
        assert column.type == String()

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_coercion_mode_with_custom_fallback(self):
        config = PySparkLoaderConfig(mode="coerce", fallback_type=Binary(length=10))
        loader = PySparkLoader(config)
        schema = StructType(
            [StructField("interval_col", CalendarIntervalType(), nullable=True)]
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PySparkLoader does not support PySpark type" in str(w[0].message)
            assert "for 'interval_col'" in str(w[0].message)
            assert "The data type will be coerced to binary(length=10)" in str(
                w[0].message
            )

        column = spec.columns[0]
        assert column.name == "interval_col"
        assert column.type == Binary(length=10)

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_raise_mode_with_unsupported_types(self):
        config = PySparkLoaderConfig(mode="raise")
        loader = PySparkLoader(config)

        schema = StructType(
            [StructField("interval_col", CalendarIntervalType(), nullable=True)]
        )
        with pytest.raises(
            UnsupportedFeatureError, match="PySparkLoader does not support PySpark type"
        ):
            loader.load(schema, name="test", version="1.0.0")

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_multiple_unsupported_types_coercion(self):
        config = PySparkLoaderConfig(mode="coerce", fallback_type=String())
        loader = PySparkLoader(config)

        schema = StructType(
            [
                StructField("interval_col", CalendarIntervalType(), nullable=True),
                StructField("normal_col", StringType(), nullable=True),
            ]
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PySparkLoader does not support PySpark type" in str(w[0].message)
            assert "for 'interval_col'" in str(w[0].message)

        assert len(spec.columns) == 2
        assert spec.columns[0].name == "interval_col"
        assert spec.columns[0].type == String()  # coerced
        assert spec.columns[1].name == "normal_col"
        assert spec.columns[1].type == String()  # normal conversion

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_field_context_in_error_messages(self):
        config = PySparkLoaderConfig(mode="raise")
        loader = PySparkLoader(config)

        schema = StructType(
            [StructField("my_field", CalendarIntervalType(), nullable=True)]
        )

        with pytest.raises(UnsupportedFeatureError) as exc_info:
            loader.load(schema, name="test", version="1.0.0")

        assert "for 'my_field'" in str(exc_info.value)

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_nested_unsupported_types_coercion(self):
        config = PySparkLoaderConfig(mode="coerce", fallback_type=String())
        loader = PySparkLoader(config)

        # Create a struct with an unsupported field
        inner_struct = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("interval_field", CalendarIntervalType(), nullable=True),
            ]
        )
        schema = StructType([StructField("nested", inner_struct, nullable=True)])

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PySparkLoader does not support PySpark type" in str(w[0].message)
            assert "for 'interval_field'" in str(w[0].message)

        # Check that only the unsupported field within the struct was coerced
        column = spec.columns[0]
        assert column.name == "nested"
        assert isinstance(column.type, Struct)  # struct preserved
        assert len(column.type.fields) == 2

        # Check individual fields
        id_field = next(f for f in column.type.fields if f.name == "id")
        assert isinstance(id_field.type, Integer)  # normal field preserved

        interval_field = next(f for f in column.type.fields if f.name == "interval_field")
        assert isinstance(interval_field.type, String)  # unsupported field coerced

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_fallback_preserves_field_nullability(self):
        """Test that field nullability is preserved during fallback."""
        schema = StructType(
            [StructField("interval_col", CalendarIntervalType(), nullable=False)]
        )
        config = PySparkLoaderConfig(mode="coerce", fallback_type=String())
        loader = PySparkLoader(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PySparkLoader does not support PySpark type" in str(w[0].message)
            assert "for 'interval_col'" in str(w[0].message)

        column = spec.columns[0]
        assert column.name == "interval_col"
        assert column.type == String()  # coerced
        assert column.is_nullable is False  # preserved
        assert len(column.constraints) == 1
        assert isinstance(column.constraints[0], NotNullConstraint)

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_fallback_preserves_field_nullability_with_binary_fallback(self):
        """Test that field nullability is preserved with binary fallback."""
        schema = StructType(
            [StructField("interval_col", CalendarIntervalType(), nullable=True)]
        )
        config = PySparkLoaderConfig(mode="coerce", fallback_type=Binary(length=10))
        loader = PySparkLoader(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test", version="1.0.0")

            assert len(w) == 1
            assert "PySparkLoader does not support PySpark type" in str(w[0].message)
            assert "for 'interval_col'" in str(w[0].message)

        column = spec.columns[0]
        assert column.name == "interval_col"
        assert column.type == Binary(length=10)  # coerced
        assert column.is_nullable is True  # preserved
        assert len(column.constraints) == 0

    @pytest.mark.skipif(
        not HAS_CALENDAR_INTERVAL_TYPE, reason="CalendarIntervalType not available"
    )
    def test_complex_nested_fallback_behavior(self):
        inner_struct = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("unsupported_field", CalendarIntervalType(), nullable=True),
                StructField("normal_string", StringType(), nullable=True),
            ]
        )

        array_of_structs = ArrayType(inner_struct, containsNull=True)

        map_with_unsupported = MapType(
            StringType(), array_of_structs, valueContainsNull=True
        )

        schema = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("complex_data", map_with_unsupported, nullable=True),
            ]
        )

        config = PySparkLoaderConfig(mode="coerce", fallback_type=String())
        loader = PySparkLoader(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            spec = loader.load(schema, name="test_complex", version="1.0.0")

        # Should have warnings for the unsupported type
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)

        warning_message = str(w[0].message)
        assert "PySparkLoader does not support PySpark type" in warning_message
        assert "unsupported_field" in warning_message

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

        # Map key should be preserved
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
