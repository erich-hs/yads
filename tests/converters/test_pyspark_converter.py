import warnings

import pytest

from yads.converters import PySparkConverter, PySparkConverterConfig
from yads.constraints import NotNullConstraint
from yads.spec import YadsSpec, Column, Field
from yads.types import (
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
    TimestampLTZ,
    TimestampNTZ,
    Duration,
    IntervalTimeUnit,
    Interval,
    Array,
    Struct,
    Map,
    JSON,
    Geometry,
    Geography,
    UUID,
    Void,
    Variant,
    Tensor,
)
from yads.exceptions import (
    UnsupportedFeatureError,
    ValidationWarning,
    ConverterConfigError,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    BooleanType,
    BinaryType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
    NullType,
)

# Optional types that may not be available in older PySpark versions

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
    from pyspark.sql.types import DayTimeIntervalType  # type: ignore[attr-defined]

    HAS_DAY_TIME_INTERVAL_TYPE = True
except ImportError:
    DayTimeIntervalType = None  # type: ignore[assignment, misc]
    HAS_DAY_TIME_INTERVAL_TYPE = False

try:
    from pyspark.sql.types import VariantType  # type: ignore[attr-defined]

    HAS_VARIANT_TYPE = True
except ImportError:
    VariantType = None  # type: ignore[assignment, misc]
    HAS_VARIANT_TYPE = False


# fmt: off
# %% Types
class TestPySparkConverterTypes:
    @pytest.mark.parametrize(
        "yads_type, expected_spark_type, expected_warning",
        [
            # String types
            (String(), StringType(), None),
            pytest.param(
                String(length=255),
                StringType(),
                "String with fixed length is not supported in PySpark DataFrame schemas",
                id="string_with_length"
            ),
            
            # Integer types - signed
            (Integer(bits=8), ByteType(), None),
            (Integer(bits=16), ShortType(), None),
            (Integer(bits=32), IntegerType(), None),
            (Integer(bits=64), LongType(), None),
            
            # Integer types - unsigned (coerced in coerce mode)
            (Integer(bits=8, signed=False), ShortType(), "Unsigned Integer(bits=8)"),
            (Integer(bits=16, signed=False), IntegerType(), "Unsigned Integer(bits=16)"),
            (Integer(bits=32, signed=False), LongType(), "Unsigned Integer(bits=32)"),
            (Integer(bits=64, signed=False), DecimalType(20, 0), "Unsigned Integer(bits=64)"),
            
            # Float types
            (Float(bits=16), FloatType(), "float(bits=16)"),  # coerced
            (Float(bits=32), FloatType(), None),
            (Float(bits=64), DoubleType(), None),
            
            # Decimal types
            (Decimal(), DecimalType(38, 18), None),
            (Decimal(precision=10, scale=2), DecimalType(10, 2), None),
            (Decimal(precision=38, scale=10), DecimalType(38, 10), None),
            
            # Boolean type
            (Boolean(), BooleanType(), None),
            
            # Binary types
            (Binary(), BinaryType(), None),
            (Binary(length=8), BinaryType(), "length constraint will be lost"),  # length ignored - constraint loss
            
            # Date type
            (Date(), DateType(), None),
            (Date(bits=32), DateType(), "bits constraint will be lost"),  # bits ignored - constraint loss
            (Date(bits=64), DateType(), "bits constraint will be lost"),  # bits ignored - constraint loss
            
            # Timestamp types
            (Timestamp(), TimestampType(), "unit constraint will be lost"),  # default unit=NS, constraint loss
            (Timestamp(unit=TimeUnit.S), TimestampType(), "unit constraint will be lost"),  # unit ignored - constraint loss
            (TimestampTZ(tz="UTC"), TimestampType(), "tz constraints will be lost"),  # tz ignored - constraint loss
            (TimestampLTZ(), TimestampType(), "unit constraint will be lost"),  # default unit=NS, constraint loss
            pytest.param(
                TimestampNTZ(),
                TimestampNTZType() if HAS_TIMESTAMP_NTZ_TYPE else StringType(),
                "unit constraint will be lost" if HAS_TIMESTAMP_NTZ_TYPE else "TimestampNTZ type",  # default unit=NS, constraint loss
                id="timestamp_ntz"
            ),
            
            # Void type
            (Void(), NullType(), None),
        ],
    )
    def test_primitive_types_coerce_mode(self, yads_type, expected_spark_type, expected_warning):
        """Test primitive type conversions in coerce mode."""
        converter = PySparkConverter(
            PySparkConverterConfig(mode="coerce", fallback_type=StringType())
        )
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_type(yads_type)
            
            assert result == expected_spark_type
            
            if expected_warning:
                assert len(w) == 1
                assert issubclass(w[0].category, ValidationWarning)
                assert expected_warning in str(w[0].message)
            else:
                # Filter out any unrelated warnings
                validation_warnings = [warning for warning in w 
                                     if issubclass(warning.category, ValidationWarning)]
                assert len(validation_warnings) == 0

    @pytest.mark.parametrize(
        "yads_type, expected_error",
        [
            # Unsigned integers should raise in raise mode
            (Integer(bits=8, signed=False), "Unsigned Integer.*is not supported"),
            (Integer(bits=16, signed=False), "Unsigned Integer.*is not supported"),
            (Integer(bits=32, signed=False), "Unsigned Integer.*is not supported"),
            (Integer(bits=64, signed=False), "Unsigned Integer.*is not supported"),
            
            # Float(bits=16) should raise in raise mode
            (Float(bits=16), "PySparkConverter does not support type: float(bits=16)*"),
            
            # Unsupported types
            (Time(), "does not support type"),
            (Duration(), "does not support type"),
            (JSON(), "does not support type"),
            (Geometry(), "does not support type"),
            (Geography(), "does not support type"),
            (UUID(), "does not support type"),
            (Tensor(element=Float(), shape=(2, 3)), "does not support type"),
        ],
    )
    def test_unsupported_types_raise_mode(self, yads_type, expected_error):
        """Test that unsupported types raise errors in raise mode."""
        converter = PySparkConverter(PySparkConverterConfig(mode="raise"))
        
        with pytest.raises(UnsupportedFeatureError, match=expected_error):
            converter._convert_type(yads_type)

    @pytest.mark.parametrize(
        "yads_type",
        [
            Time(),
            Duration(),
            JSON(),
            Geometry(),
            Geography(),
            UUID(),
            Tensor(element=Float(), shape=(2, 3)),
        ],
    )
    def test_unsupported_types_coerce_mode(self, yads_type):
        """Test that unsupported types are coerced to fallback type in coerce mode."""
        converter = PySparkConverter(
            PySparkConverterConfig(mode="coerce", fallback_type=StringType())
        )
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_type(yads_type)
            
            assert result == StringType()
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)

    @pytest.mark.skipif(
        not HAS_YEAR_MONTH_INTERVAL_TYPE, 
        reason="YearMonthIntervalType not available"
    )
    def test_year_month_interval_type(self):
        """Test year-month interval type conversions."""
        converter = PySparkConverter()
        
        # Year-Month intervals
        year_month_interval = Interval(
            interval_start=IntervalTimeUnit.YEAR, 
            interval_end=IntervalTimeUnit.MONTH
        )
        result = converter._convert_type(year_month_interval)
        assert result == YearMonthIntervalType(0, 1)  # YEAR to MONTH

    @pytest.mark.skipif(
        not HAS_DAY_TIME_INTERVAL_TYPE, 
        reason="DayTimeIntervalType not available"
    )
    def test_day_time_interval_type(self):
        """Test day-time interval type conversions."""
        converter = PySparkConverter()
        
        # Day-Time intervals
        day_second_interval = Interval(
            interval_start=IntervalTimeUnit.DAY, 
            interval_end=IntervalTimeUnit.SECOND
        )
        result = converter._convert_type(day_second_interval)
        assert result == DayTimeIntervalType(0, 3)  # DAY to SECOND

    @pytest.mark.skipif(
        HAS_YEAR_MONTH_INTERVAL_TYPE, 
        reason="Only test fallback when YearMonthIntervalType unavailable"
    )
    def test_year_month_interval_fallback_coerce_mode(self):
        """Test year-month interval fallback when type not available."""
        converter = PySparkConverter(
            PySparkConverterConfig(mode="coerce", fallback_type=StringType())
        )
        
        year_month_interval = Interval(
            interval_start=IntervalTimeUnit.YEAR, 
            interval_end=IntervalTimeUnit.MONTH
        )
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_type(year_month_interval)
            
            assert result == StringType()
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert "year-month" in str(w[0].message).lower()

    @pytest.mark.skipif(
        HAS_DAY_TIME_INTERVAL_TYPE, 
        reason="Only test fallback when DayTimeIntervalType unavailable"
    )
    def test_day_time_interval_fallback_coerce_mode(self):
        """Test day-time interval fallback when type not available."""
        converter = PySparkConverter(
            PySparkConverterConfig(mode="coerce", fallback_type=StringType())
        )
        
        day_second_interval = Interval(
            interval_start=IntervalTimeUnit.DAY, 
            interval_end=IntervalTimeUnit.SECOND
        )
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_type(day_second_interval)
            
            assert result == StringType()
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert "day-time" in str(w[0].message).lower()

    def test_array_types(self):
        """Test array type conversions."""
        converter = PySparkConverter()
        
        # Simple array
        array_type = Array(element=String())
        result = converter._convert_type(array_type)
        expected = ArrayType(StringType(), True)  # containsNull=True by default
        assert result == expected
        
        # Array with size - constraint loss warning
        sized_array = Array(element=Integer(bits=32), size=5)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_type(sized_array)
        
        expected = ArrayType(IntegerType(), True)
        assert result == expected
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "size constraint will be lost" in str(w[0].message)

    def test_struct_types(self):
        """Test struct type conversions."""
        converter = PySparkConverter()
        
        struct_type = Struct(fields=[
            Field(name="id", type=Integer(bits=64)),
            Field(name="name", type=String()),
            Field(name="active", type=Boolean()),
        ])
        
        result = converter._convert_type(struct_type)
        expected = StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("active", BooleanType(), True),
        ])
        assert result == expected

    def test_map_types(self):
        """Test map type conversions."""
        converter = PySparkConverter()
        
        # Simple map
        map_type = Map(key=String(), value=Integer(bits=32))
        result = converter._convert_type(map_type)
        expected = MapType(StringType(), IntegerType(), True)  # valueContainsNull=True
        assert result == expected
        
        # Map with keys_sorted (ignored)
        sorted_map = Map(key=String(), value=Float(), keys_sorted=True)
        result = converter._convert_type(sorted_map)
        expected = MapType(StringType(), FloatType(), True)
        assert result == expected

    @pytest.mark.skipif(
        not HAS_VARIANT_TYPE, 
        reason="VariantType not available"
    )
    def test_variant_type_available(self):
        """Test variant type when VariantType is available."""
        converter = PySparkConverter()
        result = converter._convert_type(Variant())
        assert result == VariantType()

    @pytest.mark.skipif(
        HAS_VARIANT_TYPE, 
        reason="Only test fallback when VariantType unavailable"
    )
    def test_variant_type_fallback_coerce_mode(self):
        """Test variant type fallback when type not available."""
        converter = PySparkConverter(
            PySparkConverterConfig(mode="coerce", fallback_type=StringType())
        )
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_type(Variant())
            
            assert result == StringType()
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert "variant" in str(w[0].message).lower()
# fmt: on


# %% Field conversion
class TestPySparkConverterFields:
    def test_field_conversion(self):
        """Test complete field conversion with metadata."""
        converter = PySparkConverter(PySparkConverterConfig(mode="coerce"))

        field = Field(
            name="test_field",
            type=String(length=100),
            description="Test field description",
            metadata={"custom": "value"},
            constraints=[NotNullConstraint()],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter._convert_field(field)

            expected = StructField(
                "test_field",
                StringType(),
                nullable=False,
                metadata={"description": "Test field description", "custom": "value"},
            )
            assert result == expected

            # Check for warning about fixed length string
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)

    def test_field_conversion_no_metadata(self):
        """Test field conversion without metadata."""
        converter = PySparkConverter()

        field = Field(name="simple_field", type=Integer(bits=32))

        result = converter._convert_field(field)
        expected = StructField("simple_field", IntegerType(), True, None)
        assert result == expected

    def test_field_nullable_handling(self):
        """Test nullable field handling."""
        from yads.constraints import NotNullConstraint

        converter = PySparkConverter()

        # Nullable field (default)
        nullable_field = Field(name="nullable", type=String())
        result = converter._convert_field(nullable_field)
        assert result.nullable is True

        # Non-nullable field (with constraint)
        non_nullable_field = Field(
            name="not_null", type=String(), constraints=[NotNullConstraint()]
        )
        result = converter._convert_field(non_nullable_field)
        assert result.nullable is False


# %% Schema conversion
class TestPySparkConverterSchema:
    def test_basic_schema_conversion(self):
        """Test basic schema conversion."""
        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(bits=64)),
                Column(name="name", type=String()),
                Column(name="active", type=Boolean()),
            ],
        )

        converter = PySparkConverter()
        result = converter.convert(spec)

        expected = StructType(
            [
                StructField("id", LongType(), True),
                StructField("name", StringType(), True),
                StructField("active", BooleanType(), True),
            ]
        )
        assert result == expected

    def test_complex_schema_conversion(self):
        """Test complex schema with nested types."""
        spec = YadsSpec(
            name="complex_table",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(bits=64)),
                Column(
                    name="address",
                    type=Struct(
                        fields=[
                            Field(name="street", type=String()),
                            Field(name="city", type=String()),
                            Field(name="zip", type=String(length=10)),
                        ]
                    ),
                ),
                Column(name="tags", type=Array(element=String())),
                Column(name="metadata", type=Map(key=String(), value=String())),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(mode="coerce"))

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = converter.convert(spec)

            expected_zip_type = StringType()
            expected = StructType(
                [
                    StructField("id", LongType(), True),
                    StructField(
                        "address",
                        StructType(
                            [
                                StructField("street", StringType(), True),
                                StructField("city", StringType(), True),
                                StructField("zip", expected_zip_type, True),
                            ]
                        ),
                        True,
                    ),
                    StructField("tags", ArrayType(StringType(), True), True),
                    StructField(
                        "metadata", MapType(StringType(), StringType(), True), True
                    ),
                ]
            )
            assert result == expected


# %% Configuration
class TestPySparkConverterConfig:
    def test_default_config(self):
        """Test default configuration."""
        config = PySparkConverterConfig()
        assert config.mode == "coerce"
        assert config.ignore_columns == frozenset()
        assert config.include_columns is None
        assert config.column_overrides == {}
        assert config.fallback_type is None

    def test_custom_fallback_type(self):
        """Test custom fallback type."""
        custom_fallback = BinaryType()
        config = PySparkConverterConfig(fallback_type=custom_fallback)
        assert config.fallback_type == custom_fallback

    def test_column_filtering(self):
        """Test column filtering functionality."""
        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(bits=64)),
                Column(name="name", type=String()),
                Column(name="email", type=String()),
            ],
        )

        # Test ignore_columns
        config = PySparkConverterConfig(ignore_columns=frozenset(["email"]))
        converter = PySparkConverter(config)
        result = converter.convert(spec)

        assert len(result.fields) == 2
        field_names = [f.name for f in result.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "email" not in field_names

        # Test include_columns
        config = PySparkConverterConfig(include_columns=frozenset(["id", "name"]))
        converter = PySparkConverter(config)
        result = converter.convert(spec)

        assert len(result.fields) == 2
        field_names = [f.name for f in result.fields]
        assert "id" in field_names
        assert "name" in field_names
        assert "email" not in field_names

    def test_column_overrides(self):
        """Test basic column override functionality (returns StructField)."""

        def custom_name_override(field, converter):
            return StructField(
                field.name,
                BinaryType(),
                nullable=field.is_nullable,
                metadata={"custom": "true", "override": "applied"},
            )

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(bits=64)),
                Column(name="name", type=String()),
            ],
        )

        config = PySparkConverterConfig(column_overrides={"name": custom_name_override})
        converter = PySparkConverter(config)
        result = converter.convert(spec)

        # Check that the override was applied
        name_field = next(f for f in result.fields if f.name == "name")
        assert name_field.dataType == BinaryType()
        assert name_field.metadata == {"custom": "true", "override": "applied"}

        # Check that other fields are unaffected
        id_field = next(f for f in result.fields if f.name == "id")
        assert id_field.dataType == LongType()

    def test_precedence_ignore_over_override(self):
        """ignore_columns takes precedence over column_overrides."""

        def should_not_be_called(field, converter):
            pytest.fail("Override should not be called for ignored column")

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(bits=64)),
                Column(name="ignored_col", type=String()),
            ],
        )

        config = PySparkConverterConfig(
            ignore_columns=frozenset(["ignored_col"]),
            column_overrides={"ignored_col": should_not_be_called},
        )
        converter = PySparkConverter(config)
        result = converter.convert(spec)

        field_names = [f.name for f in result.fields]
        assert "id" in field_names
        assert "ignored_col" not in field_names

    def test_precedence_override_over_default_conversion(self):
        """column_overrides takes precedence over default conversion."""

        def integer_as_string_override(field, converter):
            # Use StringType for string overrides
            override_type = StringType()
            return StructField(
                field.name,
                override_type,
                nullable=field.is_nullable,
                metadata={"converted_from": "integer"},
            )

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="normal_int", type=Integer()),
                Column(name="varchar_int", type=Integer()),
            ],
        )

        config = PySparkConverterConfig(
            column_overrides={"varchar_int": integer_as_string_override}
        )
        converter = PySparkConverter(config)
        result = converter.convert(spec)

        normal_field = next(f for f in result.fields if f.name == "normal_int")
        assert normal_field.dataType == IntegerType()

        string_field = next(f for f in result.fields if f.name == "varchar_int")
        expected_type = StringType()
        assert string_field.dataType == expected_type
        assert string_field.metadata == {"converted_from": "integer"}

    def test_precedence_override_over_fallback(self):
        """column_overrides takes precedence over fallback_type in coerce mode."""

        def custom_geometry_override(field, converter):
            return StructField(
                field.name,
                StringType(),
                nullable=field.is_nullable,
                metadata={"custom_geometry": "true"},
            )

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="fallback_geom", type=Geometry()),
                Column(name="override_geom", type=Geometry()),
            ],
        )

        config = PySparkConverterConfig(
            fallback_type=BinaryType(),
            column_overrides={"override_geom": custom_geometry_override},
        )
        converter = PySparkConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter.convert(spec, mode="coerce")

        fallback_field = next(f for f in result.fields if f.name == "fallback_geom")
        assert fallback_field.dataType == BinaryType()

        override_field = next(f for f in result.fields if f.name == "override_geom")
        assert override_field.dataType == StringType()
        assert override_field.metadata == {"custom_geometry": "true"}

        # Only one warning for the fallback field
        assert len([x for x in w if issubclass(x.category, ValidationWarning)]) == 1
        assert "fallback_geom" in str(w[0].message)

    def test_column_override_preserves_nullability(self):
        """Column overrides should preserve original field nullability."""

        def nullable_override(field, converter):
            override_type = StringType()
            return StructField(
                field.name,
                override_type,
                nullable=field.is_nullable,
                metadata={"nullable_preserved": str(field.is_nullable)},
            )

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="nullable_col", type=String()),
                Column(
                    name="non_null_col",
                    type=String(),
                    constraints=[NotNullConstraint()],
                ),
            ],
        )

        config = PySparkConverterConfig(
            column_overrides={
                "nullable_col": nullable_override,
                "non_null_col": nullable_override,
            }
        )
        converter = PySparkConverter(config)
        result = converter.convert(spec)

        nullable_field = next(f for f in result.fields if f.name == "nullable_col")
        assert nullable_field.nullable is True
        assert nullable_field.metadata == {"nullable_preserved": "True"}

        non_null_field = next(f for f in result.fields if f.name == "non_null_col")
        assert non_null_field.nullable is False
        assert non_null_field.metadata == {"nullable_preserved": "False"}

    def test_column_override_with_original_field_access(self):
        """Overrides can inspect the original field properties to build metadata."""

        def field_inspector_override(field, converter):
            meta = {
                "original_type": type(field.type).__name__,
                "has_description": str(field.description is not None),
                "constraint_count": str(len(field.constraints)),
            }
            if hasattr(field.type, "length") and field.type.length is not None:
                meta["original_length"] = str(field.type.length)
            return StructField(
                field.name, StringType(), nullable=field.is_nullable, metadata=meta
            )

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(
                    name="inspected_col",
                    type=String(length=100),
                    description="A test column",
                    constraints=[NotNullConstraint()],
                ),
            ],
        )
        config = PySparkConverterConfig(
            column_overrides={"inspected_col": field_inspector_override}
        )
        converter = PySparkConverter(config)
        result = converter.convert(spec)

        inspected_field = next(f for f in result.fields if f.name == "inspected_col")
        assert inspected_field.metadata is not None
        assert inspected_field.metadata.get("original_type") == "String"
        assert inspected_field.metadata.get("has_description") == "True"
        assert inspected_field.metadata.get("constraint_count") == "1"
        assert inspected_field.metadata.get("original_length") == "100"

    def test_invalid_column_filters(self):
        """Test validation of column filter configuration."""
        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[Column(name="id", type=Integer())],
        )

        # Test unknown ignored column
        config = PySparkConverterConfig(ignore_columns=frozenset(["nonexistent"]))
        converter = PySparkConverter(config)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in ignore_columns"
        ):
            converter.convert(spec)

        # Test unknown included column
        config_inc = PySparkConverterConfig(include_columns=frozenset(["missing"]))
        converter_inc = PySparkConverter(config_inc)
        with pytest.raises(
            ConverterConfigError, match="Unknown columns in include_columns"
        ):
            converter_inc.convert(spec)

        # Test conflicting ignore and include
        with pytest.raises(
            ConverterConfigError, match="Columns cannot be both ignored and included"
        ):
            PySparkConverterConfig(
                ignore_columns=frozenset(["id", "x"]),
                include_columns=frozenset(["id", "y"]),
            )

    def test_conversion_mode_override(self):
        """Test conversion mode override in convert method."""
        converter = PySparkConverter(
            PySparkConverterConfig(mode="raise", fallback_type=StringType())
        )

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[Column(name="unsupported", type=Time())],
        )

        # Should raise with default mode
        with pytest.raises(UnsupportedFeatureError):
            converter.convert(spec)

        # Should coerce with mode override
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = converter.convert(spec, mode="coerce")

            assert len(result.fields) == 1
            assert result.fields[0].dataType == StringType()
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)

    def test_call_override_to_coerce_does_not_persist(self):
        """Ensure per-call mode override does not persist across calls."""
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="c", type=Geometry())],
        )
        converter = PySparkConverter(
            PySparkConverterConfig(mode="raise", fallback_type=StringType())
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")
        assert isinstance(schema, StructType)
        assert any(issubclass(x.category, ValidationWarning) for x in w)

        # Next call without override should raise again
        with pytest.raises(UnsupportedFeatureError):
            converter.convert(spec)

    def test_invalid_fallback_type_raises_error(self):
        """Invalid fallback_type should raise at config creation."""
        with pytest.raises(
            UnsupportedFeatureError,
            match=r"fallback_type must be one of: StringType\(\), BinaryType\(\)",
        ):
            PySparkConverterConfig(fallback_type=IntegerType())

    @pytest.mark.parametrize("fallback_type", [StringType(), BinaryType()])
    def test_valid_fallback_types(self, fallback_type):
        """Test fallback_type application for unsupported types in coerce mode."""
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="geom", type=Geometry()),
            ],
        )
        cfg = PySparkConverterConfig(fallback_type=fallback_type)
        converter = PySparkConverter(cfg)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")
        geom_field = next(f for f in schema.fields if f.name == "geom")
        assert type(geom_field.dataType) is type(fallback_type)
        assert any(issubclass(x.category, ValidationWarning) for x in w)

    def test_field_metadata_preservation_with_fallback(self):
        """Ensure description/metadata preserved when fallback is applied."""
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(
                    name="geom",
                    type=Geometry(),
                    description="A geometry field",
                    metadata={"srid": "4326", "precision": "high"},
                )
            ],
        )
        converter = PySparkConverter(PySparkConverterConfig(fallback_type=StringType()))
        with warnings.catch_warnings(record=True) as _w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")
        geom_field = next(f for f in schema.fields if f.name == "geom")
        assert geom_field.dataType == StringType()
        assert geom_field.metadata is not None
        assert geom_field.metadata.get("description") == "A geometry field"
        assert geom_field.metadata.get("srid") == "4326"
        assert geom_field.metadata.get("precision") == "high"

    def test_field_description_preservation_with_fallback(self):
        """Ensure field description is preserved when fallback is applied."""
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(
                    name="geo",
                    type=Geography(),
                    description="Region",
                    metadata={"shape": "polygon"},
                )
            ],
        )
        converter = PySparkConverter(PySparkConverterConfig(fallback_type=StringType()))
        with warnings.catch_warnings(record=True) as _w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")
        field = next(f for f in schema.fields if f.name == "geo")
        assert field.dataType == StringType()
        assert field.metadata is not None
        assert field.metadata.get("description") == "Region"
        assert field.metadata.get("shape") == "polygon"


# %% Error handling
class TestPySparkConverterErrors:
    def test_invalid_integer_bits(self):
        """Test error for invalid integer bit sizes."""
        # Invalid bit sizes are caught at the type definition level
        with pytest.raises(Exception):  # TypeDefinitionError
            Integer(bits=128)

    def test_invalid_float_bits(self):
        """Test error for invalid float bit sizes."""
        # Invalid bit sizes are caught at the type definition level
        with pytest.raises(Exception):  # TypeDefinitionError
            Float(bits=128)


# %% Integration tests
class TestPySparkConverterIntegration:
    def test_end_to_end_conversion(self):
        """Test complete end-to-end conversion with various types."""
        spec = YadsSpec(
            name="comprehensive_table",
            version="1.0.0",
            columns=[
                # Primitive types
                Column(
                    name="id", type=Integer(bits=64), constraints=[NotNullConstraint()]
                ),
                Column(name="name", type=String(length=100)),
                Column(name="score", type=Float(bits=64)),
                Column(name="amount", type=Decimal(precision=10, scale=2)),
                Column(name="active", type=Boolean()),
                Column(name="data", type=Binary()),
                Column(name="created_date", type=Date()),
                Column(name="updated_at", type=Timestamp()),
                # Complex types
                Column(
                    name="address",
                    type=Struct(
                        fields=[
                            Field(name="street", type=String()),
                            Field(name="city", type=String()),
                            Field(name="coordinates", type=Array(element=Float(bits=64))),
                        ]
                    ),
                ),
                Column(name="tags", type=Array(element=String())),
                Column(name="properties", type=Map(key=String(), value=String())),
                # Interval type
                Column(
                    name="duration",
                    type=Interval(
                        interval_start=IntervalTimeUnit.DAY,
                        interval_end=IntervalTimeUnit.SECOND,
                    ),
                ),
            ],
        )

        converter = PySparkConverter(
            PySparkConverterConfig(mode="coerce", fallback_type=StringType())
        )

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = converter.convert(spec)

            # Verify the schema structure
            assert isinstance(result, StructType)
            assert len(result.fields) == 12

            # Check specific field types
            field_dict = {f.name: f for f in result.fields}

            assert field_dict["id"].dataType == LongType()
            assert field_dict["id"].nullable is False

            expected_name_type = StringType()
            assert field_dict["name"].dataType == expected_name_type
            assert field_dict["score"].dataType == DoubleType()
            assert field_dict["amount"].dataType == DecimalType(10, 2)
            assert field_dict["active"].dataType == BooleanType()
            assert field_dict["data"].dataType == BinaryType()
            assert field_dict["created_date"].dataType == DateType()
            assert field_dict["updated_at"].dataType == TimestampType()

            # Check complex types
            assert isinstance(field_dict["address"].dataType, StructType)
            assert len(field_dict["address"].dataType.fields) == 3

            assert isinstance(field_dict["tags"].dataType, ArrayType)
            assert field_dict["tags"].dataType.elementType == StringType()

            assert isinstance(field_dict["properties"].dataType, MapType)
            assert field_dict["properties"].dataType.keyType == StringType()
            assert field_dict["properties"].dataType.valueType == StringType()

            # Check interval type - either DayTimeIntervalType or fallback
            if HAS_DAY_TIME_INTERVAL_TYPE:
                assert isinstance(field_dict["duration"].dataType, DayTimeIntervalType)
            else:
                assert field_dict["duration"].dataType == StringType()


# %% Field-level fallback in complex types
class TestPySparkConverterFieldLevelFallback:
    def test_struct_with_unsupported_field_fallback(self):
        struct_with_unsupported = Struct(
            fields=[
                Field(name="supported_field", type=String()),
                Field(name="unsupported_field", type=Geography()),
                Field(name="another_supported", type=Integer()),
            ]
        )

        spec = YadsSpec(
            name="test_struct_fallback",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="data", type=struct_with_unsupported),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(fallback_type=StringType()))

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Should warn for unsupported field within the struct
        assert any(issubclass(x.category, ValidationWarning) for x in w)
        msgs = "\n".join(str(x.message) for x in w)
        assert "geography" in msgs and "unsupported_field" in msgs

        data_field = next(f for f in schema.fields if f.name == "data")
        assert isinstance(data_field.dataType, StructType)
        inner = data_field.dataType
        inner_fields = {f.name: f for f in inner.fields}
        assert inner_fields["supported_field"].dataType == StringType()
        assert inner_fields["another_supported"].dataType == IntegerType()
        assert inner_fields["unsupported_field"].dataType == StringType()

    def test_nested_struct_with_multiple_unsupported_fields(self):
        inner_struct = Struct(
            fields=[
                Field(name="inner_geom", type=Geometry()),
                Field(name="inner_string", type=String()),
            ]
        )

        outer_struct = Struct(
            fields=[
                Field(name="outer_geog", type=Geography()),
                Field(name="inner_data", type=inner_struct),
                Field(name="outer_int", type=Integer()),
            ]
        )

        spec = YadsSpec(
            name="test_nested_fallback",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="complex_data", type=outer_struct),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(fallback_type=StringType()))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Two warnings for geometry/geography
        assert len([x for x in w if issubclass(x.category, ValidationWarning)]) == 2

        complex_field = next(f for f in schema.fields if f.name == "complex_data")
        assert isinstance(complex_field.dataType, StructType)
        outer = complex_field.dataType
        outer_fields = {f.name: f for f in outer.fields}
        assert outer_fields["outer_geog"].dataType == StringType()
        assert outer_fields["outer_int"].dataType == IntegerType()

        inner_field = outer_fields["inner_data"]
        assert isinstance(inner_field.dataType, StructType)
        inner = inner_field.dataType
        inner_fields = {f.name: f for f in inner.fields}
        assert inner_fields["inner_geom"].dataType == StringType()
        assert inner_fields["inner_string"].dataType == StringType()

    def test_array_with_unsupported_element_type(self):
        array_with_unsupported = Array(element=Geography())

        spec = YadsSpec(
            name="test_array_fallback",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="locations", type=array_with_unsupported),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(fallback_type=StringType()))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        assert any(issubclass(x.category, ValidationWarning) for x in w)
        locations_field = next(f for f in schema.fields if f.name == "locations")
        assert isinstance(locations_field.dataType, ArrayType)
        assert locations_field.dataType.elementType == StringType()

    def test_map_with_unsupported_key_or_value(self):
        map_unsupported_key = Map(key=Geometry(), value=String())
        map_unsupported_value = Map(key=String(), value=Geography())

        spec = YadsSpec(
            name="test_map_fallback",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="geom_keys", type=map_unsupported_key),
                Column(name="geog_values", type=map_unsupported_value),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(fallback_type=StringType()))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Two warnings for unsupported key/value
        assert len([x for x in w if issubclass(x.category, ValidationWarning)]) == 2

        geom_keys_field = next(f for f in schema.fields if f.name == "geom_keys")
        assert isinstance(geom_keys_field.dataType, MapType)
        assert geom_keys_field.dataType.keyType == StringType()
        assert geom_keys_field.dataType.valueType == StringType()

        geog_values_field = next(f for f in schema.fields if f.name == "geog_values")
        assert isinstance(geog_values_field.dataType, MapType)
        assert geog_values_field.dataType.keyType == StringType()
        assert geog_values_field.dataType.valueType == StringType()

    def test_array_of_struct_with_unsupported_fields(self):
        struct_with_unsupported = Struct(
            fields=[
                Field(name="name", type=String()),
                Field(name="location", type=Geography()),
            ]
        )
        array_of_structs = Array(element=struct_with_unsupported)

        spec = YadsSpec(
            name="test_array_struct_fallback",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="items", type=array_of_structs),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(fallback_type=StringType()))
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        assert any(issubclass(x.category, ValidationWarning) for x in w)
        items_field = next(f for f in schema.fields if f.name == "items")
        assert isinstance(items_field.dataType, ArrayType)
        elem = items_field.dataType.elementType
        assert isinstance(elem, StructType)
        elem_fields = {f.name: f for f in elem.fields}
        assert elem_fields["name"].dataType == StringType()
        assert elem_fields["location"].dataType == StringType()

    @pytest.mark.parametrize("fallback_type", [StringType(), BinaryType()])
    def test_fallback_type_preservation_in_nested_structures(self, fallback_type):
        struct_with_unsupported = Struct(
            fields=[
                Field(name="geom", type=Geometry()),
                Field(name="geog", type=Geography()),
            ]
        )

        spec = YadsSpec(
            name="test_fallback_preservation",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="data", type=struct_with_unsupported),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(fallback_type=fallback_type))
        with warnings.catch_warnings(record=True) as _w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        data_field = next(f for f in schema.fields if f.name == "data")
        assert isinstance(data_field.dataType, StructType)
        inner = data_field.dataType
        inner_fields = {f.name: f for f in inner.fields}
        assert type(inner_fields["geom"].dataType) is type(fallback_type)
        assert type(inner_fields["geog"].dataType) is type(fallback_type)

    def test_field_metadata_preservation_in_nested_fallback(self):
        struct_with_metadata = Struct(
            fields=[
                Field(
                    name="geom_with_metadata",
                    type=Geometry(),
                    description="A geometry field",
                    metadata={"srid": "4326", "precision": "high"},
                ),
                Field(name="normal_field", type=String()),
            ]
        )

        spec = YadsSpec(
            name="test_nested_metadata_preservation",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="data", type=struct_with_metadata),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(fallback_type=StringType()))
        with warnings.catch_warnings(record=True) as _w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        data_field = next(f for f in schema.fields if f.name == "data")
        assert isinstance(data_field.dataType, StructType)
        inner = data_field.dataType
        inner_fields = {f.name: f for f in inner.fields}
        geom_field = inner_fields["geom_with_metadata"]
        assert geom_field.dataType == StringType()
        assert geom_field.metadata is not None
        assert geom_field.metadata.get("description") == "A geometry field"
        assert geom_field.metadata.get("srid") == "4326"
        assert geom_field.metadata.get("precision") == "high"

        normal_field = inner_fields["normal_field"]
        assert normal_field.dataType == StringType()

    def test_raise_mode_still_raises_for_nested_unsupported_types(self):
        struct_with_unsupported = Struct(
            fields=[
                Field(name="geom", type=Geometry()),
                Field(name="normal_field", type=String()),
            ]
        )

        spec = YadsSpec(
            name="test_raise_mode_nested",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="data", type=struct_with_unsupported),
            ],
        )

        converter = PySparkConverter(PySparkConverterConfig(mode="raise"))
        with pytest.raises(
            UnsupportedFeatureError, match="does not support type: geometry"
        ):
            converter.convert(spec)


# %% Column overrides with raise_or_coerce
class TestColumnOverridesWithRaiseOrCoerce:
    """Tests for using raise_or_coerce in custom column override functions."""

    def test_column_override_using_raise_or_coerce_coerce_mode(self):
        """Test custom column override using raise_or_coerce in coerce mode."""
        from pyspark.sql.types import LongType, StructField

        def custom_timestamp_override(field, converter):
            # Convert all timestamps to LongType (unix epoch)
            long_type = converter.raise_or_coerce(
                field.type,
                coerce_type=LongType(),
                error_msg=f"Converting {field.name} to unix epoch (LongType)",
            )
            return StructField(field.name, long_type, nullable=field.is_nullable)

        config = PySparkConverterConfig(
            mode="coerce", column_overrides={"created_at": custom_timestamp_override}
        )
        converter = PySparkConverter(config)

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(bits=64)),
                Column(name="created_at", type=Timestamp()),
            ],
        )

        with pytest.warns(UserWarning) as warning_list:
            schema = converter.convert(spec)

        # Verify override was applied
        assert schema.fields[1].name == "created_at"
        assert isinstance(schema.fields[1].dataType, LongType)

        # Verify warning was emitted
        assert any(
            "Converting created_at to unix epoch" in str(w.message) for w in warning_list
        )

    def test_column_override_using_raise_or_coerce_raise_mode(self):
        """Test custom column override using raise_or_coerce in raise mode."""
        from pyspark.sql.types import LongType, StructField

        def custom_timestamp_override(field, converter):
            long_type = converter.raise_or_coerce(
                field.type,
                coerce_type=LongType(),
                error_msg=f"Converting {field.name} requires manual review",
            )
            return StructField(field.name, long_type, nullable=field.is_nullable)

        config = PySparkConverterConfig(
            mode="raise", column_overrides={"created_at": custom_timestamp_override}
        )
        converter = PySparkConverter(config)

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[Column(name="created_at", type=Timestamp())],
        )

        # Should raise because mode is "raise"
        with pytest.raises(
            UnsupportedFeatureError, match="Converting created_at requires manual review"
        ):
            converter.convert(spec)

    def test_column_override_with_raise_or_coerce_custom_message(self):
        """Test that custom error messages from overrides appear in warnings."""
        from pyspark.sql.types import StringType, StructField

        def custom_override(field, converter):
            string_type = converter.raise_or_coerce(
                field.type,
                coerce_type=StringType(),
                error_msg=f"Custom coercion logic for {field.name}: JSON -> String",
            )
            return StructField(field.name, string_type, nullable=field.is_nullable)

        config = PySparkConverterConfig(
            mode="coerce", column_overrides={"data": custom_override}
        )
        converter = PySparkConverter(config)

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[Column(name="data", type=JSON())],
        )

        with pytest.warns(UserWarning) as warning_list:
            _ = converter.convert(spec)

        # Verify custom message appears in warnings
        assert any(
            "Custom coercion logic for data: JSON -> String" in str(w.message)
            for w in warning_list
        )

    def test_column_override_with_raise_or_coerce_multiple_columns(self):
        """Test using raise_or_coerce with multiple column overrides."""
        from pyspark.sql.types import LongType, StringType, StructField

        def timestamp_to_long(field, converter):
            long_type = converter.raise_or_coerce(
                field.type, coerce_type=LongType(), error_msg=f"Override: {field.name}"
            )
            return StructField(field.name, long_type, nullable=field.is_nullable)

        def any_to_string(field, converter):
            string_type = converter.raise_or_coerce(
                field.type,
                coerce_type=StringType(),
                error_msg=f"Override: {field.name}",
            )
            return StructField(field.name, string_type, nullable=field.is_nullable)

        config = PySparkConverterConfig(
            mode="coerce",
            column_overrides={
                "created_at": timestamp_to_long,
                "updated_at": timestamp_to_long,
                "metadata": any_to_string,
            },
        )
        converter = PySparkConverter(config)

        spec = YadsSpec(
            name="test_table",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(bits=64)),
                Column(name="created_at", type=Timestamp()),
                Column(name="updated_at", type=Timestamp()),
                Column(name="metadata", type=JSON()),
            ],
        )

        with pytest.warns(UserWarning) as warning_list:
            schema = converter.convert(spec)

        # Verify all overrides were applied
        assert isinstance(schema.fields[1].dataType, LongType)  # created_at
        assert isinstance(schema.fields[2].dataType, LongType)  # updated_at
        assert isinstance(schema.fields[3].dataType, StringType)  # metadata

        # Verify warnings for overridden columns
        warning_messages = [str(w.message) for w in warning_list]
        assert any("Override: created_at" in msg for msg in warning_messages)
        assert any("Override: updated_at" in msg for msg in warning_messages)
        assert any("Override: metadata" in msg for msg in warning_messages)
