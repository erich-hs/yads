import warnings

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from yads.converters import PyArrowConverter, PyArrowConverterConfig
from yads.constraints import NotNullConstraint
from yads.spec import YadsSpec, Column, Field
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
)
from yads.exceptions import (
    UnsupportedFeatureError,
    ValidationWarning,
    ConverterConfigError,
)


# fmt: off
# %% Types
class TestPyArrowConverterTypes:
    @pytest.mark.parametrize(
        "yads_type, expected_pa_type, expected_warning",
        [
            (String(), pa.string(), None),
            (String(length=255), pa.string(), None),  # length hint ignored
            (Integer(bits=8), pa.int8(), None),
            (Integer(bits=16), pa.int16(), None),
            (Integer(bits=32), pa.int32(), None),
            (Integer(bits=64), pa.int64(), None),
            (Integer(bits=8, signed=False), pa.uint8(), None),
            (Integer(bits=16, signed=False), pa.uint16(), None),
            (Integer(bits=32, signed=False), pa.uint32(), None),
            (Integer(bits=64, signed=False), pa.uint64(), None),
            (Float(bits=16), pa.float16(), None),
            (Float(bits=32), pa.float32(), None),
            (Float(bits=64), pa.float64(), None),
            (Decimal(), pa.decimal128(38, 0), None),
            (Decimal(precision=10, scale=2), pa.decimal128(10, 2), None),
            (Decimal(precision=10, scale=2, bits=128), pa.decimal128(10, 2), None),
            (Boolean(), pa.bool_(), None),
            (Binary(), pa.binary(), None),
            (Binary(length=8), pa.binary(8), None),
            (Date(), pa.date32(), None),
            (Date(bits=32), pa.date32(), None),
            (Date(bits=64), pa.date64(), None),
            (Time(), pa.time32("ms"), None),  # default unit ms -> time32
            (Time(unit=TimeUnit.S), pa.time32("s"), None),
            (Time(unit=TimeUnit.MS), pa.time32("ms"), None),
            (Time(unit=TimeUnit.US), pa.time64("us"), None),
            (Time(unit=TimeUnit.NS), pa.time64("ns"), None),
            (Time(bits=32, unit=TimeUnit.S), pa.time32("s"), None),
            (Time(bits=64, unit=TimeUnit.US), pa.time64("us"), None),
            (Timestamp(), pa.timestamp("ns"), None),
            (Timestamp(unit=TimeUnit.S), pa.timestamp("s"), None),
            (Timestamp(unit=TimeUnit.MS), pa.timestamp("ms"), None),
            (Timestamp(unit=TimeUnit.US), pa.timestamp("us"), None),
            (Timestamp(unit=TimeUnit.NS), pa.timestamp("ns"), None),
            (TimestampTZ(), pa.timestamp("ns", tz="UTC"), None),
            (TimestampTZ(unit=TimeUnit.S), pa.timestamp("s", tz="UTC"), None),
            (TimestampLTZ(), pa.timestamp("ns", tz=None), None),
            (TimestampNTZ(), pa.timestamp("ns", tz=None), None),
            (Duration(), pa.duration("ns"), None),
            (Interval(interval_start=IntervalTimeUnit.DAY), pa.month_day_nano_interval(), None),
            (Array(element=Integer()), pa.list_(pa.int32()), None),
            (Array(element=String(), size=2), pa.list_(pa.string(), list_size=2), None),
            (
                Struct(
                    fields=[
                        Field(name="a", type=Integer()),
                        Field(name="b", type=String()),
                    ]
                ),
                pa.struct([
                    pa.field("a", pa.int32()),
                    pa.field("b", pa.string()),
                ]),
                None,
            ),
            (Map(key=String(), value=Integer()), pa.map_(pa.string(), pa.int32()), None),
            (JSON(), pa.json_(storage_type=pa.utf8()), None),
            (Geometry(), pa.string(), "Data type 'GEOMETRY' is not supported for column 'col1'."),
            (Geometry(srid=4326), pa.string(), "Data type 'GEOMETRY' is not supported for column 'col1'."),
            (Geography(), pa.string(), "Data type 'GEOGRAPHY' is not supported for column 'col1'."),
            (Geography(srid=4326), pa.string(), "Data type 'GEOGRAPHY' is not supported for column 'col1'."),
            (UUID(), pa.uuid(), None),
            (Void(), pa.null(), None),
            (Variant(), pa.string(), "Data type 'VARIANT' is not supported for column 'col1'."),
        ],
    )
    def test_convert_type(
        self,
        yads_type: YadsType,
        expected_pa_type: pa.DataType,
        expected_warning: str | None,
    ):
        spec = YadsSpec(
            name="test_spec",
            version="1.0.0",
            columns=[Column(name="col1", type=yads_type)],
        )
        converter = PyArrowConverter()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Assert converted schema
        assert schema.names == ["col1"]
        assert schema.field("col1").type == expected_pa_type
        assert schema.field("col1").nullable is True

        # Assert warnings for unsupported types
        if expected_warning is not None:
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert expected_warning in str(w[0].message)
        else:
            assert len(w) == 0

    def test_non_nullable_columns_and_nested_fields(self):
        nested = Struct(
            fields=[
                Field(name="x", type=Integer(), constraints=[NotNullConstraint()]),
                Field(name="y", type=String()),
            ]
        )
        spec = YadsSpec(
            name="test_spec",
            version="1.0.0",
            columns=[
                Column(
                    name="id", type=Integer(), constraints=[NotNullConstraint()]
                ),
                Column(name="struct", type=nested),
                Column(
                    name="arr",
                    type=Array(element=Struct(fields=[Field("z", Integer())])),
                ),
            ],
        )

        schema = PyArrowConverter().convert(spec)

        id_field = schema.field("id")
        assert id_field.nullable is False
        assert id_field.type == pa.int32()

        struct_field = schema.field("struct")
        assert struct_field.nullable is True
        assert pa.types.is_struct(struct_field.type)

        # Check nested struct fields' nullability
        struct_struct = struct_field.type
        assert struct_struct.num_fields == 2
        assert struct_struct.field("x").nullable is False
        assert struct_struct.field("x").type == pa.int32()
        assert struct_struct.field("y").nullable is True
        assert struct_struct.field("y").type == pa.string()

        # Array of struct with nested field
        arr_field = schema.field("arr")
        assert pa.types.is_list(arr_field.type)
        elem_struct = arr_field.type.value_type
        assert pa.types.is_struct(elem_struct)
        assert elem_struct.field("z").type == pa.int32()

    def test_large_string_binary_and_list_flags(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(name="s", type=String()),
                Column(name="b", type=Binary()),
                Column(name="l", type=Array(element=String())),
            ],
        )
        config = PyArrowConverterConfig(
            use_large_string=True,
            use_large_binary=True,
            use_large_list=True,
        )
        schema = PyArrowConverter(config).convert(spec)

        assert schema.field("s").type == pa.large_string()
        assert schema.field("b").type == pa.large_binary()
        assert schema.field("l").type == pa.large_list(pa.large_string())

    def test_decimal_precision_coercion_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="d", type=Decimal(precision=39, scale=2, bits=128))],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = PyArrowConverter().convert(spec, mode="coerce")

        assert schema.field("d").type == pa.decimal256(39, 2)
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "Precision greater than 38 is incompatible" in str(w[0].message)

        with pytest.raises(UnsupportedFeatureError):
            PyArrowConverter(PyArrowConverterConfig(mode="raise")).convert(spec)

    def test_time_bits_unit_mismatch_coercion(self):
        # time32 with us -> coerced to time64
        spec1 = YadsSpec(
            name="t1",
            version="1.0.0",
            columns=[Column(name="t", type=Time(bits=32, unit=TimeUnit.US))],
        )
        with warnings.catch_warnings(record=True) as w1:
            warnings.simplefilter("always")
            schema1 = PyArrowConverter().convert(spec1, mode="coerce")
        assert schema1.field("t").type == pa.time64("us")
        assert len(w1) == 1
        assert issubclass(w1[0].category, ValidationWarning)
        assert "time32 supports only 's' or 'ms'" in str(w1[0].message)

        # time64 with ms -> coerced to time32
        spec2 = YadsSpec(
            name="t2",
            version="1.0.0",
            columns=[Column(name="t", type=Time(bits=64, unit=TimeUnit.MS))],
        )
        with warnings.catch_warnings(record=True) as w2:
            warnings.simplefilter("always")
            schema2 = PyArrowConverter().convert(spec2, mode="coerce")
        assert schema2.field("t").type == pa.time32("ms")
        assert len(w2) == 1
        assert issubclass(w2[0].category, ValidationWarning)
        assert "time64 supports only 'us' or 'ns'" in str(w2[0].message)

    def test_schema_and_field_metadata_coercion(self):
        field_meta = {"a": 1, "b": {"x": True}}
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            metadata={"owner": "data-eng", "cfg": {"retries": 3}},
            columns=[
                Column(name="c", type=String(), metadata=field_meta),
            ],
        )
        schema = PyArrowConverter().convert(spec)

        # Schema-level metadata comes back as bytes -> bytes
        raw_schema_meta = schema.metadata or {}
        decoded_schema_meta = {k.decode(): v.decode() for k, v in raw_schema_meta.items()}
        assert decoded_schema_meta.get("owner") == "data-eng"
        assert decoded_schema_meta.get("cfg") == "{\"retries\":3}"

        # Field metadata is attached to the field
        field = schema.field("c")
        raw_field_meta = field.metadata or {}
        decoded_field_meta = {k.decode(): v.decode() for k, v in raw_field_meta.items()}
        assert decoded_field_meta.get("a") == "1"
        assert decoded_field_meta.get("b") == "{\"x\":true}"

    @pytest.mark.parametrize(
        "yads_type, type_name",
        [
            (Geometry(), "Geometry"),
            (Geometry(srid=4326), "Geometry"),
            (Geography(), "Geography"),
            (Geography(srid=4326), "Geography"),
            (Variant(), "Variant"),
        ],
    )
    def test_raise_mode_for_unsupported_types(self, yads_type: YadsType, type_name: str):
        # Test unsupported types in raise mode
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="col1", type=yads_type)],
        )

        with pytest.raises(UnsupportedFeatureError, match=f"PyArrowConverter does not support type: {type_name}"):
            PyArrowConverter(PyArrowConverterConfig(mode="raise")).convert(spec)

    def test_raise_mode_for_incompatible_decimal_precision(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="d", type=Decimal(precision=39, scale=2, bits=128))],
        )

        with pytest.raises(UnsupportedFeatureError, match="precision > 38 is incompatible with Decimal\\(bits=128\\)"):
            PyArrowConverter(PyArrowConverterConfig(mode="raise")).convert(spec)

    def test_raise_mode_for_incompatible_time_bits_unit(self):
        # time32 with us -> should raise in raise mode
        spec1 = YadsSpec(
            name="t1",
            version="1.0.0",
            columns=[Column(name="t", type=Time(bits=32, unit=TimeUnit.US))],
        )
        with pytest.raises(UnsupportedFeatureError, match="time32 supports only 's' or 'ms' units"):
            PyArrowConverter(PyArrowConverterConfig(mode="raise")).convert(spec1)

        # time64 with ms -> should raise in raise mode
        spec2 = YadsSpec(
            name="t2",
            version="1.0.0",
            columns=[Column(name="t", type=Time(bits=64, unit=TimeUnit.MS))],
        )
        with pytest.raises(UnsupportedFeatureError, match="time64 supports only 'us' or 'ns' units"):
            PyArrowConverter(PyArrowConverterConfig(mode="raise")).convert(spec2)
# fmt: on


# %% PyArrowConverter column filtering and customization
class TestPyArrowConverterCustomization:
    def test_ignore_columns(self):
        """Test that ignore_columns excludes specified columns from the schema."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
                Column(name="secret", type=String()),
            ],
        )
        config = PyArrowConverterConfig(ignore_columns={"secret"})
        converter = PyArrowConverter(config)
        schema = converter.convert(spec)

        assert "id" in schema.names
        assert "name" in schema.names
        assert "secret" not in schema.names

    def test_include_columns(self):
        """Test that include_columns only includes specified columns in the schema."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
                Column(name="internal", type=String()),
            ],
        )
        config = PyArrowConverterConfig(include_columns={"id", "name"})
        converter = PyArrowConverter(config)
        schema = converter.convert(spec)

        assert "id" in schema.names
        assert "name" in schema.names
        assert "internal" not in schema.names

    def test_column_override_basic(self):
        """Test basic column override functionality."""

        def custom_name_override(field, converter):
            # Override name field to be large_string with custom metadata
            return pa.field(
                field.name,
                pa.large_string(),
                nullable=field.is_nullable,
                metadata={"custom": "true", "override": "applied"},
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
            ],
        )
        config = PyArrowConverterConfig(column_overrides={"name": custom_name_override})
        converter = PyArrowConverter(config)
        schema = converter.convert(spec)

        # Check that override was applied
        name_field = schema.field("name")
        assert name_field.type == pa.large_string()
        assert name_field.metadata is not None
        metadata = {k.decode(): v.decode() for k, v in name_field.metadata.items()}
        assert metadata["custom"] == "true"
        assert metadata["override"] == "applied"

        # Check that other fields use default conversion
        id_field = schema.field("id")
        assert id_field.type == pa.int32()

    def test_column_override_with_complex_type(self):
        """Test column override with complex custom type."""

        def custom_metadata_override(field, converter):
            # Create a custom struct for metadata
            metadata_struct = pa.struct(
                [
                    pa.field("version", pa.string()),
                    pa.field("tags", pa.list_(pa.string())),
                    pa.field("config", pa.map_(pa.string(), pa.string())),
                ]
            )
            return pa.field(
                field.name,
                metadata_struct,
                nullable=field.is_nullable,
                metadata={"custom_struct": "true"},
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="metadata", type=JSON()),
            ],
        )
        config = PyArrowConverterConfig(
            column_overrides={"metadata": custom_metadata_override}
        )
        converter = PyArrowConverter(config)
        schema = converter.convert(spec)

        # Check that override was applied
        metadata_field = schema.field("metadata")
        assert pa.types.is_struct(metadata_field.type)
        struct_type = metadata_field.type
        assert struct_type.num_fields == 3
        assert struct_type.field("version").type == pa.string()
        assert pa.types.is_list(struct_type.field("tags").type)
        assert pa.types.is_map(struct_type.field("config").type)

    @pytest.mark.parametrize(
        "fallback_type",
        [pa.large_string(), pa.large_binary(), pa.string(), pa.binary()],
    )
    def test_valid_fallback_types(self, fallback_type: pa.DataType):
        """Test fallback_type for unsupported types."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="geom", type=Geometry()),
            ],
        )
        config = PyArrowConverterConfig(fallback_type=fallback_type)
        converter = PyArrowConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Check fallback was applied
        geom_field = schema.field("geom")
        assert geom_field.type == fallback_type

        # Check warning was emitted
        assert len(w) == 1
        assert "GEOMETRY" in str(w[0].message)
        assert str(fallback_type) in str(w[0].message)

    def test_invalid_fallback_type_raises_error(self):
        """Test that invalid fallback_type raises UnsupportedFeatureError."""
        with pytest.raises(
            UnsupportedFeatureError,
            match="fallback_type must be one of: pa.binary\\(\\), pa.large_binary\\(\\), pa.string\\(\\), pa.large_string\\(\\)",
        ):
            PyArrowConverterConfig(fallback_type=pa.int32())

    def test_precedence_ignore_over_override(self):
        """Test that ignore_columns takes precedence over column_overrides."""

        def should_not_be_called(field, converter):
            pytest.fail("Override should not be called for ignored column")

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="ignored_col", type=String()),
            ],
        )
        config = PyArrowConverterConfig(
            ignore_columns={"ignored_col"},
            column_overrides={"ignored_col": should_not_be_called},
        )
        converter = PyArrowConverter(config)
        schema = converter.convert(spec)

        assert len(schema.names) == 1
        assert "id" in schema.names
        assert "ignored_col" not in schema.names

    def test_precedence_override_over_default_conversion(self):
        """Test that column_overrides takes precedence over default conversion."""

        def integer_as_string_override(field, converter):
            return pa.field(
                field.name,
                pa.string(),
                nullable=field.is_nullable,
                metadata={"converted_from": "integer"},
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="normal_int", type=Integer()),
                Column(name="string_int", type=Integer()),
            ],
        )
        config = PyArrowConverterConfig(
            column_overrides={"string_int": integer_as_string_override}
        )
        converter = PyArrowConverter(config)
        schema = converter.convert(spec)

        # Normal conversion
        normal_field = schema.field("normal_int")
        assert normal_field.type == pa.int32()

        # Override conversion
        string_field = schema.field("string_int")
        assert string_field.type == pa.string()
        assert string_field.metadata is not None
        metadata = {k.decode(): v.decode() for k, v in string_field.metadata.items()}
        assert metadata["converted_from"] == "integer"

    def test_precedence_override_over_fallback(self):
        """Test that column_overrides takes precedence over fallback_type."""

        def custom_geometry_override(field, converter):
            return pa.field(
                field.name,
                pa.string(),
                nullable=field.is_nullable,
                metadata={"custom_geometry": "true"},
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="fallback_geom", type=Geometry()),
                Column(name="override_geom", type=Geometry()),
            ],
        )
        config = PyArrowConverterConfig(
            fallback_type=pa.large_binary(),
            column_overrides={"override_geom": custom_geometry_override},
        )
        converter = PyArrowConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Fallback applied to fallback_geom
        fallback_field = schema.field("fallback_geom")
        assert fallback_field.type == pa.large_binary()

        # Override applied to override_geom
        override_field = schema.field("override_geom")
        assert override_field.type == pa.string()
        assert override_field.metadata is not None
        metadata = {k.decode(): v.decode() for k, v in override_field.metadata.items()}
        assert metadata["custom_geometry"] == "true"

        # Only one warning for the fallback field
        assert len(w) == 1
        assert "fallback_geom" in str(w[0].message)

    def test_field_metadata_preservation_with_fallback(self):
        """Test that field metadata is preserved when fallback is applied."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(
                    name="geom",
                    type=Geometry(),
                    metadata={"spatial_ref": "EPSG:4326", "precision": "high"},
                ),
            ],
        )
        config = PyArrowConverterConfig(fallback_type=pa.string())
        converter = PyArrowConverter(config)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        geom_field = schema.field("geom")

        # Check that fallback type was applied
        assert geom_field.type == pa.string()

        # Check that field metadata was preserved during fallback
        assert geom_field.metadata is not None
        metadata = {k.decode(): v.decode() for k, v in geom_field.metadata.items()}
        assert metadata["spatial_ref"] == "EPSG:4326"
        assert metadata["precision"] == "high"

    def test_field_description_preservation_with_fallback(self):
        """Test that field description is preserved when fallback is applied."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(
                    name="geom",
                    type=Geometry(),
                    description="A geometry field for spatial data",
                    metadata={"spatial_ref": "EPSG:4326"},
                ),
            ],
        )
        config = PyArrowConverterConfig(fallback_type=pa.string())
        converter = PyArrowConverter(config)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        geom_field = schema.field("geom")

        # Check that fallback type was applied
        assert geom_field.type == pa.string()

        # Check that both description and metadata were preserved during fallback
        assert geom_field.metadata is not None
        metadata = {k.decode(): v.decode() for k, v in geom_field.metadata.items()}
        assert metadata["description"] == "A geometry field for spatial data"
        assert metadata["spatial_ref"] == "EPSG:4326"

    def test_field_description_in_metadata(self):
        """Test that field descriptions are included in PyArrow field metadata."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(), description="Primary key identifier"),
                Column(
                    name="name",
                    type=String(),
                    description="User's full name",
                    metadata={"max_length": "255", "encoding": "utf-8"},
                ),
                Column(
                    name="age",
                    type=Integer(),
                    # No description
                ),
            ],
        )
        converter = PyArrowConverter()
        schema = converter.convert(spec)

        # Test field with description only
        id_field = schema.field("id")
        assert id_field.metadata is not None
        id_metadata = {k.decode(): v.decode() for k, v in id_field.metadata.items()}
        assert id_metadata["description"] == "Primary key identifier"
        assert len(id_metadata) == 1

        # Test field with both description and custom metadata
        name_field = schema.field("name")
        assert name_field.metadata is not None
        name_metadata = {k.decode(): v.decode() for k, v in name_field.metadata.items()}
        assert name_metadata["description"] == "User's full name"
        assert name_metadata["max_length"] == "255"
        assert name_metadata["encoding"] == "utf-8"
        assert len(name_metadata) == 3

        # Test field with no description or metadata
        age_field = schema.field("age")
        assert age_field.metadata is None

    def test_unknown_column_in_filters_raises_error(self):
        """Test that unknown columns in filters raise validation errors."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="col1", type=String())],
        )

        # Test unknown ignore_columns
        config1 = PyArrowConverterConfig(ignore_columns={"nonexistent"})
        converter1 = PyArrowConverter(config1)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in ignore_columns: nonexistent"
        ):
            converter1.convert(spec)

        # Test unknown include_columns
        config2 = PyArrowConverterConfig(include_columns={"nonexistent"})
        converter2 = PyArrowConverter(config2)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in include_columns: nonexistent"
        ):
            converter2.convert(spec)

    def test_conflicting_ignore_and_include_raises_error(self):
        """Test that overlapping ignore_columns and include_columns raises error."""
        with pytest.raises(
            ConverterConfigError, match="Columns cannot be both ignored and included"
        ):
            PyArrowConverterConfig(
                ignore_columns={"col1", "col2"}, include_columns={"col1", "col3"}
            )

    def test_column_override_preserves_nullability(self):
        """Test that column overrides preserve field nullability correctly."""

        def nullable_override(field, converter):
            return pa.field(
                field.name,
                pa.large_string(),
                nullable=field.is_nullable,  # Preserve original nullability
                metadata={"nullable_preserved": str(field.is_nullable)},
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="nullable_col", type=String()),
                Column(
                    name="non_null_col", type=String(), constraints=[NotNullConstraint()]
                ),
            ],
        )
        config = PyArrowConverterConfig(
            column_overrides={
                "nullable_col": nullable_override,
                "non_null_col": nullable_override,
            }
        )
        converter = PyArrowConverter(config)
        schema = converter.convert(spec)

        # Check nullability preservation
        nullable_field = schema.field("nullable_col")
        assert nullable_field.nullable is True
        nullable_metadata = {
            k.decode(): v.decode() for k, v in nullable_field.metadata.items()
        }
        assert nullable_metadata["nullable_preserved"] == "True"

        non_null_field = schema.field("non_null_col")
        assert non_null_field.nullable is False
        non_null_metadata = {
            k.decode(): v.decode() for k, v in non_null_field.metadata.items()
        }
        assert non_null_metadata["nullable_preserved"] == "False"

    def test_column_override_with_original_field_access(self):
        """Test that column overrides have access to original field properties."""

        def field_inspector_override(field, converter):
            # Override that inspects the original field and creates metadata based on it
            metadata = {
                "original_type": type(field.type).__name__,
                "has_description": str(field.description is not None),
                "constraint_count": str(len(field.constraints)),
            }
            if hasattr(field.type, "length") and field.type.length is not None:
                metadata["original_length"] = str(field.type.length)

            return pa.field(
                field.name, pa.string(), nullable=field.is_nullable, metadata=metadata
            )

        spec = YadsSpec(
            name="test",
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
        config = PyArrowConverterConfig(
            column_overrides={"inspected_col": field_inspector_override}
        )
        converter = PyArrowConverter(config)
        schema = converter.convert(spec)

        # Check that override had access to original field properties
        inspected_field = schema.field("inspected_col")
        metadata = {k.decode(): v.decode() for k, v in inspected_field.metadata.items()}

        assert metadata["original_type"] == "String"
        assert metadata["has_description"] == "True"
        assert metadata["constraint_count"] == "1"
        assert metadata["original_length"] == "100"
