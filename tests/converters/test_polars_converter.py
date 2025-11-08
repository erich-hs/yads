import re
import warnings

import polars as pl  # type: ignore[import-untyped]
import pytest

from yads.constraints import NotNullConstraint
from yads.converters import PolarsConverter, PolarsConverterConfig
from yads.exceptions import (
    UnsupportedFeatureError,
    ValidationWarning,
    ConverterConfigError,
)
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
    Tensor,
)

_VALID_FALLBACK_TYPES = [pl.String, pl.Binary]


# fmt: off
# %% Types
class TestPolarsConverterTypes:
    @pytest.mark.parametrize(
        "yads_type, expected_pl_type, expected_warning",
        [
            (String(), pl.String, None),
            (String(length=255), pl.String, "length constraint will be lost"),
            (Integer(bits=8), pl.Int8, None),
            (Integer(bits=16), pl.Int16, None),
            (Integer(bits=32), pl.Int32, None),
            (Integer(bits=64), pl.Int64, None),
            (Integer(bits=8, signed=False), pl.UInt8, None),
            (Integer(bits=16, signed=False), pl.UInt16, None),
            (Integer(bits=32, signed=False), pl.UInt32, None),
            (Integer(bits=64, signed=False), pl.UInt64, None),
            (Float(bits=16), pl.Float32, "PolarsConverter does not support type: float(bits=16)"),  # coerced
            (Float(bits=32), pl.Float32, None),
            (Float(bits=64), pl.Float64, None),
            (Decimal(), pl.Decimal(precision=None, scale=0), None),
            (Decimal(precision=10, scale=2), pl.Decimal(precision=10, scale=2), None),
            (Boolean(), pl.Boolean, None),
            (Binary(), pl.Binary, None),
            (Binary(length=8), pl.Binary, "length constraint will be lost"),
            (Date(), pl.Date, None),
            (Date(bits=32), pl.Date, "bits constraint will be lost"),   
            (Date(bits=64), pl.Date, "bits constraint will be lost"),
            (Time(), pl.String, "Polars Time only supports nanosecond precision"),  # default MS unit
            (Time(unit=TimeUnit.S), pl.String, "Polars Time only supports nanosecond precision"),
            (Time(unit=TimeUnit.MS), pl.String, "Polars Time only supports nanosecond precision"),
            (Time(unit=TimeUnit.US), pl.String, "Polars Time only supports nanosecond precision"),
            (Time(unit=TimeUnit.NS), pl.Time, None),
            (Timestamp(), pl.Datetime(time_unit="ns", time_zone=None), None),
            (Timestamp(unit=TimeUnit.S), pl.String, "Polars Datetime does not support 's' (second) time unit"),
            (Timestamp(unit=TimeUnit.MS), pl.Datetime(time_unit="ms", time_zone=None), None),
            (Timestamp(unit=TimeUnit.US), pl.Datetime(time_unit="us", time_zone=None), None),
            (Timestamp(unit=TimeUnit.NS), pl.Datetime(time_unit="ns", time_zone=None), None),
            (TimestampTZ(), pl.Datetime(time_unit="ns", time_zone="UTC"), None),
            (TimestampTZ(unit=TimeUnit.S, tz="America/New_York"), pl.String, "Polars Datetime does not support 's' (second) time unit"),
            (TimestampLTZ(), pl.Datetime(time_unit="ns", time_zone=None), "local timezone semantics will be lost"),
            (TimestampNTZ(), pl.Datetime(time_unit="ns", time_zone=None), None),
            (Duration(), pl.Duration(time_unit="ns"), None),
            (Duration(unit=TimeUnit.S), pl.String, "Polars Duration does not support 's' (second) time unit"),
            (Duration(unit=TimeUnit.MS), pl.Duration(time_unit="ms"), None),
            (Duration(unit=TimeUnit.US), pl.Duration(time_unit="us"), None),
            (Duration(unit=TimeUnit.NS), pl.Duration(time_unit="ns"), None),
            (Interval(interval_start=IntervalTimeUnit.DAY), pl.String, "PolarsConverter does not support type"),
            (Array(element=Integer()), pl.List(pl.Int32), None),
            (Array(element=String(), size=2), pl.Array(pl.String, shape=2), None),
            (
                Struct(
                    fields=[
                        Field(name="a", type=Integer()),
                        Field(name="b", type=String()),
                    ]
                ),
                pl.Struct([
                    pl.Field("a", pl.Int32),
                    pl.Field("b", pl.String),
                ]),
                None,
            ),
            (Map(key=String(), value=Integer()), pl.Struct([pl.Field("key", pl.String), pl.Field("value", pl.Int32)]), "PolarsConverter does not support type: map<string, integer>"),
            (JSON(), pl.String, "PolarsConverter does not support type: json"),
            (Geometry(), pl.String, "PolarsConverter does not support type: geometry for 'col1'."),
            (Geometry(srid=4326), pl.String, "PolarsConverter does not support type: geometry(srid=4326) for 'col1'."),
            (Geography(), pl.String, "PolarsConverter does not support type: geography for 'col1'."),
            (Geography(srid=4326), pl.String, "PolarsConverter does not support type: geography(srid=4326) for 'col1'."),
            (UUID(), pl.String, "PolarsConverter does not support type: uuid"),
            (Void(), pl.Null, None),
            (Variant(), pl.String, "PolarsConverter does not support type: variant for 'col1'."),
            (Tensor(element=Integer(bits=32), shape=(10, 20)), pl.Array(pl.Int32, shape=(10, 20)), None),
        ],
    )
    def test_convert_type(
        self,
        yads_type: YadsType,
        expected_pl_type: pl.DataType,
        expected_warning: str | None,
    ):
        spec = YadsSpec(
            name="test_spec",
            version="1.0.0",
            columns=[Column(name="col1", type=yads_type)],
        )
        # Set fallback_type explicitly for unsupported types
        config = PolarsConverterConfig(fallback_type=pl.String)
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Assert converted schema
        assert "col1" in schema.names()
        assert schema["col1"] == expected_pl_type

        # Assert warnings for unsupported types
        if expected_warning is not None:
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert expected_warning in str(w[0].message)
        else:
            assert len(w) == 0

    def test_non_nullable_columns_ignored_in_schema(self):
        # Polars Schema doesn't store nullability information
        # It's tracked at DataFrame/Series level, not Schema level
        spec = YadsSpec(
            name="test_spec",
            version="1.0.0",
            columns=[
                Column(
                    name="id", type=Integer(), constraints=[NotNullConstraint()]
                ),
                Column(name="name", type=String()),
            ],
        )

        schema = PolarsConverter().convert(spec)

        # Polars schema doesn't track nullability. Currently open issue:
        # https://github.com/pola-rs/polars/issues/16090
        assert "id" in schema.names()
        assert schema["id"] == pl.Int32
        assert "name" in schema.names()
        assert schema["name"] == pl.String

    def test_nested_struct_with_nullable_fields(self):
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

        schema = PolarsConverter().convert(spec)

        assert schema["id"] == pl.Int32
        assert schema["struct"] == pl.Struct([
            pl.Field("x", pl.Int32),
            pl.Field("y", pl.String),
        ])
        assert schema["arr"] == pl.List(pl.Struct([pl.Field("z", pl.Int32)]))

    def test_float16_coercion_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="f", type=Float(bits=16))],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = PolarsConverter().convert(spec, mode="coerce")

        assert schema["f"] == pl.Float32
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "PolarsConverter does not support type: float(bits=16)" in str(w[0].message)

        with pytest.raises(UnsupportedFeatureError):
            PolarsConverter(PolarsConverterConfig(mode="raise")).convert(spec)

    def test_timestampltz_no_warning(self):
        """TimestampLTZ converts with warning about lost LTZ semantics."""
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="ts", type=TimestampLTZ())],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = PolarsConverter(
                PolarsConverterConfig(fallback_type=pl.String)
            ).convert(spec, mode="coerce")

        assert schema["ts"] == pl.Datetime(time_unit="ns", time_zone=None)
        assert len(w) == 1
        assert "local timezone semantics will be lost" in str(w[0].message)

        with pytest.raises(UnsupportedFeatureError, match="local timezone semantics will be lost"):
            PolarsConverter(PolarsConverterConfig(mode="raise")).convert(spec)

    def test_interval_coercion_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="interval", type=Interval(IntervalTimeUnit.YEAR))],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = PolarsConverter(
                PolarsConverterConfig(fallback_type=pl.String)
            ).convert(spec, mode="coerce")

        assert schema["interval"] == pl.String
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "PolarsConverter does not support type" in str(w[0].message)
        assert "interval" in str(w[0].message).lower()

        with pytest.raises(UnsupportedFeatureError):
            PolarsConverter(PolarsConverterConfig(mode="raise")).convert(spec)

    def test_map_coercion_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="m", type=Map(key=String(), value=Integer()))],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = PolarsConverter().convert(spec, mode="coerce")

        # Map coerces to Struct with key/value fields
        assert schema["m"] == pl.Struct([
            pl.Field("key", pl.String),
            pl.Field("value", pl.Int32),
        ])
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "PolarsConverter does not support type: map" in str(w[0].message)

        with pytest.raises(UnsupportedFeatureError):
            PolarsConverter(PolarsConverterConfig(mode="raise")).convert(spec)

    def test_uuid_coercion_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="u", type=UUID())],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = PolarsConverter(
                PolarsConverterConfig(fallback_type=pl.String)
            ).convert(spec, mode="coerce")

        assert schema["u"] == pl.String
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "PolarsConverter does not support type" in str(w[0].message)
        assert "uuid" in str(w[0].message).lower()

        with pytest.raises(UnsupportedFeatureError):
            PolarsConverter(PolarsConverterConfig(mode="raise")).convert(spec)

    def test_json_coercion_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="j", type=JSON())],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = PolarsConverter(
                PolarsConverterConfig(fallback_type=pl.String)
            ).convert(spec, mode="coerce")

        assert schema["j"] == pl.String
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "PolarsConverter does not support type" in str(w[0].message)
        assert "json" in str(w[0].message).lower()

        with pytest.raises(UnsupportedFeatureError):
            PolarsConverter(PolarsConverterConfig(mode="raise")).convert(spec)

    @pytest.mark.parametrize(
        "yads_type, type_name",
        [
            (Geometry(), "geometry"),
            (Geometry(srid=4326), "geometry(srid=4326)"),
            (Geography(), "geography"),
            (Geography(srid=4326), "geography(srid=4326)"),
            (Variant(), "variant"),
        ],
    )
    def test_raise_mode_for_unsupported_types(self, yads_type: YadsType, type_name: str):
        # Test unsupported types in raise mode
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="col1", type=yads_type)],
        )

        with pytest.raises(UnsupportedFeatureError, match=f"PolarsConverter does not support type: {re.escape(type_name)} for 'col1'."):
            PolarsConverter(PolarsConverterConfig(mode="raise")).convert(spec)

    @pytest.mark.parametrize(
        "tensor_type, expected_pl_type",
        [
            # 1D tensors
            (Tensor(element=Integer(bits=32), shape=(10,)), pl.Array(pl.Int32, shape=(10,))),
            (Tensor(element=String(), shape=(5,)), pl.Array(pl.String, shape=(5,))),
            (Tensor(element=Float(bits=64), shape=(100,)), pl.Array(pl.Float64, shape=(100,))),
            # 2D tensors
            (Tensor(element=Integer(bits=32), shape=(10, 20)), pl.Array(pl.Int32, shape=(10, 20))),
            (Tensor(element=Float(bits=32), shape=(28, 28)), pl.Array(pl.Float32, shape=(28, 28))),
            (Tensor(element=Boolean(), shape=(3, 4)), pl.Array(pl.Boolean, shape=(3, 4))),
            # 3D tensors
            (Tensor(element=Integer(bits=64), shape=(5, 10, 15)), pl.Array(pl.Int64, shape=(5, 10, 15))),
            (Tensor(element=Float(bits=32), shape=(3, 224, 224)), pl.Array(pl.Float32, shape=(3, 224, 224))),
            # 4D tensors
            (Tensor(element=Integer(bits=8), shape=(3, 4, 5, 6)), pl.Array(pl.Int8, shape=(3, 4, 5, 6))),
            # Nested element types
            (Tensor(element=Array(element=Integer()), shape=(10, 20)), pl.Array(pl.List(pl.Int32), shape=(10, 20))),
            (Tensor(element=Struct(fields=[Field(name="x", type=Integer())]), shape=(5, 5)), 
             pl.Array(pl.Struct([pl.Field("x", pl.Int32)]), shape=(5, 5))),
        ],
    )
    def test_convert_tensor_to_array(self, tensor_type: Tensor, expected_pl_type: pl.DataType):
        """Test conversion of yads Tensor to polars Array with multi-dimensional shapes."""
        spec = YadsSpec(
            name="test_spec",
            version="1.0.0",
            columns=[Column(name="tensor_col", type=tensor_type)],
        )
        converter = PolarsConverter()
        schema = converter.convert(spec)

        assert "tensor_col" in schema.names()
        assert schema["tensor_col"] == expected_pl_type

    def test_decimal_precision_handling(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(name="d1", type=Decimal()),
                Column(name="d2", type=Decimal(precision=10, scale=2)),
                Column(name="d3", type=Decimal(precision=76, scale=38)),
            ],
        )

        schema = PolarsConverter().convert(spec)

        assert schema["d1"] == pl.Decimal(precision=None, scale=0)
        assert schema["d2"] == pl.Decimal(precision=10, scale=2)
        assert schema["d3"] == pl.Decimal(precision=76, scale=38)
# fmt: on


# %% PolarsConverter column filtering and customization
class TestPolarsConverterCustomization:
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
        config = PolarsConverterConfig(ignore_columns={"secret"})
        converter = PolarsConverter(config)
        schema = converter.convert(spec)

        assert "id" in schema.names()
        assert "name" in schema.names()
        assert "secret" not in schema.names()

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
        config = PolarsConverterConfig(include_columns={"id", "name"})
        converter = PolarsConverter(config)
        schema = converter.convert(spec)

        assert "id" in schema.names()
        assert "name" in schema.names()
        assert "internal" not in schema.names()

    def test_column_override_basic(self):
        """Test basic column override functionality."""

        def custom_name_override(field, converter):
            # Override name field to be Binary
            return pl.Field(field.name, pl.Binary)

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
            ],
        )
        config = PolarsConverterConfig(column_overrides={"name": custom_name_override})
        converter = PolarsConverter(config)
        schema = converter.convert(spec)

        # Check that override was applied
        assert schema["name"] == pl.Binary

        # Check that other fields use default conversion
        assert schema["id"] == pl.Int32

    def test_column_override_with_complex_type(self):
        """Test column override with complex custom type."""

        def custom_metadata_override(field, converter):
            # Create a custom struct for metadata
            metadata_struct = pl.Struct(
                [
                    pl.Field("version", pl.String),
                    pl.Field("tags", pl.List(pl.String)),
                ]
            )
            return pl.Field(field.name, metadata_struct)

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="metadata", type=JSON()),
            ],
        )
        config = PolarsConverterConfig(
            column_overrides={"metadata": custom_metadata_override}
        )
        converter = PolarsConverter(config)
        schema = converter.convert(spec, mode="coerce")

        # Check that override was applied (no warnings for this column)
        assert schema["metadata"] == pl.Struct(
            [
                pl.Field("version", pl.String),
                pl.Field("tags", pl.List(pl.String)),
            ]
        )

    @pytest.mark.parametrize(
        "fallback_type",
        _VALID_FALLBACK_TYPES,
    )
    def test_valid_fallback_types(self, fallback_type: pl.DataType):
        """Test fallback_type for unsupported types."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="geom", type=Geometry()),
            ],
        )
        config = PolarsConverterConfig(fallback_type=fallback_type)
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Check fallback was applied
        assert schema["geom"] == fallback_type

        # Check warning was emitted
        assert len(w) == 1
        assert "geometry" in str(w[0].message)

    def test_invalid_fallback_type_raises_error(self):
        """Test that invalid fallback_type raises UnsupportedFeatureError."""
        with pytest.raises(
            UnsupportedFeatureError,
            match="fallback_type must be one of: pl.String, pl.Binary, or None",
        ):
            PolarsConverterConfig(fallback_type=pl.Int32)

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
        config = PolarsConverterConfig(
            ignore_columns={"ignored_col"},
            column_overrides={"ignored_col": should_not_be_called},
        )
        converter = PolarsConverter(config)
        schema = converter.convert(spec)

        assert len(schema.names()) == 1
        assert "id" in schema.names()
        assert "ignored_col" not in schema.names()

    def test_precedence_override_over_default_conversion(self):
        """Test that column_overrides takes precedence over default conversion."""

        def integer_as_string_override(field, converter):
            return pl.Field(field.name, pl.String)

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="normal_int", type=Integer()),
                Column(name="string_int", type=Integer()),
            ],
        )
        config = PolarsConverterConfig(
            column_overrides={"string_int": integer_as_string_override}
        )
        converter = PolarsConverter(config)
        schema = converter.convert(spec)

        # Normal conversion
        assert schema["normal_int"] == pl.Int32

        # Override conversion
        assert schema["string_int"] == pl.String

    def test_precedence_override_over_fallback(self):
        """Test that column_overrides takes precedence over fallback_type."""

        def custom_geometry_override(field, converter):
            return pl.Field(field.name, pl.Binary)

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="fallback_geom", type=Geometry()),
                Column(name="override_geom", type=Geometry()),
            ],
        )
        config = PolarsConverterConfig(
            fallback_type=pl.String,
            column_overrides={"override_geom": custom_geometry_override},
        )
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Fallback applied to fallback_geom
        assert schema["fallback_geom"] == pl.String

        # Override applied to override_geom
        assert schema["override_geom"] == pl.Binary

        # Only one warning for the fallback field
        assert len(w) == 1
        assert "fallback_geom" in str(w[0].message)

    def test_unknown_column_in_filters_raises_error(self):
        """Test that unknown columns in filters raise validation errors."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="col1", type=String())],
        )

        # Test unknown ignore_columns
        config1 = PolarsConverterConfig(ignore_columns={"nonexistent"})
        converter1 = PolarsConverter(config1)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in ignore_columns: nonexistent"
        ):
            converter1.convert(spec)

        # Test unknown include_columns
        config2 = PolarsConverterConfig(include_columns={"nonexistent"})
        converter2 = PolarsConverter(config2)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in include_columns: nonexistent"
        ):
            converter2.convert(spec)

    def test_conflicting_ignore_and_include_raises_error(self):
        """Test that overlapping ignore_columns and include_columns raises error."""
        with pytest.raises(
            ConverterConfigError, match="Columns cannot be both ignored and included"
        ):
            PolarsConverterConfig(
                ignore_columns={"col1", "col2"}, include_columns={"col1", "col3"}
            )


# %% Field-level fallback in complex types
class TestPolarsConverterFieldLevelFallback:
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

        config = PolarsConverterConfig(fallback_type=pl.String)
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Should have warnings for the unsupported field within the struct
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "geography" in str(w[0].message)
        assert "unsupported_field" in str(w[0].message)

        # The struct field should still be a struct
        expected_struct = pl.Struct(
            [
                pl.Field("supported_field", pl.String),
                pl.Field("unsupported_field", pl.String),  # fallback
                pl.Field("another_supported", pl.Int32),
            ]
        )
        assert schema["data"] == expected_struct

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

        config = PolarsConverterConfig(fallback_type=pl.String)
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Should have warnings for both unsupported fields
        assert len(w) == 2
        assert all(issubclass(warning.category, ValidationWarning) for warning in w)

        warning_messages = [str(warning.message) for warning in w]
        assert any("geography" in msg and "outer_geog" in msg for msg in warning_messages)
        assert any("geometry" in msg and "inner_geom" in msg for msg in warning_messages)

        # Check nested structure
        expected_outer = pl.Struct(
            [
                pl.Field("outer_geog", pl.String),  # fallback
                pl.Field(
                    "inner_data",
                    pl.Struct(
                        [
                            pl.Field("inner_geom", pl.String),  # fallback
                            pl.Field("inner_string", pl.String),
                        ]
                    ),
                ),
                pl.Field("outer_int", pl.Int32),
            ]
        )
        assert schema["complex_data"] == expected_outer

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

        config = PolarsConverterConfig(fallback_type=pl.String)
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Should have warning for the unsupported element type
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "geography" in str(w[0].message)

        # The array should still be an array with fallback element
        assert schema["locations"] == pl.List(pl.String)

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

        config = PolarsConverterConfig(fallback_type=pl.String)
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Should have warning for the unsupported field within the struct
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "geography" in str(w[0].message)
        assert "location" in str(w[0].message)

        # Check nested structure
        expected = pl.List(
            pl.Struct(
                [
                    pl.Field("name", pl.String),
                    pl.Field("location", pl.String),  # fallback
                ]
            )
        )
        assert schema["items"] == expected

    def test_complex_nested_structure_with_mixed_fallbacks(self):
        innermost_struct = Struct(
            fields=[
                Field(name="variant_field", type=Variant()),
                Field(name="normal_string", type=String()),
            ]
        )

        middle_struct = Struct(
            fields=[
                Field(name="geometry_field", type=Geometry()),
                Field(name="inner_data", type=innermost_struct),
                Field(name="normal_int", type=Integer()),
            ]
        )

        array_of_middle_structs = Array(element=middle_struct)

        spec = YadsSpec(
            name="test_complex_nested_fallback",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="complex_array", type=array_of_middle_structs),
            ],
        )

        config = PolarsConverterConfig(fallback_type=pl.String)
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Should have warnings for all unsupported types
        assert len(w) == 2
        assert all(issubclass(warning.category, ValidationWarning) for warning in w)

        warning_messages = [str(warning.message) for warning in w]
        assert any("geometry" in msg for msg in warning_messages)
        assert any("variant" in msg for msg in warning_messages)

        # Check nested structure
        expected = pl.List(
            pl.Struct(
                [
                    pl.Field("geometry_field", pl.String),  # fallback
                    pl.Field(
                        "inner_data",
                        pl.Struct(
                            [
                                pl.Field("variant_field", pl.String),  # fallback
                                pl.Field("normal_string", pl.String),
                            ]
                        ),
                    ),
                    pl.Field("normal_int", pl.Int32),
                ]
            )
        )
        assert schema["complex_array"] == expected

    @pytest.mark.parametrize(
        "fallback_type",
        _VALID_FALLBACK_TYPES,
    )
    def test_fallback_type_preservation_in_nested_structures(
        self, fallback_type: pl.DataType
    ):
        struct_with_unsupported = Struct(
            fields=[
                Field(name="geom", type=Geometry()),
                Field(name="geog", type=Geography()),
                Field(name="variant", type=Variant()),
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

        config = PolarsConverterConfig(fallback_type=fallback_type)
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Should have warnings for all unsupported types
        assert len(w) == 3

        # All unsupported fields should get the same fallback type
        expected = pl.Struct(
            [
                pl.Field("geom", fallback_type),
                pl.Field("geog", fallback_type),
                pl.Field("variant", fallback_type),
            ]
        )
        assert schema["data"] == expected

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

        config = PolarsConverterConfig(mode="raise")
        converter = PolarsConverter(config)

        with pytest.raises(
            UnsupportedFeatureError,
            match="PolarsConverter does not support type: geometry for 'geom'.",
        ):
            converter.convert(spec)

    def test_map_in_struct_coerces_to_nested_struct(self):
        """Test that Map inside Struct coerces properly in coerce mode."""
        struct_with_map = Struct(
            fields=[
                Field(name="id", type=Integer()),
                Field(name="tags", type=Map(key=String(), value=String())),
            ]
        )

        spec = YadsSpec(
            name="test_map_in_struct",
            version="1.0.0",
            columns=[
                Column(name="data", type=struct_with_map),
            ],
        )

        config = PolarsConverterConfig()
        converter = PolarsConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = converter.convert(spec, mode="coerce")

        # Should have warning for Map
        assert len(w) == 1
        assert "PolarsConverter does not support type: map" in str(w[0].message)

        # Map should coerce to Struct with key/value fields
        expected = pl.Struct(
            [
                pl.Field("id", pl.Int32),
                pl.Field(
                    "tags",
                    pl.Struct(
                        [
                            pl.Field("key", pl.String),
                            pl.Field("value", pl.String),
                        ]
                    ),
                ),
            ]
        )
        assert schema["data"] == expected
