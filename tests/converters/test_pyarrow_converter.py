import warnings

import pyarrow as pa  # type: ignore[import-untyped]
import pytest

from yads.converters import PyArrowConverter
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
from yads.exceptions import UnsupportedFeatureError, ValidationWarning


# ======================================================================
# PyArrowConverter tests
# Scope: conversion to pyarrow Schema, types, constraints, metadata
# ======================================================================


# fmt: off
# %% Types
class TestPyArrowConverterTypes:
    @pytest.mark.parametrize(
        "yads_type, expected_pa_type",
        [
            (String(), pa.string()),
            (String(length=255), pa.string()),  # length hint ignored
            (Integer(bits=8), pa.int8()),
            (Integer(bits=16), pa.int16()),
            (Integer(bits=32), pa.int32()),
            (Integer(bits=64), pa.int64()),
            (Integer(bits=8, signed=False), pa.uint8()),
            (Integer(bits=16, signed=False), pa.uint16()),
            (Integer(bits=32, signed=False), pa.uint32()),
            (Integer(bits=64, signed=False), pa.uint64()),
            (Float(bits=16), pa.float16()),
            (Float(bits=32), pa.float32()),
            (Float(bits=64), pa.float64()),
            (Decimal(), pa.decimal128(38, 0)),
            (Decimal(precision=10, scale=2), pa.decimal128(10, 2)),
            (Decimal(precision=10, scale=2, bits=128), pa.decimal128(10, 2)),
            (Boolean(), pa.bool_()),
            (Binary(), pa.binary()),
            (Binary(length=8), pa.binary(8)),
            (Date(), pa.date32()),
            (Date(bits=32), pa.date32()),
            (Date(bits=64), pa.date64()),
            (Time(), pa.time32("ms")),  # default unit ms -> time32
            (Time(unit=TimeUnit.S), pa.time32("s")),
            (Time(unit=TimeUnit.MS), pa.time32("ms")),
            (Time(unit=TimeUnit.US), pa.time64("us")),
            (Time(unit=TimeUnit.NS), pa.time64("ns")),
            (Time(bits=32, unit=TimeUnit.S), pa.time32("s")),
            (Time(bits=64, unit=TimeUnit.US), pa.time64("us")),
            (Timestamp(), pa.timestamp("ns")),
            (Timestamp(unit=TimeUnit.S), pa.timestamp("s")),
            (Timestamp(unit=TimeUnit.MS), pa.timestamp("ms")),
            (Timestamp(unit=TimeUnit.US), pa.timestamp("us")),
            (Timestamp(unit=TimeUnit.NS), pa.timestamp("ns")),
            (TimestampTZ(), pa.timestamp("ns", tz="UTC")),
            (TimestampTZ(unit=TimeUnit.S), pa.timestamp("s", tz="UTC")),
            (TimestampLTZ(), pa.timestamp("ns", tz=None)),
            (TimestampNTZ(), pa.timestamp("ns", tz=None)),
            (Duration(), pa.duration("ns")),
            (Interval(interval_start=IntervalTimeUnit.DAY), pa.month_day_nano_interval()),
            (Array(element=Integer()), pa.list_(pa.int32())),
            (Array(element=String(), size=2), pa.list_(pa.string(), list_size=2)),
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
            ),
            (Map(key=String(), value=Integer()), pa.map_(pa.string(), pa.int32())),
            (JSON(), pa.json_(storage_type=pa.utf8())),
            (UUID(), pa.uuid()),
            (Void(), pa.null()),
        ],
    )
    def test_convert_type(self, yads_type: YadsType, expected_pa_type: pa.DataType):
        spec = YadsSpec(
            name="test_spec",
            version="1.0.0",
            columns=[Column(name="col1", type=yads_type)],
        )
        converter = PyArrowConverter()
        schema = converter.convert(spec)

        assert schema.names == ["col1"]
        assert schema.field("col1").type == expected_pa_type
        assert schema.field("col1").nullable is True

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
                Column(name="payload", type=nested),
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

        payload_field = schema.field("payload")
        assert payload_field.nullable is True
        assert pa.types.is_struct(payload_field.type)

        # Check nested struct fields' nullability
        payload_struct = payload_field.type
        assert payload_struct.num_fields == 2
        assert payload_struct.field("x").nullable is False
        assert payload_struct.field("x").type == pa.int32()
        assert payload_struct.field("y").nullable is True
        assert payload_struct.field("y").type == pa.string()

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
        schema = PyArrowConverter().convert(
            spec,
            use_large_string=True,
            use_large_binary=True,
            use_large_list=True,
        )

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
            PyArrowConverter(mode="raise").convert(spec)

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
            (Geometry(), "GEOMETRY"),
            (Geometry(srid=4326), "GEOMETRY"),
            (Geography(), "GEOGRAPHY"),
            (Geography(srid=4326), "GEOGRAPHY"),
            (Variant(), "VARIANT"),
        ],
    )
    def test_coerce_mode_fallback_for_unsupported_types(self, yads_type: YadsType, type_name: str):
        # Test unsupported types in coerce mode
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="col1", type=yads_type)],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = PyArrowConverter().convert(spec, mode="coerce")

        assert schema.field("col1").type == pa.binary()
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert f"Data type '{type_name}' is not supported" in str(w[0].message)

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
            PyArrowConverter(mode="raise").convert(spec)

    def test_raise_mode_for_incompatible_decimal_precision(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="d", type=Decimal(precision=39, scale=2, bits=128))],
        )

        with pytest.raises(UnsupportedFeatureError, match="precision > 38 is incompatible with Decimal\\(bits=128\\)"):
            PyArrowConverter(mode="raise").convert(spec)

    def test_raise_mode_for_incompatible_time_bits_unit(self):
        # time32 with us -> should raise in raise mode
        spec1 = YadsSpec(
            name="t1",
            version="1.0.0",
            columns=[Column(name="t", type=Time(bits=32, unit=TimeUnit.US))],
        )
        with pytest.raises(UnsupportedFeatureError, match="time32 supports only 's' or 'ms' units"):
            PyArrowConverter(mode="raise").convert(spec1)

        # time64 with ms -> should raise in raise mode
        spec2 = YadsSpec(
            name="t2",
            version="1.0.0",
            columns=[Column(name="t", type=Time(bits=64, unit=TimeUnit.MS))],
        )
        with pytest.raises(UnsupportedFeatureError, match="time64 supports only 'us' or 'ns' units"):
            PyArrowConverter(mode="raise").convert(spec2)
# fmt: on
