import warnings
from datetime import date as PyDate, datetime as PyDatetime, time as PyTime, timedelta
from decimal import Decimal as PyDecimal
from typing import Any, get_args, get_origin
from uuid import UUID as PyUUID

import pytest
from pydantic import BaseModel

from yads.converters import PydanticConverter
from yads.constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyReference,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)
from yads.exceptions import UnsupportedFeatureError, ValidationWarning
from yads.spec import Column, Field, YadsSpec
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


# ======================================================================
# PydanticConverter tests
# Scope: conversion to Pydantic model class, types, constraints, metadata
# ======================================================================


# Helpers
def extract_constraints(field_info: Any) -> dict[str, Any]:
    constraints: dict[str, Any] = {}
    for meta in getattr(field_info, "metadata", []) or []:
        for name in (
            "ge",
            "le",
            "gt",
            "lt",
            "min_length",
            "max_length",
            "max_digits",
            "decimal_places",
        ):
            value = getattr(meta, name, None)
            if value is not None:
                constraints[name] = value
    return constraints


def check_attrs(**attrs: Any):
    """Creates a function that asserts a Pydantic FieldInfo has specific attributes."""

    # This outer function is a factory that captures the expected attributes.
    def _fn(field_info):
        # The returned function takes the Pydantic FieldInfo object to be inspected.
        found = extract_constraints(field_info)
        for k, v in attrs.items():
            # Assert that the found attribute matches the expected value.
            assert found.get(k) == v

    # Return the configured assertion function.
    return _fn


def unwrap_optional(annotation: Any) -> Any:
    """Extracts the underlying type T from an Optional[T] or Union[T, None].
    If the provided annotation is not an Optional type, it is returned unchanged.

    Examples:
        >>> from typing import Optional, Union
        >>> unwrap_optional(Optional[int])
        <class 'int'>
        >>> unwrap_optional(Union[str, None])
        <class 'str'>
        >>> unwrap_optional(bool)
        <class 'bool'>
    """
    origin = get_origin(annotation)
    # If not a generic type, it can't be Optional, so return as is.
    if origin is None:
        return annotation
    args = get_args(annotation)
    non_none = [arg for arg in args if arg is not type(None)]
    # Return the first non-None type, or the original annotation if none are found.
    return non_none[0] if non_none else annotation


# fmt: off
# %% Types
class TestPydanticConverterTypes:
    @pytest.mark.parametrize(
        "yads_type, expected_py_type, expected_warning, extra_asserts",
        [
            # String types
            (String(), str, None, check_attrs(max_length=None)),
            (String(length=255), str, None, check_attrs(max_length=255)),

            # Integer types
            (Integer(), int, None, check_attrs(ge=None, le=None)),
            (Integer(bits=8), int, None, check_attrs(ge=-(2**7), le=2**7 - 1)),
            (Integer(bits=16), int, None, check_attrs(ge=-(2**15), le=2**15 - 1)),
            (Integer(bits=32), int, None, check_attrs(ge=-(2**31), le=2**31 - 1)),
            (Integer(bits=64), int, None, check_attrs(ge=-(2**63), le=2**63 - 1)),
            (Integer(signed=False), int, None, check_attrs(ge=0, le=None)),
            (Integer(bits=8, signed=False), int, None, check_attrs(ge=0, le=2**8 - 1)),
            (Integer(bits=16, signed=False), int, None, check_attrs(ge=0, le=2**16 - 1)),
            (Integer(bits=32, signed=False), int, None, check_attrs(ge=0, le=2**32 - 1)),
            (Integer(bits=64, signed=False), int, None, check_attrs(ge=0, le=2**64 - 1)),

            # Float types
            (Float(), float, None, lambda f: None),
            (Float(bits=16), float, "Float(bits=16) cannot be represented exactly", lambda f: None),
            (Float(bits=32), float, "Float(bits=32) cannot be represented exactly", lambda f: None),
            (Float(bits=64), float, None, lambda f: None),

            # Decimal
            (Decimal(), PyDecimal, None, check_attrs(max_digits=None, decimal_places=None)),
            (Decimal(precision=10, scale=2), PyDecimal, None, check_attrs(max_digits=10, decimal_places=2)),
            (Decimal(precision=10, scale=2, bits=128), PyDecimal, None, check_attrs(max_digits=10, decimal_places=2)),

            # Boolean
            (Boolean(), bool, None, lambda f: None),

            # Binary
            (Binary(), bytes, None, check_attrs(min_length=None, max_length=None)),
            (Binary(length=8), bytes, None, check_attrs(min_length=8, max_length=8)),

            # Temporal
            (Date(), PyDate, None, lambda f: None),
            (Date(bits=32), PyDate, None, lambda f: None),
            (Date(bits=64), PyDate, None, lambda f: None),
            (Time(), PyTime, None, lambda f: None),
            (Time(unit=TimeUnit.S), PyTime, None, lambda f: None),
            (Time(unit=TimeUnit.MS), PyTime, None, lambda f: None),
            (Time(unit=TimeUnit.US), PyTime, None, lambda f: None),
            (Time(unit=TimeUnit.NS), PyTime, None, lambda f: None),
            (Time(bits=32), PyTime, None, lambda f: None),
            (Time(bits=64), PyTime, None, lambda f: None),
            (Timestamp(), PyDatetime, None, lambda f: None),
            (Timestamp(unit=TimeUnit.S), PyDatetime, None, lambda f: None),
            (TimestampTZ(), PyDatetime, None, lambda f: None),
            (TimestampTZ(tz="UTC"), PyDatetime, None, lambda f: None),
            (TimestampLTZ(), PyDatetime, None, lambda f: None),
            (TimestampNTZ(), PyDatetime, None, lambda f: None),

            # Duration
            (Duration(), timedelta, None, lambda f: None),

            # Interval -> nested model
            (Interval(interval_start=IntervalTimeUnit.DAY), BaseModel, None, lambda f: None),

            # Complex types
            (Array(element=Integer()), list, None, lambda f: None),
            (Array(element=String(), size=2), list, None, check_attrs(min_length=2, max_length=2)),
            (
                Struct(
                    fields=[
                        Field(name="a", type=Integer()),
                        Field(name="b", type=String()),
                    ]
                ),
                BaseModel,
                None,
                lambda f: None,
            ),

            # Map
            (Map(key=String(), value=Integer()), dict, None, lambda f: None),

            # JSON
            (JSON(), dict, None, lambda f: None),

            # Spatial types -> coerce to object (dict) with warning
            (Geometry(), dict, "Data type 'GEOMETRY' is not supported", lambda f: None),
            (Geography(), dict, "Data type 'GEOGRAPHY' is not supported", lambda f: None),

            # Other
            (UUID(), PyUUID, None, lambda f: None),
            (Void(), type(None), None, lambda f: None),
            (Variant(), Any, None, lambda f: None),
        ],
    )
    def test_convert_type(
        self,
        yads_type: YadsType,
        expected_py_type: type[Any] | Any,
        expected_warning: str | None,
        extra_asserts,
    ):
        spec = YadsSpec(
            name="test_spec",
            version="1.0.0",
            columns=[Column(name="col1", type=yads_type)],
        )
        converter = PydanticConverter()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model_cls = converter.convert(spec, mode="coerce")

        field = model_cls.model_fields["col1"]

        # Assert annotation/type mapping
        ann = unwrap_optional(field.annotation)
        if expected_py_type is BaseModel:
            assert isinstance(ann, type) and issubclass(ann, BaseModel)
        elif expected_py_type is list:
            assert get_origin(ann) is list
        elif expected_py_type is dict:
            # Accept plain dict or typed dict[key, value]
            assert ann is dict or get_origin(ann) is dict
        else:
            assert ann == expected_py_type

        # Assert warnings for unsupported/coerced types
        if expected_warning is not None:
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert "does not support type" in str(w[0].message) or (
                expected_warning in str(w[0].message)
            )
        else:
            assert len(w) == 0

        # Type-specific FieldInfo checks
        if extra_asserts:
            extra_asserts(field)

        # Additional structural assertions for complex types
        if isinstance(yads_type, Array):
            ann_list = unwrap_optional(field.annotation)
            origin = get_origin(ann_list)
            args = get_args(ann_list)
            assert isinstance(origin, type) and issubclass(origin, list)
            elem_ann = args[0]
            # Element basic type check (only a couple representative cases)
            if isinstance(yads_type.element, Integer):
                assert isinstance(elem_ann, type) and issubclass(elem_ann, int)
            if isinstance(yads_type.element, String):
                assert isinstance(elem_ann, type) and issubclass(elem_ann, str)

        if isinstance(yads_type, Map):
            ann_map = unwrap_optional(field.annotation)
            origin = get_origin(ann_map)
            args = get_args(ann_map)
            assert isinstance(origin, type) and issubclass(origin, dict)
            if isinstance(yads_type.key, String):
                assert isinstance(args[0], type) and issubclass(args[0], str)
            if isinstance(yads_type.key, Integer):
                assert isinstance(args[0], type) and issubclass(args[0], int)
            if isinstance(yads_type.value, Integer):
                assert isinstance(args[1], type) and issubclass(args[1], int)
            if isinstance(yads_type.value, String):
                assert isinstance(args[1], type) and issubclass(args[1], str)

        if isinstance(yads_type, Struct):
            nested = unwrap_optional(field.annotation)
            assert isinstance(nested, type) and issubclass(nested, BaseModel)
            nf = nested.model_fields
            assert set(nf.keys()) == {"a", "b"}
            ann_a = unwrap_optional(nf["a"].annotation)
            ann_b = unwrap_optional(nf["b"].annotation)
            assert isinstance(ann_a, type) and issubclass(ann_a, int)
            assert isinstance(ann_b, type) and issubclass(ann_b, str)

        if isinstance(yads_type, Interval):
            interval_model = unwrap_optional(field.annotation)
            assert isinstance(interval_model, type) and issubclass(interval_model, BaseModel)
            nfields = interval_model.model_fields
            assert set(nfields.keys()) == {"months", "days", "nanoseconds"}
            for k in nfields:
                ann = unwrap_optional(nfields[k].annotation)
                assert isinstance(ann, type) and issubclass(ann, int)

    def test_nullable_vs_not_null(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(name="nn", type=String(), constraints=[NotNullConstraint()]),
                Column(name="nullable", type=Integer()),
            ],
        )
        model = PydanticConverter().convert(spec)
        nn = model.model_fields["nn"]
        nullable = model.model_fields["nullable"]

        # NotNull -> no Optional in annotation
        ann = unwrap_optional(nn.annotation)
        assert isinstance(ann, type) and issubclass(ann, str)

        # Nullable -> Optional[<type>]
        args = get_args(nullable.annotation)
        assert type(None) in args

    def test_array_and_binary_length_constraints(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(name="arr", type=Array(element=String(), size=3)),
                Column(name="bin", type=Binary(length=4)),
            ],
        )
        model = PydanticConverter().convert(spec)
        arr = model.model_fields["arr"]
        arr_c = extract_constraints(arr)
        assert arr_c.get("min_length") == 3 and arr_c.get("max_length") == 3
        binf = model.model_fields["bin"]
        bin_c = extract_constraints(binf)
        assert bin_c.get("min_length") == 4 and bin_c.get("max_length") == 4

    def test_decimal_precision_scale(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="d", type=Decimal(precision=12, scale=3))],
        )
        model = PydanticConverter().convert(spec)
        f = model.model_fields["d"]
        assert unwrap_optional(f.annotation) == PyDecimal
        dec = extract_constraints(f)
        assert dec.get("max_digits") == 12 and dec.get("decimal_places") == 3

    def test_float_bits_warning_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="f16", type=Float(bits=16))],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = PydanticConverter().convert(spec, mode="coerce")
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "Float(bits=16) cannot be represented exactly" in str(w[0].message)
        ann = unwrap_optional(model.model_fields["f16"].annotation)
        assert isinstance(ann, type) and issubclass(ann, float)

        with pytest.raises(UnsupportedFeatureError):
            PydanticConverter(mode="raise").convert(spec)

    def test_geometry_geography_coerce_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="g1", type=Geometry()), Column(name="g2", type=Geography())],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = PydanticConverter().convert(spec, mode="coerce")
        assert len(w) == 2
        msgs = "\n".join(str(x.message) for x in w)
        assert "Geometry" in msgs and "Geography" in msgs
        ann_g1 = unwrap_optional(model.model_fields["g1"].annotation)
        ann_g2 = unwrap_optional(model.model_fields["g2"].annotation)
        assert isinstance(ann_g1, type) and issubclass(ann_g1, dict)
        assert isinstance(ann_g2, type) and issubclass(ann_g2, dict)

        with pytest.raises(UnsupportedFeatureError):
            PydanticConverter(mode="raise").convert(spec)    

    def test_field_description(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="c", type=String(), description="desc")],
        )
        model = PydanticConverter().convert(spec)
        assert model.model_fields["c"].description == "desc"
# fmt: on


# %% Constraint conversion and metadata
class TestPydanticConverterConstraints:
    def test_primary_key_and_foreign_key_metadata(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer(), constraints=[PrimaryKeyConstraint()]),
                Column(
                    name="user_id",
                    type=Integer(),
                    constraints=[
                        ForeignKeyConstraint(
                            references=ForeignKeyReference(table="users", columns=["id"]),
                            name="fk_user",
                        )
                    ],
                ),
            ],
        )

        model = PydanticConverter().convert(spec)

        id_field = model.model_fields["id"]
        user_id_field = model.model_fields["user_id"]

        assert id_field.json_schema_extra is not None
        assert id_field.json_schema_extra.get("yads", {}).get("primary_key") is True

        fk_meta = user_id_field.json_schema_extra.get("yads", {}).get("foreign_key")
        assert fk_meta == {"table": "users", "columns": ["id"], "name": "fk_user"}

    def test_default_and_identity_constraints(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(
                    name="status",
                    type=String(),
                    constraints=[DefaultConstraint(value="active")],
                ),
                Column(
                    name="seq",
                    type=Integer(bits=64),
                    constraints=[IdentityConstraint(always=False, start=10, increment=5)],
                ),
            ],
        )
        model = PydanticConverter().convert(spec)
        status = model.model_fields["status"]
        seq = model.model_fields["seq"]

        assert status.default == "active"
        ident = seq.json_schema_extra.get("yads", {}).get("identity")
        assert ident == {"always": False, "start": 10, "increment": 5}

    def test_not_null_no_optional_and_nullable_optional(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[
                Column(name="nn", type=Integer(), constraints=[NotNullConstraint()]),
                Column(name="nullable", type=String()),
            ],
        )
        model = PydanticConverter().convert(spec)
        nn = model.model_fields["nn"]
        nullable = model.model_fields["nullable"]

        assert get_origin(nn.annotation) is None
        assert isinstance(nn.annotation, type) and issubclass(nn.annotation, int)
        assert type(None) in get_args(nullable.annotation)


# %% Nested struct handling
class TestPydanticConverterNested:
    def test_nested_struct_field_types_and_nullability(self):
        nested = Struct(
            fields=[
                Field(name="x", type=Integer(), constraints=[NotNullConstraint()]),
                Field(name="y", type=String()),
            ]
        )
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="s", type=nested)],
        )
        model = PydanticConverter().convert(spec)
        s = model.model_fields["s"]
        s_ann = unwrap_optional(s.annotation)
        assert isinstance(s_ann, type) and issubclass(s_ann, BaseModel)
        nf = s_ann.model_fields
        ann_x = unwrap_optional(nf["x"].annotation)
        assert isinstance(ann_x, type) and issubclass(ann_x, int)
        assert type(None) in get_args(nf["y"].annotation)


# %% Model configuration and naming
class TestPydanticConverterModelOptions:
    def test_model_config_and_custom_name(self):
        spec = YadsSpec(
            name="my.db.table",
            version="1.0.0",
            columns=[Column(name="c", type=String())],
        )
        converter = PydanticConverter()
        model = converter.convert(
            spec,
            model_name="CustomModel",
            model_config={"frozen": True, "title": "X"},
        )
        assert model.__name__ == "CustomModel"
        assert getattr(model, "model_config")["frozen"] is True
        assert getattr(model, "model_config")["title"] == "X"

    def test_default_model_name_is_spec_name_replacing_dots(self):
        spec = YadsSpec(
            name="prod.sales.orders",
            version="1.0.0",
            columns=[Column(name="id", type=Integer())],
        )
        model = PydanticConverter().convert(spec)
        assert model.__name__ == "prod_sales_orders"


# %% Mode hierarchy
class TestPydanticConverterModeHierarchy:
    def test_instance_mode_raise_used_by_default(self):
        yaml_like_spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="c", type=Geometry())],
        )
        converter = PydanticConverter(mode="raise")
        with pytest.raises(UnsupportedFeatureError):
            converter.convert(yaml_like_spec)

    def test_call_override_to_coerce_does_not_persist(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="c", type=Geometry())],
        )
        converter = PydanticConverter(mode="raise")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")
        assert issubclass(w[0].category, ValidationWarning)
        assert model is not None

        with pytest.raises(UnsupportedFeatureError):
            converter.convert(spec)
