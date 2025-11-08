import warnings
from datetime import date as PyDate, datetime as PyDatetime, time as PyTime, timedelta
from decimal import Decimal as PyDecimal
from typing import Any, get_args, get_origin
from uuid import UUID as PyUUID

import pytest
from pydantic import BaseModel, create_model
from pydantic import Field as PydanticField

from yads._dependencies import _get_installed_version, _meets_min_version
from yads.converters import PydanticConverter, PydanticConverterConfig
from yads.constraints import (
    DefaultConstraint,
    ForeignKeyConstraint,
    ForeignKeyReference,
    IdentityConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
)
from yads.exceptions import (
    UnsupportedFeatureError,
    ValidationWarning,
    ConverterConfigError,
)
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
    Tensor,
)


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

        # Handle version-specific constraints
        for k, v in attrs.items():
            # Skip decimal constraints if Pydantic doesn't support them
            if (
                k in ("max_digits", "decimal_places")
                and not supports_decimal_constraints()
            ):
                # In older Pydantic versions, these constraints should not be present
                assert found.get(k) is None
            else:
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


def supports_decimal_constraints() -> bool:
    """Check if the installed Pydantic version supports Decimal constraints."""
    pydantic_version = _get_installed_version("pydantic")
    if pydantic_version is None:
        return False
    return _meets_min_version(pydantic_version, "2.8.0")


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
            (Date(bits=32), PyDate, "bits constraint will be lost", lambda f: None),
            (Date(bits=64), PyDate, "bits constraint will be lost", lambda f: None),
            (Time(), PyTime, "bits and/or unit constraints will be lost", lambda f: None),
            (Time(unit=TimeUnit.S), PyTime, "bits and/or unit constraints will be lost", lambda f: None),
            (Time(unit=TimeUnit.MS), PyTime, "bits and/or unit constraints will be lost", lambda f: None),
            (Time(unit=TimeUnit.US), PyTime, "bits and/or unit constraints will be lost", lambda f: None),
            (Time(unit=TimeUnit.NS), PyTime, "bits and/or unit constraints will be lost", lambda f: None),
            (Time(bits=32), PyTime, "bits and/or unit constraints will be lost", lambda f: None),
            (Time(bits=64), PyTime, "bits and/or unit constraints will be lost", lambda f: None),
            (Timestamp(), PyDatetime, "unit constraint will be lost", lambda f: None),
            (Timestamp(unit=TimeUnit.S), PyDatetime, "unit constraint will be lost", lambda f: None),
            (TimestampTZ(), PyDatetime, "unit and/or tz constraints will be lost", lambda f: None),
            (TimestampTZ(tz="UTC"), PyDatetime, "unit and/or tz constraints will be lost", lambda f: None),
            (TimestampLTZ(), PyDatetime, "unit constraint will be lost", lambda f: None),
            (TimestampNTZ(), PyDatetime, "unit constraint will be lost", lambda f: None),

            # Duration
            (Duration(), timedelta, "unit constraint will be lost", lambda f: None),

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

            # Spatial types -> coerce to str with warning
            (Geometry(), str, "PydanticConverter does not support type: geometry", lambda f: None),
            (Geography(), str, "PydanticConverter does not support type: geography", lambda f: None),

            # Other
            (UUID(), PyUUID, None, lambda f: None),
            (Void(), type(None), None, lambda f: None),
            (Variant(), Any, None, lambda f: None),
            (
                Tensor(element=Integer(bits=32), shape=(10, 20)),
                str, # Coerced to str with warning
                "PydanticConverter does not support type: tensor<integer(bits=32), shape=[10, 20]>", lambda f: None
            ),
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
        converter = PydanticConverter(PydanticConverterConfig(fallback_type=str))

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
            assert expected_warning in str(w[0].message)
        else:
            # Special case: Decimal with precision/scale in Pydantic < 2.8.0
            # will emit a warning even though expected_warning is None
            if isinstance(yads_type, Decimal) and yads_type.precision is not None and not supports_decimal_constraints():
                assert len(w) == 1
                assert issubclass(w[0].category, ValidationWarning)
                assert "Decimal precision and scale constraints" in str(w[0].message)
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
        
        if supports_decimal_constraints():
            # Pydantic >= 2.8.0: Constraints should be present
            model = PydanticConverter().convert(spec)
            f = model.model_fields["d"]
            assert unwrap_optional(f.annotation) == PyDecimal
            dec = extract_constraints(f)
            assert dec.get("max_digits") == 12 and dec.get("decimal_places") == 3
        else:
            # Pydantic < 2.8.0: Constraints not supported, warning expected in coerce mode
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                model = PydanticConverter().convert(spec, mode="coerce")
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert "Decimal precision and scale constraints" in str(w[0].message)
            f = model.model_fields["d"]
            assert unwrap_optional(f.annotation) == PyDecimal
            # Constraints should not be present
            dec = extract_constraints(f)
            assert dec.get("max_digits") is None
            assert dec.get("decimal_places") is None

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
            PydanticConverter(PydanticConverterConfig(mode="raise")).convert(spec)

    def test_geometry_geography_coerce_and_raise(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="g1", type=Geometry()), Column(name="g2", type=Geography())],
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = PydanticConverter(
                PydanticConverterConfig(fallback_type=str)
            ).convert(spec, mode="coerce")
        assert len(w) == 2
        msgs = "\n".join(str(x.message) for x in w)
        assert "geometry" in msgs and "geography" in msgs
        ann_g1 = unwrap_optional(model.model_fields["g1"].annotation)
        ann_g2 = unwrap_optional(model.model_fields["g2"].annotation)
        assert isinstance(ann_g1, type) and issubclass(ann_g1, str)
        assert isinstance(ann_g2, type) and issubclass(ann_g2, str)

        with pytest.raises(UnsupportedFeatureError):
            PydanticConverter(PydanticConverterConfig(mode="raise")).convert(spec)    

    def test_field_description(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="c", type=String(), description="desc")],
        )
        model = PydanticConverter().convert(spec)
        assert model.model_fields["c"].description == "desc"

    @pytest.mark.parametrize(
        "bits,min_val,max_val",
        [
            (8, -(2**7), 2**7 - 1),  # -128 to 127
            (16, -(2**15), 2**15 - 1),  # -32_768 to 32_767
            (32, -(2**31), 2**31 - 1),  # -2_147_483_648 to 2_147_483_647
            (64, -(2**63), 2**63 - 1),  # -9_223_372_036_854_775_808 to 9_223_372_036_854_775_807
        ],
    )
    def test_integer_bit_width_boundaries_signed(self, bits: int, min_val: int, max_val: int):
        """Test that signed integer bit width constraints correctly limit values."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="int_field", type=Integer(bits=bits))],
        )
        model = PydanticConverter().convert(spec)
        field = model.model_fields["int_field"]
        
        # Extract constraints from field metadata
        constraints = extract_constraints(field)
        assert constraints.get("ge") == min_val
        assert constraints.get("le") == max_val
        
        # Test that boundary values fit within the bit width
        # For signed integers, the maximum positive value should fit in bits-1 bits
        assert max_val.bit_length() <= bits - 1
        # The minimum negative value should fit in bits bits (including sign)
        assert abs(min_val).bit_length() <= bits

    @pytest.mark.parametrize(
        "bits,min_val,max_val",
        [
            (8, 0, 2**8 - 1),  # 0 to 255
            (16, 0, 2**16 - 1),  # 0 to 65_535
            (32, 0, 2**32 - 1),  # 0 to 4_294_967_295
            (64, 0, 2**64 - 1),  # 0 to 18_446_744_073_709_551_615
        ],
    )
    def test_integer_bit_width_boundaries_unsigned(self, bits: int, min_val: int, max_val: int):
        """Test that unsigned integer bit width constraints correctly limit values."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="int_field", type=Integer(bits=bits, signed=False))],
        )
        model = PydanticConverter().convert(spec)
        field = model.model_fields["int_field"]
        
        # Extract constraints from field metadata
        constraints = extract_constraints(field)
        assert constraints.get("ge") == min_val
        assert constraints.get("le") == max_val
        
        # Test that boundary values have correct bit lengths
        assert min_val.bit_length() <= bits
        assert max_val.bit_length() <= bits
        
        # Test that values just outside boundaries would exceed bit length
        assert (max_val + 1).bit_length() > bits

    def test_integer_bit_width_edge_cases(self):
        """Test edge cases for integer bit width validation."""
        # Test 8-bit signed: -128 to 127
        spec_8bit = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="int8", type=Integer(bits=8))],
        )
        model_8bit = PydanticConverter().convert(spec_8bit)
        field_8bit = model_8bit.model_fields["int8"]
        constraints_8bit = extract_constraints(field_8bit)
        
        # Verify exact boundaries
        assert constraints_8bit.get("ge") == -128
        assert constraints_8bit.get("le") == 127

        # Test 8-bit unsigned: 0 to 255
        spec_8bit_unsigned = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="uint8", type=Integer(bits=8, signed=False))],
        )
        model_8bit_unsigned = PydanticConverter().convert(spec_8bit_unsigned)
        field_8bit_unsigned = model_8bit_unsigned.model_fields["uint8"]
        constraints_8bit_unsigned = extract_constraints(field_8bit_unsigned)
        
        # Verify exact boundaries
        assert constraints_8bit_unsigned.get("ge") == 0
        assert constraints_8bit_unsigned.get("le") == 255

    @pytest.mark.parametrize("bits", [8, 16, 32, 64])
    def test_integer_bit_width_validation_with_pydantic(self, bits: int):
        """Test that Pydantic actually enforces the bit width constraints."""
        # Calculate boundaries for the given bit width
        max_val = 2**(bits-1) - 1
        min_val = -(2**(bits-1))
        
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name=f"int{bits}", type=Integer(bits=bits))],
        )
        model = PydanticConverter().convert(spec)
        
        # Check bit width calculations
        assert max_val.bit_length() <= bits
        assert min_val.bit_length() <= bits

        # Test valid values
        valid_instance = model(**{f"int{bits}": max_val})  # Max signed value
        assert getattr(valid_instance, f"int{bits}") == max_val
        assert getattr(valid_instance, f"int{bits}").bit_length() <= bits
        
        valid_instance_min = model(**{f"int{bits}": min_val})  # Min signed value
        assert getattr(valid_instance_min, f"int{bits}") == min_val
        assert getattr(valid_instance_min, f"int{bits}").bit_length() <= bits
        
        # Test invalid values (should raise ValidationError)
        from pydantic import ValidationError
        
        # Value too large
        with pytest.raises(ValidationError):
            model(**{f"int{bits}": max_val + 1})
            
        # Value too small
        with pytest.raises(ValidationError):
            model(**{f"int{bits}": min_val - 1})

    @pytest.mark.parametrize("bits", [8, 16, 32, 64])
    def test_integer_bit_width_unsigned_validation_with_pydantic(self, bits: int):
        """Test that Pydantic enforces unsigned integer bit width constraints."""
        # Calculate boundaries for the given bit width
        max_val = 2**bits - 1
        min_val = 0
        
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name=f"uint{bits}", type=Integer(bits=bits, signed=False))],
        )
        model = PydanticConverter().convert(spec)
        
        # Check bit width calculations
        assert max_val.bit_length() <= bits
        assert min_val.bit_length() <= bits

        # Test valid values
        valid_instance = model(**{f"uint{bits}": max_val})  # Max unsigned value
        assert getattr(valid_instance, f"uint{bits}") == max_val
        assert getattr(valid_instance, f"uint{bits}").bit_length() <= bits
        
        valid_instance_min = model(**{f"uint{bits}": min_val})  # Min unsigned value
        assert getattr(valid_instance_min, f"uint{bits}") == min_val
        assert getattr(valid_instance_min, f"uint{bits}").bit_length() <= bits
        
        # Test invalid values (should raise ValidationError)
        from pydantic import ValidationError
        
        # Value too large
        with pytest.raises(ValidationError):
            model(**{f"uint{bits}": max_val + 1})
            
        # Value too small (negative)
        with pytest.raises(ValidationError):
            model(**{f"uint{bits}": -1})
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
        from yads.converters import PydanticConverterConfig

        config = PydanticConverterConfig(
            model_name="CustomModel",
            model_config={"frozen": True, "title": "X"},
        )
        converter = PydanticConverter(config)
        model = converter.convert(spec)
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

    def test_model_config_with_string_length_validation(self):
        """Test that model_config with str_max_length actually enforces validation."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="field", type=String())],
        )

        config = PydanticConverterConfig(model_config={"str_max_length": 10})
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Test that the configuration is actually applied
        from pydantic import ValidationError

        # Short string should work
        instance = model(field="short")
        assert instance.field == "short"

        # Long string should fail validation
        with pytest.raises(ValidationError) as exc_info:
            model(field="this_is_a_very_long_string_that_exceeds_ten_characters")

        assert "String should have at most 10 characters" in str(exc_info.value)

    def test_model_config_with_frozen_model(self):
        """Test that model_config with frozen=True creates immutable models."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="field", type=String())],
        )

        config = PydanticConverterConfig(model_config={"frozen": True})
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Test that the model is frozen
        instance = model(field="test")
        assert instance.field == "test"

        # Attempting to modify should raise ValidationError
        from pydantic import ValidationError

        with pytest.raises(ValidationError) as exc_info:
            instance.field = "modified"

        assert "Instance is frozen" in str(exc_info.value)

    def test_model_config_with_title_and_description(self):
        """Test that model_config with title and description works."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="field", type=String())],
        )

        config = PydanticConverterConfig(
            model_config={"title": "Test Model", "description": "A test model"}
        )
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Test that the configuration is applied
        model_config = getattr(model, "model_config")
        assert model_config["title"] == "Test Model"
        assert model_config["description"] == "A test model"

    def test_model_config_with_multiple_options(self):
        """Test that model_config with multiple options works together."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="field", type=String())],
        )

        config = PydanticConverterConfig(
            model_config={
                "frozen": True,
                "title": "Multi Config Model",
                "str_max_length": 5,
                "validate_assignment": True,
            }
        )
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Test that all configurations are applied
        model_config = getattr(model, "model_config")
        assert model_config["frozen"] is True
        assert model_config["title"] == "Multi Config Model"
        assert model_config["str_max_length"] == 5
        assert model_config["validate_assignment"] is True

        # Test that validation works
        from pydantic import ValidationError

        # Short string should work
        instance = model(field="hi")
        assert instance.field == "hi"

        # Long string should fail
        with pytest.raises(ValidationError):
            model(field="too_long")

    def test_model_config_with_dict_vs_configdict(self):
        """Test that both dict and ConfigDict work for model_config."""
        from pydantic import ConfigDict

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="field", type=String())],
        )

        # Test with regular dict
        config_dict = PydanticConverterConfig(model_config={"str_max_length": 10})
        converter_dict = PydanticConverter(config_dict)
        model_dict = converter_dict.convert(spec)

        # Test with ConfigDict
        config_configdict = PydanticConverterConfig(
            model_config=ConfigDict(str_max_length=10)
        )
        converter_configdict = PydanticConverter(config_configdict)
        model_configdict = converter_configdict.convert(spec)

        # Both should work the same way
        from pydantic import ValidationError

        # Both should accept short strings
        instance_dict = model_dict(field="short")
        instance_configdict = model_configdict(field="short")
        assert instance_dict.field == "short"
        assert instance_configdict.field == "short"

        # Both should reject long strings
        with pytest.raises(ValidationError):
            model_dict(field="this_is_too_long")

        with pytest.raises(ValidationError):
            model_configdict(field="this_is_too_long")

    def test_model_config_none_does_not_break(self):
        """Test that model_config=None doesn't break the converter."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="field", type=String())],
        )

        config = PydanticConverterConfig(model_config=None)
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Should work normally without any special config
        instance = model(field="test")
        assert instance.field == "test"

    def test_model_config_empty_dict_works(self):
        """Test that model_config={} works without issues."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[Column(name="field", type=String())],
        )

        config = PydanticConverterConfig(model_config={})
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Should work normally
        instance = model(field="test")
        assert instance.field == "test"

    def test_model_config_with_complex_types(self):
        """Test that model_config works with complex nested types."""
        nested_struct = Struct(
            fields=[
                Field(name="inner_field", type=String()),
            ]
        )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="data", type=nested_struct),
            ],
        )

        config = PydanticConverterConfig(
            model_config={"frozen": True, "title": "Complex Model"}
        )
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Test that the configuration is applied to the main model
        model_config = getattr(model, "model_config")
        assert model_config["frozen"] is True
        assert model_config["title"] == "Complex Model"

        # Test that the model works with complex types
        instance = model(id=1, data={"inner_field": "test"})
        assert instance.id == 1
        assert instance.data.inner_field == "test"

    def test_model_config_preserves_field_validation(self):
        """Test that model_config doesn't interfere with field-level validation."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="short_field", type=String(length=5)),
                Column(name="long_field", type=String()),
            ],
        )

        config = PydanticConverterConfig(model_config={"str_max_length": 10})
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        from pydantic import ValidationError

        # Field-level constraint (length=5) should still work
        instance = model(short_field="hi", long_field="hello")
        assert instance.short_field == "hi"
        assert instance.long_field == "hello"

        # Field-level constraint should be enforced
        with pytest.raises(ValidationError):
            model(short_field="too_long_for_field", long_field="hello")

        # Model-level constraint should also be enforced
        with pytest.raises(ValidationError):
            model(short_field="hi", long_field="this_is_too_long_for_model")


# %% Mode hierarchy
class TestPydanticConverterModeHierarchy:
    def test_instance_mode_raise_used_by_default(self):
        yaml_like_spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="c", type=Geometry())],
        )
        converter = PydanticConverter(PydanticConverterConfig(mode="raise"))
        with pytest.raises(UnsupportedFeatureError):
            converter.convert(yaml_like_spec)

    def test_call_override_to_coerce_does_not_persist(self):
        spec = YadsSpec(
            name="t",
            version="1.0.0",
            columns=[Column(name="c", type=Geometry())],
        )
        converter = PydanticConverter(
            PydanticConverterConfig(mode="raise", fallback_type=str)
        )
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")
        assert issubclass(w[0].category, ValidationWarning)
        assert model is not None

        with pytest.raises(UnsupportedFeatureError):
            converter.convert(spec)


# %% PydanticConverter column filtering and customization
class TestPydanticConverterCustomization:
    def test_ignore_columns(self):
        """Test that ignore_columns excludes specified columns from the model."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
                Column(name="secret", type=String()),
            ],
        )
        config = PydanticConverterConfig(ignore_columns={"secret"})
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        assert "id" in model.model_fields
        assert "name" in model.model_fields
        assert "secret" not in model.model_fields

    def test_include_columns(self):
        """Test that include_columns only includes specified columns in the model."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
                Column(name="internal", type=String()),
            ],
        )
        config = PydanticConverterConfig(include_columns={"id", "name"})
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        assert "id" in model.model_fields
        assert "name" in model.model_fields
        assert "internal" not in model.model_fields

    def test_column_override_basic(self):
        """Test basic column override functionality."""

        def custom_name_override(field, converter):
            # Override name field to be uppercase with custom validation
            return str, PydanticField(
                default=..., min_length=1, description="Custom name field"
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="name", type=String()),
            ],
        )
        config = PydanticConverterConfig(column_overrides={"name": custom_name_override})
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Check that override was applied
        name_field = model.model_fields["name"]
        name_field_annotation = unwrap_optional(name_field.annotation)
        assert isinstance(name_field_annotation, type) and issubclass(
            name_field_annotation, str
        )
        assert name_field.description == "Custom name field"
        constraints = extract_constraints(name_field)
        assert constraints.get("min_length") == 1

        # Check that other fields use default conversion
        id_field = model.model_fields["id"]
        id_field_annotation = unwrap_optional(id_field.annotation)
        assert isinstance(id_field_annotation, type) and issubclass(
            id_field_annotation, int
        )

    def test_column_override_with_complex_type(self):
        """Test column override with complex custom type."""

        def custom_metadata_override(field, converter):
            # Create a custom nested model for metadata
            metadata_model = create_model(
                "CustomMetadata",
                version=(str, PydanticField(default=...)),
                tags=(list[str], PydanticField(default_factory=list)),
            )
            return metadata_model, PydanticField(default=...)

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="metadata", type=JSON()),
            ],
        )
        config = PydanticConverterConfig(
            column_overrides={"metadata": custom_metadata_override}
        )
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Check that override was applied
        metadata_field = model.model_fields["metadata"]
        metadata_type = unwrap_optional(metadata_field.annotation)
        assert isinstance(metadata_type, type) and issubclass(metadata_type, BaseModel)
        assert set(metadata_type.model_fields.keys()) == {"version", "tags"}

        metadata_field_version = metadata_type.model_fields["version"]
        assert isinstance(metadata_field_version.annotation, type) and issubclass(
            metadata_field_version.annotation, str
        )
        metadata_field_tags = metadata_type.model_fields["tags"]
        assert get_origin(metadata_field_tags.annotation) is list
        assert isinstance(
            get_args(metadata_field_tags.annotation)[0], type
        ) and issubclass(get_args(metadata_field_tags.annotation)[0], str)

    @pytest.mark.parametrize("fallback_type", [str, dict, bytes])
    def test_fallback_valid_types(self, fallback_type: type):
        """Test fallback_type=str for unsupported types."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="geom", type=Geometry()),
            ],
        )
        config = PydanticConverterConfig(fallback_type=fallback_type)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Check fallback was applied
        geom_field = model.model_fields["geom"]
        assert unwrap_optional(geom_field.annotation) == fallback_type

        # Check warning was emitted
        assert len(w) == 1
        assert "geometry" in str(w[0].message)
        assert fallback_type.__name__ in str(w[0].message)

    def test_invalid_fallback_type_raises_error(self):
        """Test that invalid fallback_type raises UnsupportedFeatureError."""
        with pytest.raises(
            UnsupportedFeatureError,
            match="fallback_type must be one of: str, dict, bytes",
        ):
            PydanticConverterConfig(fallback_type=int)

    def test_column_override_invalid_return_type_raises_error(self):
        """Test that column override returning invalid type raises error."""

        def bad_override(field, converter):
            return "not_a_tuple"  # Should return (annotation, FieldInfo)

        config = PydanticConverterConfig(column_overrides={"col": bad_override})
        converter = PydanticConverter(config)

        # Test the override method directly to avoid exception handling in convert()
        field = Field(name="col", type=String())
        with pytest.raises(
            UnsupportedFeatureError,
            match="Pydantic column override must return \\(annotation, FieldInfo\\)",
        ):
            converter._apply_column_override(field)

    def test_column_override_invalid_field_info_raises_error(self):
        """Test that column override returning non-FieldInfo raises error."""

        def bad_override(field, converter):
            return str, "not_field_info"  # Second element must be FieldInfo

        config = PydanticConverterConfig(column_overrides={"col": bad_override})
        converter = PydanticConverter(config)

        # Test the override method directly to avoid exception handling in convert()
        field = Field(name="col", type=String())
        with pytest.raises(
            UnsupportedFeatureError,
            match="Pydantic column override second element must be a FieldInfo",
        ):
            converter._apply_column_override(field)

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
        config = PydanticConverterConfig(
            ignore_columns={"ignored_col"},
            column_overrides={"ignored_col": should_not_be_called},
        )
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        assert "id" in model.model_fields
        assert "ignored_col" not in model.model_fields

    def test_precedence_override_over_default_conversion(self):
        """Test that column_overrides takes precedence over default conversion."""

        def integer_as_string_override(field, converter):
            return str, PydanticField(
                default=..., description="Integer converted to string"
            )

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="normal_int", type=Integer()),
                Column(name="string_int", type=Integer()),
            ],
        )
        config = PydanticConverterConfig(
            column_overrides={"string_int": integer_as_string_override}
        )
        converter = PydanticConverter(config)
        model = converter.convert(spec)

        # Normal conversion
        normal_field = model.model_fields["normal_int"]
        normal_field_annotation = unwrap_optional(normal_field.annotation)
        assert isinstance(normal_field_annotation, type) and issubclass(
            normal_field_annotation, int
        )

        # Override conversion
        string_field = model.model_fields["string_int"]
        string_field_annotation = unwrap_optional(string_field.annotation)
        assert isinstance(string_field_annotation, type) and issubclass(
            string_field_annotation, str
        )
        assert string_field.description == "Integer converted to string"

    def test_precedence_override_over_fallback(self):
        """Test that column_overrides takes precedence over fallback_type."""

        def custom_geometry_override(field, converter):
            return str, PydanticField(default=..., description="Custom geometry handling")

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="fallback_geom", type=Geometry()),
                Column(name="override_geom", type=Geometry()),
            ],
        )
        config = PydanticConverterConfig(
            fallback_type=dict,
            column_overrides={"override_geom": custom_geometry_override},
        )
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Fallback applied to fallback_geom
        fallback_field = model.model_fields["fallback_geom"]
        fallback_field_annotation = unwrap_optional(fallback_field.annotation)
        assert isinstance(fallback_field_annotation, type) and issubclass(
            fallback_field_annotation, dict
        )

        # Override applied to override_geom
        override_field = model.model_fields["override_geom"]
        override_field_annotation = unwrap_optional(override_field.annotation)
        assert isinstance(override_field_annotation, type) and issubclass(
            override_field_annotation, str
        )
        assert override_field.description == "Custom geometry handling"

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
        config1 = PydanticConverterConfig(ignore_columns={"nonexistent"})
        converter1 = PydanticConverter(config1)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in ignore_columns: nonexistent"
        ):
            converter1.convert(spec)

        # Test unknown include_columns
        config2 = PydanticConverterConfig(include_columns={"nonexistent"})
        converter2 = PydanticConverter(config2)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in include_columns: nonexistent"
        ):
            converter2.convert(spec)

    def test_conflicting_ignore_and_include_raises_error(self):
        """Test that overlapping ignore_columns and include_columns raises error."""
        with pytest.raises(
            ConverterConfigError,
            match="Columns cannot be both ignored and included: \\['col1'\\]",
        ):
            PydanticConverterConfig(
                ignore_columns={"col1", "col2"}, include_columns={"col1", "col3"}
            )

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
        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        geom_field = model.model_fields["geom"]

        # Check that fallback type was applied
        geom_field_annotation = unwrap_optional(geom_field.annotation)
        assert isinstance(geom_field_annotation, type) and issubclass(
            geom_field_annotation, str
        )

        # Check that field metadata was preserved during fallback
        assert geom_field.json_schema_extra is not None
        yads_metadata = geom_field.json_schema_extra.get("yads", {})
        assert yads_metadata.get("metadata") == {
            "spatial_ref": "EPSG:4326",
            "precision": "high",
        }

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
        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        geom_field = model.model_fields["geom"]

        # Check that fallback type was applied
        geom_field_annotation = unwrap_optional(geom_field.annotation)
        assert isinstance(geom_field_annotation, type) and issubclass(
            geom_field_annotation, str
        )

        # Check that both description and metadata were preserved during fallback
        assert geom_field.description == "A geometry field for spatial data"
        assert geom_field.json_schema_extra is not None
        yads_metadata = geom_field.json_schema_extra.get("yads", {})
        assert yads_metadata.get("metadata") == {"spatial_ref": "EPSG:4326"}

    def test_field_description_and_metadata_in_schema_extra(self):
        """Test that field descriptions and metadata are included in Pydantic field json_schema_extra."""
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
                    # No description or metadata
                ),
                Column(
                    name="tags",
                    type=String(),
                    metadata={"category": "user_input", "validation": "strict"},
                    # No description
                ),
            ],
        )
        converter = PydanticConverter()
        model = converter.convert(spec)

        # Test field with description only
        id_field = model.model_fields["id"]
        assert id_field.description == "Primary key identifier"
        # No metadata should mean no json_schema_extra or empty yads section
        if id_field.json_schema_extra:
            assert id_field.json_schema_extra.get("yads", {}).get("metadata") is None

        # Test field with both description and custom metadata
        name_field = model.model_fields["name"]
        assert name_field.description == "User's full name"
        assert name_field.json_schema_extra is not None
        yads_metadata = name_field.json_schema_extra.get("yads", {})
        assert yads_metadata.get("metadata") == {"max_length": "255", "encoding": "utf-8"}

        # Test field with no description or metadata
        age_field = model.model_fields["age"]
        assert age_field.description is None
        if age_field.json_schema_extra:
            assert age_field.json_schema_extra.get("yads", {}).get("metadata") is None

        # Test field with metadata but no description
        tags_field = model.model_fields["tags"]
        assert tags_field.description is None
        assert tags_field.json_schema_extra is not None
        yads_metadata = tags_field.json_schema_extra.get("yads", {})
        assert yads_metadata.get("metadata") == {
            "category": "user_input",
            "validation": "strict",
        }

    def test_fallback_preserves_nullability(self):
        """Test that fallback fields preserve nullability annotations."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                # Nullable fields that will fallback
                Column(name="nullable_geom", type=Geometry()),
                Column(name="nullable_geo", type=Geography()),
                # Non-nullable fields that will fallback
                Column(
                    name="not_null_geom",
                    type=Geometry(),
                    constraints=[NotNullConstraint()],
                ),
                Column(
                    name="not_null_geo",
                    type=Geography(),
                    constraints=[NotNullConstraint()],
                ),
            ],
        )
        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Check nullable fallback fields
        nullable_geom_field = model.model_fields["nullable_geom"]
        nullable_geo_field = model.model_fields["nullable_geo"]

        # These should be Optional[str]
        assert type(None) in get_args(nullable_geom_field.annotation)
        assert type(None) in get_args(nullable_geo_field.annotation)

        # Unwrap Optional to get the base type
        geom_base_type = unwrap_optional(nullable_geom_field.annotation)
        geo_base_type = unwrap_optional(nullable_geo_field.annotation)
        assert isinstance(geom_base_type, type) and issubclass(geom_base_type, str)
        assert isinstance(geo_base_type, type) and issubclass(geo_base_type, str)

        # Check non-nullable fallback fields
        not_null_geom_field = model.model_fields["not_null_geom"]
        not_null_geo_field = model.model_fields["not_null_geo"]

        # These should be str (not Optional[str])
        assert type(None) not in get_args(not_null_geom_field.annotation)
        assert type(None) not in get_args(not_null_geo_field.annotation)

        # Should be plain str type
        assert isinstance(not_null_geom_field.annotation, type) and issubclass(
            not_null_geom_field.annotation, str
        )
        assert isinstance(not_null_geo_field.annotation, type) and issubclass(
            not_null_geo_field.annotation, str
        )

    def test_fallback_preserves_nullability_with_different_fallback_types(self):
        """Test nullability preservation with different fallback types."""
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="nullable_geom", type=Geometry()),
                Column(
                    name="not_null_geom",
                    type=Geometry(),
                    constraints=[NotNullConstraint()],
                ),
            ],
        )

        # Test with dict fallback
        config_dict = PydanticConverterConfig(fallback_type=dict)
        converter_dict = PydanticConverter(config_dict)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            model_dict = converter_dict.convert(spec, mode="coerce")

        nullable_dict_field = model_dict.model_fields["nullable_geom"]
        not_null_dict_field = model_dict.model_fields["not_null_geom"]

        # Nullable should be Optional[dict]
        assert type(None) in get_args(nullable_dict_field.annotation)
        nullable_dict_base = unwrap_optional(nullable_dict_field.annotation)
        assert isinstance(nullable_dict_base, type) and issubclass(
            nullable_dict_base, dict
        )

        # Non-nullable should be dict
        assert type(None) not in get_args(not_null_dict_field.annotation)
        assert isinstance(not_null_dict_field.annotation, type) and issubclass(
            not_null_dict_field.annotation, dict
        )

        # Test with bytes fallback
        config_bytes = PydanticConverterConfig(fallback_type=bytes)
        converter_bytes = PydanticConverter(config_bytes)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            model_bytes = converter_bytes.convert(spec, mode="coerce")

        nullable_bytes_field = model_bytes.model_fields["nullable_geom"]
        not_null_bytes_field = model_bytes.model_fields["not_null_geom"]

        # Nullable should be Optional[bytes]
        assert type(None) in get_args(nullable_bytes_field.annotation)
        nullable_bytes_base = unwrap_optional(nullable_bytes_field.annotation)
        assert isinstance(nullable_bytes_base, type) and issubclass(
            nullable_bytes_base, bytes
        )

        # Non-nullable should be bytes
        assert type(None) not in get_args(not_null_bytes_field.annotation)
        assert isinstance(not_null_bytes_field.annotation, type) and issubclass(
            not_null_bytes_field.annotation, bytes
        )

    def test_fallback_preserves_nullability_in_nested_structs(self):
        """Test that nullability is preserved in nested struct fields that fallback."""
        nested_struct = Struct(
            fields=[
                Field(name="nullable_geom", type=Geometry()),
                Field(
                    name="not_null_geom",
                    type=Geometry(),
                    constraints=[NotNullConstraint()],
                ),
            ]
        )
        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="struct", type=nested_struct),
            ],
        )
        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # The struct field itself should be Optional[StructModel] (since it's nullable by default)
        struct_field = model.model_fields["struct"]
        assert type(None) in get_args(struct_field.annotation)
        struct_base_type = unwrap_optional(struct_field.annotation)

        # The struct should remain a struct model, not be coerced to str
        assert isinstance(struct_base_type, type) and issubclass(
            struct_base_type, BaseModel
        )

        # Check that the struct fields are properly converted with fallback
        struct_fields = struct_base_type.model_fields
        assert set(struct_fields.keys()) == {"nullable_geom", "not_null_geom"}

        # Check nullability preservation in fallback fields
        nullable_geom_field = struct_fields["nullable_geom"]
        assert type(None) in get_args(nullable_geom_field.annotation)
        nullable_geom_type = unwrap_optional(nullable_geom_field.annotation)
        assert isinstance(nullable_geom_type, type) and issubclass(
            nullable_geom_type, str
        )

        not_null_geom_field = struct_fields["not_null_geom"]
        assert type(None) not in get_args(not_null_geom_field.annotation)
        not_null_geom_type = unwrap_optional(not_null_geom_field.annotation)
        assert isinstance(not_null_geom_type, type) and issubclass(
            not_null_geom_type, str
        )


# %% Field-level fallback in complex types
class TestPydanticConverterFieldLevelFallback:
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

        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Should have warnings for the unsupported field within the struct
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "geography" in str(w[0].message)
        assert "unsupported_field" in str(w[0].message)

        # The struct field should still be a struct model, not a fallback
        data_field = model.model_fields["data"]
        data_type = unwrap_optional(data_field.annotation)
        assert isinstance(data_type, type) and issubclass(data_type, BaseModel)

        # Check individual struct fields
        struct_fields = data_type.model_fields
        assert set(struct_fields.keys()) == {
            "supported_field",
            "unsupported_field",
            "another_supported",
        }

        # Supported fields should be converted normally
        supported_field = struct_fields["supported_field"]
        supported_type = unwrap_optional(supported_field.annotation)
        assert isinstance(supported_type, type) and issubclass(supported_type, str)

        another_supported = struct_fields["another_supported"]
        another_type = unwrap_optional(another_supported.annotation)
        assert isinstance(another_type, type) and issubclass(another_type, int)

        # Unsupported field should get fallback
        unsupported_field = struct_fields["unsupported_field"]
        unsupported_type = unwrap_optional(unsupported_field.annotation)
        assert isinstance(unsupported_type, type) and issubclass(
            unsupported_type, str
        )  # fallback type

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

        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Should have warnings for both unsupported fields
        assert len(w) == 2
        assert all(issubclass(warning.category, ValidationWarning) for warning in w)

        warning_messages = [str(warning.message) for warning in w]
        assert any("geography" in msg and "outer_geog" in msg for msg in warning_messages)
        assert any("geometry" in msg and "inner_geom" in msg for msg in warning_messages)

        # The outer struct should still be a struct
        complex_field = model.model_fields["complex_data"]
        complex_type = unwrap_optional(complex_field.annotation)
        assert isinstance(complex_type, type) and issubclass(complex_type, BaseModel)

        outer_struct_fields = complex_type.model_fields
        assert set(outer_struct_fields.keys()) == {
            "outer_geog",
            "inner_data",
            "outer_int",
        }

        # Check outer level fields
        outer_geog_field = outer_struct_fields["outer_geog"]
        outer_geog_type = unwrap_optional(outer_geog_field.annotation)
        assert isinstance(outer_geog_type, type) and issubclass(
            outer_geog_type, str
        )  # fallback

        outer_int_field = outer_struct_fields["outer_int"]
        outer_int_type = unwrap_optional(outer_int_field.annotation)
        assert isinstance(outer_int_type, type) and issubclass(
            outer_int_type, int
        )  # normal conversion

        # Check inner struct
        inner_data_field = outer_struct_fields["inner_data"]
        inner_data_type = unwrap_optional(inner_data_field.annotation)
        assert isinstance(inner_data_type, type) and issubclass(
            inner_data_type, BaseModel
        )

        inner_struct_fields = inner_data_type.model_fields
        assert set(inner_struct_fields.keys()) == {"inner_geom", "inner_string"}

        inner_geom_field = inner_struct_fields["inner_geom"]
        inner_geom_type = unwrap_optional(inner_geom_field.annotation)
        assert isinstance(inner_geom_type, type) and issubclass(
            inner_geom_type, str
        )  # fallback

        inner_string_field = inner_struct_fields["inner_string"]
        inner_string_type = unwrap_optional(inner_string_field.annotation)
        assert isinstance(inner_string_type, type) and issubclass(
            inner_string_type, str
        )  # normal conversion

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

        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Should have warning for the unsupported element type
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "geography" in str(w[0].message)

        # The array field should still be an array, not a fallback
        locations_field = model.model_fields["locations"]
        locations_type = unwrap_optional(locations_field.annotation)
        assert get_origin(locations_type) is list

        # The element type should be the fallback
        element_type = get_args(locations_type)[0]
        assert isinstance(element_type, type) and issubclass(
            element_type, str
        )  # fallback type

    def test_map_with_unsupported_key_or_value(self):
        # Map with unsupported key
        map_unsupported_key = Map(key=Geometry(), value=String())

        # Map with unsupported value
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

        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Should have warnings for both unsupported types
        assert len(w) == 2
        assert all(issubclass(warning.category, ValidationWarning) for warning in w)

        warning_messages = [str(warning.message) for warning in w]
        assert any("geometry" in msg for msg in warning_messages)
        assert any("geography" in msg for msg in warning_messages)

        # Both map fields should still be maps
        geom_keys_field = model.model_fields["geom_keys"]
        geom_keys_type = unwrap_optional(geom_keys_field.annotation)
        assert get_origin(geom_keys_type) is dict

        geog_values_field = model.model_fields["geog_values"]
        geog_values_type = unwrap_optional(geog_values_field.annotation)
        assert get_origin(geog_values_type) is dict

        # Check key/value types
        geom_keys_args = get_args(geom_keys_type)
        assert isinstance(geom_keys_args[0], type) and issubclass(
            geom_keys_args[0], str
        )  # fallback for key
        assert isinstance(geom_keys_args[1], type) and issubclass(
            geom_keys_args[1], str
        )  # normal conversion for value

        geog_values_args = get_args(geog_values_type)
        assert isinstance(geog_values_args[0], type) and issubclass(
            geog_values_args[0], str
        )  # normal conversion for key
        assert isinstance(geog_values_args[1], type) and issubclass(
            geog_values_args[1], str
        )  # fallback for value

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

        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Should have warning for the unsupported field within the struct
        assert len(w) == 1
        assert issubclass(w[0].category, ValidationWarning)
        assert "geography" in str(w[0].message)
        assert "location" in str(w[0].message)

        # The array field should still be an array
        items_field = model.model_fields["items"]
        items_type = unwrap_optional(items_field.annotation)
        assert get_origin(items_type) is list

        # The element should be a struct model
        element_type = get_args(items_type)[0]
        assert isinstance(element_type, type) and issubclass(element_type, BaseModel)

        # Check struct fields
        struct_fields = element_type.model_fields
        assert set(struct_fields.keys()) == {"name", "location"}

        name_field = struct_fields["name"]
        name_type = unwrap_optional(name_field.annotation)
        assert isinstance(name_type, type) and issubclass(
            name_type, str
        )  # normal conversion

        location_field = struct_fields["location"]
        location_type = unwrap_optional(location_field.annotation)
        assert isinstance(location_type, type) and issubclass(
            location_type, str
        )  # fallback

    def test_complex_nested_structure_with_mixed_fallbacks(self):
        innermost_struct = Struct(
            fields=[
                Field(name="geography_field", type=Geography()),
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

        map_with_unsupported = Map(key=Geography(), value=array_of_middle_structs)

        spec = YadsSpec(
            name="test_complex_nested_fallback",
            version="1.0.0",
            columns=[
                Column(name="id", type=Integer()),
                Column(name="complex_map", type=map_with_unsupported),
            ],
        )

        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Should have warnings for unsupported types (Geometry and Geography, but not Variant)
        assert len(w) == 3
        assert all(issubclass(warning.category, ValidationWarning) for warning in w)

        warning_messages = [str(warning.message) for warning in w]
        assert any("geography" in msg for msg in warning_messages)
        assert any("geometry" in msg for msg in warning_messages)

        # The map field should still be a map
        complex_map_field = model.model_fields["complex_map"]
        complex_map_type = unwrap_optional(complex_map_field.annotation)
        assert get_origin(complex_map_type) is dict

        map_args = get_args(complex_map_type)
        assert isinstance(map_args[0], type) and issubclass(
            map_args[0], str
        )  # fallback for Geography key

        # The value should be an array
        value_type = map_args[1]
        assert get_origin(value_type) is list
        array_element_type = get_args(value_type)[0]

        # The array element should be a struct
        assert isinstance(array_element_type, type) and issubclass(
            array_element_type, BaseModel
        )
        middle_struct_fields = array_element_type.model_fields
        assert set(middle_struct_fields.keys()) == {
            "geometry_field",
            "inner_data",
            "normal_int",
        }

        # Check middle struct fields
        geometry_field = middle_struct_fields["geometry_field"]
        geometry_type = unwrap_optional(geometry_field.annotation)
        assert isinstance(geometry_type, type) and issubclass(
            geometry_type, str
        )  # fallback

        normal_int_field = middle_struct_fields["normal_int"]
        normal_int_type = unwrap_optional(normal_int_field.annotation)
        assert isinstance(normal_int_type, type) and issubclass(
            normal_int_type, int
        )  # normal conversion

        # Check inner struct
        inner_data_field = middle_struct_fields["inner_data"]
        inner_data_type = unwrap_optional(inner_data_field.annotation)
        assert isinstance(inner_data_type, type) and issubclass(
            inner_data_type, BaseModel
        )

        inner_struct_fields = inner_data_type.model_fields
        assert set(inner_struct_fields.keys()) == {"geography_field", "normal_string"}

        geography_field = inner_struct_fields["geography_field"]
        geography_type = unwrap_optional(geography_field.annotation)
        assert isinstance(geography_type, type) and issubclass(
            geography_type, str
        )  # fallback

        normal_string_field = inner_struct_fields["normal_string"]
        normal_string_type = unwrap_optional(normal_string_field.annotation)
        assert isinstance(normal_string_type, type) and issubclass(
            normal_string_type, str
        )  # normal conversion

    @pytest.mark.parametrize("fallback_type", [str, dict, bytes])
    def test_fallback_type_preservation_in_nested_structures(self, fallback_type: type):
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

        config = PydanticConverterConfig(fallback_type=fallback_type)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Should have warnings for unsupported types (Geometry and Geography, but not Variant)
        assert len(w) == 2

        # The struct should still be a struct
        data_field = model.model_fields["data"]
        data_type = unwrap_optional(data_field.annotation)
        assert isinstance(data_type, type) and issubclass(data_type, BaseModel)

        struct_fields = data_type.model_fields
        assert set(struct_fields.keys()) == {"geom", "geog", "variant"}

        # Unsupported fields should get the fallback type
        geom_field = struct_fields["geom"]
        geom_type = unwrap_optional(geom_field.annotation)
        assert isinstance(geom_type, type) and issubclass(geom_type, fallback_type)

        geog_field = struct_fields["geog"]
        geog_type = unwrap_optional(geog_field.annotation)
        assert isinstance(geog_type, type) and issubclass(geog_type, fallback_type)

        # Variant is supported and converts to Any
        variant_field = struct_fields["variant"]
        variant_type = unwrap_optional(variant_field.annotation)
        assert variant_type is Any

    def test_field_metadata_preservation_in_nested_fallback(self):
        """Test that field metadata is preserved in nested fallback."""
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

        config = PydanticConverterConfig(fallback_type=str)
        converter = PydanticConverter(config)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            model = converter.convert(spec, mode="coerce")

        # Should have warning for the unsupported field
        assert len(w) == 1

        # Check that metadata was preserved
        data_field = model.model_fields["data"]
        data_type = unwrap_optional(data_field.annotation)
        struct_fields = data_type.model_fields

        geom_field = struct_fields["geom_with_metadata"]
        geom_type = unwrap_optional(geom_field.annotation)
        assert isinstance(geom_type, type) and issubclass(
            geom_type, str
        )  # fallback applied

        # Check that metadata was preserved
        assert geom_field.description == "A geometry field"
        assert geom_field.json_schema_extra is not None
        yads_metadata = geom_field.json_schema_extra.get("yads", {})
        assert yads_metadata.get("metadata") == {"srid": "4326", "precision": "high"}

        # Normal field should work as expected
        normal_field = struct_fields["normal_field"]
        normal_type = unwrap_optional(normal_field.annotation)
        assert isinstance(normal_type, type) and issubclass(normal_type, str)

    def test_raise_mode_still_raises_for_nested_unsupported_types(self):
        """Test that raise mode still raises for nested unsupported types."""
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

        config = PydanticConverterConfig(mode="raise")
        converter = PydanticConverter(config)

        with pytest.raises(
            UnsupportedFeatureError,
            match="PydanticConverter does not support type: geometry",
        ):
            converter.convert(spec)
