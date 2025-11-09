import pytest
from yads.exceptions import TypeDefinitionError
from yads.types import (
    TYPE_ALIASES,
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
    Geography,
    Geometry,
    UUID,
    Void,
    Variant,
    Tensor,
)


class TestStringType:
    def test_string_default(self):
        t = String()
        assert t.length is None
        assert str(t) == "string"

    def test_string_with_length(self):
        t = String(length=255)
        assert t.length == 255
        assert str(t) == "string(length=255)"

    @pytest.mark.parametrize("invalid_length", [0, -1, -100])
    def test_string_invalid_length_raises_error(self, invalid_length):
        with pytest.raises(
            TypeDefinitionError, match="String 'length' must be a positive integer."
        ):
            String(length=invalid_length)


class TestIntegerType:
    def test_integer_default(self):
        t = Integer()
        assert t.bits is None
        assert t.signed is True
        assert str(t) == "integer"

    @pytest.mark.parametrize("bits", [8, 16, 32, 64])
    def test_integer_with_valid_bits(self, bits):
        t = Integer(bits=bits)
        assert t.bits == bits
        assert str(t) == f"integer(bits={bits})"

    @pytest.mark.parametrize("invalid_bits", [-1, 0, 1, 12, 24, 128])
    def test_integer_with_invalid_bits_raises_error(self, invalid_bits):
        with pytest.raises(
            TypeDefinitionError,
            match=f"Integer 'bits' must be one of 8, 16, 32, 64, not {invalid_bits}.",
        ):
            Integer(bits=invalid_bits)

    def test_integer_unsigned_no_bits(self):
        t = Integer(signed=False)
        assert t.bits is None
        assert t.signed is False
        assert str(t) == "integer(signed=False)"

    @pytest.mark.parametrize("bits", [8, 16, 32, 64])
    def test_integer_unsigned_with_bits(self, bits):
        t = Integer(bits=bits, signed=False)
        assert t.bits == bits
        assert t.signed is False
        assert str(t) == f"integer(bits={bits}, signed=False)"

    def test_integer_invalid_signed_type_raises_error(self):
        with pytest.raises(
            TypeDefinitionError, match="Integer 'signed' must be a boolean."
        ):
            Integer(bits=32, signed="no")  # type: ignore[arg-type]


class TestFloatType:
    def test_float_default(self):
        t = Float()
        assert t.bits is None
        assert str(t) == "float"

    @pytest.mark.parametrize("bits", [16, 32, 64])
    def test_float_with_valid_bits(self, bits):
        t = Float(bits=bits)
        assert t.bits == bits
        assert str(t) == f"float(bits={bits})"

    @pytest.mark.parametrize("invalid_bits", [-1, 0, 1, 8, 128])
    def test_float_with_invalid_bits_raises_error(self, invalid_bits):
        with pytest.raises(
            TypeDefinitionError,
            match=f"Float 'bits' must be one of 16, 32, or 64, not {invalid_bits}.",
        ):
            Float(bits=invalid_bits)


class TestDecimalType:
    def test_decimal_default(self):
        t = Decimal()
        assert t.precision is None
        assert t.scale is None
        assert t.bits is None
        assert str(t) == "decimal"

    def test_decimal_with_precision_and_scale(self):
        t = Decimal(precision=10, scale=2)
        assert t.precision == 10
        assert t.scale == 2
        assert str(t) == "decimal(precision=10, scale=2)"

    def test_decimal_with_only_precision_raises_error(self):
        with pytest.raises(
            TypeDefinitionError,
            match="Decimal type requires both 'precision' and 'scale', or neither.",
        ):
            Decimal(precision=10)

    def test_decimal_with_only_scale_raises_error(self):
        with pytest.raises(
            TypeDefinitionError,
            match="Decimal type requires both 'precision' and 'scale', or neither.",
        ):
            Decimal(scale=2)

    def test_decimal_with_negative_scale(self):
        t = Decimal(precision=10, scale=-2)
        assert t.precision == 10
        assert t.scale == -2
        assert str(t) == "decimal(precision=10, scale=-2)"

    def test_decimal_bits_parameter(self):
        # Default bits omitted in str
        assert str(Decimal()) == "decimal"
        # Explicit bits in parameter-only form
        assert str(Decimal(bits=256)) == "decimal(bits=256)"
        # With precision/scale include bits when non-default
        assert (
            str(Decimal(precision=10, scale=2, bits=256))
            == "decimal(precision=10, scale=2, bits=256)"
        )

    @pytest.mark.parametrize("invalid_bits", [8, 16, 32, 64, 0, -1, 129])
    def test_decimal_invalid_bits_raises_error(self, invalid_bits):
        with pytest.raises(
            TypeDefinitionError,
            match=f"Decimal 'bits' must be one of 128 or 256, not {invalid_bits}.",
        ):
            Decimal(bits=invalid_bits)


class TestIntervalType:
    # Test valid Year-Month intervals
    @pytest.mark.parametrize(
        "start, end, expected_str",
        [
            (IntervalTimeUnit.YEAR, None, "interval(interval_start=YEAR)"),
            (IntervalTimeUnit.MONTH, None, "interval(interval_start=MONTH)"),
            (
                IntervalTimeUnit.YEAR,
                IntervalTimeUnit.MONTH,
                "interval(interval_start=YEAR, interval_end=MONTH)",
            ),
            (
                IntervalTimeUnit.YEAR,
                IntervalTimeUnit.YEAR,
                "interval(interval_start=YEAR)",
            ),
        ],
    )
    def test_valid_year_month_intervals(self, start, end, expected_str):
        t = Interval(interval_start=start, interval_end=end)
        assert str(t) == expected_str

    # Test valid Day-Time intervals
    @pytest.mark.parametrize(
        "start, end, expected_str",
        [
            (IntervalTimeUnit.DAY, None, "interval(interval_start=DAY)"),
            (IntervalTimeUnit.HOUR, None, "interval(interval_start=HOUR)"),
            (IntervalTimeUnit.MINUTE, None, "interval(interval_start=MINUTE)"),
            (IntervalTimeUnit.SECOND, None, "interval(interval_start=SECOND)"),
            (
                IntervalTimeUnit.DAY,
                IntervalTimeUnit.HOUR,
                "interval(interval_start=DAY, interval_end=HOUR)",
            ),
            (
                IntervalTimeUnit.DAY,
                IntervalTimeUnit.SECOND,
                "interval(interval_start=DAY, interval_end=SECOND)",
            ),
            (
                IntervalTimeUnit.MINUTE,
                IntervalTimeUnit.SECOND,
                "interval(interval_start=MINUTE, interval_end=SECOND)",
            ),
            (
                IntervalTimeUnit.SECOND,
                IntervalTimeUnit.SECOND,
                "interval(interval_start=SECOND)",
            ),
        ],
    )
    def test_valid_day_time_intervals(self, start, end, expected_str):
        t = Interval(interval_start=start, interval_end=end)
        assert str(t) == expected_str

    def test_invalid_mixed_category_interval_raises_error(self):
        with pytest.raises(TypeDefinitionError, match="must belong to the same category"):
            Interval(
                interval_start=IntervalTimeUnit.YEAR, interval_end=IntervalTimeUnit.DAY
            )

    def test_invalid_order_interval_raises_error(self):
        with pytest.raises(TypeDefinitionError, match="cannot be less significant than"):
            Interval(
                interval_start=IntervalTimeUnit.MONTH,
                interval_end=IntervalTimeUnit.YEAR,
            )

        with pytest.raises(TypeDefinitionError, match="cannot be less significant than"):
            Interval(
                interval_start=IntervalTimeUnit.SECOND,
                interval_end=IntervalTimeUnit.HOUR,
            )


class TestComplexTypes:
    def test_array_type(self):
        t = Array(element=String(length=50))
        assert isinstance(t.element, String)
        assert t.element.length == 50
        assert str(t) == "array<string(length=50)>"

    def test_nested_array_type(self):
        t = Array(element=Array(element=Integer(bits=32)))
        assert isinstance(t.element, Array)
        assert isinstance(t.element.element, Integer)
        assert t.element.element.bits == 32
        assert str(t) == "array<array<integer(bits=32)>>"

    def test_array_with_size(self):
        t = Array(element=String(), size=3)
        assert t.size == 3
        assert str(t) == "array<string, size=3>"

    def test_map_type(self):
        t = Map(key=String(), value=Integer())
        assert isinstance(t.key, String)
        assert isinstance(t.value, Integer)
        assert str(t) == "map<string, integer>"

    def test_map_keys_sorted_true(self):
        t = Map(key=String(), value=Integer(), keys_sorted=True)
        assert t.keys_sorted is True
        assert str(t) == "map<string, integer, keys_sorted=True>"

    def test_struct_type_is_not_tested_here(self):
        """
        Tests for Struct type are deferred to an integration test with the loader,
        as its 'fields' attribute requires a 'Field' object, which creates a circular
        dependency between `types.py` and `spec.py` at the unit test level.
        """
        pass


class TestSimpleTypes:
    @pytest.mark.parametrize(
        "type_class, expected_str",
        [
            (Boolean, "boolean"),
            (JSON, "json"),
            (UUID, "uuid"),
            (Void, "void"),
            (Variant, "variant"),
        ],
    )
    def test_simple_type_creation_and_str(self, type_class, expected_str):
        t = type_class()
        assert isinstance(t, YadsType)
        assert str(t) == expected_str


class TestBinaryType:
    def test_binary_default(self):
        t = Binary()
        assert t.length is None
        assert str(t) == "binary"

    def test_binary_with_length(self):
        t = Binary(length=16)
        assert t.length == 16
        assert str(t) == "binary(length=16)"

    @pytest.mark.parametrize("invalid_length", [0, -1, -100])
    def test_binary_invalid_length_raises_error(self, invalid_length):
        with pytest.raises(
            TypeDefinitionError, match="Binary 'length' must be a positive integer."
        ):
            Binary(length=invalid_length)


class TestDateType:
    def test_date_default_and_str(self):
        d = Date()
        assert d.bits is None
        assert str(d) == "date"

    @pytest.mark.parametrize("bits", [32, 64])
    def test_date_bits_validation(self, bits):
        d = Date(bits=bits)
        assert d.bits == bits
        assert str(d) == f"date(bits={bits})"

    @pytest.mark.parametrize("bits", [8, 16, 24, 128, 256])
    def test_date_invalid_bits_raise(self, bits):
        with pytest.raises(TypeDefinitionError, match="Date 'bits' must be one of"):
            Date(bits=bits)


class TestTimeType:
    def test_time_default_and_str(self):
        t = Time()
        assert t.unit == TimeUnit.MS
        assert t.bits is None
        assert str(t) == "time(unit=ms)"

    @pytest.mark.parametrize("unit", [TimeUnit.S, TimeUnit.MS, TimeUnit.US, TimeUnit.NS])
    def test_time_valid_units(self, unit):
        t = Time(unit=unit)
        assert t.unit == unit
        assert str(t) == f"time(unit={unit.value})"

    def test_time_bits_validation(self):
        assert Time(bits=32).bits == 32
        assert Time(bits=64, unit=TimeUnit.US).bits == 64
        with pytest.raises(TypeDefinitionError, match="Time 'bits' must be one of"):
            Time(bits=16)

    @pytest.mark.parametrize("unit", ["m", "seconds", "NS", 1, None])
    def test_time_invalid_units_raise(self, unit):
        with pytest.raises(TypeDefinitionError, match="Time 'unit' must be one of"):
            Time(unit=unit)


class TestTimestampTypes:
    def test_timestamp_default_and_str(self):
        t = Timestamp()
        assert t.unit == "ns"
        assert str(t) == "timestamp(unit=ns)"

    def test_timestamptz_default_and_str(self):
        t = TimestampTZ()
        assert t.unit == "ns"
        assert t.tz == "UTC"
        assert str(t) == "timestamptz(unit=ns, tz=UTC)"

    def test_timestampltz_default_and_str(self):
        t = TimestampLTZ()
        assert t.unit == "ns"
        assert str(t) == "timestampltz(unit=ns)"

    def test_timestampntz_default_and_str(self):
        t = TimestampNTZ()
        assert t.unit == "ns"
        assert str(t) == "timestampntz(unit=ns)"

    @pytest.mark.parametrize("unit", [TimeUnit.S, TimeUnit.MS, TimeUnit.US, TimeUnit.NS])
    def test_timestamp_valid_units(self, unit):
        t = Timestamp(unit=unit)
        assert t.unit == unit
        assert str(t) == f"timestamp(unit={unit.value})"

    @pytest.mark.parametrize("unit", [TimeUnit.S, TimeUnit.MS, TimeUnit.US, TimeUnit.NS])
    def test_timestamptz_valid_units(self, unit):
        t = TimestampTZ(unit=unit)
        assert t.unit == unit
        assert str(t) == f"timestamptz(unit={unit.value}, tz=UTC)"

    @pytest.mark.parametrize("unit", [TimeUnit.S, TimeUnit.MS, TimeUnit.US, TimeUnit.NS])
    def test_timestampltz_valid_units(self, unit):
        t = TimestampLTZ(unit=unit)
        assert t.unit == unit
        assert str(t) == f"timestampltz(unit={unit.value})"

    @pytest.mark.parametrize("unit", [TimeUnit.S, TimeUnit.MS, TimeUnit.US, TimeUnit.NS])
    def test_timestampntz_valid_units(self, unit):
        t = TimestampNTZ(unit=unit)
        assert t.unit == unit
        assert str(t) == f"timestampntz(unit={unit.value})"

    @pytest.mark.parametrize("unit", ["m", "seconds", "NS", 1, None])
    def test_timestamp_invalid_units_raise(self, unit):
        with pytest.raises(TypeDefinitionError, match="Timestamp 'unit' must be one of"):
            Timestamp(unit=unit)

    @pytest.mark.parametrize("unit", ["m", "seconds", "NS", 1, None])
    def test_timestamptz_invalid_units_raise(self, unit):
        with pytest.raises(
            TypeDefinitionError, match="TimestampTZ 'unit' must be one of"
        ):
            TimestampTZ(unit=unit)

    @pytest.mark.parametrize("unit", ["m", "seconds", "NS", 1, None])
    def test_timestampltz_invalid_units_raise(self, unit):
        with pytest.raises(
            TypeDefinitionError, match="TimestampLTZ 'unit' must be one of"
        ):
            TimestampLTZ(unit=unit)

    def test_timestamptz_tz_validation(self):
        with pytest.raises(TypeDefinitionError, match="non-empty string"):
            TimestampTZ(tz="")
        with pytest.raises(TypeDefinitionError, match="must not be None"):
            TimestampTZ(tz=None)

    def test_timestamptz_custom_tz_and_str(self):
        t = TimestampTZ(unit=TimeUnit.US, tz="America/New_York")
        assert str(t) == "timestamptz(unit=us, tz=America/New_York)"

    @pytest.mark.parametrize("unit", ["m", "seconds", "NS", 1, None])
    def test_timestampntz_invalid_units_raise(self, unit):
        with pytest.raises(
            TypeDefinitionError, match="TimestampNTZ 'unit' must be one of"
        ):
            TimestampNTZ(unit=unit)


class TestDurationType:
    def test_duration_default_and_str(self):
        d = Duration()
        assert d.unit == TimeUnit.NS
        assert str(d) == "duration(unit=ns)"

    @pytest.mark.parametrize("unit", [TimeUnit.S, TimeUnit.MS, TimeUnit.US, TimeUnit.NS])
    def test_duration_valid_units(self, unit):
        d = Duration(unit=unit)
        assert d.unit == unit
        assert str(d) == f"duration(unit={unit.value})"

    @pytest.mark.parametrize("unit", ["m", "seconds", "NS", 1, None])
    def test_duration_invalid_units_raise(self, unit):
        with pytest.raises(TypeDefinitionError, match="Duration 'unit' must be one of"):
            Duration(unit=unit)


class TestTensorType:
    def test_tensor_basic(self):
        """Test basic tensor creation with integer elements."""
        t = Tensor(element=Integer(bits=32), shape=[10, 20])
        assert t.element == Integer(bits=32)
        assert t.shape == [10, 20]
        assert str(t) == "tensor<integer(bits=32), shape=[10, 20]>"

    def test_tensor_float_elements(self):
        """Test tensor with float elements."""
        t = Tensor(element=Float(bits=64), shape=[5, 10, 15])
        assert t.element == Float(bits=64)
        assert t.shape == [5, 10, 15]
        assert str(t) == "tensor<float(bits=64), shape=[5, 10, 15]>"

    def test_tensor_single_dimension(self):
        """Test tensor with single dimension."""
        t = Tensor(element=String(), shape=[100])
        assert t.element == String()
        assert t.shape == [100]
        assert str(t) == "tensor<string, shape=[100]>"

    def test_tensor_complex_element(self):
        """Test tensor with complex element type."""
        struct_type = Struct(fields=[])
        t = Tensor(element=struct_type, shape=[2, 3])
        assert t.element == struct_type
        assert t.shape == [2, 3]
        assert str(t) == "tensor<struct<\n\n>, shape=[2, 3]>"

    @pytest.mark.parametrize(
        "invalid_shape, expected_error",
        [
            ([], "Tensor 'shape' cannot be empty"),
            ([0], "Tensor 'shape' must contain only positive integers"),
            ([-1], "Tensor 'shape' must contain only positive integers"),
            ([1, 0, 2], "Tensor 'shape' must contain only positive integers"),
            ([1, -5], "Tensor 'shape' must contain only positive integers"),
        ],
    )
    def test_tensor_invalid_shape_raises_error(self, invalid_shape, expected_error):
        """Test that invalid shapes raise TypeDefinitionError."""
        with pytest.raises(TypeDefinitionError, match=expected_error):
            Tensor(element=Integer(), shape=invalid_shape)

    def test_tensor_empty_shape_raises_error(self):
        """Test that empty shape raises TypeDefinitionError."""
        with pytest.raises(TypeDefinitionError, match="Tensor 'shape' cannot be empty"):
            Tensor(element=Integer(), shape=[])

    def test_tensor_non_integer_shape_raises_error(self):
        """Test that non-integer shape elements raise TypeDefinitionError."""
        with pytest.raises(
            TypeDefinitionError,
            match="Tensor 'shape' must contain only positive integers",
        ):
            Tensor(element=Integer(), shape=[1, "2", 3])  # type: ignore[arg-type]


class TestTypeAliases:
    @pytest.mark.parametrize(
        "alias, expected_type, expected_params",
        [
            # String Types
            ("str", String, {}),
            ("string", String, {}),
            ("text", String, {}),
            ("varchar", String, {}),
            ("char", String, {}),
            # Numeric Types
            ("int8", Integer, {"bits": 8}),
            ("uint8", Integer, {"bits": 8, "signed": False}),
            ("tinyint", Integer, {"bits": 8}),
            ("byte", Integer, {"bits": 8}),
            ("int16", Integer, {"bits": 16}),
            ("uint16", Integer, {"bits": 16, "signed": False}),
            ("smallint", Integer, {"bits": 16}),
            ("short", Integer, {"bits": 16}),
            ("int32", Integer, {"bits": 32}),
            ("uint32", Integer, {"bits": 32, "signed": False}),
            ("int", Integer, {"bits": 32}),
            ("integer", Integer, {"bits": 32}),
            ("int64", Integer, {"bits": 64}),
            ("uint64", Integer, {"bits": 64, "signed": False}),
            ("bigint", Integer, {"bits": 64}),
            ("long", Integer, {"bits": 64}),
            ("float16", Float, {"bits": 16}),
            ("float", Float, {"bits": 32}),
            ("float32", Float, {"bits": 32}),
            ("float64", Float, {"bits": 64}),
            ("double", Float, {"bits": 64}),
            ("decimal", Decimal, {}),
            ("numeric", Decimal, {}),
            # Boolean Types
            ("bool", Boolean, {}),
            ("boolean", Boolean, {}),
            # Binary Types
            ("blob", Binary, {}),
            ("binary", Binary, {}),
            ("bytes", Binary, {}),
            # Temporal Types
            ("date", Date, {}),
            ("date32", Date, {"bits": 32}),
            ("date64", Date, {"bits": 64}),
            ("time", Time, {}),
            ("time32", Time, {"bits": 32, "unit": "ms"}),
            ("time64", Time, {"bits": 64, "unit": "ns"}),
            ("datetime", Timestamp, {}),
            ("timestamp", Timestamp, {}),
            ("timestamptz", TimestampTZ, {}),
            ("timestamp_tz", TimestampTZ, {}),
            ("timestampltz", TimestampLTZ, {}),
            ("timestamp_ltz", TimestampLTZ, {}),
            ("timestampntz", TimestampNTZ, {}),
            ("timestamp_ntz", TimestampNTZ, {}),
            ("duration", Duration, {}),
            ("interval", Interval, {}),
            # Complex Types
            ("array", Array, {}),
            ("list", Array, {}),
            ("struct", Struct, {}),
            ("record", Struct, {}),
            ("map", Map, {}),
            ("dictionary", Map, {}),
            ("json", JSON, {}),
            # Spatial Types
            ("geometry", Geometry, {}),
            ("geography", Geography, {}),
            # Other Types
            ("uuid", UUID, {}),
            ("void", Void, {}),
            ("null", Void, {}),
            ("variant", Variant, {}),
            ("tensor", Tensor, {}),
        ],
    )
    def test_type_aliases(self, alias, expected_type, expected_params):
        """
        Tests that each alias maps to the correct base type and default parameters.
        This test does not instantiate the types, only checks the mapping.
        """
        base_type_class, default_params = TYPE_ALIASES[alias]
        assert base_type_class == expected_type
        assert default_params == expected_params

    def test_all_aliases_are_covered(self):
        """Ensures that every alias in TYPE_ALIASES is included in the test."""
        # The parametrize decorator stores the parameter sets in `args[1]`
        parameter_list = self.test_type_aliases.pytestmark[0].args[1]
        tested_aliases = {params[0] for params in parameter_list}
        defined_aliases = set(TYPE_ALIASES.keys())

        assert tested_aliases == defined_aliases, (
            f"Mismatch between tested aliases and defined aliases.\\n"
            f"Missing from tests: {sorted(list(defined_aliases - tested_aliases))}\\n"
            f"Unexpected in tests: {sorted(list(tested_aliases - defined_aliases))}"
        )
