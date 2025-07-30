import pytest
from yads.types import (
    TYPE_ALIASES,
    Array,
    Binary,
    Boolean,
    Date,
    Decimal,
    Float,
    Integer,
    Interval,
    IntervalTimeUnit,
    JSON,
    Map,
    String,
    Struct,
    Timestamp,
    TimestampTZ,
    Type,
    UUID,
)


class TestStringType:
    def test_string_default(self):
        t = String()
        assert t.length is None
        assert str(t) == "string"

    def test_string_with_length(self):
        t = String(length=255)
        assert t.length == 255
        assert str(t) == "string(255)"

    @pytest.mark.parametrize("invalid_length", [0, -1, -100])
    def test_string_invalid_length_raises_error(self, invalid_length):
        with pytest.raises(
            ValueError, match="String 'length' must be a positive integer."
        ):
            String(length=invalid_length)


class TestIntegerType:
    def test_integer_default(self):
        t = Integer()
        assert t.bits is None
        assert str(t) == "integer"

    @pytest.mark.parametrize("bits", [8, 16, 32, 64])
    def test_integer_with_valid_bits(self, bits):
        t = Integer(bits=bits)
        assert t.bits == bits
        assert str(t) == f"integer(bits={bits})"

    @pytest.mark.parametrize("invalid_bits", [-1, 0, 1, 12, 24, 128])
    def test_integer_with_invalid_bits_raises_error(self, invalid_bits):
        with pytest.raises(
            ValueError,
            match=f"Integer 'bits' must be one of 8, 16, 32, 64, not {invalid_bits}.",
        ):
            Integer(bits=invalid_bits)


class TestFloatType:
    def test_float_default(self):
        t = Float()
        assert t.bits is None
        assert str(t) == "float"

    @pytest.mark.parametrize("bits", [32, 64])
    def test_float_with_valid_bits(self, bits):
        t = Float(bits=bits)
        assert t.bits == bits
        assert str(t) == f"float(bits={bits})"

    @pytest.mark.parametrize("invalid_bits", [-1, 0, 1, 8, 16, 128])
    def test_float_with_invalid_bits_raises_error(self, invalid_bits):
        with pytest.raises(
            ValueError,
            match=f"Float 'bits' must be one of 32 or 64, not {invalid_bits}.",
        ):
            Float(bits=invalid_bits)


class TestDecimalType:
    def test_decimal_default(self):
        t = Decimal()
        assert t.precision is None
        assert t.scale is None
        assert str(t) == "decimal"

    def test_decimal_with_precision_and_scale(self):
        t = Decimal(precision=10, scale=2)
        assert t.precision == 10
        assert t.scale == 2
        assert str(t) == "decimal(10, 2)"

    def test_decimal_with_only_precision_raises_error(self):
        with pytest.raises(
            ValueError,
            match="Decimal type requires both 'precision' and 'scale', or neither.",
        ):
            Decimal(precision=10)

    def test_decimal_with_only_scale_raises_error(self):
        with pytest.raises(
            ValueError,
            match="Decimal type requires both 'precision' and 'scale', or neither.",
        ):
            Decimal(scale=2)


class TestIntervalType:
    # Test valid Year-Month intervals
    @pytest.mark.parametrize(
        "start, end, expected_str",
        [
            (IntervalTimeUnit.YEAR, None, "interval(YEAR)"),
            (IntervalTimeUnit.MONTH, None, "interval(MONTH)"),
            (IntervalTimeUnit.YEAR, IntervalTimeUnit.MONTH, "interval(YEAR to MONTH)"),
            (IntervalTimeUnit.YEAR, IntervalTimeUnit.YEAR, "interval(YEAR)"),
        ],
    )
    def test_valid_year_month_intervals(self, start, end, expected_str):
        t = Interval(interval_start=start, interval_end=end)
        assert str(t) == expected_str

    # Test valid Day-Time intervals
    @pytest.mark.parametrize(
        "start, end, expected_str",
        [
            (IntervalTimeUnit.DAY, None, "interval(DAY)"),
            (IntervalTimeUnit.HOUR, None, "interval(HOUR)"),
            (IntervalTimeUnit.MINUTE, None, "interval(MINUTE)"),
            (IntervalTimeUnit.SECOND, None, "interval(SECOND)"),
            (IntervalTimeUnit.DAY, IntervalTimeUnit.HOUR, "interval(DAY to HOUR)"),
            (IntervalTimeUnit.DAY, IntervalTimeUnit.SECOND, "interval(DAY to SECOND)"),
            (
                IntervalTimeUnit.MINUTE,
                IntervalTimeUnit.SECOND,
                "interval(MINUTE to SECOND)",
            ),
            (IntervalTimeUnit.SECOND, IntervalTimeUnit.SECOND, "interval(SECOND)"),
        ],
    )
    def test_valid_day_time_intervals(self, start, end, expected_str):
        t = Interval(interval_start=start, interval_end=end)
        assert str(t) == expected_str

    def test_invalid_mixed_category_interval_raises_error(self):
        with pytest.raises(ValueError, match="must belong to the same category"):
            Interval(
                interval_start=IntervalTimeUnit.YEAR, interval_end=IntervalTimeUnit.DAY
            )

    def test_invalid_order_interval_raises_error(self):
        with pytest.raises(ValueError, match="cannot be less significant than"):
            Interval(
                interval_start=IntervalTimeUnit.MONTH,
                interval_end=IntervalTimeUnit.YEAR,
            )

        with pytest.raises(ValueError, match="cannot be less significant than"):
            Interval(
                interval_start=IntervalTimeUnit.SECOND,
                interval_end=IntervalTimeUnit.HOUR,
            )


class TestComplexTypes:
    def test_array_type(self):
        t = Array(element=String(length=50))
        assert isinstance(t.element, String)
        assert t.element.length == 50
        assert str(t) == "array<string(50)>"

    def test_nested_array_type(self):
        t = Array(element=Array(element=Integer(bits=32)))
        assert isinstance(t.element, Array)
        assert isinstance(t.element.element, Integer)
        assert t.element.element.bits == 32
        assert str(t) == "array<array<integer(bits=32)>>"

    def test_map_type(self):
        t = Map(key=String(), value=Integer())
        assert isinstance(t.key, String)
        assert isinstance(t.value, Integer)
        assert str(t) == "map<string, integer>"

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
            (Date, "date"),
            (Timestamp, "timestamp"),
            (TimestampTZ, "timestamptz"),
            (Binary, "binary"),
            (JSON, "json"),
            (UUID, "uuid"),
        ],
    )
    def test_simple_type_creation_and_str(self, type_class, expected_str):
        t = type_class()
        assert isinstance(t, Type)
        assert str(t) == expected_str


class TestTypeAliases:
    @pytest.mark.parametrize(
        "alias, expected_type, expected_params",
        [
            # Numeric Types
            ("int8", Integer, {"bits": 8}),
            ("tinyint", Integer, {"bits": 8}),
            ("byte", Integer, {"bits": 8}),
            ("int16", Integer, {"bits": 16}),
            ("smallint", Integer, {"bits": 16}),
            ("short", Integer, {"bits": 16}),
            ("int32", Integer, {"bits": 32}),
            ("int", Integer, {"bits": 32}),
            ("integer", Integer, {"bits": 32}),
            ("int64", Integer, {"bits": 64}),
            ("bigint", Integer, {"bits": 64}),
            ("long", Integer, {"bits": 64}),
            ("float", Float, {"bits": 32}),
            ("float32", Float, {"bits": 32}),
            ("float64", Float, {"bits": 64}),
            ("double", Float, {"bits": 64}),
            ("decimal", Decimal, {}),
            ("numeric", Decimal, {}),
            # String Types
            ("string", String, {}),
            ("text", String, {}),
            ("varchar", String, {}),
            ("char", String, {}),
            # Binary Types
            ("blob", Binary, {}),
            ("binary", Binary, {}),
            ("bytes", Binary, {}),
            # Boolean Types
            ("bool", Boolean, {}),
            ("boolean", Boolean, {}),
            # Temporal Types
            ("date", Date, {}),
            ("datetime", Timestamp, {}),
            ("timestamp", Timestamp, {}),
            ("timestamp_tz", TimestampTZ, {}),
            ("interval", Interval, {}),
            # Complex Types
            ("array", Array, {}),
            ("list", Array, {}),
            ("struct", Struct, {}),
            ("record", Struct, {}),
            ("map", Map, {}),
            ("dictionary", Map, {}),
            ("json", JSON, {}),
            # Other Types
            ("uuid", UUID, {}),
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
