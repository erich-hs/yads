import pytest
import warnings
from sqlglot import exp

from yads.spec import YadsSpec, Column, Field
from yads.converters.sql import SparkSQLConverter, SQLGlotConverterConfig
from yads.exceptions import AstValidationError
from yads.loaders import from_yaml_string, from_yaml_path
from yads.exceptions import ValidationWarning
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


# %% Types
class TestSparkSQLConverterTypes:
    @pytest.mark.parametrize(
        "yads_type, expected_sql, expected_warning",
        [
            (String(), "STRING", None),
            (String(length=255), "VARCHAR(255)", None),
            (Integer(bits=8), "TINYINT", None),
            (Integer(bits=16), "SMALLINT", None),
            (Integer(bits=32), "INT", None),
            (Integer(bits=64), "BIGINT", None),
            (
                Integer(bits=8, signed=False),
                "TINYINT",
                "Data type 'UTINYINT' is not supported",
            ),
            (
                Integer(bits=16, signed=False),
                "SMALLINT",
                "Data type 'USMALLINT' is not supported",
            ),
            (Integer(bits=32, signed=False), "INT", "Data type 'UINT' is not supported"),
            (
                Integer(bits=64, signed=False),
                "BIGINT",
                "Data type 'UBIGINT' is not supported",
            ),
            (
                Float(bits=16),
                "FLOAT",
                "SQLGlotConverter does not support half-precision Float (bits=16).",
            ),
            (Float(bits=32), "FLOAT", None),
            (Float(bits=64), "DOUBLE", None),
            (Decimal(), "DECIMAL", None),
            (Decimal(precision=10, scale=2), "DECIMAL(10, 2)", None),
            (
                Decimal(precision=10, scale=-2),
                "DECIMAL(12)",
                "The precision will be increased by the absolute value of the negative scale, and the scale will be set to 0.",
            ),
            (Decimal(precision=10, scale=2, bits=128), "DECIMAL(10, 2)", None),
            (Boolean(), "BOOLEAN", None),
            (Binary(), "BINARY", None),
            (Binary(length=8), "BINARY", "The length parameter will be removed."),
            (Date(), "DATE", None),
            (Date(bits=32), "DATE", None),
            (Date(bits=64), "DATE", None),
            (
                Time(),
                "TIMESTAMP",
                None,
            ),  # sqlglot coerces TIME to TIMESTAMP without warning
            (Time(unit=TimeUnit.S), "TIMESTAMP", None),
            (Time(unit=TimeUnit.MS), "TIMESTAMP", None),
            (Time(unit=TimeUnit.US), "TIMESTAMP", None),
            (Time(unit=TimeUnit.NS), "TIMESTAMP", None),
            (Time(bits=32), "TIMESTAMP", None),
            (Time(bits=64), "TIMESTAMP", None),
            (Timestamp(), "TIMESTAMP", None),
            (Timestamp(unit=TimeUnit.S), "TIMESTAMP", None),
            (Timestamp(unit=TimeUnit.MS), "TIMESTAMP", None),
            (Timestamp(unit=TimeUnit.US), "TIMESTAMP", None),
            (Timestamp(unit=TimeUnit.NS), "TIMESTAMP", None),
            (TimestampTZ(), "TIMESTAMP", None),  # TimestampTZ is Spark's default
            (TimestampTZ(unit=TimeUnit.S), "TIMESTAMP", None),
            (TimestampTZ(unit=TimeUnit.MS), "TIMESTAMP", None),
            (TimestampTZ(unit=TimeUnit.US), "TIMESTAMP", None),
            (TimestampTZ(unit=TimeUnit.NS), "TIMESTAMP", None),
            (TimestampTZ(tz="UTC"), "TIMESTAMP", None),
            (TimestampLTZ(), "TIMESTAMP_LTZ", None),
            (TimestampLTZ(unit=TimeUnit.S), "TIMESTAMP_LTZ", None),
            (TimestampLTZ(unit=TimeUnit.MS), "TIMESTAMP_LTZ", None),
            (TimestampLTZ(unit=TimeUnit.US), "TIMESTAMP_LTZ", None),
            (TimestampLTZ(unit=TimeUnit.NS), "TIMESTAMP_LTZ", None),
            (TimestampNTZ(), "TIMESTAMP_NTZ", None),
            (TimestampNTZ(unit=TimeUnit.S), "TIMESTAMP_NTZ", None),
            (TimestampNTZ(unit=TimeUnit.MS), "TIMESTAMP_NTZ", None),
            (TimestampNTZ(unit=TimeUnit.US), "TIMESTAMP_NTZ", None),
            (TimestampNTZ(unit=TimeUnit.NS), "TIMESTAMP_NTZ", None),
            (
                Duration(),
                "STRING",
                "SQLGlotConverter does not support type: duration(unit=ns)",
            ),
            (Interval(interval_start=IntervalTimeUnit.DAY), "INTERVAL DAY", None),
            (
                Interval(
                    interval_start=IntervalTimeUnit.DAY, interval_end=IntervalTimeUnit.DAY
                ),
                "INTERVAL DAY",
                None,
            ),
            (
                Interval(
                    interval_start=IntervalTimeUnit.YEAR,
                    interval_end=IntervalTimeUnit.MONTH,
                ),
                "INTERVAL YEAR TO MONTH",
                None,
            ),
            (Array(element=Integer()), "ARRAY<INT>", None),
            (Array(element=String(), size=2), "ARRAY<STRING>", None),
            (
                Struct(
                    fields=[
                        Field(name="nested_int", type=Integer()),
                        Field(name="nested_string", type=String()),
                    ]
                ),
                "STRUCT<nested_int: INT, nested_string: STRING>",
                None,
            ),
            (
                Struct(
                    fields=[
                        Field(name="nested_int", type=Integer()),
                        Field(
                            name="nested_struct",
                            type=Struct(
                                fields=[Field(name="nested_string", type=String())]
                            ),
                        ),
                    ]
                ),
                "STRUCT<nested_int: INT, nested_struct: STRUCT<nested_string: STRING>>",
                None,
            ),
            (Map(key=String(), value=Integer()), "MAP<STRING, INT>", None),
            (
                Map(key=String(), value=Array(element=String())),
                "MAP<STRING, ARRAY<STRING>>",
                None,
            ),
            (
                Map(key=String(), value=Integer(), keys_sorted=True),
                "MAP<STRING, INT>",
                None,
            ),
            (JSON(), "STRING", "Data type 'JSON' is not supported for column 'col1'."),
            (
                Geometry(),
                "STRING",
                "Data type 'GEOMETRY' is not supported for column 'col1'.",
            ),
            (
                Geometry(srid=4326),
                "STRING",
                "Data type 'GEOMETRY' is not supported for column 'col1'.",
            ),
            (
                Geography(),
                "STRING",
                "Data type 'GEOGRAPHY' is not supported for column 'col1'.",
            ),
            (
                Geography(srid=4326),
                "STRING",
                "Data type 'GEOGRAPHY' is not supported for column 'col1'.",
            ),
            (UUID(), "STRING", "Data type 'UUID' is not supported for column 'col1'."),
            (Void(), "VOID", None),
            (Variant(), "VARIANT", None),
        ],
    )
    def test_convert_type(
        self, yads_type: YadsType, expected_sql: str, expected_warning: str | None
    ):
        spec = YadsSpec(
            name="test_spec",
            version="1.0.0",
            columns=[Column(name="col1", type=yads_type)],
        )
        converter = SparkSQLConverter(
            ast_config=SQLGlotConverterConfig(fallback_type=exp.DataType.Type.TEXT)
        )

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ddl = converter.convert(spec, mode="coerce")

        # Assert warnings for unsupported types
        if expected_warning is not None:
            assert len(w) == 1
            assert issubclass(w[0].category, ValidationWarning)
            assert expected_warning in str(w[0].message)
        else:
            assert len(w) == 0

        # Assert converted SQL
        assert ddl.strip() == f"CREATE TABLE test_spec (col1 {expected_sql})"


# %% Dialect behavior
class TestSparkSQLConverterDialect:
    def test_convert_full_spec_matches_spark_fixture(self):
        spec = from_yaml_path("tests/fixtures/spec/valid/full_spec.yaml")
        converter = SparkSQLConverter()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ddl = converter.convert(spec, pretty=True)

        assert len(w) == 5
        assert all(issubclass(wi.category, ValidationWarning) for wi in w)
        messages = "\n".join(str(wi.message) for wi in w)
        assert "Data type 'JSON' is not supported for column 'c_json'." in messages
        assert (
            "Data type 'GEOMETRY' is not supported for column 'c_geometry'." in messages
        )
        assert (
            "Data type 'GEOGRAPHY' is not supported for column 'c_geography'." in messages
        )
        assert "The data type will be replaced with 'TEXT'." in messages

        with open("tests/fixtures/sql/spark/full_spec.sql", "r") as f:
            expected_sql = f.read().strip()

        assert ddl.strip() == expected_sql


# %% Validation rules wiring
class TestSparkSQLConverterValidation:
    @pytest.mark.parametrize(
        "yads_type, original_type_sql",
        [
            ("json", "JSON"),
            ("geometry", "GEOMETRY"),
            ("geography", "GEOGRAPHY"),
            ("uuid", "UUID"),
        ],
    )
    def test_coerce_mode_replaces_to_string_and_warns(
        self, yads_type: str, original_type_sql: str
    ):
        yaml_string = f"""
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: {yads_type}
        """
        spec = from_yaml_string(yaml_string)

        converter = SparkSQLConverter(
            ast_config=SQLGlotConverterConfig(fallback_type=exp.DataType.Type.TEXT)
        )
        with pytest.warns(
            UserWarning,
            match=f"Data type '{original_type_sql}' is not supported for column 'col1'.",
        ):
            ddl = converter.convert(spec, mode="coerce", pretty=True)

        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 STRING
)"""
        assert ddl.strip() == expected_ddl

    @pytest.mark.parametrize(
        "yads_type, original_type_sql",
        [
            ("json", "JSON"),
            ("geometry", "GEOMETRY"),
            ("geography", "GEOGRAPHY"),
            ("uuid", "UUID"),
        ],
    )
    def test_raise_mode_raises_ast_validation_error(
        self, yads_type: str, original_type_sql: str
    ):
        yaml_string = f"""
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: {yads_type}
        """
        spec = from_yaml_string(yaml_string)

        converter = SparkSQLConverter()
        with pytest.raises(
            AstValidationError,
            match=f"Data type '{original_type_sql}' is not supported for column 'col1'.",
        ):
            converter.convert(spec, mode="raise")
