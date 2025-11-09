import pytest
import warnings
from sqlglot import exp

from yads.spec import YadsSpec, Column, Field
from yads.converters.sql import DuckdbSQLConverter, SQLGlotConverterConfig
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
class TestDuckdbSQLConverterTypes:
    @pytest.mark.parametrize(
        "yads_type, expected_sql, expected_warning",
        [
            (String(), "TEXT", None),
            (String(length=255), "TEXT(255)", None),
            (Integer(bits=8), "TINYINT", None),
            (Integer(bits=16), "SMALLINT", None),
            (Integer(bits=32), "INT", None),
            (Integer(bits=64), "BIGINT", None),
            (Integer(bits=8, signed=False), "UTINYINT", None),
            (
                Integer(bits=16, signed=False),
                "USMALLINT",
                None,
            ),
            (Integer(bits=32, signed=False), "UINTEGER", None),
            (
                Integer(bits=64, signed=False),
                "UBIGINT",
                None,
            ),
            (
                Float(bits=16),
                "REAL",
                "SQLGlotConverter does not support half-precision Float (bits=16).",
            ),
            (Float(bits=32), "REAL", None),
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
            (Binary(), "BLOB", None),
            (Binary(length=8), "BLOB", "The length parameter will be removed."),
            (Date(), "DATE", None),
            (Date(bits=32), "DATE", None),
            (Date(bits=64), "DATE", None),
            (Time(), "TIME", None),
            (Time(unit=TimeUnit.S), "TIME", None),
            (Time(unit=TimeUnit.MS), "TIME", None),
            (Time(unit=TimeUnit.US), "TIME", None),
            (Time(unit=TimeUnit.NS), "TIME", None),
            (Time(bits=32), "TIME", None),
            (Time(bits=64), "TIME", None),
            (Timestamp(), "TIMESTAMP", None),
            (Timestamp(unit=TimeUnit.S), "TIMESTAMP", None),
            (Timestamp(unit=TimeUnit.MS), "TIMESTAMP", None),
            (Timestamp(unit=TimeUnit.US), "TIMESTAMP", None),
            (Timestamp(unit=TimeUnit.NS), "TIMESTAMP", None),
            (TimestampTZ(), "TIMESTAMPTZ", None),
            (TimestampTZ(unit=TimeUnit.S), "TIMESTAMPTZ", None),
            (TimestampTZ(unit=TimeUnit.MS), "TIMESTAMPTZ", None),
            (TimestampTZ(unit=TimeUnit.US), "TIMESTAMPTZ", None),
            (TimestampTZ(unit=TimeUnit.NS), "TIMESTAMPTZ", None),
            (TimestampTZ(tz="UTC"), "TIMESTAMPTZ", None),
            (
                TimestampLTZ(),
                "TIMESTAMPTZ",
                "Data type 'TIMESTAMPLTZ' is not supported for column 'col1'.",
            ),
            (
                TimestampLTZ(unit=TimeUnit.S),
                "TIMESTAMPTZ",
                "Data type 'TIMESTAMPLTZ' is not supported for column 'col1'.",
            ),
            (
                TimestampLTZ(unit=TimeUnit.MS),
                "TIMESTAMPTZ",
                "Data type 'TIMESTAMPLTZ' is not supported for column 'col1'.",
            ),
            (
                TimestampLTZ(unit=TimeUnit.US),
                "TIMESTAMPTZ",
                "Data type 'TIMESTAMPLTZ' is not supported for column 'col1'.",
            ),
            (
                TimestampLTZ(unit=TimeUnit.NS),
                "TIMESTAMPTZ",
                "Data type 'TIMESTAMPLTZ' is not supported for column 'col1'.",
            ),
            (
                TimestampNTZ(),
                "TIMESTAMP",
                None,
            ),  # Default TIMESTAMP in Duckdb is timezone unaware
            (TimestampNTZ(unit=TimeUnit.S), "TIMESTAMP", None),
            (TimestampNTZ(unit=TimeUnit.MS), "TIMESTAMP", None),
            (TimestampNTZ(unit=TimeUnit.US), "TIMESTAMP", None),
            (TimestampNTZ(unit=TimeUnit.NS), "TIMESTAMP", None),
            (
                Duration(),
                "TEXT",
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
            (Array(element=Integer()), "INT[]", None),
            (Array(element=String(), size=2), "TEXT[]", None),
            (
                Struct(
                    fields=[
                        Field(name="nested_int", type=Integer()),
                        Field(name="nested_string", type=String()),
                    ]
                ),
                "STRUCT(nested_int INT, nested_string TEXT)",
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
                "STRUCT(nested_int INT, nested_struct STRUCT(nested_string TEXT))",
                None,
            ),
            (Map(key=String(), value=Integer()), "MAP(TEXT, INT)", None),
            (
                Map(key=String(), value=Array(element=String())),
                "MAP(TEXT, TEXT[])",
                None,
            ),
            (
                Map(key=String(), value=Integer(), keys_sorted=True),
                "MAP(TEXT, INT)",
                None,
            ),
            (JSON(), "JSON", None),
            (Geometry(), "GEOMETRY", None),
            (
                Geometry(srid=4326),
                "GEOMETRY",
                "Parameterized 'GEOMETRY' is not supported for column 'col1'.",
            ),
            (
                Geography(),
                "TEXT",
                "Data type 'GEOGRAPHY' is not supported for column 'col1'.",
            ),
            (
                Geography(srid=4326),
                "TEXT",
                "Data type 'GEOGRAPHY' is not supported for column 'col1'.",
            ),
            (UUID(), "UUID", None),
            (Void(), "TEXT", "Data type 'VOID' is not supported for column 'col1'."),
            (
                Variant(),
                "TEXT",
                "Data type 'VARIANT' is not supported for column 'col1'.",
            ),
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
        converter = DuckdbSQLConverter(
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
class TestDuckdbSQLConverterDialect:
    def test_convert_full_spec_matches_duckdb_fixture(self):
        spec = from_yaml_path("tests/fixtures/spec/valid/full_spec.yaml")
        converter = DuckdbSQLConverter()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ddl = converter.convert(spec, pretty=True)

        assert len(w) == 8
        assert all(issubclass(wi.category, ValidationWarning) for wi in w)
        messages = "\n".join(str(wi.message) for wi in w)
        assert (
            "Data type 'TIMESTAMPLTZ' is not supported for column 'c_timestamp_ltz'."
            in messages
        )
        assert "Data type 'VOID' is not supported for column 'c_void'." in messages
        assert (
            "Data type 'GEOGRAPHY' is not supported for column 'c_geography'." in messages
        )
        assert (
            "Parameterized 'GEOMETRY' is not supported for column 'c_geometry'."
            in messages
        )
        assert "Data type 'VARIANT' is not supported for column 'c_variant'." in messages
        assert (
            "GENERATED ALWAYS AS IDENTITY is not supported for column 'c_int32_identity'."
            in messages
        )
        assert "NULLS FIRST is not supported in PRIMARY KEY constraints." in messages
        assert "The data type will be replaced with 'TIMESTAMPTZ'." in messages
        assert "The data type will be replaced with 'TEXT'." in messages
        assert "The parameters will be removed." in messages
        assert "The NULLS FIRST attribute will be removed." in messages

        with open("tests/fixtures/sql/duckdb/full_spec.sql", "r") as f:
            expected_sql = f.read().strip()

        assert ddl.strip() == expected_sql


# %% Validation rules wiring
class TestDuckdbSQLConverterValidation:
    @pytest.mark.parametrize(
        "yads_type, original_type_sql, expected_sql",
        [
            ("timestampltz", "TIMESTAMPLTZ", "TIMESTAMPTZ"),
            ("void", "VOID", "TEXT"),
            ("geography", "GEOGRAPHY", "TEXT"),
            ("variant", "VARIANT", "TEXT"),
        ],
    )
    def test_coerce_mode_replaces_to_duckdb_supported_and_warns(
        self, yads_type: str, original_type_sql: str, expected_sql: str
    ):
        yaml_string = f"""
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: {yads_type}
        """
        spec = from_yaml_string(yaml_string)

        converter = DuckdbSQLConverter()
        with pytest.warns(
            UserWarning,
            match=f"Data type '{original_type_sql}' is not supported for column 'col1'.",
        ):
            ddl = converter.convert(spec, mode="coerce", pretty=True)

        expected_ddl = f"""CREATE TABLE my_db.my_table (
  col1 {expected_sql}
)"""
        assert ddl.strip() == expected_ddl

    def test_coerce_mode_removes_geometry_parameters_and_warns(self):
        yaml_string = """
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: geometry
            params:
              srid: 4326
        """
        spec = from_yaml_string(yaml_string)

        converter = DuckdbSQLConverter()
        with pytest.warns(
            UserWarning,
            match="Parameterized 'GEOMETRY' is not supported for column 'col1'.",
        ):
            ddl = converter.convert(spec, mode="coerce", pretty=True)

        expected_ddl = """CREATE TABLE my_db.my_table (
  col1 GEOMETRY
)"""
        assert ddl.strip() == expected_ddl

    @pytest.mark.parametrize(
        "yads_type, original_type_sql",
        [
            ("timestampltz", "TIMESTAMPLTZ"),
            ("void", "VOID"),
            ("geography", "GEOGRAPHY"),
            ("variant", "VARIANT"),
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

        converter = DuckdbSQLConverter()
        with pytest.raises(
            AstValidationError,
            match=f"Data type '{original_type_sql}' is not supported for column 'col1'.",
        ):
            converter.convert(spec, mode="raise")

    def test_raise_mode_raises_for_parameterized_geometry(self):
        yaml_string = """
        name: my_db.my_table
        version: 1
        columns:
          - name: col1
            type: geometry
            params:
              srid: 4326
        """
        spec = from_yaml_string(yaml_string)

        converter = DuckdbSQLConverter()
        with pytest.raises(
            AstValidationError,
            match="Parameterized 'GEOMETRY' is not supported for column 'col1'.",
        ):
            converter.convert(spec, mode="raise")
