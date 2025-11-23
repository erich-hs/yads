import pytest

from yads import spec
from yads.exceptions import SpecParsingError, SpecValidationError
from yads.serializers import SpecSerializer


def _build_complex_spec_dict() -> dict:
    return {
        "name": "catalog.db.users",
        "version": 2,
        "yads_spec_version": "0.0.2",
        "description": "Serialized spec",
        "metadata": {"team": "data-eng"},
        "storage": {
            "format": "delta",
            "location": "s3://bucket/users",
            "tbl_properties": {"delta.appendOnly": "true"},
        },
        "columns": [
            {
                "name": "id",
                "type": "integer",
                "constraints": {"not_null": True},
            },
            {
                "name": "profile",
                "type": "struct",
                "fields": [
                    {
                        "name": "username",
                        "type": "string",
                        "constraints": {"not_null": True},
                    },
                    {
                        "name": "attributes",
                        "type": "map",
                        "key": {"type": "string"},
                        "value": {"type": "string"},
                    },
                ],
            },
            {"name": "created_at", "type": "timestamp"},
            {
                "name": "created_date",
                "type": "date",
                "generated_as": {"column": "created_at", "transform": "date"},
            },
        ],
        "partitioned_by": [{"column": "created_date"}],
        "table_constraints": [
            {"type": "primary_key", "name": "pk_users", "columns": ["id"]}
        ],
    }


class TestSpecSerializer:
    def test_roundtrip_dict(self):
        original = _build_complex_spec_dict()
        parsed_spec = spec.from_dict(original)

        serializer = SpecSerializer()
        serialized = serializer.serialize(parsed_spec)

        assert serialized == original
        assert spec.from_dict(serialized) == parsed_spec

    def test_yads_spec_to_dict(self):
        spec_dict = _build_complex_spec_dict()
        parsed_spec = spec.from_dict(spec_dict)

        assert parsed_spec.to_dict() == spec_dict


class TestGeneratedColumnDeserialization:
    def _create_spec_with_generated_column(self, generated_as_def: dict | None) -> dict:
        column_def: dict = {"name": "generated_col", "type": "string"}
        if generated_as_def is not None:
            column_def["generated_as"] = generated_as_def

        return {
            "name": "test_spec",
            "version": 1,
            "columns": [
                {"name": "source_col", "type": "string"},
                column_def,
            ],
        }

    def test_generation_clause_missing_column_raises_error(self):
        spec_dict = self._create_spec_with_generated_column({"transform": "upper"})
        with pytest.raises(
            SpecParsingError,
            match=r"Missing required key\(s\) in generation clause: column\.",
        ):
            spec.from_dict(spec_dict)

    def test_generation_clause_missing_transform_raises_error(self):
        spec_dict = self._create_spec_with_generated_column({"column": "source_col"})
        with pytest.raises(
            SpecParsingError,
            match=r"Missing required key\(s\) in generation clause: transform\.",
        ):
            spec.from_dict(spec_dict)

    def test_generation_clause_empty_transform_raises_error(self):
        spec_dict = self._create_spec_with_generated_column(
            {"column": "source_col", "transform": ""}
        )
        with pytest.raises(
            SpecParsingError,
            match="'transform' cannot be empty in a generation clause",
        ):
            spec.from_dict(spec_dict)

    def test_generation_clause_unknown_key_raises_error(self):
        spec_dict = self._create_spec_with_generated_column(
            {"column": "source_col", "transform": "upper", "params": [1]}
        )
        with pytest.raises(
            SpecParsingError,
            match=r"Unknown key\(s\) in generation clause: params\.",
        ):
            spec.from_dict(spec_dict)

    def test_valid_generation_clause_deserialization(self):
        spec_dict = self._create_spec_with_generated_column(
            {
                "column": "source_col",
                "transform": "upper",
                "transform_args": ["arg1"],
            }
        )
        parsed_spec = spec.from_dict(spec_dict)

        generated_col = parsed_spec.columns[1]
        assert generated_col.generated_as is not None
        assert generated_col.generated_as.column == "source_col"
        assert generated_col.generated_as.transform == "upper"
        assert generated_col.generated_as.transform_args == ["arg1"]


class TestStorageDeserialization:
    def test_storage_section(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "storage": {
                "format": "parquet",
                "location": "/path/to/data",
                "tbl_properties": {"compression": "snappy"},
            },
        }
        parsed_spec = spec.from_dict(spec_dict)

        assert parsed_spec.storage is not None
        assert parsed_spec.storage.format == "parquet"
        assert parsed_spec.storage.location == "/path/to/data"
        assert parsed_spec.storage.tbl_properties == {"compression": "snappy"}

    def test_storage_with_unknown_key_raises_error(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "storage": {
                "format": "parquet",
                "invalid_key": True,
            },
        }
        with pytest.raises(
            SpecParsingError,
            match=r"Unknown key\(s\) in storage definition: invalid_key\.",
        ):
            spec.from_dict(spec_dict)


class TestPartitionDefinitionDeserialization:
    def test_partitioned_by_missing_column_raises_error(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"transform": "year"}],
        }
        with pytest.raises(
            SpecParsingError,
            match=r"Missing required key\(s\) in partitioned_by item: column\.",
        ):
            spec.from_dict(spec_dict)

    def test_partitioned_by_unknown_key_raises_error(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"column": "col1", "params": [1]}],
        }
        with pytest.raises(
            SpecParsingError,
            match=r"Unknown key\(s\) in partitioned_by item: params\.",
        ):
            spec.from_dict(spec_dict)

    def test_partitioned_by_deserialization(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [
                {"name": "col1", "type": "string"},
                {"name": "date_col", "type": "date"},
            ],
            "partitioned_by": [
                {"column": "col1"},
                {
                    "column": "date_col",
                    "transform": "year",
                    "transform_args": [2023],
                },
            ],
        }
        parsed_spec = spec.from_dict(spec_dict)

        assert len(parsed_spec.partitioned_by) == 2

        first_partition = parsed_spec.partitioned_by[0]
        assert first_partition.column == "col1"
        assert first_partition.transform is None
        assert first_partition.transform_args == []

        second_partition = parsed_spec.partitioned_by[1]
        assert second_partition.column == "date_col"
        assert second_partition.transform == "year"
        assert second_partition.transform_args == [2023]


class TestSpecTopLevelValidation:
    def test_unknown_top_level_key_raises_error(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "foo": "bar",
            "columns": [{"name": "col1", "type": "string"}],
        }
        with pytest.raises(
            SpecParsingError,
            match=r"Unknown key\(s\) in spec definition: foo\.",
        ):
            spec.from_dict(spec_dict)


class TestSpecSemanticValidation:
    def test_validate_columns_duplicate_names(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [
                {"name": "col1", "type": "string"},
                {"name": "col1", "type": "integer"},
            ],
        }
        with pytest.raises(
            SpecValidationError, match="Duplicate column name found: 'col1'"
        ):
            spec.from_dict(spec_dict)

    def test_validate_partitions_undefined_column(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"column": "undefined_col"}],
        }
        with pytest.raises(
            SpecValidationError,
            match="Partition column 'undefined_col' must be defined as a column in the schema",
        ):
            spec.from_dict(spec_dict)

    def test_validate_generated_columns_undefined_source(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [
                {
                    "name": "generated_col",
                    "type": "string",
                    "generated_as": {
                        "column": "undefined_source",
                        "transform": "upper",
                    },
                }
            ],
        }
        with pytest.raises(
            SpecValidationError,
            match="Source column 'undefined_source' for generated column 'generated_col' not found in schema",
        ):
            spec.from_dict(spec_dict)

    def test_validate_table_constraints_undefined_column(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "table_constraints": [
                {
                    "type": "primary_key",
                    "name": "pk_test",
                    "columns": ["undefined_col"],
                }
            ],
        }
        with pytest.raises(SpecValidationError) as excinfo:
            spec.from_dict(spec_dict)

        assert "Column 'undefined_col'" in str(excinfo.value)
        assert "not found in schema" in str(excinfo.value)
