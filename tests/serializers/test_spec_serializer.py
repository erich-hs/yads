from pathlib import Path

import pytest
import yaml

from yads.constraints import (
    DefaultConstraint,
    ForeignKeyTableConstraint,
    NotNullConstraint,
    PrimaryKeyTableConstraint,
    PrimaryKeyConstraint,
)
from yads.exceptions import (
    InvalidConstraintError,
    SpecParsingError,
    SpecValidationError,
    TypeDefinitionError,
    UnknownConstraintError,
    UnknownTypeError,
)
from yads.serializers import SpecDeserializer, SpecSerializer
from yads.spec import Field, Column, Storage, TransformedColumnReference, YadsSpec
import yads.types as ytypes

FIXTURE_DIR = Path(__file__).parent.parent / "fixtures" / "spec"
VALID_SPEC_DIR = FIXTURE_DIR / "valid"
INVALID_SPEC_DIR = FIXTURE_DIR / "invalid"
valid_spec_files = list(VALID_SPEC_DIR.glob("*.yaml"))


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


@pytest.fixture(params=valid_spec_files, ids=[f.name for f in valid_spec_files])
def valid_spec_path(request):
    return request.param


@pytest.fixture
def valid_spec_content(valid_spec_path):
    return valid_spec_path.read_text()


@pytest.fixture
def valid_spec_dict(valid_spec_content):
    return yaml.safe_load(valid_spec_content)


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


class TestSpecSerializerRoundtrip:
    def setup_method(self) -> None:
        self.serializer = SpecSerializer()
        self.deserializer = SpecDeserializer()

    def test_roundtrip_dict(self):
        original = _build_complex_spec_dict()
        parsed_spec = self.deserializer.deserialize(original)
        serialized = self.serializer.serialize(parsed_spec)
        assert serialized == original
        assert self.deserializer.deserialize(serialized) == parsed_spec

    def test_yads_spec_to_dict(self):
        spec_dict = _build_complex_spec_dict()
        parsed_spec = self.deserializer.deserialize(spec_dict)

        assert parsed_spec.to_dict() == spec_dict


class TestSpecSerializerFixtureRoundtrip:
    def setup_method(self) -> None:
        self.serializer = SpecSerializer()
        self.deserializer = SpecDeserializer()

    def test_fixtures_roundtrip_and_stay_stable(self, valid_spec_dict):
        parsed = self.deserializer.deserialize(valid_spec_dict)

        serialized_once = self.serializer.serialize(parsed)
        reparsed = self.deserializer.deserialize(serialized_once)
        serialized_twice = self.serializer.serialize(reparsed)

        assert reparsed == parsed
        assert serialized_twice == serialized_once


class TestSpecToDictBehaviors:
    def test_minimal_spec_to_dict_contains_required_fields_only(self):
        spec = YadsSpec(
            name="catalog.db.minimal",
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
        )

        assert spec.to_dict() == {
            "name": "catalog.db.minimal",
            "version": 1,
            "yads_spec_version": spec.yads_spec_version,
            "columns": [{"name": "id", "type": "integer"}],
        }

    def test_nested_fields_preserve_metadata_and_constraints(self):
        spec = YadsSpec(
            name="catalog.db.nested",
            version=1,
            columns=[
                Column(
                    name="id",
                    type=ytypes.Integer(),
                    constraints=[NotNullConstraint()],
                ),
                Column(
                    name="profile",
                    type=ytypes.Struct(
                        fields=[
                            Field(
                                name="handle",
                                type=ytypes.String(length=16),
                                metadata={"source": "app"},
                            ),
                            Field(
                                name="age",
                                type=ytypes.Integer(),
                                constraints=[NotNullConstraint()],
                            ),
                        ]
                    ),
                ),
            ],
        )

        serialized = spec.to_dict()
        reparsed = SpecDeserializer().deserialize(serialized)

        assert reparsed.name == spec.name
        assert reparsed.version == spec.version
        assert reparsed.columns[0].constraints == [NotNullConstraint()]
        assert reparsed.columns[0].name == "id"

        profile_field = next(
            column for column in serialized["columns"] if column["name"] == "profile"
        )
        handle_field = next(
            field for field in profile_field["fields"] if field["name"] == "handle"
        )
        assert handle_field["metadata"] == {"source": "app"}

        age_field = next(
            field for field in profile_field["fields"] if field["name"] == "age"
        )
        assert age_field["constraints"] == {"not_null": True}

        assert SpecSerializer().serialize(reparsed) == serialized


class TestSpecSerializerOptionalFields:
    def test_serializer_emits_optional_fields(self):
        spec = YadsSpec(
            name="catalog.db.users",
            version=2,
            yads_spec_version="0.0.2",
            description="Serialized spec with extras",
            external=True,
            metadata={"team": "data-eng"},
            storage=Storage(
                format="delta",
                location="s3://bucket/users",
                tbl_properties={"delta.appendOnly": "true"},
            ),
            partitioned_by=[
                TransformedColumnReference(
                    column="bucket", transform="mod", transform_args=[16]
                )
            ],
            table_constraints=[
                PrimaryKeyTableConstraint(columns=["id"], name="pk_users")
            ],
            columns=[
                Column(
                    name="id",
                    type=ytypes.Integer(bits=32),
                    description="identifier",
                    metadata={"origin": "system"},
                    constraints=[NotNullConstraint(), PrimaryKeyConstraint()],
                ),
                Column(
                    name="bucket",
                    type=ytypes.Integer(bits=32),
                    generated_as=TransformedColumnReference(
                        column="id", transform="hash", transform_args=[16]
                    ),
                ),
            ],
        )

        serialized = SpecSerializer().serialize(spec)

        assert serialized["external"] is True
        assert serialized["description"] == "Serialized spec with extras"
        assert serialized["metadata"] == {"team": "data-eng"}
        assert serialized["storage"] == {
            "format": "delta",
            "location": "s3://bucket/users",
            "tbl_properties": {"delta.appendOnly": "true"},
        }
        assert serialized["partitioned_by"] == [
            {"column": "bucket", "transform": "mod", "transform_args": [16]}
        ]
        bucket_column = next(c for c in serialized["columns"] if c["name"] == "bucket")
        assert bucket_column["generated_as"]["transform_args"] == [16]
        id_column = next(c for c in serialized["columns"] if c["name"] == "id")
        assert "constraints" in id_column
        assert serialized["table_constraints"] == [
            {"type": "primary_key", "name": "pk_users", "columns": ["id"]}
        ]


class TestSpecDeserializerFullSpec:
    @pytest.fixture(scope="class")
    def parsed_spec(self) -> YadsSpec:
        return SpecDeserializer().deserialize(
            _load_yaml(VALID_SPEC_DIR / "full_spec.yaml")
        )

    def test_top_level_attributes(self, parsed_spec: YadsSpec):
        assert parsed_spec.name == "catalog.db.full_spec"
        assert parsed_spec.version == 1
        assert parsed_spec.description == "A full spec with all features."
        assert parsed_spec.metadata == {"owner": "data-team", "sensitive": False}
        assert parsed_spec.external is True

    def test_storage_attributes(self, parsed_spec: YadsSpec):
        assert parsed_spec.storage is not None
        assert parsed_spec.storage.location == "/data/full.spec"
        assert parsed_spec.storage.format == "parquet"
        assert parsed_spec.storage.tbl_properties == {"write_compression": "snappy"}

    def test_partitioning(self, parsed_spec: YadsSpec):
        assert len(parsed_spec.partitioned_by) == 3
        assert parsed_spec.partitioned_by[0].column == "c_string_len"
        assert parsed_spec.partitioned_by[1].column == "c_string"
        assert parsed_spec.partitioned_by[1].transform == "truncate"
        assert parsed_spec.partitioned_by[1].transform_args == [10]
        assert parsed_spec.partitioned_by[2].column == "c_date"
        assert parsed_spec.partitioned_by[2].transform == "month"

    def test_columns(self, parsed_spec: YadsSpec):
        assert len(parsed_spec.columns) == 34

    def test_column_constraints(self, parsed_spec: YadsSpec):
        constrained_columns = {
            column.name
            for column in parsed_spec.columns
            if any(isinstance(cons, NotNullConstraint) for cons in column.constraints)
        }
        assert constrained_columns == {"c_uuid", "c_date"}

        defaults = {
            column.name: next(
                (
                    cons
                    for cons in column.constraints
                    if isinstance(cons, DefaultConstraint)
                ),
                None,
            )
            for column in parsed_spec.columns
            if any(isinstance(cons, DefaultConstraint) for cons in column.constraints)
        }
        assert set(defaults) == {"c_string"}
        assert defaults["c_string"] is not None
        assert defaults["c_string"].value == "default_string"

    def test_table_constraints(self, parsed_spec: YadsSpec):
        assert len(parsed_spec.table_constraints) == 2

        pk_constraint = next(
            (
                constraint
                for constraint in parsed_spec.table_constraints
                if isinstance(constraint, PrimaryKeyTableConstraint)
            ),
            None,
        )
        assert pk_constraint is not None
        assert pk_constraint.name == "pk_full_spec"
        assert pk_constraint.columns == ["c_uuid", "c_date"]

        fk_constraint = next(
            (
                constraint
                for constraint in parsed_spec.table_constraints
                if isinstance(constraint, ForeignKeyTableConstraint)
            ),
            None,
        )
        assert fk_constraint is not None
        assert fk_constraint.name == "fk_other_table"
        assert fk_constraint.columns == ["c_int64"]
        assert fk_constraint.references.table == "other_table"
        assert fk_constraint.references.columns == ["id"]

    def test_get_column(self, parsed_spec: YadsSpec):
        column = next((col for col in parsed_spec.columns if col.name == "c_uuid"), None)
        assert column is not None
        assert column.name == "c_uuid"
        assert str(column.type) == "uuid"
        assert column.description == "Primary key part 1"
        assert not column.metadata


class TestSpecDeserializerFromDict:
    def test_with_valid_spec(self, valid_spec_dict):
        parsed_spec = SpecDeserializer().deserialize(valid_spec_dict)
        assert isinstance(parsed_spec, YadsSpec)
        assert parsed_spec.name == valid_spec_dict["name"]


class TestGeneratedColumnDeserialization:
    def setup_method(self) -> None:
        self.deserializer = SpecDeserializer()

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
            self.deserializer.deserialize(spec_dict)

    def test_generation_clause_missing_transform_raises_error(self):
        spec_dict = self._create_spec_with_generated_column({"column": "source_col"})
        with pytest.raises(
            SpecParsingError,
            match=r"Missing required key\(s\) in generation clause: transform\.",
        ):
            self.deserializer.deserialize(spec_dict)

    def test_generation_clause_empty_transform_raises_error(self):
        spec_dict = self._create_spec_with_generated_column(
            {"column": "source_col", "transform": ""}
        )
        with pytest.raises(
            SpecParsingError,
            match="'transform' cannot be empty in a generation clause",
        ):
            self.deserializer.deserialize(spec_dict)

    def test_generation_clause_unknown_key_raises_error(self):
        spec_dict = self._create_spec_with_generated_column(
            {"column": "source_col", "transform": "upper", "params": [1]}
        )
        with pytest.raises(
            SpecParsingError,
            match=r"Unknown key\(s\) in generation clause: params\.",
        ):
            self.deserializer.deserialize(spec_dict)

    def test_valid_generation_clause_deserialization(self):
        spec_dict = self._create_spec_with_generated_column(
            {
                "column": "source_col",
                "transform": "upper",
                "transform_args": ["arg1"],
            }
        )
        parsed_spec = self.deserializer.deserialize(spec_dict)

        generated_col = parsed_spec.columns[1]
        assert generated_col.generated_as is not None
        assert generated_col.generated_as.column == "source_col"
        assert generated_col.generated_as.transform == "upper"
        assert generated_col.generated_as.transform_args == ["arg1"]


class TestStorageDeserialization:
    def setup_method(self) -> None:
        self.deserializer = SpecDeserializer()

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
        parsed_spec = self.deserializer.deserialize(spec_dict)

        assert parsed_spec.storage is not None
        assert parsed_spec.storage.format == "parquet"
        assert parsed_spec.storage.location == "/path/to/data"
        assert parsed_spec.storage.tbl_properties == {"compression": "snappy"}

    def test_storage_type_validation(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "storage": {"format": 123},
        }
        with pytest.raises(
            SpecParsingError, match="'storage.format' must be a string when specified"
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["storage"] = {"location": 99}
        with pytest.raises(
            SpecParsingError, match="'storage.location' must be a string when specified"
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["storage"] = {"tbl_properties": "bad"}
        with pytest.raises(
            SpecParsingError,
            match="'storage.tbl_properties' must be a mapping of strings",
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["storage"] = {"tbl_properties": {1: "x"}}
        with pytest.raises(
            SpecParsingError, match="must only contain string keys and values"
        ):
            self.deserializer.deserialize(spec_dict)

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
            self.deserializer.deserialize(spec_dict)


class TestPartitionDefinitionDeserialization:
    def setup_method(self) -> None:
        self.deserializer = SpecDeserializer()

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
            self.deserializer.deserialize(spec_dict)

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
            self.deserializer.deserialize(spec_dict)

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
        parsed_spec = self.deserializer.deserialize(spec_dict)

        assert len(parsed_spec.partitioned_by) == 2

        first_partition = parsed_spec.partitioned_by[0]
        assert first_partition.column == "col1"
        assert first_partition.transform is None
        assert first_partition.transform_args == []

        second_partition = parsed_spec.partitioned_by[1]
        assert second_partition.column == "date_col"
        assert second_partition.transform == "year"
        assert second_partition.transform_args == [2023]

    def test_partitioned_by_rejects_string_and_non_mapping_items(self):
        base_spec = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
        }
        spec_dict = {**base_spec, "partitioned_by": "col1"}
        with pytest.raises(
            SpecParsingError, match="'partitioned_by' must be a sequence of mappings"
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict = {**base_spec, "partitioned_by": [123]}
        with pytest.raises(
            SpecParsingError, match="Partition definition at index 0 must be a mapping"
        ):
            self.deserializer.deserialize(spec_dict)

    def test_partition_reference_type_validation(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"column": 1}],
        }
        with pytest.raises(
            SpecParsingError, match="'column' in partitioned_by item must be a string"
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["partitioned_by"] = [{"column": "col1", "transform": 5}]
        with pytest.raises(
            SpecParsingError,
            match="'transform' in partitioned_by item must be a string when specified",
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["partitioned_by"] = [{"column": "col1", "transform_args": "bad_args"}]
        with pytest.raises(
            SpecParsingError,
            match="'transform_args' in partitioned_by item must be provided as a list",
        ):
            self.deserializer.deserialize(spec_dict)


@pytest.mark.parametrize(
    "spec_path, error_type, error_msg",
    [
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_name.yaml",
            SpecParsingError,
            r"Missing required key\(s\) in spec definition: name\.",
        ),
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_columns.yaml",
            SpecParsingError,
            r"Missing required key\(s\) in spec definition: columns\.",
        ),
        (
            INVALID_SPEC_DIR / "missing_column_field" / "missing_name.yaml",
            SpecParsingError,
            "'name' is a required field in a column definition",
        ),
        (
            INVALID_SPEC_DIR / "missing_column_field" / "missing_type.yaml",
            SpecParsingError,
            "'type' is a required field in a column definition",
        ),
        (
            INVALID_SPEC_DIR / "unknown_type.yaml",
            UnknownTypeError,
            "Unknown type: 'invalid_type'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_type_def.yaml",
            TypeDefinitionError,
            "The 'type' of a column must be a string",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "array_missing_element.yaml",
            TypeDefinitionError,
            "Array type definition must include 'element'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "struct_missing_fields.yaml",
            TypeDefinitionError,
            "Struct type definition must include 'fields'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "map_missing_key.yaml",
            TypeDefinitionError,
            "Map type definition must include 'key' and 'value'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "map_missing_value.yaml",
            TypeDefinitionError,
            "Map type definition must include 'key' and 'value'",
        ),
        (
            INVALID_SPEC_DIR / "unknown_constraint.yaml",
            UnknownConstraintError,
            "Unknown column constraint: invalid_constraint",
        ),
        (
            INVALID_SPEC_DIR / "generated_as_undefined_column.yaml",
            SpecValidationError,
            "Source column 'non_existent_col' for generated column 'col2' not found in schema.",
        ),
        (
            INVALID_SPEC_DIR / "partitioned_by_undefined_column.yaml",
            SpecValidationError,
            "Partition column 'non_existent_col' must be defined as a column in the schema.",
        ),
        (
            INVALID_SPEC_DIR / "identity_with_increment_zero.yaml",
            InvalidConstraintError,
            "Identity 'increment' must be a non-zero integer",
        ),
        (
            INVALID_SPEC_DIR / "invalid_interval" / "missing_start.yaml",
            TypeDefinitionError,
            "Interval type definition must include 'interval_start'",
        ),
    ],
)
def test_deserialize_invalid_spec_raises_error(spec_path, error_type, error_msg):
    with open(spec_path) as file:
        content = file.read()
    with pytest.raises(error_type, match=error_msg):
        SpecDeserializer().deserialize(yaml.safe_load(content))


class TestSpecDeserializerValidationGuards:
    def setup_method(self) -> None:
        self.deserializer = SpecDeserializer()

    def _base_spec(self) -> dict:
        return {
            "name": "test_spec",
            "version": 1,
            "columns": [
                {
                    "name": "id",
                    "type": "string",
                }
            ],
        }

    def test_spec_name_must_be_string(self):
        spec_dict = self._base_spec()
        spec_dict["name"] = 123
        with pytest.raises(SpecParsingError, match="'name' must be a non-empty string"):
            self.deserializer.deserialize(spec_dict)

    def test_spec_metadata_requires_mapping(self):
        spec_dict = self._base_spec()
        spec_dict["metadata"] = []
        with pytest.raises(
            SpecParsingError, match="Metadata for spec metadata must be a mapping"
        ):
            self.deserializer.deserialize(spec_dict)

    def test_spec_version_rejects_non_integer(self):
        spec_dict = self._base_spec()
        spec_dict["version"] = "latest"
        with pytest.raises(
            SpecParsingError, match="'version' must be an integer when specified"
        ):
            self.deserializer.deserialize(spec_dict)

    def test_spec_version_rejects_boolean(self):
        spec_dict = self._base_spec()
        spec_dict["version"] = True
        with pytest.raises(
            SpecParsingError, match="'version' must be an integer when specified"
        ):
            self.deserializer.deserialize(spec_dict)
        spec_dict["version"] = 0
        with pytest.raises(
            SpecParsingError, match="'version' must be a positive integer"
        ):
            self.deserializer.deserialize(spec_dict)

    def test_spec_external_must_be_boolean(self):
        spec_dict = self._base_spec()
        spec_dict["external"] = "true"
        with pytest.raises(
            SpecParsingError, match="'external' must be a boolean when specified"
        ):
            self.deserializer.deserialize(spec_dict)

    def test_spec_yads_version_must_be_string(self):
        spec_dict = self._base_spec()
        spec_dict["yads_spec_version"] = 123
        with pytest.raises(
            SpecParsingError, match="'yads_spec_version' must be a non-empty string"
        ):
            self.deserializer.deserialize(spec_dict)

    def test_generated_column_empty_mapping_not_ignored(self):
        spec_dict = self._base_spec()
        spec_dict["columns"][0]["generated_as"] = {}
        with pytest.raises(SpecParsingError, match="generation clause"):
            self.deserializer.deserialize(spec_dict)

    def test_generated_column_requires_mapping(self):
        spec_dict = self._base_spec()
        spec_dict["columns"][0]["generated_as"] = "upper"
        with pytest.raises(
            SpecParsingError,
            match="Generated column definition must be a mapping when provided",
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["columns"][0]["generated_as"] = {
            "column": "id",
            "transform": 5,
        }
        with pytest.raises(
            SpecParsingError,
            match="'transform' in generation clause must be a string",
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["columns"][0]["generated_as"] = {
            "column": 3,
            "transform": "upper",
        }
        with pytest.raises(
            SpecParsingError, match="'column' in generation clause must be a string"
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["columns"][0]["generated_as"] = {
            "column": "id",
            "transform": "upper",
            "transform_args": "bad",
        }
        with pytest.raises(
            SpecParsingError,
            match="'transform_args' in generation clause must be provided as a list",
        ):
            self.deserializer.deserialize(spec_dict)

    def test_partitioned_by_mapping_rejected(self):
        spec_dict = self._base_spec()
        spec_dict["partitioned_by"] = {"column": "id"}
        with pytest.raises(SpecParsingError, match="'partitioned_by' must be a sequence"):
            self.deserializer.deserialize(spec_dict)

    def test_storage_empty_mapping_preserved(self):
        spec_dict = self._base_spec()
        spec_dict["storage"] = {}
        parsed_spec = self.deserializer.deserialize(spec_dict)
        assert parsed_spec.storage == Storage()

    def test_storage_invalid_type_raises(self):
        spec_dict = self._base_spec()
        spec_dict["storage"] = []
        with pytest.raises(
            SpecParsingError, match="Storage definition must be a mapping"
        ):
            self.deserializer.deserialize(spec_dict)

    def test_unknown_non_string_keys_reported(self):
        spec_dict = self._base_spec()
        spec_dict[1] = "bad"
        with pytest.raises(
            SpecParsingError, match="Unknown key\\(s\\) in spec definition: 1"
        ):
            self.deserializer.deserialize(spec_dict)

    def test_column_name_must_be_non_empty_string(self):
        spec_dict = self._base_spec()
        spec_dict["columns"][0]["name"] = None
        with pytest.raises(
            SpecParsingError,
            match="The 'name' of a column must be a non-empty string",
        ):
            self.deserializer.deserialize(spec_dict)

    def test_columns_must_be_sequence_of_mappings(self):
        spec_dict = self._base_spec()
        spec_dict["columns"] = "col1"
        with pytest.raises(
            SpecParsingError, match="columns definition must be a sequence of mappings"
        ):
            self.deserializer.deserialize(spec_dict)

        spec_dict["columns"] = [123]
        with pytest.raises(
            SpecParsingError,
            match="Entry at index 0 in columns definition must be a mapping",
        ):
            self.deserializer.deserialize(spec_dict)

    def test_type_params_unknown_field_raises_type_definition_error(self):
        spec_dict = self._base_spec()
        spec_dict["columns"][0]["type"] = "integer"
        spec_dict["columns"][0]["params"] = {"bogus": 1}
        with pytest.raises(
            TypeDefinitionError, match="Failed to instantiate type 'integer'"
        ):
            self.deserializer.deserialize(spec_dict)


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
            SpecDeserializer().deserialize(spec_dict)

    def test_metadata_key_must_be_string(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "metadata": {1: "bad"},
            "columns": [{"name": "col1", "type": "string"}],
        }
        with pytest.raises(
            SpecParsingError,
            match="Metadata keys for spec metadata must be strings",
        ):
            SpecDeserializer().deserialize(spec_dict)


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
            SpecDeserializer().deserialize(spec_dict)

    def test_duplicate_constraint_definition_warns(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [
                {
                    "name": "id",
                    "type": "string",
                    "constraints": {"primary_key": True},
                }
            ],
            "table_constraints": [
                {"type": "primary_key", "name": "pk_test", "columns": ["id"]}
            ],
        }
        with pytest.warns(UserWarning, match="PrimaryKeyConstraint defined at both"):
            SpecDeserializer().deserialize(spec_dict)

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
            SpecDeserializer().deserialize(spec_dict)

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
            SpecDeserializer().deserialize(spec_dict)

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
            SpecDeserializer().deserialize(spec_dict)

        assert "Column 'undefined_col'" in str(excinfo.value)
        assert "not found in schema" in str(excinfo.value)

    def test_validate_generated_column_undefined_source(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [
                {
                    "name": "gen_col",
                    "type": "string",
                    "generated_as": {"column": "missing", "transform": "upper"},
                }
            ],
        }
        with pytest.raises(
            SpecValidationError,
            match="Source column 'missing' for generated column 'gen_col' not found in schema",
        ):
            SpecDeserializer().deserialize(spec_dict)

    def test_validate_partitioned_by_undefined_column(self):
        spec_dict = {
            "name": "test_spec",
            "version": 1,
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"column": "missing"}],
        }
        with pytest.raises(
            SpecValidationError,
            match="Partition column 'missing' must be defined as a column in the schema",
        ):
            SpecDeserializer().deserialize(spec_dict)
