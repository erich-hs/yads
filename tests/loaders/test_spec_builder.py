import pytest
import yaml
from pathlib import Path

from yads.constraints import (
    DefaultConstraint,
    ForeignKeyTableConstraint,
    NotNullConstraint,
    PrimaryKeyTableConstraint,
)
from yads.exceptions import (
    InvalidConstraintError,
    SpecParsingError,
    SpecValidationError,
    TypeDefinitionError,
    UnknownConstraintError,
    UnknownTypeError,
)
from yads.loaders import from_dict, from_yaml_path, from_yaml_string
from yads.spec import YadsSpec, Storage

# Define paths to the fixture directories
VALID_SPEC_DIR = Path(__file__).parent.parent / "fixtures" / "spec" / "valid"
INVALID_SPEC_DIR = Path(__file__).parent.parent / "fixtures" / "spec" / "invalid"


# Get all valid spec files
valid_spec_files = list(VALID_SPEC_DIR.glob("*.yaml"))


@pytest.fixture(params=valid_spec_files, ids=[f.name for f in valid_spec_files])
def valid_spec_path(request):
    return request.param


@pytest.fixture
def valid_spec_content(valid_spec_path):
    return valid_spec_path.read_text()


@pytest.fixture
def valid_spec_dict(valid_spec_content):
    return yaml.safe_load(valid_spec_content)


class TestFromDict:
    def test_with_valid_spec(self, valid_spec_dict):
        spec = from_dict(valid_spec_dict)
        assert isinstance(spec, YadsSpec)
        assert spec.name == valid_spec_dict["name"]


# %% Full spec building (integration)
class TestFullSpecBuilding:
    @pytest.fixture(scope="class")
    def spec(self) -> YadsSpec:
        return from_yaml_path(VALID_SPEC_DIR / "full_spec.yaml")

    def test_top_level_attributes(self, spec: YadsSpec):
        assert spec.name == "catalog.db.full_spec"
        assert spec.version == 1
        assert spec.description == "A full spec with all features."
        assert spec.metadata == {"owner": "data-team", "sensitive": False}
        assert spec.external is True

    def test_storage_attributes(self, spec: YadsSpec):
        assert spec.storage is not None
        assert spec.storage.location == "/data/full.spec"
        assert spec.storage.format == "parquet"
        assert spec.storage.tbl_properties == {"write_compression": "snappy"}

    def test_partitioning(self, spec: YadsSpec):
        assert len(spec.partitioned_by) == 3
        assert spec.partitioned_by[0].column == "c_string_len"
        assert spec.partitioned_by[1].column == "c_string"
        assert spec.partitioned_by[1].transform == "truncate"
        assert spec.partitioned_by[1].transform_args == [10]
        assert spec.partitioned_by[2].column == "c_date"
        assert spec.partitioned_by[2].transform == "month"

    def test_columns(self, spec: YadsSpec):
        assert len(spec.columns) == 34

    def test_column_constraints(self, spec: YadsSpec):
        # Test not_null constraint
        c_with_not_null = {
            c.name
            for c in spec.columns
            if any(isinstance(cons, NotNullConstraint) for cons in c.constraints)
        }
        assert len(c_with_not_null) == 2
        assert "c_uuid" in c_with_not_null
        assert "c_date" in c_with_not_null

        # Test default constraint
        c_with_default = {
            c.name: next(
                (cons for cons in c.constraints if isinstance(cons, DefaultConstraint)),
                None,
            )
            for c in spec.columns
            if any(isinstance(cons, DefaultConstraint) for cons in c.constraints)
        }
        assert len(c_with_default) == 1
        assert "c_string" in c_with_default
        assert c_with_default["c_string"] is not None
        assert c_with_default["c_string"].value == "default_string"

    def test_table_constraints(self, spec: YadsSpec):
        assert len(spec.table_constraints) == 2

        pk_constraint = next(
            (
                c
                for c in spec.table_constraints
                if isinstance(c, PrimaryKeyTableConstraint)
            ),
            None,
        )
        assert pk_constraint is not None
        assert pk_constraint.name == "pk_full_spec"
        assert pk_constraint.columns == ["c_uuid", "c_date"]

        fk_constraint = next(
            (
                c
                for c in spec.table_constraints
                if isinstance(c, ForeignKeyTableConstraint)
            ),
            None,
        )
        assert fk_constraint is not None
        assert fk_constraint.name == "fk_other_table"
        assert fk_constraint.columns == ["c_int64"]
        assert fk_constraint.references.table == "other_table"
        assert fk_constraint.references.columns == ["id"]

    def test_get_column(self, spec: YadsSpec):
        column = next((c for c in spec.columns if c.name == "c_uuid"), None)
        assert column is not None
        assert column.name == "c_uuid"
        assert str(column.type) == "uuid"
        assert column.description == "Primary key part 1"
        assert not column.metadata


# %% Invalid spec matrix (parametrized)
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
def test_from_string_with_invalid_spec_raises_error(spec_path, error_type, error_msg):
    with open(spec_path) as f:
        content = f.read()
    with pytest.raises(error_type, match=error_msg):
        from_yaml_string(content)


class TestSpecValidationGuards:
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
            from_dict(spec_dict)

    def test_spec_metadata_requires_mapping(self):
        spec_dict = self._base_spec()
        spec_dict["metadata"] = []
        with pytest.raises(
            SpecParsingError, match="Metadata for spec metadata must be a mapping"
        ):
            from_dict(spec_dict)

    def test_spec_version_rejects_non_integer(self):
        spec_dict = self._base_spec()
        spec_dict["version"] = "latest"
        with pytest.raises(
            SpecParsingError, match="'version' must be an integer when specified"
        ):
            from_dict(spec_dict)

    def test_spec_version_rejects_boolean(self):
        spec_dict = self._base_spec()
        spec_dict["version"] = True
        with pytest.raises(
            SpecParsingError, match="'version' must be an integer when specified"
        ):
            from_dict(spec_dict)

    def test_spec_external_must_be_boolean(self):
        spec_dict = self._base_spec()
        spec_dict["external"] = "true"
        with pytest.raises(
            SpecParsingError, match="'external' must be a boolean when specified"
        ):
            from_dict(spec_dict)

    def test_spec_yads_version_must_be_string(self):
        spec_dict = self._base_spec()
        spec_dict["yads_spec_version"] = 123
        with pytest.raises(
            SpecParsingError, match="'yads_spec_version' must be a non-empty string"
        ):
            from_dict(spec_dict)

    def test_generated_column_empty_mapping_not_ignored(self):
        spec_dict = self._base_spec()
        spec_dict["columns"][0]["generated_as"] = {}
        with pytest.raises(SpecParsingError, match="generation clause"):
            from_dict(spec_dict)

    def test_partitioned_by_mapping_rejected(self):
        spec_dict = self._base_spec()
        spec_dict["partitioned_by"] = {"column": "id"}
        with pytest.raises(SpecParsingError, match="'partitioned_by' must be a sequence"):
            from_dict(spec_dict)

    def test_storage_empty_mapping_preserved(self):
        spec_dict = self._base_spec()
        spec_dict["storage"] = {}
        spec = from_dict(spec_dict)
        assert spec.storage == Storage()

    def test_storage_invalid_type_raises(self):
        spec_dict = self._base_spec()
        spec_dict["storage"] = []
        with pytest.raises(
            SpecParsingError, match="Storage definition must be a mapping"
        ):
            from_dict(spec_dict)

    def test_unknown_non_string_keys_reported(self):
        spec_dict = self._base_spec()
        spec_dict[1] = "bad"
        with pytest.raises(
            SpecParsingError, match="Unknown key\\(s\\) in spec definition: 1"
        ):
            from_dict(spec_dict)

    def test_column_name_must_be_non_empty_string(self):
        spec_dict = self._base_spec()
        spec_dict["columns"][0]["name"] = None
        with pytest.raises(
            SpecParsingError,
            match="The 'name' of a column must be a non-empty string",
        ):
            from_dict(spec_dict)

    def test_type_params_unknown_field_raises_type_definition_error(self):
        spec_dict = self._base_spec()
        spec_dict["columns"][0]["type"] = "integer"
        spec_dict["columns"][0]["params"] = {"bogus": 1}
        with pytest.raises(
            TypeDefinitionError, match="Failed to instantiate type 'integer'"
        ):
            from_dict(spec_dict)
