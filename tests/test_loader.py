import pytest
import yaml
from pathlib import Path

from yads.constraints import NotNullConstraint, PrimaryKeyTableConstraint
from yads.loader import from_dict, from_string, from_yaml
from yads.spec import SchemaSpec
from yads.constraints import DefaultConstraint, ForeignKeyTableConstraint

# Define paths to the fixture directories
VALID_SPEC_DIR = Path(__file__).parent / "fixtures" / "spec" / "valid"
INVALID_SPEC_DIR = Path(__file__).parent / "fixtures" / "spec" / "invalid"

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
        assert isinstance(spec, SchemaSpec)
        assert spec.name == valid_spec_dict["name"]


class TestFromString:
    def test_with_valid_spec(self, valid_spec_content, valid_spec_dict):
        spec = from_string(valid_spec_content)
        assert isinstance(spec, SchemaSpec)
        assert spec.name == valid_spec_dict["name"]


class TestFromYaml:
    def test_with_valid_spec(self, valid_spec_path, valid_spec_dict):
        spec = from_yaml(str(valid_spec_path))
        assert isinstance(spec, SchemaSpec)
        assert spec.name == valid_spec_dict["name"]


class TestFullSpec:
    """Tests the loading of a comprehensive spec with all features."""

    @pytest.fixture(scope="class")
    def spec(self) -> SchemaSpec:
        """Fixture to load the full_spec.yaml file."""
        return from_yaml(str(VALID_SPEC_DIR / "full_spec.yaml"))

    def test_top_level_attributes(self, spec: SchemaSpec):
        assert spec.name == "catalog.db.full_schema"
        assert spec.version == "2.1.0"
        assert spec.description == "A full schema with all features."
        assert spec.metadata == {"owner": "data-team", "sensitive": False}
        assert spec.external is True

    def test_storage_attributes(self, spec: SchemaSpec):
        assert spec.storage is not None
        assert spec.storage.location == "/data/full.schema"
        assert spec.storage.format == "parquet"
        assert spec.storage.tbl_properties == {"write_compression": "snappy"}

    def test_partitioning(self, spec: SchemaSpec):
        assert len(spec.partitioned_by) == 2
        assert spec.partitioned_by[0].column == "c_string_len"
        assert spec.partitioned_by[1].column == "c_date"
        assert spec.partitioned_by[1].transform == "month"

    def test_columns(self, spec: SchemaSpec):
        assert len(spec.columns) == 20

    def test_column_constraints(self, spec: SchemaSpec):
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

    def test_table_constraints(self, spec: SchemaSpec):
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
        assert pk_constraint.name == "pk_full_schema"
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

    def test_get_column(self, spec: SchemaSpec):
        column = next((c for c in spec.columns if c.name == "c_uuid"), None)
        assert column is not None
        assert column.name == "c_uuid"
        assert str(column.type) == "uuid"
        assert column.description == "Primary key part 1"
        assert not column.metadata


@pytest.mark.parametrize(
    "spec_path, error_msg",
    [
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_name.yaml",
            "'name' is a required field",
        ),
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_version.yaml",
            "'version' is a required field",
        ),
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_columns.yaml",
            "'columns' is a required field",
        ),
        (
            INVALID_SPEC_DIR / "missing_column_field" / "missing_name.yaml",
            "'name' is a required field in a column definition",
        ),
        (
            INVALID_SPEC_DIR / "missing_column_field" / "missing_type.yaml",
            "'type' is a required field in a column definition",
        ),
        (INVALID_SPEC_DIR / "unknown_type.yaml", "Unknown type: 'invalid_type'"),
        (
            INVALID_SPEC_DIR / "invalid_type_def.yaml",
            "The 'type' of a field must be a string",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "array_missing_element.yaml",
            "Array type definition must include 'element'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "struct_missing_fields.yaml",
            "Struct type definition must include 'fields'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "map_missing_key.yaml",
            "Map type definition must include 'key' and 'value'",
        ),
        (
            INVALID_SPEC_DIR / "invalid_complex_type" / "map_missing_value.yaml",
            "Map type definition must include 'key' and 'value'",
        ),
        (
            INVALID_SPEC_DIR / "unknown_constraint.yaml",
            "Unknown column constraint: invalid_constraint",
        ),
        (
            INVALID_SPEC_DIR / "generated_as_undefined_column.yaml",
            "Source column 'non_existent_col' for generated column 'col2' not found in schema.",
        ),
        (
            INVALID_SPEC_DIR / "partitioned_by_undefined_column.yaml",
            "Partition column 'non_existent_col' must be defined as a column in the schema.",
        ),
        (
            INVALID_SPEC_DIR / "identity_with_increment_zero.yaml",
            "The 'increment' for an identity constraint cannot be 0",
        ),
        (
            INVALID_SPEC_DIR / "invalid_interval" / "missing_start.yaml",
            "Interval type definition must include 'interval_start'",
        ),
    ],
)
def test_from_string_with_invalid_spec_raises_error(spec_path, error_msg):
    with open(spec_path) as f:
        content = f.read()
    with pytest.raises(ValueError, match=error_msg):
        from_string(content)


def test_invalid_yaml_content_raises_error():
    """Test that non-dictionary YAML content raises a ``TypeError``."""
    content = "- item1\n- item2"  # A list, not a dictionary
    with pytest.raises(
        TypeError, match="Loaded YAML content did not parse to a dictionary"
    ):
        from_string(content)
