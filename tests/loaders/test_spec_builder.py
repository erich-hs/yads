import pytest
import yaml
from pathlib import Path

from yads.constraints import NotNullConstraint, PrimaryKeyTableConstraint
from yads.exceptions import (
    InvalidConstraintError,
    SchemaParsingError,
    SchemaValidationError,
    TypeDefinitionError,
    UnknownConstraintError,
    UnknownTypeError,
)
from yads.loaders import from_dict, from_string, from_yaml
from yads.spec import SchemaSpec
from yads.constraints import DefaultConstraint, ForeignKeyTableConstraint
from yads.types import (
    String,
    Integer,
    Float,
    Boolean,
    Decimal,
    Date,
    Timestamp,
    TimestampTZ,
    TimestampLTZ,
    TimestampNTZ,
    Binary,
    UUID,
    Void,
    Interval,
    IntervalTimeUnit,
    Array,
    Struct,
    Map,
)

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


class TestConstraintParsing:
    def _create_minimal_spec_with_constraint(self, constraint_def: dict) -> dict:
        return {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [
                {
                    "name": "test_column",
                    "type": "string",
                    "constraints": constraint_def,
                }
            ],
        }

    def test_not_null_constraint_with_non_boolean_raises_error(self):
        spec_dict = self._create_minimal_spec_with_constraint({"not_null": "true"})
        with pytest.raises(
            InvalidConstraintError, match="The 'not_null' constraint expects a boolean"
        ):
            from_dict(spec_dict)

    def test_primary_key_constraint_with_non_boolean_raises_error(self):
        spec_dict = self._create_minimal_spec_with_constraint({"primary_key": "true"})
        with pytest.raises(
            InvalidConstraintError,
            match="The 'primary_key' constraint expects a boolean",
        ):
            from_dict(spec_dict)

    def test_foreign_key_constraint_with_non_dict_raises_error(self):
        spec_dict = self._create_minimal_spec_with_constraint({"foreign_key": "table"})
        with pytest.raises(
            InvalidConstraintError,
            match="The 'foreign_key' constraint expects a dictionary",
        ):
            from_dict(spec_dict)

    def test_foreign_key_constraint_missing_references_raises_error(self):
        spec_dict = self._create_minimal_spec_with_constraint(
            {"foreign_key": {"name": "fk_test"}}
        )
        with pytest.raises(
            InvalidConstraintError,
            match="The 'foreign_key' constraint must specify 'references'",
        ):
            from_dict(spec_dict)

    def test_identity_constraint_with_non_dict_raises_error(self):
        spec_dict = self._create_minimal_spec_with_constraint({"identity": True})
        with pytest.raises(
            InvalidConstraintError,
            match="The 'identity' constraint expects a dictionary",
        ):
            from_dict(spec_dict)

    def test_default_constraint_parsing(self):
        spec_dict = self._create_minimal_spec_with_constraint({"default": "test_value"})
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        default_constraints = [
            c for c in column.constraints if isinstance(c, DefaultConstraint)
        ]
        assert len(default_constraints) == 1
        assert default_constraints[0].value == "test_value"

    def test_identity_constraint_parsing(self):
        spec_dict = self._create_minimal_spec_with_constraint(
            {"identity": {"always": False, "start": 10, "increment": 2}}
        )
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        identity_constraints = [
            c for c in column.constraints if c.__class__.__name__ == "IdentityConstraint"
        ]
        assert len(identity_constraints) == 1
        identity = identity_constraints[0]
        assert identity.always is False
        assert identity.start == 10
        assert identity.increment == 2

    def test_identity_constraint_with_negative_increment_parsing(self):
        spec_dict = self._create_minimal_spec_with_constraint(
            {"identity": {"always": False, "start": 10, "increment": -2}}
        )
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        identity_constraints = [
            c for c in column.constraints if c.__class__.__name__ == "IdentityConstraint"
        ]
        assert len(identity_constraints) == 1
        identity = identity_constraints[0]
        assert identity.always is False
        assert identity.start == 10
        assert identity.increment == -2

    def test_foreign_key_constraint_parsing(self):
        spec_dict = self._create_minimal_spec_with_constraint(
            {
                "foreign_key": {
                    "name": "fk_test",
                    "references": {"table": "other_table", "columns": ["id"]},
                }
            }
        )
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        fk_constraints = [
            c
            for c in column.constraints
            if c.__class__.__name__ == "ForeignKeyConstraint"
        ]
        assert len(fk_constraints) == 1
        fk = fk_constraints[0]
        assert fk.name == "fk_test"
        assert fk.references.table == "other_table"
        assert fk.references.columns == ["id"]


class TestGenerationClauseParsing:
    def _create_spec_with_generated_column(self, generated_as_def: dict | None) -> dict:
        column_def: dict = {
            "name": "generated_col",
            "type": "string",
        }
        if generated_as_def is not None:
            column_def["generated_as"] = generated_as_def

        return {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [
                {"name": "source_col", "type": "string"},
                column_def,
            ],
        }

    def test_generation_clause_missing_column_raises_error(self):
        spec_dict = self._create_spec_with_generated_column({"transform": "upper"})
        with pytest.raises(
            SchemaParsingError,
            match=r"Missing required key\(s\) in generation clause: column\.",
        ):
            from_dict(spec_dict)

    def test_generation_clause_missing_transform_raises_error(self):
        spec_dict = self._create_spec_with_generated_column({"column": "source_col"})
        with pytest.raises(
            SchemaParsingError,
            match=r"Missing required key\(s\) in generation clause: transform\.",
        ):
            from_dict(spec_dict)

    def test_generation_clause_empty_transform_raises_error(self):
        spec_dict = self._create_spec_with_generated_column(
            {"column": "source_col", "transform": ""}
        )
        with pytest.raises(
            SchemaParsingError,
            match="'transform' cannot be empty in a generation clause",
        ):
            from_dict(spec_dict)

    def test_generation_clause_unknown_key_raises_error(self):
        spec_dict = self._create_spec_with_generated_column(
            {"column": "source_col", "transform": "upper", "params": [1]}
        )
        with pytest.raises(
            SchemaParsingError,
            match=r"Unknown key\(s\) in generation clause: params\.",
        ):
            from_dict(spec_dict)

    def test_valid_generation_clause_parsing(self):
        spec_dict = self._create_spec_with_generated_column(
            {
                "column": "source_col",
                "transform": "upper",
                "transform_args": ["arg1"],
            }
        )
        spec = from_dict(spec_dict)

        generated_col = spec.columns[1]
        assert generated_col.generated_as is not None
        assert generated_col.generated_as.column == "source_col"
        assert generated_col.generated_as.transform == "upper"
        assert generated_col.generated_as.transform_args == ["arg1"]


class TestTableConstraintParsing:
    def _create_spec_with_table_constraint(self, constraint_def: dict) -> dict:
        return {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [
                {"name": "col1", "type": "string"},
                {"name": "col2", "type": "integer"},
            ],
            "table_constraints": [constraint_def],
        }

    def test_primary_key_table_constraint_missing_columns_raises_error(self):
        spec_dict = self._create_spec_with_table_constraint(
            {"type": "primary_key", "name": "pk_test"}
        )
        with pytest.raises(
            InvalidConstraintError,
            match="Primary key table constraint must specify 'columns'",
        ):
            from_dict(spec_dict)

    def test_primary_key_table_constraint_with_no_name_raises_error(self):
        spec_dict = self._create_spec_with_table_constraint(
            {"type": "primary_key", "columns": ["col1"]}
        )
        with pytest.raises(
            InvalidConstraintError,
            match="Primary key table constraint must specify 'name'",
        ):
            from_dict(spec_dict)

    def test_foreign_key_table_constraint_missing_columns_raises_error(self):
        spec_dict = self._create_spec_with_table_constraint(
            {
                "type": "foreign_key",
                "name": "fk_test",
                "references": {"table": "other_table"},
            }
        )
        with pytest.raises(
            InvalidConstraintError,
            match="Foreign key table constraint must specify 'columns'",
        ):
            from_dict(spec_dict)

    def test_foreign_key_table_constraint_with_no_name_raises_error(self):
        spec_dict = self._create_spec_with_table_constraint(
            {
                "type": "foreign_key",
                "columns": ["col1"],
                "references": {"table": "other_table"},
            }
        )
        with pytest.raises(
            InvalidConstraintError,
            match="Foreign key table constraint must specify 'name'",
        ):
            from_dict(spec_dict)

    def test_foreign_key_table_constraint_missing_references_raises_error(self):
        spec_dict = self._create_spec_with_table_constraint(
            {"type": "foreign_key", "name": "fk_test", "columns": ["col1"]}
        )
        with pytest.raises(
            InvalidConstraintError,
            match="Foreign key table constraint must specify 'references'",
        ):
            from_dict(spec_dict)

    def test_table_constraint_missing_type_raises_error(self):
        spec_dict = self._create_spec_with_table_constraint(
            {"name": "test_constraint", "columns": ["col1"]}
        )
        with pytest.raises(
            InvalidConstraintError,
            match="Table constraint definition must have a 'type'",
        ):
            from_dict(spec_dict)

    def test_foreign_key_references_missing_table_raises_error(self):
        spec_dict = self._create_spec_with_table_constraint(
            {
                "type": "foreign_key",
                "name": "fk_test",
                "columns": ["col1"],
                "references": {"columns": ["id"]},
            }
        )
        with pytest.raises(
            InvalidConstraintError,
            match="The 'references' of a foreign key must be a dictionary with a 'table' key",
        ):
            from_dict(spec_dict)

    def test_valid_primary_key_table_constraint_parsing(self):
        spec_dict = self._create_spec_with_table_constraint(
            {"type": "primary_key", "name": "pk_test", "columns": ["col1", "col2"]}
        )
        spec = from_dict(spec_dict)

        assert len(spec.table_constraints) == 1
        pk_constraint = spec.table_constraints[0]
        assert isinstance(pk_constraint, PrimaryKeyTableConstraint)
        assert pk_constraint.name == "pk_test"
        assert pk_constraint.columns == ["col1", "col2"]

    def test_valid_foreign_key_table_constraint_parsing(self):
        spec_dict = self._create_spec_with_table_constraint(
            {
                "type": "foreign_key",
                "name": "fk_test",
                "columns": ["col1"],
                "references": {"table": "other_table", "columns": ["id"]},
            }
        )
        spec = from_dict(spec_dict)

        assert len(spec.table_constraints) == 1
        fk_constraint = spec.table_constraints[0]
        assert isinstance(fk_constraint, ForeignKeyTableConstraint)
        assert fk_constraint.name == "fk_test"
        assert fk_constraint.columns == ["col1"]
        assert fk_constraint.references.table == "other_table"
        assert fk_constraint.references.columns == ["id"]


class TestStorageParsing:
    def test_storage_parsing(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "storage": {
                "format": "parquet",
                "location": "/path/to/data",
                "tbl_properties": {"compression": "snappy"},
            },
        }
        spec = from_dict(spec_dict)

        assert spec.storage is not None
        assert spec.storage.format == "parquet"
        assert spec.storage.location == "/path/to/data"
        assert spec.storage.tbl_properties == {"compression": "snappy"}

    def test_storage_with_unknown_key_raises_error(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "storage": {
                "format": "parquet",
                "invalid_key": True,
            },
        }
        with pytest.raises(
            SchemaParsingError,
            match=r"Unknown key\(s\) in storage definition: invalid_key\.",
        ):
            from_dict(spec_dict)

    def test_partitioned_by_missing_column_raises_error(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"transform": "year"}],
        }
        with pytest.raises(
            SchemaParsingError,
            match=r"Missing required key\(s\) in partitioned_by item: column\.",
        ):
            from_dict(spec_dict)

    def test_partitioned_by_unknown_key_raises_error(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"column": "col1", "params": [1]}],
        }
        with pytest.raises(
            SchemaParsingError,
            match=r"Unknown key\(s\) in partitioned_by item: params\.",
        ):
            from_dict(spec_dict)


class TestPartitioningParsing:
    def test_partitioned_by_parsing(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
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
        spec = from_dict(spec_dict)

        assert len(spec.partitioned_by) == 2

        first_partition = spec.partitioned_by[0]
        assert first_partition.column == "col1"
        assert first_partition.transform is None
        assert first_partition.transform_args == []

        second_partition = spec.partitioned_by[1]
        assert second_partition.column == "date_col"
        assert second_partition.transform == "year"
        assert second_partition.transform_args == [2023]


class TestTopLevelSpecValidation:
    def test_schema_with_unknown_top_level_key_raises_error(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
            "foo": "bar",
            "columns": [{"name": "col1", "type": "string"}],
        }
        with pytest.raises(
            SchemaParsingError,
            match=r"Unknown key\(s\) in schema definition: foo\.",
        ):
            from_dict(spec_dict)


class TestValidationMethods:
    def test_validate_columns_duplicate_names(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [
                {"name": "col1", "type": "string"},
                {"name": "col1", "type": "integer"},
            ],
        }
        with pytest.raises(
            SchemaValidationError, match="Duplicate column name found: 'col1'"
        ):
            from_dict(spec_dict)

    def test_validate_partitions_undefined_column(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"column": "undefined_col"}],
        }
        with pytest.raises(
            SchemaValidationError,
            match="Partition column 'undefined_col' must be defined as a column in the schema",
        ):
            from_dict(spec_dict)

    def test_validate_generated_columns_undefined_source(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
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
            SchemaValidationError,
            match="Source column 'undefined_source' for generated column 'generated_col' not found in schema",
        ):
            from_dict(spec_dict)

    def test_validate_table_constraints_undefined_column(self):
        spec_dict = {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "table_constraints": [
                {
                    "type": "primary_key",
                    "name": "pk_test",
                    "columns": ["undefined_col"],
                }
            ],
        }
        with pytest.raises(SchemaValidationError) as excinfo:
            from_dict(spec_dict)

        assert "Column 'undefined_col'" in str(excinfo.value)
        assert "not found in schema" in str(excinfo.value)


class TestFullSpecFromYaml:
    @pytest.fixture(scope="class")
    def spec(self) -> SchemaSpec:
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
        assert len(spec.partitioned_by) == 3
        assert spec.partitioned_by[0].column == "c_string_len"
        assert spec.partitioned_by[1].column == "c_string"
        assert spec.partitioned_by[1].transform == "truncate"
        assert spec.partitioned_by[1].transform_args == [10]
        assert spec.partitioned_by[2].column == "c_date"
        assert spec.partitioned_by[2].transform == "month"

    def test_columns(self, spec: SchemaSpec):
        assert len(spec.columns) == 23

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
    "spec_path, error_type, error_msg",
    [
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_name.yaml",
            SchemaParsingError,
            r"Missing required key\(s\) in schema definition: name\.",
        ),
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_version.yaml",
            SchemaParsingError,
            r"Missing required key\(s\) in schema definition: version\.",
        ),
        (
            INVALID_SPEC_DIR / "missing_required_field" / "missing_columns.yaml",
            SchemaParsingError,
            r"Missing required key\(s\) in schema definition: columns\.",
        ),
        (
            INVALID_SPEC_DIR / "missing_column_field" / "missing_name.yaml",
            SchemaParsingError,
            "'name' is a required field in a column definition",
        ),
        (
            INVALID_SPEC_DIR / "missing_column_field" / "missing_type.yaml",
            SchemaParsingError,
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
            SchemaValidationError,
            "Source column 'non_existent_col' for generated column 'col2' not found in schema.",
        ),
        (
            INVALID_SPEC_DIR / "partitioned_by_undefined_column.yaml",
            SchemaValidationError,
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
        from_string(content)


def test_invalid_yaml_content_raises_error():
    content = "- item1\n- item2"  # A list, not a dictionary
    with pytest.raises(
        SchemaParsingError, match="Loaded YAML content did not parse to a dictionary"
    ):
        from_string(content)


def test_unquoted_null_type_gives_helpful_error():
    """Test that unquoted 'null' in YAML gives a helpful error message."""
    content = """
name: test_schema
version: 1.0.0
columns:
  - name: col1
    type: null  # This will parse as None, not "null"
"""
    with pytest.raises(
        TypeDefinitionError,
        match=r"Use quoted \"null\" or the synonym 'void' instead to specify a void type",
    ):
        from_string(content)


class TestTypeLoading:
    def _create_minimal_spec_with_type(self, type_def: dict) -> dict:
        return {
            "name": "test_schema",
            "version": "1.0.0",
            "columns": [{"name": "test_column", **type_def}],
        }

    @pytest.mark.parametrize(
        "type_def, expected_type, expected_str",
        [
            # String types
            ({"type": "string"}, String(), "string"),
            ({"type": "text"}, String(), "string"),
            ({"type": "varchar"}, String(), "string"),
            ({"type": "char"}, String(), "string"),
            (
                {"type": "string", "params": {"length": 255}},
                String(length=255),
                "string(255)",
            ),
            # Integer types
            ({"type": "integer"}, Integer(bits=32), "integer(bits=32)"),
            ({"type": "int8"}, Integer(bits=8), "integer(bits=8)"),
            ({"type": "tinyint"}, Integer(bits=8), "integer(bits=8)"),
            ({"type": "byte"}, Integer(bits=8), "integer(bits=8)"),
            ({"type": "int16"}, Integer(bits=16), "integer(bits=16)"),
            ({"type": "smallint"}, Integer(bits=16), "integer(bits=16)"),
            ({"type": "short"}, Integer(bits=16), "integer(bits=16)"),
            ({"type": "int32"}, Integer(bits=32), "integer(bits=32)"),
            ({"type": "int"}, Integer(bits=32), "integer(bits=32)"),
            ({"type": "int64"}, Integer(bits=64), "integer(bits=64)"),
            ({"type": "bigint"}, Integer(bits=64), "integer(bits=64)"),
            ({"type": "long"}, Integer(bits=64), "integer(bits=64)"),
            # Float types
            ({"type": "float"}, Float(bits=32), "float(bits=32)"),
            ({"type": "float32"}, Float(bits=32), "float(bits=32)"),
            ({"type": "float64"}, Float(bits=64), "float(bits=64)"),
            ({"type": "double"}, Float(bits=64), "float(bits=64)"),
            # Boolean types
            ({"type": "boolean"}, Boolean(), "boolean"),
            ({"type": "bool"}, Boolean(), "boolean"),
            # Decimal types
            ({"type": "decimal"}, Decimal(), "decimal"),
            ({"type": "numeric"}, Decimal(), "decimal"),
            (
                {"type": "decimal", "params": {"precision": 10, "scale": 2}},
                Decimal(precision=10, scale=2),
                "decimal(10, 2)",
            ),
            # Temporal types
            ({"type": "date"}, Date(), "date"),
            ({"type": "timestamp"}, Timestamp(), "timestamp"),
            ({"type": "datetime"}, Timestamp(), "timestamp"),
            ({"type": "timestamptz"}, TimestampTZ(), "timestamptz"),
            ({"type": "timestamp_tz"}, TimestampTZ(), "timestamptz"),
            ({"type": "timestampltz"}, TimestampLTZ(), "timestampltz"),
            ({"type": "timestamp_ltz"}, TimestampLTZ(), "timestampltz"),
            ({"type": "timestampntz"}, TimestampNTZ(), "timestampntz"),
            ({"type": "timestamp_ntz"}, TimestampNTZ(), "timestampntz"),
            # Binary types
            ({"type": "binary"}, Binary(), "binary"),
            ({"type": "blob"}, Binary(), "binary"),
            ({"type": "bytes"}, Binary(), "binary"),
            # Other types
            ({"type": "uuid"}, UUID(), "uuid"),
            ({"type": "null"}, Void(), "void"),
            ({"type": "void"}, Void(), "void"),
            # ({"type": "json"}, JSON(), "json"),  # Not supported yet
            # Interval types
            (
                {"type": "interval", "params": {"interval_start": "YEAR"}},
                Interval(interval_start=IntervalTimeUnit.YEAR),
                "interval(YEAR)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "MONTH"}},
                Interval(interval_start=IntervalTimeUnit.MONTH),
                "interval(MONTH)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "DAY"}},
                Interval(interval_start=IntervalTimeUnit.DAY),
                "interval(DAY)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "HOUR"}},
                Interval(interval_start=IntervalTimeUnit.HOUR),
                "interval(HOUR)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "MINUTE"}},
                Interval(interval_start=IntervalTimeUnit.MINUTE),
                "interval(MINUTE)",
            ),
            (
                {"type": "interval", "params": {"interval_start": "SECOND"}},
                Interval(interval_start=IntervalTimeUnit.SECOND),
                "interval(SECOND)",
            ),
            (
                {
                    "type": "interval",
                    "params": {"interval_start": "YEAR", "interval_end": "MONTH"},
                },
                Interval(
                    interval_start=IntervalTimeUnit.YEAR,
                    interval_end=IntervalTimeUnit.MONTH,
                ),
                "interval(YEAR to MONTH)",
            ),
            (
                {
                    "type": "interval",
                    "params": {"interval_start": "DAY", "interval_end": "SECOND"},
                },
                Interval(
                    interval_start=IntervalTimeUnit.DAY,
                    interval_end=IntervalTimeUnit.SECOND,
                ),
                "interval(DAY to SECOND)",
            ),
        ],
    )
    def test_simple_type_loading(self, type_def, expected_type, expected_str):
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert column.type == expected_type
        assert str(column.type) == expected_str

    @pytest.mark.parametrize(
        "type_def, expected_element_type",
        [
            # Array types
            ({"type": "array", "element": {"type": "string"}}, String()),
            ({"type": "list", "element": {"type": "int"}}, Integer(bits=32)),
            (
                {
                    "type": "array",
                    "element": {
                        "type": "decimal",
                        "params": {"precision": 10, "scale": 2},
                    },
                },
                Decimal(precision=10, scale=2),
            ),
            # Nested arrays
            (
                {
                    "type": "array",
                    "element": {"type": "array", "element": {"type": "boolean"}},
                },
                Array(element=Boolean()),
            ),
        ],
    )
    def test_array_type_loading(self, type_def, expected_element_type):
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert isinstance(column.type, Array)
        assert column.type.element == expected_element_type

    @pytest.mark.parametrize(
        "type_def, expected_key_type, expected_value_type",
        [
            # Map types
            (
                {"type": "map", "key": {"type": "string"}, "value": {"type": "int"}},
                String(),
                Integer(bits=32),
            ),
            (
                {
                    "type": "dictionary",
                    "key": {"type": "uuid"},
                    "value": {"type": "double"},
                },
                UUID(),
                Float(bits=64),
            ),
            (
                {
                    "type": "map",
                    "key": {"type": "int"},
                    "value": {"type": "array", "element": {"type": "string"}},
                },
                Integer(bits=32),
                Array(element=String()),
            ),
        ],
    )
    def test_map_type_loading(self, type_def, expected_key_type, expected_value_type):
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert isinstance(column.type, Map)
        assert column.type.key == expected_key_type
        assert column.type.value == expected_value_type

    def test_struct_type_loading(self):
        type_def = {
            "type": "struct",
            "fields": [
                {"name": "field1", "type": "string"},
                {"name": "field2", "type": "int"},
                {"name": "field3", "type": "boolean"},
            ],
        }
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert isinstance(column.type, Struct)
        assert len(column.type.fields) == 3

        # Check individual fields
        field1, field2, field3 = column.type.fields
        assert field1.name == "field1"
        assert field1.type == String()
        assert field2.name == "field2"
        assert field2.type == Integer(bits=32)
        assert field3.name == "field3"
        assert field3.type == Boolean()

    def test_nested_struct_type_loading(self):
        type_def = {
            "type": "struct",
            "fields": [
                {"name": "simple_field", "type": "string"},
                {
                    "name": "nested_struct",
                    "type": "struct",
                    "fields": [{"name": "inner_field", "type": "int"}],
                },
            ],
        }
        spec_dict = self._create_minimal_spec_with_type(type_def)
        spec = from_dict(spec_dict)

        column = spec.columns[0]
        assert isinstance(column.type, Struct)
        assert len(column.type.fields) == 2

        simple_field, nested_field = column.type.fields
        assert simple_field.name == "simple_field"
        assert simple_field.type == String()

        assert nested_field.name == "nested_struct"
        assert isinstance(nested_field.type, Struct)
        assert len(nested_field.type.fields) == 1
        assert nested_field.type.fields[0].name == "inner_field"
        assert nested_field.type.fields[0].type == Integer(bits=32)
