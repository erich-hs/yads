import pytest
import yaml
from pathlib import Path

from yads.constraints import NotNullConstraint, PrimaryKeyTableConstraint
from yads.exceptions import (
    InvalidConstraintError,
    SpecParsingError,
    SpecValidationError,
    TypeDefinitionError,
    UnknownConstraintError,
    UnknownTypeError,
)
from yads.loaders import from_dict, from_yaml_path, from_yaml_string
from yads.spec import YadsSpec
from yads.constraints import DefaultConstraint, ForeignKeyTableConstraint
from yads.types import (
    String,
    Integer,
    Float,
    Decimal,
    Boolean,
    Binary,
    Date,
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

# Define paths to the fixture directories
VALID_SPEC_DIR = Path(__file__).parent.parent / "fixtures" / "spec" / "valid"
INVALID_SPEC_DIR = Path(__file__).parent.parent / "fixtures" / "spec" / "invalid"

# ======================================================================
# Spec builder tests
# Scope: parsing (dict/yaml), constraint parsing, generation, storage,
# partitioning, top-level parsing errors, semantic validation, full spec,
# invalid spec matrix, and type loading.
# ======================================================================

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


# %% Column constraint parsing
class TestConstraintParsing:
    def _create_minimal_spec_with_constraint(self, constraint_def: dict) -> dict:
        return {
            "name": "test_spec",
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

    def test_constraints_attribute_as_list_raises_error(self):
        spec_dict = self._create_minimal_spec_with_constraint([{"primary_key": True}])
        with pytest.raises(
            SpecParsingError,
            match=r"The 'constraints' attribute of a column must be a dictionary",
        ):
            from_dict(spec_dict)


# %% Generated column clause parsing
class TestGenerationClauseParsing:
    def _create_spec_with_generated_column(self, generated_as_def: dict | None) -> dict:
        column_def: dict = {
            "name": "generated_col",
            "type": "string",
        }
        if generated_as_def is not None:
            column_def["generated_as"] = generated_as_def

        return {
            "name": "test_spec",
            "version": "1.0.0",
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
            from_dict(spec_dict)

    def test_generation_clause_missing_transform_raises_error(self):
        spec_dict = self._create_spec_with_generated_column({"column": "source_col"})
        with pytest.raises(
            SpecParsingError,
            match=r"Missing required key\(s\) in generation clause: transform\.",
        ):
            from_dict(spec_dict)

    def test_generation_clause_empty_transform_raises_error(self):
        spec_dict = self._create_spec_with_generated_column(
            {"column": "source_col", "transform": ""}
        )
        with pytest.raises(
            SpecParsingError,
            match="'transform' cannot be empty in a generation clause",
        ):
            from_dict(spec_dict)

    def test_generation_clause_unknown_key_raises_error(self):
        spec_dict = self._create_spec_with_generated_column(
            {"column": "source_col", "transform": "upper", "params": [1]}
        )
        with pytest.raises(
            SpecParsingError,
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


# %% Table constraint parsing
class TestTableConstraintParsing:
    def _create_spec_with_table_constraint(self, constraint_def: dict) -> dict:
        return {
            "name": "test_spec",
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


# %% Storage parsing
class TestStorageParsing:
    def test_storage_parsing(self):
        spec_dict = {
            "name": "test_spec",
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
            "name": "test_spec",
            "version": "1.0.0",
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
            from_dict(spec_dict)

    def test_partitioned_by_missing_column_raises_error(self):
        spec_dict = {
            "name": "test_spec",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"transform": "year"}],
        }
        with pytest.raises(
            SpecParsingError,
            match=r"Missing required key\(s\) in partitioned_by item: column\.",
        ):
            from_dict(spec_dict)

    def test_partitioned_by_unknown_key_raises_error(self):
        spec_dict = {
            "name": "test_spec",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"column": "col1", "params": [1]}],
        }
        with pytest.raises(
            SpecParsingError,
            match=r"Unknown key\(s\) in partitioned_by item: params\.",
        ):
            from_dict(spec_dict)


# %% Partitioning parsing
class TestPartitioningParsing:
    def test_partitioned_by_parsing(self):
        spec_dict = {
            "name": "test_spec",
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


# %% Top-level spec parsing errors
class TestTopLevelSpecParsingErrors:
    def test_spec_with_unknown_top_level_key_raises_error(self):
        spec_dict = {
            "name": "test_spec",
            "version": "1.0.0",
            "foo": "bar",
            "columns": [{"name": "col1", "type": "string"}],
        }
        with pytest.raises(
            SpecParsingError,
            match=r"Unknown key\(s\) in spec definition: foo\.",
        ):
            from_dict(spec_dict)


# %% Semantic validation
class TestSemanticValidation:
    def test_validate_columns_duplicate_names(self):
        spec_dict = {
            "name": "test_spec",
            "version": "1.0.0",
            "columns": [
                {"name": "col1", "type": "string"},
                {"name": "col1", "type": "integer"},
            ],
        }
        with pytest.raises(
            SpecValidationError, match="Duplicate column name found: 'col1'"
        ):
            from_dict(spec_dict)

    def test_validate_partitions_undefined_column(self):
        spec_dict = {
            "name": "test_spec",
            "version": "1.0.0",
            "columns": [{"name": "col1", "type": "string"}],
            "partitioned_by": [{"column": "undefined_col"}],
        }
        with pytest.raises(
            SpecValidationError,
            match="Partition column 'undefined_col' must be defined as a column in the schema",
        ):
            from_dict(spec_dict)

    def test_validate_generated_columns_undefined_source(self):
        spec_dict = {
            "name": "test_spec",
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
            SpecValidationError,
            match="Source column 'undefined_source' for generated column 'generated_col' not found in schema",
        ):
            from_dict(spec_dict)

    def test_validate_table_constraints_undefined_column(self):
        spec_dict = {
            "name": "test_spec",
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
        with pytest.raises(SpecValidationError) as excinfo:
            from_dict(spec_dict)

        assert "Column 'undefined_col'" in str(excinfo.value)
        assert "not found in schema" in str(excinfo.value)


# %% Full spec building (integration)
class TestFullSpecBuilding:
    @pytest.fixture(scope="class")
    def spec(self) -> YadsSpec:
        return from_yaml_path(VALID_SPEC_DIR / "full_spec.yaml")

    def test_top_level_attributes(self, spec: YadsSpec):
        assert spec.name == "catalog.db.full_spec"
        assert spec.version == "2.1.0"
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
        assert len(spec.columns) == 31

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
            INVALID_SPEC_DIR / "missing_required_field" / "missing_version.yaml",
            SpecParsingError,
            r"Missing required key\(s\) in spec definition: version\.",
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


# %% Type loading
def test_unquoted_null_type_gives_helpful_error():
    content = """
name: test_spec
version: 1.0.0
columns:
  - name: col1
    type: null  # This will parse as None, not "null"
"""
    with pytest.raises(
        TypeDefinitionError,
        match=r"Use quoted \"null\" or the synonym 'void' instead to specify a void type",
    ):
        from_yaml_string(content)


class TestTypeLoading:
    def _create_minimal_spec_with_type(self, type_def: dict) -> dict:
        return {
            "name": "test_spec",
            "version": "1.0.0",
            "columns": [{"name": "test_column", **type_def}],
        }

    @pytest.mark.parametrize(
        "type_def, expected_type, expected_str",
        [
            # String types
            ({"type": "string"}, String(), "string"),
            (
                {"type": "string", "params": {"length": 255}},
                String(length=255),
                "string(255)",
            ),
            # Integer types
            ({"type": "int8"}, Integer(bits=8), "integer(bits=8)"),
            ({"type": "int16"}, Integer(bits=16), "integer(bits=16)"),
            ({"type": "int32"}, Integer(bits=32), "integer(bits=32)"),
            ({"type": "int64"}, Integer(bits=64), "integer(bits=64)"),
            # Float types
            ({"type": "float32"}, Float(bits=32), "float(bits=32)"),
            ({"type": "float64"}, Float(bits=64), "float(bits=64)"),
            # Decimal types
            ({"type": "decimal"}, Decimal(), "decimal"),
            (
                {"type": "decimal", "params": {"precision": 10, "scale": 2}},
                Decimal(precision=10, scale=2),
                "decimal(10, 2)",
            ),
            # Boolean types
            ({"type": "boolean"}, Boolean(), "boolean"),
            # Binary types
            ({"type": "binary"}, Binary(), "binary"),
            ({"type": "blob"}, Binary(), "binary"),
            ({"type": "bytes"}, Binary(), "binary"),
            # Temporal types
            ({"type": "date"}, Date(), "date"),
            ({"type": "time"}, Time(), "time(ns)"),
            ({"type": "time", "params": {"unit": "s"}}, Time(unit="s"), "time(s)"),
            ({"type": "timestamp"}, Timestamp(), "timestamp(ns)"),
            (
                {"type": "timestamp", "params": {"unit": "s"}},
                Timestamp(unit="s"),
                "timestamp(s)",
            ),
            ({"type": "timestamptz"}, TimestampTZ(), "timestamptz(ns)"),
            (
                {"type": "timestamptz", "params": {"unit": "s"}},
                TimestampTZ(unit="s"),
                "timestamptz(s)",
            ),
            ({"type": "timestamp_tz"}, TimestampTZ(), "timestamptz(ns)"),
            ({"type": "timestampltz"}, TimestampLTZ(), "timestampltz(ns)"),
            (
                {"type": "timestampltz", "params": {"unit": "s"}},
                TimestampLTZ(unit="s"),
                "timestampltz(s)",
            ),
            ({"type": "timestamp_ltz"}, TimestampLTZ(), "timestampltz(ns)"),
            ({"type": "timestampntz"}, TimestampNTZ(), "timestampntz(ns)"),
            (
                {"type": "timestampntz", "params": {"unit": "s"}},
                TimestampNTZ(unit="s"),
                "timestampntz(s)",
            ),
            ({"type": "timestamp_ntz"}, TimestampNTZ(), "timestampntz(ns)"),
            ({"type": "duration"}, Duration(), "duration(ns)"),
            (
                {"type": "duration", "params": {"unit": "s"}},
                Duration(unit="s"),
                "duration(s)",
            ),
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
            # Complex types
            ({"type": "json"}, JSON(), "json"),
            # Other complex types have dedicated tests below
            # Spatial types
            ({"type": "geometry"}, Geometry(), "geometry"),
            ({"type": "geography"}, Geography(), "geography"),
            (
                {"type": "geometry", "params": {"srid": 4326}},
                Geometry(srid=4326),
                "geometry(4326)",
            ),
            (
                {"type": "geography", "params": {"srid": 4326}},
                Geography(srid=4326),
                "geography(4326)",
            ),
            # Other types
            ({"type": "uuid"}, UUID(), "uuid"),
            ({"type": "void"}, Void(), "void"),
            ({"type": "variant"}, Variant(), "variant"),
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
