import pytest
from yads import spec, types
from yads.constraints import NotNullConstraint, PrimaryKeyTableConstraint
from yads.exceptions import SpecValidationError


class TestTransformedColumnReference:
    def test_instantiation_and_str_with_transform(self):
        tc = spec.TransformedColumnReference(column="a", transform="identity")
        assert tc.column == "a"
        assert tc.transform == "identity"
        assert tc.transform_args == []
        assert str(tc) == "identity(a)"

    def test_instantiation_with_args_and_str(self):
        tc = spec.TransformedColumnReference(
            column="a", transform="bucket", transform_args=[16]
        )
        assert tc.column == "a"
        assert tc.transform == "bucket"
        assert tc.transform_args == [16]
        assert str(tc) == "bucket(a, 16)"

    def test_simple_column(self):
        tc = spec.TransformedColumnReference(column="dt")
        assert tc.column == "dt"
        assert tc.transform is None
        assert tc.transform_args == []
        assert str(tc) == "dt"

    def test_transformed_column(self):
        tc = spec.TransformedColumnReference(column="ts", transform="hour")
        assert str(tc) == "hour(ts)"

    def test_transformed_column_with_args(self):
        tc = spec.TransformedColumnReference(
            column="id", transform="bucket", transform_args=[4]
        )
        assert str(tc) == "bucket(id, 4)"


class TestField:
    def test_simple_field(self):
        field = spec.Field(name="username", type=types.String())
        assert field.name == "username"
        assert field.type == types.String()
        assert field.description is None
        assert field.metadata == {}
        assert not field.has_metadata
        assert str(field) == "username: string"

    def test_field_with_metadata(self):
        field = spec.Field(
            name="username",
            type=types.String(length=50),
            description="User's login name",
            metadata={"source": "ldap"},
        )
        assert field.name == "username"
        assert field.type == types.String(length=50)
        assert field.description == "User's login name"
        assert field.metadata == {"source": "ldap"}
        assert field.has_metadata
        expected_str = (
            "username: string(length=50)(\n"
            '  description="User\'s login name",\n'
            "  metadata={source='ldap'}\n"
            ")"
        )
        assert str(field) == expected_str


class TestColumn:
    def test_simple_column(self):
        column = spec.Column(name="username", type=types.String())
        assert column.name == "username"
        assert column.type == types.String()
        assert column.description is None
        assert column.constraints == []
        assert column.metadata == {}
        assert column.generated_as is None
        assert not column.is_generated
        assert column.is_nullable
        assert not column.has_constraints
        assert column.constraint_types == set()
        assert str(column) == "username: string"

    def test_full_column(self):
        constraint = NotNullConstraint()
        gen_clause = spec.TransformedColumnReference(
            column="other_col", transform="identity"
        )

        column = spec.Column(
            name="username",
            type=types.String(length=50),
            description="User's login name",
            constraints=[constraint],
            metadata={"source": "ldap"},
            generated_as=gen_clause,
        )
        assert column.name == "username"
        assert column.type == types.String(length=50)
        assert column.description == "User's login name"
        assert column.constraints == [constraint]
        assert column.metadata == {"source": "ldap"}
        assert column.generated_as == gen_clause
        assert column.is_generated
        assert not column.is_nullable  # Has NotNullConstraint
        assert column.has_constraints
        assert column.constraint_types == {NotNullConstraint}

        expected_str = (
            "username: string(length=50)(\n"
            '  description="User\'s login name",\n'
            "  constraints=[NotNullConstraint()],\n"
            "  metadata={source='ldap'},\n"
            "  generated_as=identity(other_col)\n"
            ")"
        )
        assert str(column) == expected_str


class TestStorage:
    def test_empty_storage(self):
        storage = spec.Storage()
        assert str(storage) == "Storage(\n\n)"

    def test_full_storage(self):
        storage = spec.Storage(
            format="parquet",
            location="/path/to/table",
            tbl_properties={"compression": "snappy"},
        )
        expected_str = (
            "Storage(\n"
            "  format='parquet',\n"
            "  location='/path/to/table',\n"
            "  tbl_properties={\n"
            "    compression='snappy'\n"
            "  }\n"
            ")"
        )
        assert str(storage) == expected_str


class TestYadsSpec:
    @pytest.fixture
    def minimal_spec(self) -> spec.YadsSpec:
        return spec.YadsSpec(
            name="my_table",
            version="1.0.0",
            columns=[spec.Column(name="id", type=types.Integer())],
        )

    def test_minimal_spec_instantiation(self, minimal_spec):
        assert minimal_spec.name == "my_table"
        assert minimal_spec.version == "1.0.0"
        assert len(minimal_spec.columns) == 1
        assert minimal_spec.description is None
        assert not minimal_spec.external
        assert minimal_spec.storage is None
        assert minimal_spec.partitioned_by == []
        assert minimal_spec.table_constraints == []
        assert minimal_spec.metadata == {}

    def test_spec_properties(self):
        gen_clause = spec.TransformedColumnReference(
            column="raw_id", transform="identity"
        )
        spec_instance = spec.YadsSpec(
            name="users",
            version="1.0",
            columns=[
                spec.Column(name="id", type=types.Integer()),
                spec.Column(name="username", type=types.String()),
                spec.Column(name="raw_id", type=types.Integer()),
                spec.Column(name="gen_id", type=types.Integer(), generated_as=gen_clause),
            ],
            partitioned_by=[spec.TransformedColumnReference(column="username")],
        )
        assert spec_instance.column_names == {"id", "username", "gen_id", "raw_id"}
        assert spec_instance.partition_column_names == {"username"}
        assert spec_instance.generated_columns == {"gen_id": "raw_id"}

    def test_str_representation_minimal(self, minimal_spec):
        expected_str = (
            "spec my_table(version='1.0.0')(\n  columns=[\n    id: integer\n  ]\n)"
        )
        assert str(minimal_spec) == expected_str

    def test_str_representation_full(self):
        pk_constraint = PrimaryKeyTableConstraint(columns=["id"])
        spec_instance = spec.YadsSpec(
            name="my_table",
            version="1.0",
            description="A test table.",
            external=True,
            columns=[
                spec.Column(
                    name="id",
                    type=types.Integer(),
                    constraints=[NotNullConstraint()],
                ),
                spec.Column(name="data", type=types.String()),
            ],
            storage=spec.Storage(format="parquet", tbl_properties={"k": "v"}),
            partitioned_by=[spec.TransformedColumnReference(column="id")],
            table_constraints=[pk_constraint],
            metadata={"owner": "tester"},
        )
        expected_str = """spec my_table(version='1.0')(
  description='A test table.'
  metadata={
    owner='tester'
  }
  external=True
  storage=Storage(
    format='parquet',
    tbl_properties={
      k='v'
    }
  )
  partitioned_by=[id]
  table_constraints=[
    PrimaryKeyTableConstraint(
      columns=[
        'id'
      ]
    )
  ]
  columns=[
    id: integer(
      constraints=[NotNullConstraint()]
    )
    data: string
  ]
)"""
        assert str(spec_instance) == expected_str


class TestYadsSpecValidation:
    @pytest.fixture
    def base_spec_kwargs(self) -> dict:
        """Provides a base set of kwargs for creating a `YadsSpec`."""
        return {
            "name": "test_table",
            "version": "1.0",
            "columns": [spec.Column(name="id", type=types.Integer())],
        }

    def test_duplicate_column_name(self, base_spec_kwargs):
        """Test that duplicate column names raise a ValueError."""
        base_spec_kwargs["columns"].append(spec.Column(name="id", type=types.String()))
        with pytest.raises(
            SpecValidationError, match="Duplicate column name found: 'id'"
        ):
            spec.YadsSpec(**base_spec_kwargs)

    def test_partition_column_must_be_in_columns(self, base_spec_kwargs):
        """Test that a partition column must also be defined in 'columns'."""
        base_spec_kwargs["partitioned_by"] = [
            spec.TransformedColumnReference(column="non_existent")
        ]
        with pytest.raises(
            SpecValidationError,
            match="Partition column 'non_existent' must be defined as a column in the schema",
        ):
            spec.YadsSpec(**base_spec_kwargs)

    def test_generated_column_source_missing(self, base_spec_kwargs):
        """Test that the source for a generated column must exist."""
        gen_clause = spec.TransformedColumnReference(
            column="non_existent", transform="identity"
        )
        base_spec_kwargs["columns"].append(
            spec.Column(name="gen_col", type=types.Integer(), generated_as=gen_clause)
        )
        with pytest.raises(
            SpecValidationError,
            match="Source column 'non_existent' for generated column 'gen_col'",
        ):
            spec.YadsSpec(**base_spec_kwargs)

    def test_table_constraint_column_missing(self, base_spec_kwargs):
        """Test that a column in a table constraint must exist."""
        pk_constraint = PrimaryKeyTableConstraint(columns=["pk", "id"])
        base_spec_kwargs["table_constraints"] = [pk_constraint]
        with pytest.raises(SpecValidationError, match="Column 'pk' in constraint"):
            spec.YadsSpec(**base_spec_kwargs)
