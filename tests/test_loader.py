import pytest

from yads.loader import from_dict, from_string, from_yaml
from yads.spec import Field, SchemaSpec
from yads.types import Array, Decimal, Integer, Map, String, Struct, UUID
from yads.constraints import (
    DefaultConstraint,
    NotNullConstraint,
    PrimaryKeyConstraint,
    PrimaryKeyTableConstraint,
    ForeignKeyConstraint,
    ForeignKeyTableConstraint,
    Reference,
)


def test_from_string_valid_basic_schema():
    """Tests parsing a minimal valid schema from a YAML string."""
    yaml_content = """
name: "test.schema"
version: "1.0.0"
columns:
  - name: "id"
    type: "integer"
    constraints:
      not_null: true
  - name: "name"
    type: "string"
"""
    spec = from_string(yaml_content)

    assert isinstance(spec, SchemaSpec)
    assert spec.name == "test.schema"
    assert spec.version == "1.0.0"
    assert spec.description is None
    assert spec.metadata == {}
    assert len(spec.columns) == 2

    id_col = spec.columns[0]
    assert isinstance(id_col, Field)
    assert id_col.name == "id"
    assert isinstance(id_col.type, Integer)
    assert len(id_col.constraints) == 1
    assert isinstance(id_col.constraints[0], NotNullConstraint)

    name_col = spec.columns[1]
    assert isinstance(name_col, Field)
    assert name_col.name == "name"
    assert isinstance(name_col.type, String)
    assert len(name_col.constraints) == 0  # Nullable by default


def test_from_string_with_all_fields():
    """Tests parsing a schema with all optional fields present."""
    yaml_content = """
name: "full.schema"
version: "2.1.0"
description: "A full schema with all features."
metadata:
  owner: "data-team"
  sensitive: false
columns:
  - name: "user_id"
    type: "uuid"
    description: "User identifier"
    constraints:
      not_null: true
    metadata:
      pii: true
  - name: "score"
    type: "decimal"
    params:
      precision: 5
      scale: 2
"""
    spec = from_string(yaml_content)

    assert spec.name == "full.schema"
    assert spec.version == "2.1.0"
    assert spec.description == "A full schema with all features."
    assert spec.metadata == {"owner": "data-team", "sensitive": False}

    user_id_col = spec.columns[0]
    assert user_id_col.name == "user_id"
    assert isinstance(user_id_col.type, UUID)
    assert user_id_col.description == "User identifier"
    assert any(isinstance(c, NotNullConstraint) for c in user_id_col.constraints)
    assert user_id_col.metadata == {"pii": True}

    score_col = spec.columns[1]
    assert score_col.name == "score"
    assert isinstance(score_col.type, Decimal)
    assert score_col.type.precision == 5
    assert score_col.type.scale == 2


def test_from_string_with_nested_types():
    """Tests parsing a schema with nested types like Array and Struct."""
    yaml_content = """
name: "nested.schema"
version: "1.0.0"
columns:
  - name: "items"
    type: "array"
    element:
      type: "struct"
      fields:
        - name: "product_id"
          type: "integer"
        - name: "name"
          type: "string"
          params:
            length: 100
"""
    spec = from_string(yaml_content)
    assert len(spec.columns) == 1
    items_col = spec.columns[0]
    assert items_col.name == "items"
    assert isinstance(items_col.type, Array)

    element_type = items_col.type.element
    assert isinstance(element_type, Struct)

    assert len(element_type.fields) == 2
    product_id_field, name_field = element_type.fields

    assert isinstance(product_id_field, Field)
    assert product_id_field.name == "product_id"
    assert isinstance(product_id_field.type, Integer)

    assert name_field.name == "name"
    assert isinstance(name_field.type, String)
    assert name_field.type.length == 100


def test_from_string_with_map_type():
    """Tests parsing a schema with a Map type."""
    yaml_content = """
name: "map.schema"
version: "1.0.0"
columns:
  - name: "metadata"
    type: "map"
    key:
      type: "string"
      params:
        length: 50
    value:
      type: "integer"
"""
    spec = from_string(yaml_content)
    assert len(spec.columns) == 1
    metadata_col = spec.columns[0]
    assert metadata_col.name == "metadata"
    assert isinstance(metadata_col.type, Map)
    assert isinstance(metadata_col.type.key, String)
    assert metadata_col.type.key.length == 50
    assert isinstance(metadata_col.type.value, Integer)


@pytest.mark.parametrize("missing_field", ["name", "version", "columns"])
def test_from_string_missing_required_field_raises_error(missing_field):
    """Tests that a missing top-level required field raises a ValueError."""
    yaml_templates = {
        "name": """
version: "1.0.0"
columns:
  - name: "col1"
    type: "string"
""",
        "version": """
name: "test.schema"
columns:
  - name: "col1"
    type: "string"
""",
        "columns": """
name: "test.schema"
version: "1.0.0"
""",
    }
    yaml_content = yaml_templates[missing_field]

    with pytest.raises(ValueError, match=f"'{missing_field}' is a required field"):
        from_string(yaml_content)


@pytest.mark.parametrize("missing_col_field", ["name", "type"])
def test_from_string_missing_required_column_field_raises_error(missing_col_field):
    """Tests that a missing required field in a column definition raises a ValueError."""
    yaml_templates = {
        "name": """
name: "test.schema"
version: "1.0.0"
columns:
  - type: "string"
""",
        "type": """
name: "test.schema"
version: "1.0.0"
columns:
  - name: "col1"
""",
    }
    yaml_content = yaml_templates[missing_col_field]

    with pytest.raises(
        ValueError,
        match=f"'{missing_col_field}' is a required field in a column definition",
    ):
        from_string(yaml_content)


def test_from_string_unknown_type_raises_error():
    """Tests that an unrecognized type name raises a ValueError."""
    yaml_content = """
name: "test.schema"
version: "1.0.0"
columns:
  - name: "col1"
    type: "invalid_type"
"""
    with pytest.raises(ValueError, match="Unknown type: 'invalid_type'"):
        from_string(yaml_content)


@pytest.mark.parametrize(
    "yaml_content, error_msg",
    [
        (
            """
name: "test.schema"
version: "1.0.0"
columns:
  - name: "col1"
    type: "array"
""",
            "Array type definition must include 'element'",
        ),
        (
            """
name: "test.schema"
version: "1.0.0"
columns:
  - name: "col1"
    type: "struct"
""",
            "Struct type definition must include 'fields'",
        ),
        (
            """
name: "test.schema"
version: "1.0.0"
columns:
  - name: "col1"
    type: "map"
    value:
      type: "string"
""",
            "Map type definition must include 'key' and 'value'",
        ),
        (
            """
name: "test.schema"
version: "1.0.0"
columns:
  - name: "col1"
    type: "map"
    key:
      type: "string"
""",
            "Map type definition must include 'key' and 'value'",
        ),
    ],
)
def test_from_string_invalid_complex_type_def_raises_error(yaml_content, error_msg):
    """Tests that an invalid complex type definition raises a ValueError."""
    with pytest.raises(ValueError, match=error_msg):
        from_string(yaml_content)


def test_from_string_with_column_constraints():
    """Tests parsing a schema with various column constraints."""
    yaml_content = """
name: "constraints.schema"
version: "1.0.0"
columns:
  - name: "id"
    type: "uuid"
    constraints:
      primary_key: true
  - name: "status"
    type: "string"
    constraints:
      default: "pending"
"""
    spec = from_string(yaml_content)

    assert len(spec.columns) == 2

    id_col = spec.columns[0]
    assert id_col.name == "id"
    assert isinstance(id_col.type, UUID)
    assert len(id_col.constraints) == 1
    assert isinstance(id_col.constraints[0], PrimaryKeyConstraint)

    status_col = spec.columns[1]
    assert status_col.name == "status"
    assert isinstance(status_col.type, String)
    assert len(status_col.constraints) == 1
    assert isinstance(status_col.constraints[0], DefaultConstraint)
    assert status_col.constraints[0].value == "pending"


def test_from_string_with_column_foreign_key_constraint():
    """Tests parsing a schema with a column-level foreign key constraint."""
    yaml_content = """
name: "fk.schema"
version: "1.0.0"
columns:
  - name: "user_id"
    type: "uuid"
    constraints:
      foreign_key:
        name: "fk_user_id"
        references:
          table: "users"
          columns: ["id"]
"""
    spec = from_string(yaml_content)
    user_id_col = spec.columns[0]
    assert len(user_id_col.constraints) == 1
    fk_constraint = user_id_col.constraints[0]
    assert isinstance(fk_constraint, ForeignKeyConstraint)
    assert fk_constraint.name == "fk_user_id"
    assert fk_constraint.references == Reference(table="users", columns=["id"])


def test_from_string_with_table_foreign_key_constraint():
    """Tests parsing a schema with a table-level foreign key constraint."""
    yaml_content = """
name: "fk.schema"
version: "1.0.0"
table_constraints:
  - type: "foreign_key"
    name: "fk_order_items"
    columns: ["order_id", "product_id"]
    references:
      table: "order_items"
      columns: ["order_id", "product_id"]
columns:
  - name: "order_id"
    type: "uuid"
  - name: "product_id"
    type: "integer"
"""
    spec = from_string(yaml_content)
    assert len(spec.table_constraints) == 1
    fk_constraint = spec.table_constraints[0]
    assert isinstance(fk_constraint, ForeignKeyTableConstraint)
    assert fk_constraint.name == "fk_order_items"
    assert fk_constraint.columns == ["order_id", "product_id"]
    assert fk_constraint.references == Reference(
        table="order_items", columns=["order_id", "product_id"]
    )


def test_from_yaml_loads_from_file(tmp_path):
    """Tests loading a schema from a YAML file."""
    schema_file = tmp_path / "test_schema.yml"
    yaml_content = """
name: "file.schema"
version: "1.0.0"
columns:
  - name: "id"
    type: "integer"
"""
    schema_file.write_text(yaml_content)

    spec = from_yaml(str(schema_file))

    assert isinstance(spec, SchemaSpec)
    assert spec.name == "file.schema"
    assert len(spec.columns) == 1
    assert isinstance(spec.columns[0].type, Integer)


def test_from_dict_valid_basic_schema():
    """Tests loading a schema from a Python dictionary."""
    schema_dict = {
        "name": "dict.schema",
        "version": "1.0.0",
        "columns": [
            {"name": "id", "type": "integer", "constraints": {"not_null": True}}
        ],
    }
    spec = from_dict(schema_dict)

    assert isinstance(spec, SchemaSpec)
    assert spec.name == "dict.schema"
    assert len(spec.columns) == 1
    id_col = spec.columns[0]
    assert id_col.name == "id"
    assert isinstance(id_col.type, Integer)
    assert any(isinstance(c, NotNullConstraint) for c in id_col.constraints)


def test_from_string_with_table_constraints():
    """Tests parsing a YAML string with table constraints."""
    yaml_content = """
    name: "test_schema"
    version: "1.0"
    description: "A test schema with table constraints."
    table_constraints:
      - type: "primary_key"
        name: "test_pk"
        columns:
          - "id"
          - "name"
    columns:
      - name: "id"
        type: "integer"
      - name: "name"
        type: "string"
    """
    spec = from_string(yaml_content)
    assert len(spec.table_constraints) == 1
    pk_constraint = spec.table_constraints[0]
    assert isinstance(pk_constraint, PrimaryKeyTableConstraint)
    assert pk_constraint.name == "test_pk"
    assert pk_constraint.columns == ["id", "name"]


def test_duplicate_pk_constraint_warning():
    """Tests that a warning is issued for duplicate primary key constraints."""
    yaml_content = """
    name: "duplicate.pk.schema"
    version: "1.0.0"
    table_constraints:
      - type: "primary_key"
        name: "pk_table"
        columns:
          - "id"
    columns:
      - name: "id"
        type: "integer"
        constraints:
          primary_key: true
      - name: "name"
        type: "string"
    """
    with pytest.warns(
        UserWarning, match=r"Columns \['id'\] have a PrimaryKeyConstraint"
    ):
        from_string(yaml_content)


def test_undefined_columns_in_table_constraint_warning():
    """Tests that a warning is issued for table constraints on undefined columns."""
    yaml_content = """
    name: "undefined.columns.schema"
    version: "1.0.0"
    table_constraints:
      - type: "primary_key"
        name: "pk_table"
        columns:
          - "id"
          - "non_existent_col"
    columns:
      - name: "id"
        type: "integer"
    """
    with pytest.warns(
        UserWarning,
        match=r"Table constraint 'pk_table' references undefined columns: \['non_existent_col'\]",
    ):
        from_string(yaml_content)


def test_no_duplicate_pk_constraint_no_warning(recwarn):
    """Tests that no warning is issued when there are no duplicate constraints."""
    yaml_content = """
    name: "no.duplicate.pk.schema"
    version: "1.0.0"
    table_constraints:
      - type: "primary_key"
        name: "pk_table"
        columns:
          - "id"
    columns:
      - name: "id"
        type: "integer"
        constraints:
          not_null: true
      - name: "name"
        type: "string"
    """
    from_string(yaml_content)
    assert len(recwarn) == 0
