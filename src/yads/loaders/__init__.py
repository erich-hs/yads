"""Entry points for loading `SchemaSpec` from various sources.

This module exposes simple functions to construct a `SchemaSpec` from:
- YAML files (`from_yaml`)
- YAML strings (`from_string`)
- Python dictionaries (`from_dict`)
"""

from __future__ import annotations

from typing import Any

import yaml

from ..exceptions import SchemaParsingError
from ..loaders.common import SpecBuilder
from ..spec import SchemaSpec


def from_dict(data: dict[str, Any]) -> SchemaSpec:
    """Load a `SchemaSpec` from a dictionary representation.

    Args:
        data: Parsed specification dictionary.

    Returns:
        A validated immutable `SchemaSpec` instance.

    Example:
        >>> data = {
        ...     "name": "users",
        ...     "version": "1.0.0",
        ...     "columns": [
        ...         {
        ...             "name": "id",
        ...             "type": "integer",
        ...             "constraints": {"not_null": True, "primary_key": True}
        ...         },
        ...         {
        ...             "name": "email",
        ...             "type": "string",
        ...             "params": {"length": 255},
        ...             "constraints": {"not_null": True}
        ...         }
        ...     ]
        ... }
        >>> spec = from_dict(data)
        >>> print(f"Loaded schema: {spec.name} v{spec.version}")
        Loaded schema: users v1.0.0
    """

    return SpecBuilder(data).build()


def from_string(content: str) -> SchemaSpec:
    """Load a schema specification from a YAML string.

    Parses YAML content and converts it into a validated SchemaSpec object.

    Args:
        content: YAML string containing the schema specification.

    Returns:
        A validated immutable SchemaSpec object.

    Raises:
        SchemaParsingError: If the YAML content is invalid or doesn't parse
                          to a dictionary.

    Example:
        >>> yaml_content = '''
        ... name: products
        ... version: "2.1.0"
        ... description: Product catalog table
        ... columns:
        ...   - name: product_id
        ...     type: string
        ...     constraints:
        ...       not_null: true
        ...       primary_key: true
        ...   - name: name
        ...     type: string
        ...     params:
        ...       length: 200
        ... '''
        >>> spec = from_string(yaml_content)
        >>> print(f"Loaded {len(spec.columns)} columns")
        Loaded 2 columns
    """

    data = yaml.safe_load(content)
    if not isinstance(data, dict):
        raise SchemaParsingError("Loaded YAML content did not parse to a dictionary.")
    return from_dict(data)


def from_yaml(path: str) -> SchemaSpec:
    """Load a schema specification from a YAML file.

    Reads and parses a YAML file containing a schema specification.

    Args:
        path: Path to the YAML file containing the schema specification.

    Returns:
        A validated immutable SchemaSpec object.

    Example:
        >>> # Load from file
        >>> spec = from_yaml("schemas/users.yaml")
        >>> print(f"Loaded schema: {spec.name}")
        Loaded schema: users
    """

    with open(path) as f:
        content = f.read()
    return from_string(content)
