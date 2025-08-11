"""Entry points for loading `YadsSpec` from various sources.

This module exposes simple functions to construct a `YadsSpec` from:
- YAML files (`from_yaml`)
- YAML strings (`from_string`)
- Python dictionaries (`from_dict`)
"""

from __future__ import annotations

from typing import Any

import yaml

from ..exceptions import SpecParsingError
from ..spec import YadsSpec
from .common import SpecBuilder


def from_dict(data: dict[str, Any]) -> YadsSpec:
    """Load a `YadsSpec` from a dictionary representation.

    Args:
        data: Parsed specification dictionary.

    Returns:
        A validated immutable `YadsSpec` instance.

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
        >>> print(f"Loaded spec: {spec.name} v{spec.version}")
        Loaded spec: users v1.0.0
    """

    return SpecBuilder(data).build()


def from_string(content: str) -> YadsSpec:
    """Load a spec from a YAML string.

    Parses YAML content and converts it into a validated `YadsSpec` object.

    Args:
        content: YAML string containing the spec.

    Returns:
        A validated immutable `YadsSpec` object.

    Raises:
        SpecParsingError: If the YAML content is invalid or doesn't parse
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
        raise SpecParsingError("Loaded YAML content did not parse to a dictionary.")
    return from_dict(data)


def from_yaml(path: str) -> YadsSpec:
    """Load a spec from a YAML file.

    Reads and parses a YAML file containing a spec.

    Args:
        path: Path to the YAML file containing the spec.

    Returns:
        A validated immutable `YadsSpec` object.

    Example:
        >>> # Load from file
        >>> spec = from_yaml("specs/users.yaml")
        >>> print(f"Loaded spec: {spec.name}")
        Loaded spec: users
    """

    with open(path) as f:
        content = f.read()
    return from_string(content)
