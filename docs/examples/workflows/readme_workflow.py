"""Executable README workflow snippets."""

from __future__ import annotations

import yads
from io import StringIO
from ..base import ExampleBlockRequest, ExampleDefinition


_SPEC_YAML = """name: catalog.crm.customers
version: 1
yads_spec_version: 0.0.2
columns:
  - name: id
    type: bigint
    constraints:
      not_null: true
  - name: email
    type: string
  - name: created_at
    type: timestamptz
  - name: spend
    type: decimal
    params:
      precision: 10
      scale: 2
  - name: tags
    type: array
    element:
      type: string
"""

spec_with_file_path = "# registry/specs/customers.yaml\n" + _SPEC_YAML
spec = yads.from_yaml(StringIO(_SPEC_YAML))


def _readme_pyarrow_step() -> None:
    import yads

    pa_schema = yads.to_pyarrow(spec)
    print(pa_schema)


EXAMPLE = ExampleDefinition(
    example_id="readme-workflow",
    callable=_readme_pyarrow_step,
    blocks=(
        ExampleBlockRequest(
            slug="spec-yaml",
            language="yaml",
            source="literal",
            text=spec_with_file_path.strip(),
        ),
        ExampleBlockRequest(slug="pyarrow-code", language="python", source="callable"),
        ExampleBlockRequest(slug="pyarrow-output", language="text", source="stdout"),
    ),
)


if __name__ == "__main__":
    _readme_pyarrow_step()
