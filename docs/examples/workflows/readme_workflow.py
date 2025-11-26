"""Executable README workflow snippets."""

from __future__ import annotations

from yads.constraints import NotNullConstraint
from yads.spec import Column, YadsSpec
import yads.types as ytypes

from ..base import ExampleBlockRequest, ExampleDefinition


_SPEC_YAML = """# registry/specs/customers.yaml
name: catalog.crm.customers
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


spec = YadsSpec(
    name="catalog.crm.customers",
    version=1,
    columns=[
        Column(
            name="id",
            type=ytypes.Integer(bits=64),
            constraints=[NotNullConstraint()],
        ),
        Column(name="email", type=ytypes.String()),
        Column(name="created_at", type=ytypes.TimestampTZ(tz="UTC")),
        Column(
            name="spend",
            type=ytypes.Decimal(precision=10, scale=2),
        ),
        Column(name="tags", type=ytypes.Array(element=ytypes.String())),
    ],
)


def _generate_yaml_step() -> None:
    import yaml
    from yads.serializers import SpecSerializer

    serialized_spec = SpecSerializer().serialize(spec)
    yaml_spec = yaml.safe_dump(serialized_spec, sort_keys=False, default_flow_style=False)
    print("# registry/specs/customers.yaml")
    print(yaml_spec)


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
            text=_SPEC_YAML.strip(),
        ),
        ExampleBlockRequest(slug="pyarrow-code", language="python", source="callable"),
        ExampleBlockRequest(slug="pyarrow-output", language="text", source="stdout"),
    ),
)


if __name__ == "__main__":
    _generate_yaml_step()
    _readme_pyarrow_step()
