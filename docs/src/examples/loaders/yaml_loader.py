"""Executable examples for loading specs from YAML strings."""

from __future__ import annotations

from ..base import ExampleBlockRequest, ExampleDefinition


def _yaml_loader_lowlevel_example() -> None:
    from yads.loaders import YamlLoader

    yaml_string = """
name: "prod.assessments.submissions"
version: 1
yads_spec_version: "0.0.2"

columns:
  - name: "submission_id"
    type: "bigint"
    constraints:
      primary_key: true
      not_null: true

  - name: "completion_percent"
    type: "decimal"
    params:
      precision: 5
      scale: 2
    constraints:
      default: 0.00

  - name: "time_taken_seconds"
    type: "integer"

  - name: "submitted_at"
    type: "timestamptz"
    params:
      tz: "UTC"
    """

    loader = YamlLoader()
    spec = loader.load(yaml_string)
    print(spec)


EXAMPLE = ExampleDefinition(
    example_id="yaml-loader-basic",
    blocks=(
        ExampleBlockRequest(
            slug="loader-example-lowlevel-code",
            language="python",
            source="callable",
            callable=_yaml_loader_lowlevel_example,
        ),
        ExampleBlockRequest(
            slug="loader-example-lowlevel-output",
            language="text",
            source="stdout",
            callable=_yaml_loader_lowlevel_example,
        ),
    ),
)
