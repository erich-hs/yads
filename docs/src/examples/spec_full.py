from __future__ import annotations

from pathlib import Path

from .base import ExampleBlockRequest, ExampleDefinition

FIXTURE = Path("tests/fixtures/spec/valid/full_spec.yaml")


def _print_full_spec() -> None:
    """Emit the full example spec fixture to stdout for docs embedding."""
    print(FIXTURE.read_text())


EXAMPLE = ExampleDefinition(
    example_id="full-spec",
    blocks=(
        ExampleBlockRequest(
            slug="full-spec-yaml",
            language="yaml",
            source="stdout",
            callable=_print_full_spec,
        ),
    ),
)
