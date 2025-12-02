from __future__ import annotations

from pathlib import Path

import yads
import warnings

from ..base import ExampleBlockRequest, ExampleDefinition

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parents[4]
SPEC_REFERENCE = "docs/src/specs/submissions.yaml"
SPEC_FILE_PATH = REPO_ROOT / SPEC_REFERENCE

spec = yads.from_yaml(SPEC_FILE_PATH)
Submission = yads.to_pydantic(spec, model_name="Submission")

spec_with_file_path = f"# {SPEC_REFERENCE}\n{SPEC_FILE_PATH.read_text().strip()}"


def _spec_to_pyarrow_step() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_pyarrow(spec)

    print(submissions_schema)


def _spec_to_polars_step() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_polars(spec)

    print(submissions_schema)


def _spec_to_pydantic_step() -> None:
    import json
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    Submission = yads.to_pydantic(spec, model_name="Submission")

    print(json.dumps(Submission.model_json_schema(), indent=2))


EXAMPLE = ExampleDefinition(
    example_id="concise-yaml-to-others",
    blocks=(
        ExampleBlockRequest(
            slug="spec-yaml",
            language="yaml",
            source="literal",
            text=spec_with_file_path.strip(),
        ),
        ExampleBlockRequest(
            slug="pyarrow-code",
            language="python",
            source="callable",
            callable=_spec_to_pyarrow_step,
        ),
        ExampleBlockRequest(
            slug="pyarrow-output",
            language="text",
            source="stdout",
            callable=_spec_to_pyarrow_step,
        ),
        ExampleBlockRequest(
            slug="polars-code",
            language="python",
            source="callable",
            callable=_spec_to_polars_step,
        ),
        ExampleBlockRequest(
            slug="polars-output",
            language="text",
            source="stdout",
            callable=_spec_to_polars_step,
        ),
        ExampleBlockRequest(
            slug="pydantic-code",
            language="python",
            source="callable",
            callable=_spec_to_pydantic_step,
        ),
        ExampleBlockRequest(
            slug="pydantic-output",
            language="text",
            source="stdout",
            callable=_spec_to_pydantic_step,
        ),
    ),
)
