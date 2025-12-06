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

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none


def _spec_to_pyarrow_step() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_pyarrow(spec)

    print(submissions_schema)


def _spec_to_polars_step() -> None:
    import yads
    from pprint import pprint

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_polars(spec)

    pprint(submissions_schema, width=120)


def _spec_to_pyspark_step() -> None:
    import yads
    import json

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_pyspark(spec)

    print(json.dumps(submissions_schema.jsonValue(), indent=2))


def _spec_to_pydantic_step() -> None:
    import yads
    import json

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    Submission = yads.to_pydantic(spec, model_name="Submission")

    print(json.dumps(Submission.model_json_schema(), indent=2))


def _spec_to_sql_step() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    spark_ddl = yads.to_sql(spec, dialect="spark", pretty=True)

    print(spark_ddl)


EXAMPLE = ExampleDefinition(
    example_id="submissions-yaml-to-others",
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
            slug="pyspark-code",
            language="python",
            source="callable",
            callable=_spec_to_pyspark_step,
        ),
        ExampleBlockRequest(
            slug="pyspark-output",
            language="text",
            source="stdout",
            callable=_spec_to_pyspark_step,
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
        ExampleBlockRequest(
            slug="sql-code",
            language="python",
            source="callable",
            callable=_spec_to_sql_step,
        ),
        ExampleBlockRequest(
            slug="sql-output",
            language="sql",
            source="stdout",
            callable=_spec_to_sql_step,
        ),
    ),
)
