from __future__ import annotations

from pathlib import Path
import shutil
import warnings

import yads

from ..base import ExampleBlockRequest, ExampleDefinition

warnings.filterwarnings("ignore")

REPO_ROOT = Path(__file__).resolve().parents[4]
SPEC_REFERENCE = "docs/src/specs/submissions_det.yaml"
SPEC_FILE_PATH = REPO_ROOT / SPEC_REFERENCE
REGISTRY_ENTRY_PATH = (
    REPO_ROOT / "docs/src/specs/registry/prod.assessments.submissions_det"
)

if REGISTRY_ENTRY_PATH.exists():
    shutil.rmtree(REGISTRY_ENTRY_PATH)

spec = yads.from_yaml(SPEC_FILE_PATH)
Submission = yads.to_pydantic(spec, model_name="Submission")

spec_with_file_path = f"# {SPEC_REFERENCE}\n{SPEC_FILE_PATH.read_text().strip()}"

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none


def _spec_from_yaml() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions_det.yaml")
    print(spec)


def _spec_from_core_api() -> None:
    import yads.spec as yspec
    import yads.types as ytypes
    from yads.constraints import (
        PrimaryKeyConstraint,
        NotNullConstraint,
        DefaultConstraint,
    )

    spec = yspec.YadsSpec(
        name="prod.assessments.submissions",
        version=1,
        description="Assessments submissions details.",
        metadata={"owner": "data-team", "sensitive": False},
        columns=[
            yspec.Column(
                name="submission_id",
                type=ytypes.Integer(bits=64),
                description="Submissions unique identifier.",
                constraints=[PrimaryKeyConstraint(), NotNullConstraint()],
            ),
            yspec.Column(
                name="completion_percent",
                type=ytypes.Decimal(precision=5, scale=2),
                description="Assessment completion percentage.",
                constraints=[DefaultConstraint(0.0)],
            ),
            yspec.Column(
                name="time_taken_seconds",
                type=ytypes.Integer(bits=32),
                description="Time taken submitting this assessment in seconds.",
            ),
            yspec.Column(
                name="submitted_at",
                type=ytypes.TimestampTZ(tz="UTC"),
                description="Timestamp in UTC when submitted.",
            ),
        ],
    )

    print(spec)


def _spec_to_pyarrow_step() -> None:
    submissions_schema = yads.to_pyarrow(spec)
    print(submissions_schema)


def _spec_to_polars_step() -> None:
    from pprint import pprint

    submissions_schema = yads.to_polars(spec)
    pprint(submissions_schema, width=120)


def _spec_to_pyspark_step() -> None:
    import json

    submissions_schema = yads.to_pyspark(spec)
    print(json.dumps(submissions_schema.jsonValue(), indent=2))


def _spec_to_pydantic_step() -> None:
    import json

    Submission = yads.to_pydantic(spec, model_name="Submission")
    print(json.dumps(Submission.model_json_schema(), indent=2))


def _spec_to_sql_step() -> None:
    spark_ddl = yads.to_sql(spec, dialect="spark", pretty=True)
    print(spark_ddl)


def _register_spec_step() -> None:
    from yads.registries import FileSystemRegistry

    registry = FileSystemRegistry("docs/src/specs/registry/")
    version = registry.register(spec)
    print(f"Registered spec '{spec.name}' as version {version}")


EXAMPLE = ExampleDefinition(
    example_id="submissions-quickstart",
    blocks=(
        ExampleBlockRequest(
            slug="spec-yaml",
            language="yaml",
            source="literal",
            text=spec_with_file_path.strip(),
        ),
        ExampleBlockRequest(
            slug="spec-from-yaml",
            language="python",
            source="callable",
            callable=_spec_from_yaml,
        ),
        ExampleBlockRequest(
            slug="spec-from-yaml-output",
            language="text",
            source="stdout",
            callable=_spec_from_yaml,
        ),
        ExampleBlockRequest(
            slug="spec-from-core-api",
            language="python",
            source="callable",
            callable=_spec_from_core_api,
        ),
        ExampleBlockRequest(
            slug="spec-from-core-api-output",
            language="text",
            source="stdout",
            callable=_spec_from_core_api,
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
        ExampleBlockRequest(
            slug="registry-code",
            language="python",
            source="callable",
            callable=_register_spec_step,
        ),
        ExampleBlockRequest(
            slug="registry-output",
            language="text",
            source="stdout",
            callable=_register_spec_step,
        ),
    ),
)
