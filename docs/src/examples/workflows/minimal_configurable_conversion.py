from __future__ import annotations

from pathlib import Path

import yads

from ..base import ExampleBlockRequest, ExampleDefinition

REPO_ROOT = Path(__file__).resolve().parents[4]
SPEC_REFERENCE = "docs/src/specs/customers.yaml"
SPEC_FILE = REPO_ROOT / SPEC_REFERENCE

spec = yads.from_yaml(SPEC_FILE)

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none, reportUnknownParameterType=none
# pyright: reportMissingImports=none, reportMissingParameterType=none


def _spark_subset_code() -> None:
    import yads

    spec = yads.from_yaml("docs/src/specs/customers.yaml")
    ddl_min = yads.to_sql(
        spec,
        dialect="spark",
        include_columns={"id", "email"},
        pretty=True,
    )

    print(ddl_min)


def _column_override_code() -> None:
    from pydantic import Field

    def email_override(field, conv):
        # Enforce example.com domain with a regex pattern
        return str, Field(pattern=r"^.+@example\.com$")

    Model = yads.to_pydantic(spec, column_overrides={"email": email_override})

    try:
        Model(
            id=1,
            email="user@other.com",
            created_at="2024-01-01T00:00:00+00:00",
            spend="42.50",
            tags=["beta"],
        )
    except Exception as e:
        print(type(e).__name__ + ":\n" + str(e))


EXAMPLE = ExampleDefinition(
    example_id="minimal-configurable-conversion",
    blocks=(
        ExampleBlockRequest(
            slug="spark-config-code",
            language="python",
            source="callable",
            callable=_spark_subset_code,
        ),
        ExampleBlockRequest(
            slug="spark-config-output",
            language="sql",
            source="stdout",
            callable=_spark_subset_code,
        ),
        ExampleBlockRequest(
            slug="column-override-code",
            language="python",
            source="callable",
            callable=_column_override_code,
        ),
        ExampleBlockRequest(
            slug="column-override-output",
            language="text",
            source="stdout",
            callable=_column_override_code,
        ),
    ),
)
