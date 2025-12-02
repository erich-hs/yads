# yads

`yads` is a canonical, typed data [specification](spec/index.md) to solve schema management across your data stack. Define a schema once; load and convert it deterministically between formats with minimal loss of semantics.

<!-- BEGIN:example concise-yaml-to-others spec-yaml -->
```yaml
# docs/src/specs/submissions.yaml
name: prod.assessments.submissions
version: 1
yads_spec_version: 0.0.2
columns:
  - name: submission_id
    type: bigint
    constraints:
      primary_key: true
      not_null: true
  - name: completion_percent
    type: decimal
    params:
      precision: 5
      scale: 2
    constraints:
      default: 0.00
  - name: time_taken_seconds
    type: integer
  - name: submitted_at
    type: timestamptz
    params:
      tz: UTC
```
<!-- END:example concise-yaml-to-others spec-yaml -->
=== "Polars"
    <!-- BEGIN:example concise-yaml-to-others polars-code -->
    <!-- END:example concise-yaml-to-others polars-code -->
    <!-- BEGIN:example concise-yaml-to-others polars-output -->
    <!-- END:example concise-yaml-to-others polars-output -->

=== "PyArrow"
    <!-- BEGIN:example concise-yaml-to-others pyarrow-code -->
    ```python
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_pyarrow(spec)

    print(submissions_schema)
    ```
    <!-- END:example concise-yaml-to-others pyarrow-code -->
    <!-- BEGIN:example concise-yaml-to-others pyarrow-output -->
    ```text
    submission_id: int64 not null
    completion_percent: decimal128(5, 2)
    time_taken_seconds: int32
    submitted_at: timestamp[ns, tz=UTC]
    ```
    <!-- END:example concise-yaml-to-others pyarrow-output -->

=== "Pydantic"
    <!-- BEGIN:example concise-yaml-to-others pydantic-code -->
    ```python
    import json
    import yads

    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    Submission = yads.to_pydantic(spec, model_name="Submission")

    print(json.dumps(Submission.model_json_schema(), indent=2))
    ```
    <!-- END:example concise-yaml-to-others pydantic-code -->
    <!-- BEGIN:example concise-yaml-to-others pydantic-output -->
    ```text
    {
      "properties": {
        "submission_id": {
          "maximum": 9223372036854775807,
          "minimum": -9223372036854775808,
          "title": "Submission Id",
          "type": "integer",
          "yads": {
            "primary_key": true
          }
        },
        "completion_percent": {
          "anyOf": [
            {
              "type": "number"
            },
            {
              "pattern": "^(?!^[-+.]*$)[+-]?0*(?:\\d{0,3}|(?=[\\d.]{1,6}0*$)\\d{0,3}\\.\\d{0,2}0*$)",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": 0.0,
          "title": "Completion Percent"
        },
        "time_taken_seconds": {
          "anyOf": [
            {
              "maximum": 2147483647,
              "minimum": -2147483648,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "title": "Time Taken Seconds"
        },
        "submitted_at": {
          "anyOf": [
            {
              "format": "date-time",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "title": "Submitted At"
        }
      },
      "required": [
        "submission_id",
        "time_taken_seconds",
        "submitted_at"
      ],
      "title": "Submission",
      "type": "object"
    }
    ```
    <!-- END:example concise-yaml-to-others pydantic-output -->

## Why yads?

- **Single source of truth** – Ship one spec that downstream tools can trust.
- **Typed by default** – Logical types carry rich semantics, constraints, and metadata so conversion between formats stay faithful to the intent of the model.
- **Deterministic conversion** – Potentially lossy changes are never applied implicitly. Types with no value-preserving representation fail fast with clear errors and extension guidance.

## Install

Use `uv` (preferred) or pip. Optional dependency extras pull in the converter
you need.

```bash
uv add yads
uv add yads[pyarrow]  # include the PyArrow converter helpers
```

## Typical workflow

1. **Author a spec** – Start from YAML and describe each column with its logical
   type plus any constraints.
2. **Load it** – `yads.from_yaml` handles file paths, file-like objects, or raw
   strings and validates them against the current schema version.
3. **Convert it** – Use helpers like `yads.to_pyarrow`, `yads.to_sql`, or
   `yads.to_pydantic` to move between runtimes without rewriting schema logic.

```yaml
# docs/src/specs/customers.yaml
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
  - name: spend
    type: decimal
    params:
      precision: 10
      scale: 2
```

```python
import yads

spec = yads.from_yaml("docs/src/specs/customers.yaml")
pyd_model = yads.to_pydantic(spec, model_name="Customer")
arrow_schema = yads.to_pyarrow(spec)
```

## What's next?

- Explore the [converters](converters/pyarrow.md) starting with PyArrow.
- Browse the loaders and converters in `src/yads/` for more examples.
- Check `CONTRIBUTING.md` if you want to add new runtimes or tighten docs.
