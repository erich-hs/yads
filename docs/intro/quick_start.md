---
icon: "lucide/zap"
---
# Quick Start

`yads` is designed to be a lightweight drop-in for existing workflows and data pipelines. Its [loaders](../api/loaders/index.md) and [converters](../api/converters/index.md) handle broad dependency ranges for source and target formats, so it seamlessly slots into your Python jobs.

You can author and interact with the canonical `yads` specification primarily through `yads` Python API.

## Install `yads`

`yads` Python API is available on PyPI. Install with `pip` or `uv`:

=== "uv"
    ```bash
    uv add yads
    ```
    Optionally, install dependencies for your target formats:

    ```bash
    uv add 'yads[pyarrow]'
    ```

=== "pip"
    ```bash
    pip install yads
    ```
    Optionally, install dependencies for your target formats:

    ```bash
    pip install 'yads[pyarrow]'
    ```

Check the [converters documentation](converters/index.md) for install instructions and supported versions of optional depencies.

## Author a spec

Typical workflows start with a `yads` [spec](specification.md) authored in YAML format or using the [Python core API](../api/spec.md).

=== "With a `yads.yaml` file"
    <!-- BEGIN:example submissions-quickstart spec-yaml -->
    ```yaml
    # docs/src/specs/submissions_det.yaml
    name: "prod.assessments.submissions_det"
    version: 1
    yads_spec_version: "0.0.2"
    description: "Assessments submissions details."
    
    metadata:
      owner: "data-team"
      sensitive: false
    
    columns:
      - name: "submission_id"
        type: "bigint"
        description: "Submissions unique identifier."
        constraints:
          primary_key: true
          not_null: true
    
      - name: "completion_percent"
        type: "decimal"
        description: "Assessment completion percentage."
        params:
          precision: 5
          scale: 2
        constraints:
          default: 0.00
    
      - name: "time_taken_seconds"
        type: "integer"
        description: "Time taken submitting this assessment in seconds."
    
      - name: "submitted_at"
        type: "timestamptz"
        description: "Timestamp in UTC when submitted."
        params:
          tz: "UTC"
    ```
    <!-- END:example submissions-quickstart spec-yaml -->
    !!! tip
        Check the [Specification](specification.md) section for a detailed authoring guide of your spec in YAML.
    <!-- BEGIN:example submissions-quickstart spec-from-yaml -->
    ```python
    import yads
    
    spec = yads.from_yaml("docs/src/specs/submissions_det.yaml")
    print(spec)
    ```
    <!-- END:example submissions-quickstart spec-from-yaml -->
    <!-- BEGIN:example submissions-quickstart spec-from-yaml-output -->
    ```text
    spec prod.assessments.submissions_det(version=1)(
      description='Assessments submissions details.'
      metadata={
        owner='data-team',
        sensitive=False
      }
      columns=[
        submission_id: integer(bits=64)(
          description='Submissions unique identifier.',
          constraints=[PrimaryKeyConstraint(), NotNullConstraint()]
        )
        completion_percent: decimal(precision=5, scale=2)(
          description='Assessment completion percentage.',
          constraints=[DefaultConstraint(value=0.0)]
        )
        time_taken_seconds: integer(bits=32)(
          description='Time taken submitting this assessment in seconds.'
        )
        submitted_at: timestamptz(unit=ns, tz=UTC)(
          description='Timestamp in UTC when submitted.'
        )
      ]
    )
    ```
    <!-- END:example submissions-quickstart spec-from-yaml-output -->

=== "With the Python API"
    <!-- BEGIN:example submissions-quickstart spec-from-core-api -->
    ```python
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
    ```
    <!-- END:example submissions-quickstart spec-from-core-api -->
    <!-- BEGIN:example submissions-quickstart spec-from-core-api-output -->
    ```text
    spec prod.assessments.submissions(version=1)(
      description='Assessments submissions details.'
      metadata={
        owner='data-team',
        sensitive=False
      }
      columns=[
        submission_id: integer(bits=64)(
          description='Submissions unique identifier.',
          constraints=[PrimaryKeyConstraint(), NotNullConstraint()]
        )
        completion_percent: decimal(precision=5, scale=2)(
          description='Assessment completion percentage.',
          constraints=[DefaultConstraint(value=0.0)]
        )
        time_taken_seconds: integer(bits=32)(
          description='Time taken submitting this assessment in seconds.'
        )
        submitted_at: timestamptz(unit=ns, tz=UTC)(
          description='Timestamp in UTC when submitted.'
        )
      ]
    )
    ```
    <!-- END:example submissions-quickstart spec-from-core-api-output -->

## Convert to a target format

Turn your `spec` into typed schemas for your target formats.

=== "PyArrow"
    <!-- BEGIN:example submissions-quickstart pyarrow-code -->
    ```python
    submissions_schema = yads.to_pyarrow(spec)
    print(submissions_schema)
    ```
    <!-- END:example submissions-quickstart pyarrow-code -->
    <!-- BEGIN:example submissions-quickstart pyarrow-output -->
    ```text
    submission_id: int64 not null
      -- field metadata --
      description: 'Submissions unique identifier.'
    completion_percent: decimal128(5, 2)
      -- field metadata --
      description: 'Assessment completion percentage.'
    time_taken_seconds: int32
      -- field metadata --
      description: 'Time taken submitting this assessment in seconds.'
    submitted_at: timestamp[ns, tz=UTC]
      -- field metadata --
      description: 'Timestamp in UTC when submitted.'
    -- schema metadata --
    owner: 'data-team'
    sensitive: 'false'
    ```
    <!-- END:example submissions-quickstart pyarrow-output -->

=== "Polars"
    <!-- BEGIN:example submissions-quickstart polars-code -->
    ```python
    from pprint import pprint
    
    submissions_schema = yads.to_polars(spec)
    pprint(submissions_schema, width=120)
    ```
    <!-- END:example submissions-quickstart polars-code -->
    <!-- BEGIN:example submissions-quickstart polars-output -->
    ```text
    Schema([('submission_id', Int64),
            ('completion_percent', Decimal(precision=5, scale=2)),
            ('time_taken_seconds', Int32),
            ('submitted_at', Datetime(time_unit='ns', time_zone='UTC'))])
    ```
    <!-- END:example submissions-quickstart polars-output -->

=== "PySpark"
    <!-- BEGIN:example submissions-quickstart pyspark-code -->
    ```python
    import json
    
    submissions_schema = yads.to_pyspark(spec)
    print(json.dumps(submissions_schema.jsonValue(), indent=2))
    ```
    <!-- END:example submissions-quickstart pyspark-code -->
    <!-- BEGIN:example submissions-quickstart pyspark-output -->
    ```text
    {
      "type": "struct",
      "fields": [
        {
          "name": "submission_id",
          "type": "long",
          "nullable": false,
          "metadata": {
            "description": "Submissions unique identifier."
          }
        },
        {
          "name": "completion_percent",
          "type": "decimal(5,2)",
          "nullable": true,
          "metadata": {
            "description": "Assessment completion percentage."
          }
        },
        {
          "name": "time_taken_seconds",
          "type": "integer",
          "nullable": true,
          "metadata": {
            "description": "Time taken submitting this assessment in seconds."
          }
        },
        {
          "name": "submitted_at",
          "type": "timestamp",
          "nullable": true,
          "metadata": {
            "description": "Timestamp in UTC when submitted."
          }
        }
      ]
    }
    ```
    <!-- END:example submissions-quickstart pyspark-output -->

=== "Pydantic"
    <!-- BEGIN:example submissions-quickstart pydantic-code -->
    ```python
    import json
    
    Submission = yads.to_pydantic(spec, model_name="Submission")
    print(json.dumps(Submission.model_json_schema(), indent=2))
    ```
    <!-- END:example submissions-quickstart pydantic-code -->
    <!-- BEGIN:example submissions-quickstart pydantic-output -->
    ```text
    {
      "properties": {
        "submission_id": {
          "description": "Submissions unique identifier.",
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
          "description": "Assessment completion percentage.",
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
          "description": "Time taken submitting this assessment in seconds.",
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
          "description": "Timestamp in UTC when submitted.",
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
    <!-- END:example submissions-quickstart pydantic-output -->

=== "SQL"
    <!-- BEGIN:example submissions-quickstart sql-code -->
    ```python
    spark_ddl = yads.to_sql(spec, dialect="spark", pretty=True)
    print(spark_ddl)
    ```
    <!-- END:example submissions-quickstart sql-code -->
    <!-- BEGIN:example submissions-quickstart sql-output -->
    ```sql
    CREATE TABLE prod.assessments.submissions_det (
      submission_id BIGINT PRIMARY KEY NOT NULL,
      completion_percent DECIMAL(5, 2) DEFAULT 0.0,
      time_taken_seconds INT,
      submitted_at TIMESTAMP
    )
    ```
    <!-- END:example submissions-quickstart sql-output -->

!!! tip
    Check the complete [converters API reference](../api/converters/index.md) for all available converters and advanced customization options.

## Register the spec

Use a `yads` [registry](../api/registries/index.md) to version your `spec`.

<!-- BEGIN:example submissions-quickstart registry-code -->
```python
from yads.registries import FileSystemRegistry

registry = FileSystemRegistry("docs/src/specs/registry/")
version = registry.register(spec)
print(f"Registered spec '{spec.name}' as version {version}")
```
<!-- END:example submissions-quickstart registry-code -->
<!-- BEGIN:example submissions-quickstart registry-output -->
```text
Registered spec 'prod.assessments.submissions_det' as version 1
```
<!-- END:example submissions-quickstart registry-output -->

## Load from a typed source

Alternatively, a `yads` spec can be loaded from a knwon typed source with a supported [loader](../api/loaders/index.md).

<!-- BEGIN:example minimal-roundtrip-conversion pyarrow-code -->
```python
import yads
import pyarrow as pa

schema = pa.schema(
    [
        pa.field(
            "id",
            pa.int64(),
            nullable=False,
            metadata={"description": "Customer ID"},
        ),
        pa.field(
            "name",
            pa.string(),
            metadata={"description": "Customer preferred name"},
        ),
        pa.field(
            "email",
            pa.string(),
            metadata={"description": "Customer email address"},
        ),
        pa.field(
            "created_at",
            pa.timestamp("ns", tz="UTC"),
            metadata={"description": "Customer creation timestamp"},
        ),
    ]
)

spec = yads.from_pyarrow(schema, name="catalog.crm.customers", version=1)
print(spec)
```
<!-- END:example minimal-roundtrip-conversion pyarrow-code -->
<!-- BEGIN:example minimal-roundtrip-conversion pyarrow-output -->
```text
spec catalog.crm.customers(version=1)(
  columns=[
    id: integer(bits=64)(
      description='Customer ID',
      constraints=[NotNullConstraint()]
    )
    name: string(
      description='Customer preferred name'
    )
    email: string(
      description='Customer email address'
    )
    created_at: timestamptz(unit=ns, tz=UTC)(
      description='Customer creation timestamp'
    )
  ]
)
```
<!-- END:example minimal-roundtrip-conversion pyarrow-output -->

And transpiled into other formats.

<!-- BEGIN:example minimal-roundtrip-conversion duckdb-code -->
```python
duckdb_ddl = yads.to_sql(spec, dialect="duckdb", pretty=True)
print(duckdb_ddl)
```
<!-- END:example minimal-roundtrip-conversion duckdb-code -->
<!-- BEGIN:example minimal-roundtrip-conversion duckdb-output -->
```sql
CREATE TABLE catalog.crm.customers (
  id BIGINT NOT NULL,
  name TEXT,
  email TEXT,
  created_at TIMESTAMPTZ
)
```
<!-- END:example minimal-roundtrip-conversion duckdb-output -->
