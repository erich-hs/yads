---
icon: "lucide/book-open"
---
# What is yads?

`yads` is an expressive, canonical data [specification](intro/specification.md) to solve schema management throughout your data stack. Used for, among other things:

* Transpiling schemas between data formats.
* Managing a canonical schema registry.
* Validating data with schema-on-read enforcement.
* Simplifying interoperability across heterogeneous data pipelines.

All while preserving logical [types](api/types.md), [constraints](api/constraints.md), and metadata for minimal loss of semantics.

<!-- BEGIN:example submissions-yaml-to-others spec-yaml -->
```yaml
# docs/src/specs/submissions.yaml
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
```
<!-- END:example submissions-yaml-to-others spec-yaml -->

=== "PyArrow"
    <!-- BEGIN:example submissions-yaml-to-others pyarrow-code -->
    ```python
    import yads
    
    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_pyarrow(spec)
    
    print(submissions_schema)
    ```
    <!-- END:example submissions-yaml-to-others pyarrow-code -->
    <!-- BEGIN:example submissions-yaml-to-others pyarrow-output -->
    ```text
    submission_id: int64 not null
    completion_percent: decimal128(5, 2)
    time_taken_seconds: int32
    submitted_at: timestamp[ns, tz=UTC]
    ```
    <!-- END:example submissions-yaml-to-others pyarrow-output -->

=== "Polars"
    <!-- BEGIN:example submissions-yaml-to-others polars-code -->
    ```python
    import yads
    from pprint import pprint
    
    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_polars(spec)
    
    pprint(submissions_schema, width=120)
    ```
    <!-- END:example submissions-yaml-to-others polars-code -->
    <!-- BEGIN:example submissions-yaml-to-others polars-output -->
    ```text
    Schema([('submission_id', Int64),
            ('completion_percent', Decimal(precision=5, scale=2)),
            ('time_taken_seconds', Int32),
            ('submitted_at', Datetime(time_unit='ns', time_zone='UTC'))])
    ```
    <!-- END:example submissions-yaml-to-others polars-output -->

=== "PySpark"
    <!-- BEGIN:example submissions-yaml-to-others pyspark-code -->
    ```python
    import yads
    import json
    
    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    submissions_schema = yads.to_pyspark(spec)
    
    print(json.dumps(submissions_schema.jsonValue(), indent=2))
    ```
    <!-- END:example submissions-yaml-to-others pyspark-code -->
    <!-- BEGIN:example submissions-yaml-to-others pyspark-output -->
    ```text
    {
      "type": "struct",
      "fields": [
        {
          "name": "submission_id",
          "type": "long",
          "nullable": false,
          "metadata": {}
        },
        {
          "name": "completion_percent",
          "type": "decimal(5,2)",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "time_taken_seconds",
          "type": "integer",
          "nullable": true,
          "metadata": {}
        },
        {
          "name": "submitted_at",
          "type": "timestamp",
          "nullable": true,
          "metadata": {}
        }
      ]
    }
    ```
    <!-- END:example submissions-yaml-to-others pyspark-output -->

=== "Pydantic"
    <!-- BEGIN:example submissions-yaml-to-others pydantic-code -->
    ```python
    import yads
    import json
    
    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    Submission = yads.to_pydantic(spec, model_name="Submission")
    
    print(json.dumps(Submission.model_json_schema(), indent=2))
    ```
    <!-- END:example submissions-yaml-to-others pydantic-code -->
    <!-- BEGIN:example submissions-yaml-to-others pydantic-output -->
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
    <!-- END:example submissions-yaml-to-others pydantic-output -->

=== "SQL"
    <!-- BEGIN:example submissions-yaml-to-others sql-code -->
    ```python
    import yads
    
    spec = yads.from_yaml("docs/src/specs/submissions.yaml")
    spark_ddl = yads.to_sql(spec, dialect="spark", pretty=True)
    
    print(spark_ddl)
    ```
    <!-- END:example submissions-yaml-to-others sql-code -->
    <!-- BEGIN:example submissions-yaml-to-others sql-output -->
    ```sql
    CREATE TABLE prod.assessments.submissions (
      submission_id BIGINT PRIMARY KEY NOT NULL,
      completion_percent DECIMAL(5, 2) DEFAULT 0.0,
      time_taken_seconds INT,
      submitted_at TIMESTAMP
    )
    ```
    <!-- END:example submissions-yaml-to-others sql-output -->

`yads` is an Open Source project under the Apache 2.0 license. We welcome feedback and contributions to help shape the specification for the data community.

Visit the [GitHub repository](https://github.com/erich-hs/yads) and the [Contributors Guide](develop/contributing.md) to get involved.