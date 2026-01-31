# YAML Loader

`YamlLoader` turns a YAML string into a canonical `YadsSpec`.

<!-- BEGIN:example yaml-loader-basic loader-example-lowlevel-code -->
```python
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
```
<!-- END:example yaml-loader-basic loader-example-lowlevel-code -->
<!-- BEGIN:example yaml-loader-basic loader-example-lowlevel-output -->
```text
spec prod.assessments.submissions(version=1)(
  columns=[
    submission_id: integer(bits=64)(
      constraints=[PrimaryKeyConstraint(), NotNullConstraint()]
    )
    completion_percent: decimal(precision=5, scale=2)(
      constraints=[DefaultConstraint(value=0.0)]
    )
    time_taken_seconds: integer(bits=32)
    submitted_at: timestamptz(unit=ns, tz=UTC)
  ]
)
```
<!-- END:example yaml-loader-basic loader-example-lowlevel-output -->

::: yads.loaders.yaml_loader.YamlLoader