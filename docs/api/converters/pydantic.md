# Pydantic Converter

`PydanticConverter` builds runtime `BaseModel` classes directly from a validated
`YadsSpec`. It honors include/exclude filters, column overrides, and lets you
name or configure the generated class so request/response payloads stay aligned
with the canonical schema.

<!-- BEGIN:example pydantic-converter-basic code -->
```python
import yads
from yads.converters import PydanticConverter, PydanticConverterConfig
import json

spec = yads.from_yaml("docs/src/specs/submissions.yaml")

converter = PydanticConverter(PydanticConverterConfig(mode="coerce"))
Submission = converter.convert(spec)
print(json.dumps(Submission.model_json_schema(), indent=2))
```
<!-- END:example pydantic-converter-basic code -->
<!-- BEGIN:example pydantic-converter-basic output -->
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
  "title": "prod_assessments_submissions",
  "type": "object"
}
```
<!-- END:example pydantic-converter-basic output -->

!!! info
    Install one of the supported versions of Pydantic to use this converter with `uv add 'yads[pydantic]'`

::: yads.converters.pydantic_converter.PydanticConverter

::: yads.converters.pydantic_converter.PydanticConverterConfig
