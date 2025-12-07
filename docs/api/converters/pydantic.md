# Pydantic Converter

`PydanticConverter` builds runtime `BaseModel` classes directly from a validated
`YadsSpec`. It honors include/exclude filters, column overrides, and lets you
name or configure the generated class so request/response payloads stay aligned
with the canonical schema.

<!-- BEGIN:example pydantic-converter-basic code -->
```python
from decimal import Decimal
from pprint import pprint

import yads.types as ytypes
from yads.spec import Column, YadsSpec
from yads.constraints import NotNullConstraint
from yads.converters import PydanticConverter

spec = YadsSpec(
    name="catalog.crm.customers",
    version=1,
    columns=[
        Column(
            name="id",
            type=ytypes.Integer(bits=64),
            constraints=[NotNullConstraint()],
        ),
        Column(name="email", type=ytypes.String()),
        Column(name="created_at", type=ytypes.Timestamp()),
        Column(
            name="spend",
            type=ytypes.Decimal(precision=10, scale=2),
        ),
        Column(name="tags", type=ytypes.Array(element=ytypes.String())),
    ],
)

Customer = PydanticConverter().convert(spec)
alice = Customer(
    id=1,
    email="alice@example.com",
    created_at="2024-01-02T15:04:05Z",
    spend=Decimal("12.34"),
    tags=["vip", "beta"],
)
pprint(alice.model_dump())
```
<!-- END:example pydantic-converter-basic code -->
<!-- BEGIN:example pydantic-converter-basic output -->
```text
{'created_at': datetime.datetime(2024, 1, 2, 15, 4, 5, tzinfo=TzInfo(0)),
 'email': 'alice@example.com',
 'id': 1,
 'spend': Decimal('12.34'),
 'tags': ['vip', 'beta']}
```
<!-- END:example pydantic-converter-basic output -->

!!! tip
    Install one of the supported versions of Pydantic to use this converter with `uv add yads[pydantic]`

::: yads.converters.pydantic_converter.PydanticConverter

::: yads.converters.pydantic_converter.PydanticConverterConfig
