# yads

`yads` is a canonical, typed data specification that keeps columnar schemas
consistent across your stack. Define it once as YAML and generate deterministic
loaders or converters for PyArrow, SQL dialects, Polars, Pydantic, and more.

## Why yads?

- **Single source of truth** – Ship one spec that downstream tools can trust.
- **Typed by default** – Logical types carry metadata like precision, scale, and
  timezone information so converters stay faithful to the intent of the model.
- **Deterministic converters** – Every converter is versioned and tested so you
  see predictable output, even when optional dependencies evolve.

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
# docs/specs/customers.yaml
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

spec = yads.from_yaml("docs/specs/customers.yaml")
pyd_model = yads.to_pydantic(spec, model_name="Customer")
arrow_schema = yads.to_pyarrow(spec)
```

## What's next?

- Explore the [converters](converters/pyarrow.md) starting with PyArrow.
- Browse the loaders and converters in `src/yads/` for more examples.
- Check `CONTRIBUTING.md` if you want to add new runtimes or tighten docs.
