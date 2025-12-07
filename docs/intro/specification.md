---
title: Specification
icon: lucide/book-text
---
# `yads` Specification

This page is the practical authoring guide for the `yads` YAML specification. Use
it while drafting or reviewing specs so every table definition is predictable,
well-typed, and friendly to downstream converters.

!!! note "Current spec format"
    Target `yads_spec_version`: `0.0.2`

## Authoring essentials
- Start from a complete YAML map; top-level unknown keys are rejected.
- Keep required fields (`name`, `version`, `yads_spec_version`, `columns`) front
  and center.
- Prefer concise descriptions and deterministic defaults—surface intent through
  constraints and generated columns.
- Use lower-case types for readability; the schema is case-insensitive.

## Top-level shape
The document is a single YAML object with these sections:
- Identity and bookkeeping: `name`, `version`, `yads_spec_version`, `description`,
  `external`, `metadata`.
- Physical layout: `storage`, `partitioned_by`.
- Integrity: `table_constraints`.
- Columns: ordered list of column definitions in `columns`.

## Attributes
Each attribute below lists intent, requirements, and common patterns. Tables
capture the exact field semantics; each section includes a collapsible YAML
example with quoted strings.

### name
- Fully qualified identifier: `[catalog].[database].[table]`. Keep stable across
  revisions; avoid whitespace or quoting.
- Required: Yes

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| name | Fully qualified table identifier. | string | Yes | — |

??? example "Example YAML"
    ```yaml
    name: "catalog.db.table_name"
    ```

### version
- Integer that increments with every published change; the registry owns this
  number. Use `1` for the first publication, then bump monotonically.
- Required: Yes

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| version | Registry-assigned monotonic version. | integer | Yes | — |

??? example "Example YAML"
    ```yaml
    version: 3
    ```

### yads_spec_version
- Tracks the spec format version used to validate the file.
- Required: Yes

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| yads_spec_version | Specification format version string. | string | Yes | — |

??? example "Example YAML"
    ```yaml
    yads_spec_version: "0.0.2"
    ```

### description
- Free-form summary of the table. Aim for one sentence.
- Required: No

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| description | Human-readable table description. | string | No | — |

??? example "Example YAML"
    ```yaml
    description: "Customer transaction facts."
    ```

### external
- When `true`, SQL DDL converters emit `CREATE EXTERNAL`.
- Required: No

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| external | Emit external table DDL. | boolean | No | false |

??? example "Example YAML"
    ```yaml
    external: true
    ```

### metadata
- Arbitrary key/value map for ownership, tags, sensitivity flags, etc.
- Required: No

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| metadata | Free-form metadata map. | object | No | — |

??? example "Example YAML"
    ```yaml
    metadata:
      owner: "data-team"
      sensitivity: "internal"
    ```

### storage
- Describes physical storage; omit entirely for logical-only definitions. No
  additional keys allowed.
- Required: No

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| format | Engine/storage format (`parquet`, `iceberg`, `orc`, `csv`, …). | string | No | — |
| location | Path or URI to the table root. | string | No | — |
| tbl_properties | Key/value map for table properties. | object | No | — |

??? example "Example YAML"
    ```yaml
    storage:
      format: "parquet"
      location: "/data/warehouse/customers"
      tbl_properties:
        write_compression: "snappy"
    ```

### partitioned_by
- Ordered list of partition expressions. Order matters for many engines.
- Required: No

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| column | Source column name. | string | Yes | — |
| transform | Transform applied to the column (`month`, `year`, `truncate`, `bucket`, …). | string | No | — |
| transform_args | Unnamed arguments passed to the transform. | array | No | — |

??? example "Example YAML"
    ```yaml
    partitioned_by:
      - column: "event_date"
        transform: "month"
      - column: "country_code"
        transform: "truncate"
        transform_args: [2]
    ```

### table_constraints
- Table-level constraints for composite keys.
- Required: No

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| type | Constraint type: `primary_key` or `foreign_key`. | string | Yes | — |
| name | Constraint identifier. | string | Yes | — |
| columns | Columns participating in the constraint. | array(string) | Yes | — |
| references.table | Referenced table (foreign keys only). | string | Foreign keys | — |
| references.columns | Referenced columns. | array(string) | No | — |

??? example "Example YAML"
    ```yaml
    table_constraints:
      - type: "primary_key"
        name: "pk_orders"
        columns: ["order_id", "order_date"]
      - type: "foreign_key"
        name: "fk_customer"
        columns: ["customer_id"]
        references:
          table: "dim_customers"
          columns: ["id"]
    ```

### columns
- Ordered column definitions combining types, constraints, generated expressions,
  and metadata. Avoid additional keys; the schema is strict.
- Required: Yes

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| name | Column name. | string | Yes | — |
| type | Data type token (case-insensitive). Strings, integers, floats, decimals, booleans, binary, temporal, complex (`array`, `struct`, `map`, `json`, `geometry`, `geography`, `uuid`, `variant`, `void`/`null`, `tensor`). | string | Yes | — |
| params | Type-specific parameters (see below). | object | No | — |
| element | Nested type for arrays/tensors. | type_spec | No | — |
| fields | Struct fields (array of columns). | array(column) | No | — |
| key | Map key type. | type_spec | No | — |
| value | Map value type. | type_spec | No | — |
| description | Column description. | string | No | — |
| constraints | Column-level constraints. | object | No | — |
| generated_as | Generated column definition. | object | No | — |
| metadata | Free-form column metadata. | object | No | — |

#### Type parameters (`params`)

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| length | String/binary length. | integer | No | — |
| bits | Bit width for integer aliases (8, 16, 32, 64, 128, 256). | integer | No | — |
| signed | Signedness when `bits` is used. | boolean | No | true |
| precision | Decimal precision. | integer | No | — |
| scale | Decimal scale (may be negative). | integer | No | — |
| unit | Temporal precision unit (`s`, `ms`, `us`, `ns`). | string | No | — |
| tz | IANA timezone for `timestamptz`. | string | No | — |
| size | Max array length for fixed-size arrays. | integer | No | — |
| keys_sorted | Whether map keys are sorted. | boolean | No | false |
| interval_start | Interval lower unit (`year`, `month`, `day`, `hour`, `minute`, `second`). | string | No | — |
| interval_end | Interval upper unit (`year`, `month`, `day`, `hour`, `minute`, `second`). | string | No | — |
| shape | Fixed dimensions for tensors. | array(integer) | No | — |

#### Column constraints (`constraints`)

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| not_null | Column cannot be null. | boolean | No | — |
| primary_key | Column is a single-column primary key (prefer table-level for composites). | boolean | No | — |
| identity.always | Identity value always generated. | boolean | No | true |
| identity.start | Starting value for identity. | integer | No | — |
| identity.increment | Increment for identity. | integer | No | — |
| foreign_key.name | Foreign key constraint name. | string | No | — |
| foreign_key.references.table | Referenced table. | string | Foreign keys | — |
| foreign_key.references.columns | Referenced columns. | array(string) | No | — |
| default | Literal default value. | any | No | — |

#### Generated columns (`generated_as`)

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| column | Source column for derivation. | string | Yes | — |
| transform | Transform name (`upper`, `cast`, `month`, etc.). | string | No | — |
| transform_args | Extra arguments for the transform. | array | No | — |

??? example "Example: basic columns"
    ```yaml
    columns:
      - name: "customer_id"
        type: "bigint"
        constraints:
          not_null: true
          primary_key: true
      - name: "email"
        type: "string"
        params:
          length: 320
        constraints:
          not_null: true
    ```

??? example "Example: generated and defaults"
    ```yaml
    columns:
      - name: "created_at"
        type: "timestamp"
        constraints:
          default: "CURRENT_TIMESTAMP"
      - name: "created_date"
        type: "date"
        generated_as:
          column: "created_at"
          transform: "cast"
          transform_args: ["date"]
    ```

??? example "Example: complex types"
    ```yaml
    columns:
      - name: "tags"
        type: "array"
        element:
          type: "string"
        params:
          size: 10
      - name: "attributes"
        type: "struct"
        fields:
          - name: "status"
            type: "string"
          - name: "score"
            type: "double"
      - name: "metrics"
        type: "tensor"
        params:
          shape: [3, 3]
    ```

## Complete spec example

Below is a complete example of the `yads` specification.

<!-- BEGIN:example full-spec full-spec-yaml -->
```yaml
name: "catalog.db.full_spec"
version: 1
yads_spec_version: "0.0.2"
description: "A full spec with all features."
external: true

metadata:
  owner: "data-team"
  sensitive: false

storage:
  format: "parquet"
  location: "/data/full.spec"
  tbl_properties:
    write_compression: "snappy"

partitioned_by:
  - column: "c_string_len"
  - column: "c_string"
    transform: "truncate"
    transform_args: [10]
  - column: "c_date"
    transform: "month"

table_constraints:
  - type: "primary_key"
    name: "pk_full_spec"
    columns: ["c_uuid", "c_date"]
  - type: "foreign_key"
    name: "fk_other_table"
    columns: ["c_int64"]
    references:
      table: "other_table"
      columns: ["id"]

columns:
  - name: "c_string"
    type: "string"
    description: "A string column with a default value."
    constraints:
      default: "default_string"

  - name: "c_string_len"
    type: "string"
    params:
      length: 255
    description: "A string column with a specific length."

  - name: "c_string_upper"
    type: "string"
    description: "A string column with a generated value."
    generated_as:
      column: "c_string"
      transform: "upper"

  - name: "c_int8"
    type: "tinyint"
    description: "A tiny integer."

  - name: "c_int16"
    type: "smallint"
    description: "A small integer."

  - name: "c_int32_identity"
    type: "int"
    description: "An integer with an identity constraint."
    constraints:
      identity:
        always: true
        start: 1
        increment: 1

  - name: "c_int64"
    type: "bigint"
    description: "A big integer, part of a foreign key."

  - name: "c_float32"
    type: "float"
    description: "A 32-bit float."

  - name: "c_float64"
    type: "double"
    description: "A 64-bit float."

  - name: "c_decimal"
    type: "decimal"
    description: "A decimal with default precision/scale."

  - name: "c_decimal_ps"
    type: "decimal"
    params:
      precision: 10
      scale: 2
    description: "A decimal with specified precision/scale."

  - name: "c_boolean"
    type: "boolean"
    description: "A boolean value."

  - name: "c_binary"
    type: "binary"
    description: "A binary data column."

  - name: "c_binary_len"
    type: "binary"
    params:
      length: 8
    description: "A fixed-length binary column."

  - name: "c_date"
    type: "date"
    description: "A date value, part of the primary key."
    constraints:
      not_null: true

  - name: "c_date_month"
    type: "int"
    description: "An integer column with a generated value."
    generated_as:
      column: "c_date"
      transform: "month"

  - name: "c_time"
    type: "time"
    description: "A time value."

  - name: "c_timestamp"
    type: "timestamp"
    description: "A timestamp."

  - name: "c_timestamp_date"
    type: "date"
    description: "A date column with a generated value."
    generated_as:
      column: "c_timestamp"
      transform: "cast"
      transform_args: ["date"]

  - name: "c_timestamp_tz"
    type: "timestamptz"
    description: "A timestamp with timezone."

  - name: "c_timestamp_ltz"
    type: "timestampltz"
    description: "A timestamp with local timezone."

  - name: "c_timestamp_ntz"
    type: "timestampntz"
    description: "A timestamp without timezone."

  - name: "c_interval_ym"
    type: "interval"
    params:
      interval_start: "YEAR"
      interval_end: "MONTH"
    description: "A year-month interval."

  - name: "c_interval_d"
    type: "interval"
    params:
      interval_start: "DAY"
    description: "A day interval."

  - name: "c_array"
    type: "array"
    element:
      type: "int"
    description: "An array of integers."

  - name: "c_array_sized"
    type: "array"
    element:
      type: "string"
    params:
      size: 2
    description: "A fixed-size array of strings."

  - name: "c_struct"
    type: "struct"
    fields:
      - name: "nested_int"
        type: "int"
        description: "A nested integer."
      - name: "nested_string"
        type: "string"
        description: "A nested string."
    description: "A struct column."

  - name: "c_map"
    type: "map"
    key:
      type: "string"
    value:
      type: "double"
    description: "A map from string to double."
  
  - name: "c_json"
    type: "json"
    description: "A JSON column."

  - name: "c_geometry"
    type: "geometry"
    params:
      srid: 4326
    description: "A geometry column."

  - name: "c_geography"
    type: "geography"
    params:
      srid: 4326
    description: "A geography column."

  - name: "c_uuid"
    type: "uuid"
    description: "Primary key part 1"
    constraints:
      not_null: true

  - name: "c_void"
    type: "void"
    description: "A void column."

  - name: "c_variant"
    type: "variant"
    description: "A variant column."
```
<!-- END:example full-spec full-spec-yaml -->
