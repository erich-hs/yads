---
title: Specification
icon: lucide/book-text
---
# yads Specification

This page is the practical authoring guide for the `yads` YAML specification. Use
it while drafting or reviewing specs so every table definition is predictable,
well-typed, and friendly to downstream converters.

!!! note "Current spec format"
    Target `yads_spec_version`: `0.0.2`

The [specification JSON schema](https://github.com/erich-hs/yads/blob/main/spec/yads_spec_v0.0.2.json) is available in the GitHub repo.

### Required header

Top-level named parameters describing the spec.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | Yes | Fully-qualified identifier `[catalog].[database].[table]`. |
| `version` | integer | Yes | Monotonic registry version, starting at `1`. |
| `yads_spec_version` | string | No | Spec format version used to validate the document. When not specified, the loader will default to the current version of the specification. |
| `columns` | array | Yes | Ordered list of column definitions. |

???+ example "Required header example"
    ```yaml
    name: "catalog.db.table_name"
    version: 3
    yads_spec_version: "0.0.2"
    columns:
      - name: "id"
        type: "bigint"
    ```

### Identity and metadata

Optional top-level named parameters.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `description` | string | No | Short summary of table intent. |
| `external` | boolean | No | Emit `CREATE EXTERNAL` for compatible converters when True. Defaults to `false`. |
| `metadata` | map | No | Arbitrary container for metadata as key-value pairs. |

???+ example "Identity and metadata example"
    ```yaml
    description: "Customer transaction facts."
    external: true
    metadata:
      owner: "data-team"
      sensitivity: "internal"
    ```

### Storage layout

Optional top-level block for storage-related metadata.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `storage.format` | string | No | Storage file format such as `parquet`, `iceberg`, `orc`, `csv`. |
| `storage.location` | string | No | URI or path for the table root. |
| `storage.tbl_properties` | map | No | Engine-specific key/value properties. |

???+ example "Storage block"
    ```yaml
    storage:
      format: "parquet"
      location: "/data/warehouse/customers"
      tbl_properties:
        write_compression: "snappy"
    ```

### Partitioning

Optional top-level block for physical table partition information.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `partitioned_by[].column` | string | Yes | Column backing the partition. Required when the `partitioned_by` block is defined. |
| `partitioned_by[].transform` | string | No | Transform name (`month`, `year`, `truncate`, `bucket`, ...). |
| `partitioned_by[].transform_args` | array | No | Unnamed arguments passed to the transform. |

???+ example "Partition definitions"
    ```yaml
    partitioned_by:
      - column: "event_date"
        transform: "month"
      - column: "country_code"
        transform: "truncate"
        transform_args: [2]
    ```

### Table constraints

Optional top-level block for Primary Key and Foreign Key table constraints.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `table_constraints[].type` | string | Yes | `primary_key` or `foreign_key`. Required when the `table_constraints` block is defined. |
| `table_constraints[].name` | string | Yes | Stable constraint identifier. Required when the `table_constraints` block is defined. |
| `table_constraints[].columns` | array(string) | Yes | Participating columns. Required when the `table_constraints` block is defined. |
| `table_constraints[].references.table` | string | Foreign keys | Referenced table. Required for `foreign_key` table constraints. |
| `table_constraints[].references.columns` | array(string) | No | Referenced columns on `foreign_key` table constraints. Defaults to matching columns when not specified. |

???+ example "Composite constraints"
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

## Column reference

Each entry in `columns` captures a single field plus metadata. The spec is strict:
unrecognized keys within a column block cause validation failures.

### Column fields
| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | string | Yes | Column identifier. |
| `type` | string | Yes | Case-insensitive token from the [type catalog](#type-catalog). |
| `params` | map | No | Type-specific arguments (length, precision, tz, ...). |
| `element` | column | Arrays or tensors | Nested type for arrays or tensors. |
| `fields` | array(column) | Structs | Ordered struct fields. |
| `key` | column | Maps | Map key type. |
| `value` | column | Maps | Map value type. |
| `description` | string | No | Text-form field or column description. |
| `constraints` | map | No | See [column constraints](#column-constraints). |
| `generated_as` | map | No | See [generated columns](#generated-columns). |
| `metadata` | map | No | Arbitrary per-column metadata. |

???+ example "Column entry"
    ```yaml
    columns:
      - name: "customer_id"
        type: "bigint"
        description: "Surrogate primary key."
        constraints:
          primary_key: true
          not_null: true

      - name: "created_at"
        type: "timestamptz"
        params:
          tz: "UTC"

      - name: "created_date"
        type: "date"
        generated_as:
          column: "created_at"
          transform: "cast"
          transform_args: ["date"]
    ```

### Column constraints
| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `not_null` | boolean | No | Disallow nulls. |
| `primary_key` | boolean | No | Declare a single-column primary key (prefer table-level blocks for composites). |
| `default` | literal | No | Literal default consumed by downstream systems. |
| `identity.start` | integer | No | Starting value for identity sequences. |
| `identity.increment` | integer | No | Step for identity sequences. |
| `identity.always` | boolean | No | Whether identity is always generated. Defaults to `true` when an `identity` constraint is defined. |
| `foreign_key.name` | string | No | Column-level foreign key name. |
| `foreign_key.references.table` | string | Foreign keys | Referenced foreign key table name. |
| `foreign_key.references.columns` | array(string) | No | Referenced foreign key column names. |

???+ example "Column constraints"
    ```yaml
    - name: "submission_id"
      type: "bigint"
      constraints:
        primary_key: true
        not_null: true
        identity:
          start: 1
          increment: 1
    ```

### Generated columns
| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `column` | string | Yes | Source column supplying values. |
| `transform` | string | No | Transform name (`upper`, `month`, `cast`, ...). |
| `transform_args` | array | No | Extra positional arguments. |

???+ example "Generated column"
    ```yaml
    - name: "created_date"
      type: "date"
      generated_as:
        column: "created_at"
        transform: "cast"
        transform_args: ["date"]
    ```

## Type catalog

Type tokens are lower-case by convention but case-insensitive. Each table below
represents the keys accepted under `params` for a column entry. Additional keys
(such as `element` or `fields`) are called out in their sections.

### Scalar types

#### [`string`](../api/types.md#yads.types.String)
UTF-8 text with optional fixed length.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `length` | Maximum characters allowed. | integer | No | Unlimited |

???+ example "string"
    ```yaml
    - name: "email"
      type: "string"
      params:
        length: 320
    ```

#### [`binary`](../api/types.md#yads.types.Binary)
Byte arrays or VARBINARY columns.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `length` | Maximum number of bytes. | integer | No | Unlimited |

???+ example "binary"
    ```yaml
    - name: "payload"
      type: "binary"
      params:
        length: 16
    ```

#### [`boolean`](../api/types.md#yads.types.Boolean)
True/false values. No additional parameters.

???+ example "boolean"
    ```yaml
    - name: "is_active"
      type: "boolean"
    ```

#### [`integer`](../api/types.md#yads.types.Integer)
Signed or unsigned whole numbers. Aliases include `tinyint`, `smallint`,
`int`, `bigint`, `int8`, `uint32`, etc.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `bits` | Bit width (`8`, `16`, `32`, `64`). | integer | No | Target default |
| `signed` | Include negative values. | boolean | No | true |

???+ example "integer"
    ```yaml
    - name: "partition_bucket"
      type: "int"
      params:
        bits: 32
        signed: false
    ```

#### [`float`](../api/types.md#yads.types.Float)
IEEE floating point numbers.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `bits` | Bit width (`16`, `32`, `64`). | integer | No | Target default |

???+ example "float"
    ```yaml
    - name: "confidence"
      type: "float"
      params:
        bits: 32
    ```

#### [`decimal`](../api/types.md#yads.types.Decimal)
Exact precision decimals.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `precision` | Total digits. | integer | Precision & scale together | — |
| `scale` | Digits to the right of the decimal point (can be negative). | integer | Precision & scale together | — |
| `bits` | Storage width (`128` or `256`). | integer | No | Target default |

???+ example "decimal"
    ```yaml
    - name: "completion_percent"
      type: "decimal"
      params:
        precision: 5
        scale: 2
    ```

### Temporal types

#### [`date`](../api/types.md#yads.types.Date)
Calendar date.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `bits` | Logical width (`32` or `64`). | integer | No | 32 |

???+ example "date"
    ```yaml
    - name: "invoice_date"
      type: "date"
      params:
        bits: 64
    ```

#### [`time`](../api/types.md#yads.types.Time)
Wall-clock time with fractional precision.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `unit` | Granularity `s`, `ms`, `us`, `ns`. | string | No | `ms` |
| `bits` | Storage width (`32` or `64`). | integer | No | Target default |

???+ example "time"
    ```yaml
    - name: "alarm_time"
      type: "time"
      params:
        unit: "us"
        bits: 64
    ```

#### [`timestamp`](../api/types.md#yads.types.Timestamp)
Timezone-naive timestamp.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `unit` | Granularity `s`, `ms`, `us`, `ns`. | string | No | `ns` |

???+ example "timestamp"
    ```yaml
    - name: "processed_at"
      type: "timestamp"
      params:
        unit: "ms"
    ```

#### [`timestamptz`](../api/types.md#yads.types.TimestampTZ)
Timestamp with explicit timezone.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `unit` | Granularity `s`, `ms`, `us`, `ns`. | string | No | `ns` |
| `tz` | IANA timezone name. | string | Yes | `UTC` |

???+ example "timestamptz"
    ```yaml
    - name: "submitted_at"
      type: "timestamptz"
      params:
        tz: "UTC"
    ```

#### [`timestampltz`](../api/types.md#yads.types.TimestampLTZ)
Timestamp interpreted in the session's timezone.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `unit` | Granularity `s`, `ms`, `us`, `ns`. | string | No | `ns` |

???+ example "timestampltz"
    ```yaml
    - name: "user_created_at"
      type: "timestampltz"
      params:
        unit: "us"
    ```

#### [`timestampntz`](../api/types.md#yads.types.TimestampNTZ)
Timestamp with explicit "no timezone" semantics.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `unit` | Granularity `s`, `ms`, `us`, `ns`. | string | No | `ns` |

???+ example "timestampntz"
    ```yaml
    - name: "materialized_at"
      type: "timestampntz"
      params:
        unit: "s"
    ```

#### [`duration`](../api/types.md#yads.types.Duration)
Elapsed amount of time.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `unit` | Granularity `s`, `ms`, `us`, `ns`. | string | No | `ns` |

???+ example "duration"
    ```yaml
    - name: "session_length"
      type: "duration"
      params:
        unit: "ms"
    ```

#### [`interval`](../api/types.md#yads.types.Interval)
SQL-style intervals bounded by start and optional end units.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `interval_start` | Most significant unit. Year-Month: [`YEAR`, `MONTH`], or Day-Time: [`DAY`, `HOUR`, `MINUTE`, `SECOND`]. | string | Yes | — |
| `interval_end` | Least significant unit. Must be same category as `interval_start`. | string | No | Single-unit interval |

???+ example "interval"
    ```yaml
    - name: "contract_term"
      type: "interval"
      params:
        interval_start: "YEAR"
        interval_end: "MONTH"
    ```

### Collection types

#### [`array`](../api/types.md#yads.types.Array)
Ordered list of values sharing the same element type.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `element` | Nested type definition. | [column](#column-reference) | Yes | — |
| `params.size` | Max array length for fixed-size arrays. | integer | No | Unlimited |

???+ example "array"
    ```yaml
    - name: "tags"
      type: "array"
      element:
        type: "string"
      params:
        size: 10
    ```

#### [`struct`](../api/types.md#yads.types.Struct)
Named grouping of heterogenous fields.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `fields` | Ordered list of embedded column definitions. | array([column](#column-reference)) | Yes | — |

???+ example "struct"
    ```yaml
    - name: "address"
      type: "struct"
      fields:
        - name: "street"
          type: "string"
        - name: "city"
          type: "string"
        - name: "postal_code"
          type: "string"
    ```

#### [`map`](../api/types.md#yads.types.Map)
Key/value pairs.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `key` | Type definition for map keys. | [column](#column-reference) | Yes | — |
| `value` | Type definition for map values. | [column](#column-reference) | Yes | — |
| `params.keys_sorted` | Whether keys are guaranteed sorted. | boolean | No | false |

???+ example "map"
    ```yaml
    - name: "attributes"
      type: "map"
      key:
        type: "string"
      value:
        type: "variant"
      params:
        keys_sorted: true
    ```

#### [`tensor`](../api/types.md#yads.types.Tensor)
Multi-dimensional numeric data.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `element` | Base type for tensor entries. | [column](#column-reference) | Yes | — |
| `params.shape` | Positive integers describing each dimension. | array(integer) | Yes | — |

???+ example "tensor"
    ```yaml
    - name: "embedding"
      type: "tensor"
      element:
        type: "float"
      params:
        shape: [3, 128]
    ```

### Semi-structured, spatial, and identifiers

#### [`json`](../api/types.md#yads.types.JSON)
Semi-structured JSON payload. No additional parameters.

???+ example "json"
    ```yaml
    - name: "event_payload"
      type: "json"
    ```

#### [`variant`](../api/types.md#yads.types.Variant)
Union-style semi-structured payload. No additional parameters.

???+ example "variant"
    ```yaml
    - name: "document"
      type: "variant"
    ```

#### [`uuid`](../api/types.md#yads.types.UUID)
128-bit identifiers formatted as canonical UUID strings.

???+ example "uuid"
    ```yaml
    - name: "record_uuid"
      type: "uuid"
      constraints:
        not_null: true
    ```

#### [`void`](../api/types.md#yads.types.Void)
Represents `NULL` or placeholder fields.

???+ example "void"
    ```yaml
    - name: "reserved"
      type: "void"
    ```

#### [`geometry`](../api/types.md#yads.types.Geometry)
Planar geometry column.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `srid` | Spatial reference identifier. | integer or string | No | — |

???+ example "geometry"
    ```yaml
    - name: "parcel_shape"
      type: "geometry"
      params:
        srid: 4326
    ```

#### [`geography`](../api/types.md#yads.types.Geography)
Spherical geometry column.

| Parameter | Description | Type | Required | Default |
| --- | --- | --- | --- | --- |
| `srid` | Spatial reference identifier. | integer or string | No | — |

???+ example "geography"
    ```yaml
    - name: "customer_location"
      type: "geography"
      params:
        srid: 4326
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
