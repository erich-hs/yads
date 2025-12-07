---
icon: "lucide/zap"
---
# Quick Start

## Install `yads`

`yads` Python API is available on PyPI. Install with `pip` or `uv`:

=== "uv"
    ```bash
    uv add yads
    ```
    Optionally, install dependencies for your target formats:

    ```bash
    uv add yads[pyarrow]
    ```

=== "pip"
    ```bash
    pip install yads
    ```
    Optionally, install dependencies for your target formats:

    ```bash
    pip install yads[pyarrow]
    ```

Check the [converters documentation](converters/index.md) for install instructions and supported versions of optional depencies.

## Author a schema

Typical workflows start with a `yads` [spec](specification.md) authored in YAML format, [loaded](../api/loaders/index.md) from a known, typed source, or constructed from [core objects](../api/spec.md).

=== "From a `yads.yaml` file"

=== "From core objects"

=== "From a typed source"

## Convert to a target format

## Register the `yads` spec