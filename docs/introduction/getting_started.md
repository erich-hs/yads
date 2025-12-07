---
icon: "lucide/zap"
---

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

## Authoring schemas

