# File System Registry

Python `fsspec`-based registry for local file systems or cloud object stores.

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

Install `fsspec` with optional dependencies for the backend you need:

=== "uv"
    | Backend | Install |
    | --- | --- |
    | Local paths | `uv add 'yads[fs]'` |
    | S3 | `uv add 'yads[s3]'` |
    | Azure Blob Storage | `uv add 'yads[abfs]'` |
    | Google Cloud Storage | `uv add 'yads[gcs]'` |

=== "pip"
    | Backend | Install |
    | --- | --- |
    | Local paths | `pip install "yads[fs]"` |
    | S3 | `pip install "yads[s3]"` |
    | Azure Blob Storage | `pip install "yads[abfs]"` |
    | Google Cloud Storage | `pip install "yads[gcs]"` |

::: yads.registries.filesystem_registry.FileSystemRegistry
