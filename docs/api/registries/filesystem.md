# File System Registry

This registry uses `fsspec` to support cloud object stores. Install `fsspec` with the backend you need:

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
