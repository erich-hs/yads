---
icon: "lucide/folder-archive"
---
# Registries

Registries store and version canonical `YadsSpec` definitions so they can be
shared across teams or environments.

Yads ships a single registry today:
[`FileSystemRegistry`](filesystem.md#yads.registries.filesystem_registry.FileSystemRegistry),
which uses `fsspec` to manage specs on local disks or common cloud filesystems.
Additional backends can be added as your deployment needs grow.
