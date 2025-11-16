"""FileSystem-based registry implementation using fsspec.

This module provides a filesystem registry for yads specifications that works
across local filesystems, S3, GCS, and Azure Blob Storage.

Example:
    >>> from yads.registries import FileSystemRegistry
    >>> import yads
    >>>
    >>> # Local filesystem
    >>> registry = FileSystemRegistry("/path/to/registry")
    >>>
    >>> # S3 (requires yads[s3])
    >>> registry = FileSystemRegistry("s3://bucket/registry/")
    >>>
    >>> # Register a spec
    >>> spec = yads.from_yaml("specs/customers.yaml")
    >>> version = registry.register(spec)
    >>> print(f"Registered as version {version}")
    >>>
    >>> # Retrieve latest version
    >>> latest = registry.get("catalog.crm.customers")
    >>> print(f"Latest version: {latest.version}")
    >>>
    >>> # Retrieve specific version
    >>> v1 = registry.get("catalog.crm.customers", version=1)
"""

from __future__ import annotations

# pyright: reportUnknownArgumentType=none, reportUnknownMemberType=none
# pyright: reportUnknownVariableType=none

import logging
import urllib.parse
import warnings
from typing import TYPE_CHECKING, Any

import fsspec  # type: ignore[import]
import yaml

from ..exceptions import (
    DuplicateSpecWarning,
    InvalidSpecNameError,
    RegistryConnectionError,
    RegistryError,
    SpecNotFoundError,
)
from ..loaders import from_yaml_string
from .base import BaseRegistry

if TYPE_CHECKING:
    from ..spec import YadsSpec


class FileSystemRegistry(BaseRegistry):
    """Filesystem-based registry using fsspec for multi-cloud support.

    Stores specs in a simple directory structure:

        {base_path}/
        └── {url_encoded_spec_name}/
            └── versions/
                ├── 1.yaml
                ├── 2.yaml
                └── 3.yaml

    The registry assigns monotonically increasing version numbers automatically.
    If a spec with identical content (excluding version) is registered, the
    existing version number is returned.

    Thread Safety:
        This implementation is not thread-safe. Concurrent registrations may
        result in race conditions. For production use, ensure only one process
        (e.g., a CI/CD pipeline) has write access to the registry.

    Args:
        base_path: Base path for the registry. Can be local path or cloud URL:
            - Local: "/path/to/registry"
            - S3: "s3://bucket/registry/"
            - GCS: "gs://bucket/registry/"
            - Azure: "az://container/registry/"
        logger: Optional logger for registry operations. If None, creates
            a default logger at "yads.registries.filesystem".
        **fsspec_kwargs: Additional arguments passed to fsspec for authentication
            and configuration (e.g., profile="production" for S3).

    Raises:
        RegistryConnectionError: If the base path is invalid or inaccessible.

    Example:
        >>> # Local registry
        >>> registry = FileSystemRegistry("/data/specs")
        >>>
        >>> # S3 with specific profile
        >>> registry = FileSystemRegistry(
        ...     "s3://my-bucket/schemas/",
        ...     profile="production"
        ... )
        >>>
        >>> # With custom logger
        >>> import logging
        >>> logger = logging.getLogger("my_app.registry")
        >>> registry = FileSystemRegistry("/data/specs", logger=logger)
    """

    # Characters not allowed in spec names (filesystem-unsafe)
    INVALID_NAME_CHARS = frozenset({"/", "\\", ":", "*", "?", "<", ">", "|", "\0"})

    def __init__(
        self,
        base_path: str,
        logger: logging.Logger | None = None,
        **fsspec_kwargs: Any,
    ):
        """Initialize the FileSystemRegistry.

        Args:
            base_path: Base path for the registry storage.
            logger: Optional logger instance.
            **fsspec_kwargs: Additional fsspec configuration.
        """
        # Initialize logger
        self.logger = logger or logging.getLogger("yads.registries.filesystem")

        # Initialize filesystem
        try:
            fs_obj, resolved_base_path = fsspec.core.url_to_fs(base_path, **fsspec_kwargs)
            # Validate base path exists by attempting to access it
            fs_obj.exists(resolved_base_path)
        except Exception as e:
            raise RegistryConnectionError(
                f"Failed to connect to registry at '{base_path}': {e}"
            ) from e

        self.fs = fs_obj
        self.base_path = resolved_base_path
        self.logger.info(f"Initialized FileSystemRegistry at: {self.base_path}")

    def register(self, spec: YadsSpec) -> int:
        """Register a spec and assign it a version number.

        If the spec content matches the latest version (excluding the version
        field), returns the existing version number without creating a new entry.

        Args:
            spec: The YadsSpec to register.

        Returns:
            The assigned or existing version number.

        Raises:
            InvalidSpecNameError: If spec.name contains invalid characters.
            RegistryError: If registration fails.
        """
        # Validate spec name
        self._validate_spec_name(spec.name)

        # URL-encode the spec name for filesystem safety
        encoded_name = urllib.parse.quote(spec.name, safe="")

        # Get latest version
        latest_version = self._get_latest_version(encoded_name)

        # Check if content is identical to latest
        if latest_version is not None:
            try:
                latest_spec = self._read_spec(encoded_name, latest_version)
                if self._specs_equal(spec, latest_spec):
                    warnings.warn(
                        f"Spec '{spec.name}' content is identical to version "
                        f"{latest_version}. Skipping registration.",
                        DuplicateSpecWarning,
                        stacklevel=2,
                    )
                    self.logger.warning(
                        f"Duplicate content for '{spec.name}'. "
                        f"Returning existing version {latest_version}."
                    )
                    return latest_version
            except Exception as e:
                self.logger.debug(f"Could not read latest version for comparison: {e}")

        # Assign new version
        new_version = (latest_version or 0) + 1

        # Write the new version
        try:
            self._write_spec(encoded_name, new_version, spec)
            self.logger.info(f"Registered '{spec.name}' as version {new_version}")
            return new_version
        except Exception as e:
            raise RegistryError(f"Failed to register spec '{spec.name}': {e}") from e

    def get(self, name: str, version: int | None = None) -> YadsSpec:
        """Retrieve a spec by name and optional version.

        Args:
            name: The fully qualified spec name.
            version: Optional version number. If None, retrieves latest.

        Returns:
            The requested YadsSpec with version field set.

        Raises:
            SpecNotFoundError: If the spec or version doesn't exist.
            RegistryError: If retrieval fails.
        """
        encoded_name = urllib.parse.quote(name, safe="")

        # Determine version to retrieve
        if version is None:
            version = self._get_latest_version(encoded_name)
            if version is None:
                raise SpecNotFoundError(f"Spec '{name}' not found in registry")
            self.logger.debug(f"Retrieving latest version {version} of '{name}'")
        else:
            self.logger.debug(f"Retrieving version {version} of '{name}'")

        # Read and return the spec
        try:
            spec = self._read_spec(encoded_name, version)
            self.logger.info(f"Retrieved '{name}' version {version}")
            return spec
        except FileNotFoundError:
            raise SpecNotFoundError(
                f"Spec '{name}' version {version} not found in registry"
            )
        except Exception as e:
            raise RegistryError(
                f"Failed to retrieve spec '{name}' version {version}: {e}"
            ) from e

    def list_versions(self, name: str) -> list[int]:
        """List all available versions for a spec.

        Args:
            name: The fully qualified spec name.

        Returns:
            Sorted list of version numbers, or empty list if not found.

        Raises:
            RegistryError: If listing fails.
        """
        encoded_name = urllib.parse.quote(name, safe="")
        versions_dir = f"{self.base_path}/{encoded_name}/versions"

        try:
            if not self.fs.exists(versions_dir):
                self.logger.debug(f"No versions found for '{name}'")
                return []

            # List all files in versions directory
            files = self.fs.ls(versions_dir, detail=False)

            # Extract version numbers from filenames
            versions = []
            for file_path in files:
                filename = file_path.split("/")[-1]
                if filename.endswith(".yaml"):
                    try:
                        version_num = int(filename[:-5])  # Remove .yaml extension
                        versions.append(version_num)
                    except ValueError:
                        self.logger.warning(f"Skipping non-version file: {filename}")

            versions.sort()
            self.logger.debug(f"Found {len(versions)} versions for '{name}'")
            return versions

        except Exception as e:
            raise RegistryError(f"Failed to list versions for '{name}': {e}") from e

    def exists(self, name: str) -> bool:
        """Check if a spec exists in the registry.

        Args:
            name: The fully qualified spec name.

        Returns:
            True if the spec exists, False otherwise.
        """
        encoded_name = urllib.parse.quote(name, safe="")
        spec_dir = f"{self.base_path}/{encoded_name}"

        try:
            result = self.fs.exists(spec_dir)
            self.logger.debug(f"Spec '{name}' exists: {result}")
            return result
        except Exception as e:
            self.logger.error(f"Failed to check existence of '{name}': {e}")
            return False

    # Private helper methods

    def _validate_spec_name(self, name: str) -> None:
        """Validate that spec name doesn't contain filesystem-unsafe characters.

        Args:
            name: The spec name to validate.

        Raises:
            InvalidSpecNameError: If name contains invalid characters.
        """
        if not name:
            raise InvalidSpecNameError("Spec name cannot be empty")

        invalid_found = set(name) & self.INVALID_NAME_CHARS
        if invalid_found:
            chars_str = ", ".join(repr(c) for c in sorted(invalid_found))
            raise InvalidSpecNameError(
                f"Spec name '{name}' contains invalid characters: {chars_str}"
            )

    def _get_latest_version(self, encoded_name: str) -> int | None:
        """Get the latest version number for a spec.

        Args:
            encoded_name: URL-encoded spec name.

        Returns:
            Latest version number, or None if no versions exist.
        """
        versions = self.list_versions(urllib.parse.unquote(encoded_name))
        return max(versions) if versions else None

    def _specs_equal(self, spec1: YadsSpec, spec2: YadsSpec) -> bool:
        """Compare two specs for equality, excluding the version field.

        Args:
            spec1: First spec to compare.
            spec2: Second spec to compare.

        Returns:
            True if specs are equal (excluding version), False otherwise.
        """
        # Create copies with same version for comparison
        # Since YadsSpec is a frozen dataclass, we need to compare field by field
        return (
            spec1.name == spec2.name
            and spec1.yads_spec_version == spec2.yads_spec_version
            and spec1.columns == spec2.columns
            and spec1.description == spec2.description
            and spec1.external == spec2.external
            and spec1.storage == spec2.storage
            and spec1.partitioned_by == spec2.partitioned_by
            and spec1.table_constraints == spec2.table_constraints
            and spec1.metadata == spec2.metadata
        )

    def _write_spec(self, encoded_name: str, version: int, spec: YadsSpec) -> None:
        """Write a spec to the registry.

        Args:
            encoded_name: URL-encoded spec name.
            version: Version number to assign.
            spec: The spec to write.
        """
        # Serialize spec to YAML
        yaml_content = self._serialize_spec(spec, version)

        # Construct file path
        versions_dir = f"{self.base_path}/{encoded_name}/versions"
        file_path = f"{versions_dir}/{version}.yaml"

        # Ensure directory exists
        self.fs.makedirs(versions_dir, exist_ok=True)

        # Write file
        with self.fs.open(file_path, "w") as f:
            f.write(yaml_content)

    def _read_spec(self, encoded_name: str, version: int) -> YadsSpec:
        """Read a spec from the registry.

        Args:
            encoded_name: URL-encoded spec name.
            version: Version number to read.

        Returns:
            The loaded YadsSpec.

        Raises:
            FileNotFoundError: If the version file doesn't exist.
        """
        file_path = f"{self.base_path}/{encoded_name}/versions/{version}.yaml"

        with self.fs.open(file_path, "r") as f:
            yaml_content = f.read()

        # Load spec from YAML
        return from_yaml_string(yaml_content)

    def _serialize_spec(self, spec: YadsSpec, version: int) -> str:
        """Serialize a spec to YAML string with specified version.

        Args:
            spec: The spec to serialize.
            version: Version number to set in the YAML.

        Returns:
            YAML string representation.
        """
        # Create a dictionary representation
        # We'll manually build this to control the order and formatting
        spec_dict: dict[str, Any] = {
            "name": spec.name,
            "version": version,
            "yads_spec_version": spec.yads_spec_version,
        }

        if spec.description:
            spec_dict["description"] = spec.description

        if spec.external:
            spec_dict["external"] = spec.external

        if spec.metadata:
            spec_dict["metadata"] = spec.metadata

        if spec.storage:
            storage_dict: dict[str, Any] = {}
            if spec.storage.format:
                storage_dict["format"] = spec.storage.format
            if spec.storage.location:
                storage_dict["location"] = spec.storage.location
            if spec.storage.tbl_properties:
                storage_dict["tbl_properties"] = spec.storage.tbl_properties
            if storage_dict:
                spec_dict["storage"] = storage_dict

        if spec.partitioned_by:
            partitions = []
            for p in spec.partitioned_by:
                p_dict: dict[str, Any] = {"column": p.column}
                if p.transform:
                    p_dict["transform"] = p.transform
                if p.transform_args:
                    p_dict["transform_args"] = p.transform_args
                partitions.append(p_dict)
            spec_dict["partitioned_by"] = partitions

        if spec.table_constraints:
            constraints = []
            for tc in spec.table_constraints:
                from ..constraints import (
                    ForeignKeyTableConstraint,
                    PrimaryKeyTableConstraint,
                )

                constraint_type = (
                    tc.__class__.__name__.replace("TableConstraint", "")
                    .lower()
                    .replace("primarykey", "primary_key")
                    .replace("foreignkey", "foreign_key")
                )
                c_dict: dict[str, Any] = {
                    "type": constraint_type,
                }

                if isinstance(tc, (PrimaryKeyTableConstraint, ForeignKeyTableConstraint)):
                    if tc.name:
                        c_dict["name"] = tc.name
                    c_dict["columns"] = list(tc.columns)

                if isinstance(tc, ForeignKeyTableConstraint) and tc.references:
                    references_dict: dict[str, Any] = {
                        "table": tc.references.table,
                    }
                    if tc.references.columns:
                        references_dict["columns"] = list(tc.references.columns)
                    c_dict["references"] = references_dict
                constraints.append(c_dict)
            spec_dict["table_constraints"] = constraints

        # Add columns (simplified - this is complex, but we'll use yaml.dump)
        # For now, we'll just store the full spec and reconstruct
        # In practice, for a real implementation, you'd want to serialize
        # each column properly. For the MVP, let's use a simpler approach:
        # Load the spec, modify version, dump back to YAML

        # Actually, let's just build a minimal dict and let the loader handle it
        columns = []
        for col in spec.columns:
            col_dict = self._serialize_column(col)
            columns.append(col_dict)

        spec_dict["columns"] = columns

        return yaml.dump(spec_dict, default_flow_style=False, sort_keys=False)

    def _serialize_column(self, column: Any) -> dict[str, Any]:
        """Serialize a column to a dictionary."""
        col_dict = {"name": column.name}

        # Serialize type - if it returns a dict, merge it; if string, set as 'type'
        type_data = self._serialize_type(column.type)
        if isinstance(type_data, str):
            col_dict["type"] = type_data
        else:
            # Merge the dict (contains 'type', and possibly 'params', 'element', etc.)
            col_dict.update(type_data)

        if column.description:
            col_dict["description"] = column.description

        if column.metadata:
            col_dict["metadata"] = column.metadata

        if column.constraints:
            constraints_dict: dict[str, Any] = {}
            for constraint in column.constraints:
                constraint_name = constraint.__class__.__name__.replace(
                    "Constraint", ""
                ).lower()
                if constraint_name == "notnull":
                    constraints_dict["not_null"] = True
                elif constraint_name == "primarykey":
                    constraints_dict["primary_key"] = True
                elif constraint_name == "default":
                    constraints_dict["default"] = constraint.value
                elif constraint_name == "identity":
                    identity_dict: dict[str, Any] = {}
                    if constraint.always is not None:
                        identity_dict["always"] = constraint.always
                    if constraint.start is not None:
                        identity_dict["start"] = constraint.start
                    if constraint.increment is not None:
                        identity_dict["increment"] = constraint.increment
                    constraints_dict["identity"] = identity_dict
                elif constraint_name == "foreignkey":
                    fk_dict: dict[str, Any] = {}
                    if constraint.name:
                        fk_dict["name"] = constraint.name
                    fk_dict["references"] = {"table": constraint.references.table}
                    if constraint.references.columns:
                        fk_dict["references"]["columns"] = list(
                            constraint.references.columns
                        )
                    constraints_dict["foreign_key"] = fk_dict
            if constraints_dict:
                col_dict["constraints"] = constraints_dict

        if column.generated_as:
            gen_dict = {"column": column.generated_as.column}
            if column.generated_as.transform:
                gen_dict["transform"] = column.generated_as.transform
            if column.generated_as.transform_args:
                gen_dict["transform_args"] = column.generated_as.transform_args
            col_dict["generated_as"] = gen_dict

        return col_dict

    def _serialize_type(self, yads_type: Any) -> dict[str, Any] | str:
        """Serialize a yads type to dict or string."""
        # For simple types, return string
        type_name = yads_type.__class__.__name__.lower()

        # Get the canonical name from TYPE_ALIASES or use the class name
        from .. import types as ytypes

        # Find canonical name
        canonical_name = None
        for alias, (type_class, _) in ytypes.TYPE_ALIASES.items():
            if type_class == yads_type.__class__:
                canonical_name = alias
                break

        if canonical_name is None:
            canonical_name = type_name

        # Check if type has parameters
        has_params = False
        type_dict = {"type": canonical_name}
        params = {}

        # Add parameters based on type
        if hasattr(yads_type, "length") and yads_type.length is not None:
            params["length"] = yads_type.length
            has_params = True
        if hasattr(yads_type, "bits") and yads_type.bits is not None:
            # Only add if not default
            params["bits"] = yads_type.bits
            has_params = True
        if hasattr(yads_type, "signed") and hasattr(yads_type, "bits"):
            if not yads_type.signed:  # Only add if False
                params["signed"] = yads_type.signed
                has_params = True
        if hasattr(yads_type, "precision") and yads_type.precision is not None:
            params["precision"] = yads_type.precision
            has_params = True
        if hasattr(yads_type, "scale") and yads_type.scale is not None:
            params["scale"] = yads_type.scale
            has_params = True
        if hasattr(yads_type, "unit") and yads_type.unit is not None:
            params["unit"] = str(yads_type.unit.value)
            has_params = True
        if hasattr(yads_type, "tz") and yads_type.tz is not None:
            params["tz"] = yads_type.tz
            has_params = True
        if hasattr(yads_type, "srid") and yads_type.srid is not None:
            params["srid"] = yads_type.srid
            has_params = True
        if hasattr(yads_type, "size") and yads_type.size is not None:
            params["size"] = yads_type.size
            has_params = True
        if hasattr(yads_type, "keys_sorted") and yads_type.keys_sorted:
            params["keys_sorted"] = yads_type.keys_sorted
            has_params = True
        if hasattr(yads_type, "interval_start") and yads_type.interval_start is not None:
            params["interval_start"] = yads_type.interval_start.value
            has_params = True
        if hasattr(yads_type, "interval_end") and yads_type.interval_end is not None:
            params["interval_end"] = yads_type.interval_end.value
            has_params = True
        if hasattr(yads_type, "shape") and yads_type.shape is not None:
            params["shape"] = list(yads_type.shape)
            has_params = True

        if params:
            type_dict["params"] = params

        # Handle complex types
        if hasattr(yads_type, "element") and yads_type.element is not None:
            type_dict["element"] = self._serialize_type(yads_type.element)
            has_params = True
        if hasattr(yads_type, "key") and yads_type.key is not None:
            key_data = self._serialize_type(yads_type.key)
            # Map keys must be dict format, normalize string to dict
            if isinstance(key_data, str):
                type_dict["key"] = {"type": key_data}
            else:
                type_dict["key"] = key_data
            has_params = True
        if hasattr(yads_type, "value") and yads_type.value is not None:
            value_data = self._serialize_type(yads_type.value)
            # Map values must be dict format, normalize string to dict
            if isinstance(value_data, str):
                type_dict["value"] = {"type": value_data}
            else:
                type_dict["value"] = value_data
            has_params = True
        if hasattr(yads_type, "fields") and yads_type.fields:
            fields = []
            for field in yads_type.fields:
                field_dict: dict[str, Any] = {
                    "name": field.name,
                }
                field_type_data = self._serialize_type(field.type)
                if isinstance(field_type_data, dict):
                    field_dict.update(field_type_data)
                else:
                    field_dict["type"] = field_type_data
                if field.description:
                    field_dict["description"] = field.description
                if field.metadata:
                    field_dict["metadata"] = field.metadata
                if field.constraints:
                    # Serialize constraints
                    pass  # Simplified for now
                fields.append(field_dict)
            type_dict["fields"] = fields
            has_params = True

        # If no params or complex structure, return just the type name
        if not has_params and len(type_dict) == 1:
            return canonical_name

        return type_dict
