"""FileSystem-based registry implementation using fsspec."""

from __future__ import annotations

import logging
import urllib.parse
import warnings
from dataclasses import replace
from typing import IO, TYPE_CHECKING, Any, Protocol, cast

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
from ..serializers import SpecSerializer
from .base import BaseRegistry

if TYPE_CHECKING:
    from ..spec import YadsSpec


class FileSystemProtocol(Protocol):
    """Minimal filesystem protocol used by the registry."""

    def exists(self, path: str, **kwargs: Any) -> bool: ...

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[str]: ...

    def makedirs(self, path: str, exist_ok: bool = False) -> None: ...

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> IO[str]: ...


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
        ```python
        # Local registry
        registry = FileSystemRegistry("/data/specs")

        # S3 with specific profile
        registry = FileSystemRegistry(
            "s3://my-bucket/schemas/",
            profile="production"
        )

        # With custom logger
        import logging
        logger = logging.getLogger("my_app.registry")
        registry = FileSystemRegistry("/data/specs", logger=logger)
        ```
    """

    # Characters not allowed in spec names (filesystem-unsafe)
    INVALID_NAME_CHARS = frozenset({"/", "\\", ":", "*", "?", "<", ">", "|", "\0"})

    def __init__(
        self,
        base_path: str,
        logger: logging.Logger | None = None,
        serializer: SpecSerializer | None = None,
        **fsspec_kwargs: Any,
    ):
        """Initialize the FileSystemRegistry.

        Args:
            base_path: Base path for the registry storage.
            logger: Optional logger instance.
            serializer: Optional spec serializer override used for YAML exports.
            **fsspec_kwargs: Additional fsspec configuration.
        """
        # Initialize logger
        self.logger = logger or logging.getLogger("yads.registries.filesystem")

        # Initialize filesystem
        try:
            fs_result = cast(
                tuple[Any, Any],
                fsspec.core.url_to_fs(  # pyright: ignore[reportUnknownMemberType]
                    base_path, **fsspec_kwargs
                ),
            )
            fs_obj_any, resolved_base_path_any = fs_result
            fs_obj = cast(FileSystemProtocol, fs_obj_any)
            resolved_base_path = str(resolved_base_path_any)
            # Validate base path exists by attempting to access it
            fs_obj.exists(resolved_base_path)
        except Exception as e:
            raise RegistryConnectionError(
                f"Failed to connect to registry at '{base_path}': {e}"
            ) from e

        self.fs: FileSystemProtocol = fs_obj
        self.base_path: str = resolved_base_path
        self._serializer = serializer or SpecSerializer()
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
            InvalidSpecNameError: If `spec.name` contains invalid characters.
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
            versions: list[int] = []
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
        return self._normalized_spec_dict(spec1) == self._normalized_spec_dict(spec2)

    def _normalized_spec_dict(self, spec: YadsSpec) -> dict[str, Any]:
        """Serialize a spec into a dict suitable for equality checks."""
        normalized = self._serializer.serialize(spec)
        normalized["version"] = 0
        return normalized

    def _write_spec(self, encoded_name: str, version: int, spec: YadsSpec) -> None:
        """Write a spec to the registry.

        Args:
            encoded_name: URL-encoded spec name.
            version: Version number to assign.
            spec: The spec to write.
        """
        yaml_content = self._serialize_spec(spec, version)
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
        serialized_spec = self._serializer.serialize(replace(spec, version=version))
        return yaml.safe_dump(serialized_spec, sort_keys=False, default_flow_style=False)
