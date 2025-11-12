"""Abstract registry interface for spec storage and retrieval."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..spec import YadsSpec


class BaseRegistry(ABC):
    """Abstract base class for spec registry implementations."""

    @abstractmethod
    def register(self, spec: YadsSpec) -> int:
        """Register a spec in the registry and assign it a version number.

        Args:
            spec: The YadsSpec to register.

        Returns:
            The assigned or existing version number.

        Raises:
            RegistryError: If registration fails due to registry issues.
            InvalidSpecNameError: If the spec name contains invalid characters.
        """
        ...

    @abstractmethod
    def get(self, name: str, version: int | None = None) -> YadsSpec:
        """Retrieve a spec by name and optional version.

        If no version is specified, returns the latest version of the spec.

        Args:
            name: The fully qualified spec name.
            version: Optional version number. If None, retrieves the latest version.

        Returns:
            The requested YadsSpec with its version field set appropriately.

        Raises:
            SpecNotFoundError: If the spec name or version doesn't exist.
            RegistryError: If retrieval fails due to registry issues.
        """
        ...

    @abstractmethod
    def list_versions(self, name: str) -> list[int]:
        """List all available versions for a spec.

        Args:
            name: The fully qualified spec name.

        Returns:
            Sorted list of version numbers, or empty list if spec doesn't exist.

        Raises:
            RegistryError: If listing fails due to registry issues.
        """
        ...

    @abstractmethod
    def exists(self, name: str) -> bool:
        """Check if a spec exists in the registry.

        Args:
            name: The fully qualified spec name.

        Returns:
            True if the spec exists, False otherwise.

        Raises:
            RegistryError: If the check fails due to registry issues.
        """
        ...
