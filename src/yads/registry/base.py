"""Abstract registry interface for schema storage and retrieval.

This module defines the base interface for schema registries in yads. Registries
are responsible for persisting and retrieving schema specifications from various
storage backends.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from yads.spec import SchemaSpec


class BaseRegistry(ABC):
    """Abstract base class for schema registry implementations.

    A registry provides persistent storage and retrieval for schema specifications,
    enabling centralized schema management and version control. Implementations
    can target various storage backends including databases, object stores, file
    systems, or specialized schema registry services.

    Registries are responsible for:
    - Persisting schemas with proper versioning
    - Retrieving specific schema versions
    - Managing schema metadata and lifecycle
    - Ensuring data consistency and availability

    Key design considerations for implementations:
    - Versioning: How to handle schema evolution and version conflicts
    - Concurrency: Safe concurrent access to shared schema storage
    - Performance: Efficient retrieval and caching strategies
    - Security: Access control and authentication mechanisms
    """

    @abstractmethod
    def register_schema(self, spec: SchemaSpec) -> str:
        """Register (persist) a schema specification to the storage backend.

        This method stores the complete schema specification in the registry's
        persistent storage. The implementation should handle versioning logic,
        typically using the schema's name and version to create a unique storage
        identifier or path.

        The method should be idempotent when called with the same schema name
        and version, either succeeding silently or raising an appropriate error
        if the version already exists but differs in content.

        Args:
            spec: The complete SchemaSpec object to register.

        Returns:
            A unique identifier for the registered schema.
        """
        raise NotImplementedError

    @abstractmethod
    def get_schema(self, name: str, version: str) -> SchemaSpec:
        """Retrieve a specific version of a schema from the storage backend.

        This method fetches and reconstructs a previously stored schema
        specification. The implementation should handle deserialization
        from the storage format back to a SchemaSpec object.

        Args:
            name: The name of the schema to retrieve. Should match the name
                  used when the schema was originally registered.
            version: The specific version of the schema to retrieve. Version
                    strings should match exactly as stored.

        Returns:
            A SchemaSpec object for the requested schema.
        """
        raise NotImplementedError
