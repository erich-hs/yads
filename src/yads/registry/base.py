from abc import ABC, abstractmethod

from yads.spec import SchemaSpec


class BaseRegistry(ABC):
    """
    Abstract base class for all schema registries.

    A registry is responsible for publishing and retrieving schema definitions
    to and from a persistent storage backend (e.g., S3, a database).
    """

    @abstractmethod
    def register_schema(self, spec: SchemaSpec) -> str:
        """
        Registers (publishes) a schema to the backend.

        This method should handle versioning logic, typically by using the
        schema's name and version to construct a unique storage path.

        Args:
            spec: The SchemaSpec object to register.

        Returns:
            A string identifier for the registered schema, such as its
            path or URL.
        """
        raise NotImplementedError

    @abstractmethod
    def get_schema(self, name: str, version: str) -> SchemaSpec:
        """
        Retrieves a specific version of a schema from the backend.

        Args:
            name: The name of the schema to retrieve.
            version: The version of the schema to retrieve.

        Returns:
            A SchemaSpec object for the requested schema.
        """
        raise NotImplementedError
