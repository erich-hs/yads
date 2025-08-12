"""Base loader for `YadsSpec` instances."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from .common import SpecBuilder

if TYPE_CHECKING:
    from ..spec import YadsSpec


class BaseLoader(ABC):
    """Abstract base class for all loaders.

    Subclasses must implement the `load` method, which is responsible for
    parsing input from a given source and returning a `YadsSpec` instance.
    """

    @abstractmethod
    def load(self) -> YadsSpec:
        """Load a `YadsSpec` from a source.

        Returns:
            A validated immutable `YadsSpec` instance.
        """
        ...


class DictLoader(BaseLoader):
    """Loads a `YadsSpec` from a Python dictionary."""

    def __init__(self, data: dict[str, Any]):
        """
        Args:
            data: The dictionary representation of the spec.
        """
        self._data = data

    def load(self) -> YadsSpec:
        """Builds the spec from the dictionary.

        Returns:
            A `YadsSpec` instance.
        """
        return SpecBuilder(self._data).build()
