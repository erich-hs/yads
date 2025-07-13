"""
This module provides the base class for all yads objects.
"""

from collections.abc import MutableMapping
from typing import Any, Dict, Iterator


class BaseObject(MutableMapping):
    """
    An internal base class that provides both dictionary-like and
    attribute-like access to its data.
    """

    def __init__(self, **data: Any):
        """
        Initializes the BaseObject with keyword arguments.

        Args:
            **data: The key-value pairs to store in the object.
        """
        self._data: Dict[str, Any] = data

    def __setitem__(self, key: str, value: Any) -> None:
        """Sets an item in the internal dictionary."""
        self._data[key] = value

    def __getitem__(self, key: str) -> Any:
        """Gets an item from the internal dictionary."""
        return self._data[key]

    def __delitem__(self, key: str) -> None:
        """Deletes an item from the internal dictionary."""
        del self._data[key]

    def __iter__(self) -> Iterator[str]:
        """Returns an iterator over the keys of the internal dictionary."""
        return iter(self._data)

    def __len__(self) -> int:
        """Returns the number of items in the internal dictionary."""
        return len(self._data)

    def __getattr__(self, name: str) -> Any:
        """
        Provides attribute-like access to the internal dictionary.

        Args:
            name: The attribute name (which is a dictionary key).

        Returns:
            The value associated with the key.

        Raises:
            AttributeError: If the key is not found.
        """
        if name in self._data:
            return self[name]
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    def __setattr__(self, name: str, value: Any) -> None:
        """
        Sets an attribute, which is stored as a key-value pair in the internal
        dictionary. A special case is made for the '_data' attribute itself.
        """
        if name == "_data":
            super().__setattr__(name, value)
        else:
            self[name] = value

    def __repr__(self) -> str:
        """Returns a string representation of the object."""
        return f"{type(self).__name__}({self._data!r})"
