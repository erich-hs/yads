"""Loads a `YadsSpec` from a YAML source."""

from __future__ import annotations

from typing import TYPE_CHECKING
import yaml

from ..exceptions import SpecParsingError
from .base import BaseLoader, DictLoader

if TYPE_CHECKING:
    from ..spec import YadsSpec


class YamlLoader(BaseLoader):
    """Loads a `YadsSpec` from a YAML string."""

    def __init__(self, content: str):
        """Initializes the loader with YAML content.

        Args:
            content: The YAML string content.
        """
        self._content = content

    def load(self) -> YadsSpec:
        """Parses the YAML content and builds the spec.

        Returns:
            A `YadsSpec` instance.

        Raises:
            SpecParsingError: If the YAML content is invalid or does not
                              parse to a dictionary.
        """
        data = yaml.safe_load(self._content)
        if not isinstance(data, dict):
            raise SpecParsingError("Loaded YAML content did not parse to a dictionary.")
        return DictLoader(data).load()
