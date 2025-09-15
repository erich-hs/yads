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

    def load(self, content: str) -> YadsSpec:
        """Parses the YAML content and builds the spec.

        Args:
            content: The YAML string content.

        Returns:
            A `YadsSpec` instance.

        Raises:
            SpecParsingError: If the YAML content is invalid or does not
                              parse to a dictionary.
        """
        data = yaml.safe_load(content)
        if not isinstance(data, dict):
            raise SpecParsingError("Loaded YAML content did not parse to a dictionary.")
        return DictLoader().load(data)
