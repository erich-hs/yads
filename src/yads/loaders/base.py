"""Base loader for `YadsSpec` instances."""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Generator, Literal

from ..exceptions import LoaderConfigError
from .common import SpecBuilder

if TYPE_CHECKING:
    from ..spec import YadsSpec


@dataclass(frozen=True)
class BaseLoaderConfig:
    """Base configuration for all yads loaders.

    This configuration is immutable and provides common settings for loaders
    that support configuration-based behavior.

    Args:
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
    """

    mode: Literal["raise", "coerce"] = "coerce"

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if self.mode not in {"raise", "coerce"}:
            raise LoaderConfigError("mode must be one of 'raise' or 'coerce'.")


class BaseLoader(ABC):
    """Abstract base class for all loaders.

    Subclasses must implement the `load` method, which is responsible for
    parsing input from a given source and returning a `YadsSpec` instance.
    """

    @abstractmethod
    def load(self, *args: Any, **kwargs: Any) -> YadsSpec:
        """Load a `YadsSpec` from a source.

        Returns:
            A validated immutable `YadsSpec` instance.
        """
        ...


class ConfigurableLoader(BaseLoader, ABC):
    """Base class for loaders that support configuration.

    This class provides configuration management and context handling for
    loaders that need to handle mode overrides and other configuration-based
    behavior.
    """

    def __init__(self, config: BaseLoaderConfig | None = None) -> None:
        """Initialize the ConfigurableLoader.

        Args:
            config: Configuration object. If None, uses default BaseLoaderConfig.
        """
        self.config = config or BaseLoaderConfig()
        self._current_field_name: str | None = None

    @contextmanager
    def load_context(
        self,
        *,
        mode: Literal["raise", "coerce"] | None = None,
        field: str | None = None,
    ) -> Generator[None, None, None]:
        """Temporarily set loading mode and field context.

        This context manager centralizes handling of loader state used for
        warnings and coercions, ensuring that values are restored afterwards.

        Args:
            mode: Optional override for the current loading mode.
            field: Optional field name for contextual warnings.
        """
        # Snapshot current state
        previous_config = self.config
        previous_field = self._current_field_name

        try:
            if mode is not None:
                if mode not in ("raise", "coerce"):
                    raise LoaderConfigError("mode must be one of 'raise' or 'coerce'.")
                self.config = replace(self.config, mode=mode)
            if field is not None:
                self._current_field_name = field
            yield
        finally:
            # Restore prior state
            self.config = previous_config
            self._current_field_name = previous_field


class DictLoader(BaseLoader):
    """Loads a `YadsSpec` from a Python dictionary."""

    def load(self, data: dict[str, Any]) -> YadsSpec:
        """Builds the spec from the dictionary.

        Args:
            data: The dictionary representation of the spec.

        Returns:
            A `YadsSpec` instance.
        """
        return SpecBuilder(data).build()
