"""Base converter interface for spec transformations.

This module defines the abstract base class for all yads converters. Converters
are responsible for transforming YadsSpec objects into target formats such as
SQL DDL, framework-specific schemas or other representations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Generator, Literal

from ..exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ..spec import YadsSpec


@dataclass(frozen=True)
class BaseConverterConfig:
    """Base configuration for all yads converters.

    Args:
        mode: Conversion mode. "raise" will raise exceptions on unsupported features,
            "coerce" will attempt to coerce unsupported features to supported ones
            with warnings. Defaults to "coerce".
    """

    mode: Literal["raise", "coerce"] = "coerce"

    def __post_init__(self) -> None:
        if self.mode not in {"raise", "coerce"}:
            raise UnsupportedFeatureError("mode must be one of 'raise' or 'coerce'.")


class BaseConverter(ABC):
    """Abstract base class for spec converters."""

    def __init__(self, config: BaseConverterConfig | None = None) -> None:
        """Initialize the BaseConverter.

        Args:
            config: Configuration object. If None, uses default BaseConverterConfig.
        """
        self.config = config or BaseConverterConfig()
        self._current_field_name: str | None = None

    @abstractmethod
    def convert(self, spec: YadsSpec) -> Any:
        """Convert a YadsSpec to the target format."""
        ...

    @contextmanager
    def conversion_context(
        self,
        *,
        mode: Literal["raise", "coerce"] | None = None,
        field: str | None = None,
    ) -> Generator[None, None, None]:
        """Temporarily set conversion mode and field context.

        This context manager centralizes handling of converter state used for
        warnings and coercions, ensuring that values are restored afterwards.

        Args:
            mode: Optional override for the current conversion mode.
            field: Optional field name for contextual warnings.
        """
        # Snapshot current state
        previous_config = self.config
        previous_field = self._current_field_name

        try:
            if mode is not None:
                if mode not in ("raise", "coerce"):  # pragma: no cover
                    raise UnsupportedFeatureError(
                        "mode must be one of 'raise' or 'coerce'."
                    )
                self.config = replace(self.config, mode=mode)
            if field is not None:
                self._current_field_name = field
            yield
        finally:
            # Restore prior state
            self.config = previous_config
            self._current_field_name = previous_field
