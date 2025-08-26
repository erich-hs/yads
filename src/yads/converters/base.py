"""Base converter interface for spec transformations.

This module defines the abstract base class for all yads converters. Converters
are responsible for transforming YadsSpec objects into target formats such as
SQL DDL, framework-specific schemas or other representations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator, Literal

from ..exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from ..spec import YadsSpec


class BaseConverter(ABC):
    """Abstract base class for spec converters."""

    def __init__(self, mode: Literal["raise", "coerce"] = "coerce") -> None:
        # Default conversion context shared by all concrete converters.
        if mode not in {"raise", "coerce"}:
            raise UnsupportedFeatureError("mode must be one of 'raise' or 'coerce'.")
        self._mode: Literal["raise", "coerce"] = mode
        self._current_field_name: str | None = None

    @abstractmethod
    def convert(self, spec: YadsSpec, **kwargs: Any) -> Any:
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
        previous_mode: Literal["raise", "coerce"] = getattr(self, "_mode", "coerce")
        previous_field = getattr(self, "_current_field_name", None)

        try:
            if mode is not None:
                if mode not in ("raise", "coerce"):  # pragma: no cover
                    raise UnsupportedFeatureError(
                        "mode must be one of 'raise' or 'coerce'."
                    )
                self._mode = mode
            if field is not None:
                self._current_field_name = field
            yield
        finally:
            # Restore prior state
            self._mode = previous_mode
            self._current_field_name = previous_field
