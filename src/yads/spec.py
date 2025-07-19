from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .constraints import BaseConstraint
from .types import Type


@dataclass(frozen=True)
class Field:
    """Represents a named and typed data field."""

    name: str
    type: Type
    description: str | None = None
    constraints: list[BaseConstraint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SchemaSpec:
    """A data class representing a full schema specification."""

    name: str
    version: str
    columns: list[Field]
    description: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
