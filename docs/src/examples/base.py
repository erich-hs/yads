"""Data structures for executable documentation examples."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal

ExampleCallable = Callable[[], None]
ExampleBlockSource = Literal["callable", "stdout", "literal"]


@dataclass(frozen=True)
class ExampleBlockRequest:
    """Describe how a documentation block should be generated."""

    slug: str
    language: str
    source: ExampleBlockSource
    text: str | None = None
    callable: ExampleCallable | None = None


@dataclass(frozen=True)
class ExampleDefinition:
    """An executable example that can populate documentation snippets."""

    example_id: str
    blocks: tuple[ExampleBlockRequest, ...]
