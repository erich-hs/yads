"""Data structures for executable documentation examples."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Mapping

ExampleCallable = Callable[[], None]
ExampleBlockSource = Literal["callable", "stdout", "literal"]


@dataclass(frozen=True)
class ExampleBlockRequest:
    """Describe how a documentation block should be generated."""

    slug: str
    language: str
    source: ExampleBlockSource
    text: str | None = None
    step: str | None = None


@dataclass(frozen=True)
class ExampleDefinition:
    """An executable example that can populate documentation snippets."""

    example_id: str
    callables: Mapping[str, ExampleCallable]
    blocks: tuple[ExampleBlockRequest, ...]

    def __post_init__(self) -> None:
        normalized = dict(self.callables)
        if not normalized:
            msg = "ExampleDefinition requires at least one callable."
            raise ValueError(msg)
        object.__setattr__(self, "callables", normalized)
