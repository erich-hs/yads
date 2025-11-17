"""Placeholder for type deserialization helpers."""

from __future__ import annotations

from typing import Any

from .. import types as ytypes


class TypeDeserializer:
    """Parse type definitions into `YadsType` instances.

    This stub will gain the concrete logic during the extraction step.
    """

    def parse(self, type_name: str, type_def: dict[str, Any]) -> ytypes.YadsType:
        raise NotImplementedError
