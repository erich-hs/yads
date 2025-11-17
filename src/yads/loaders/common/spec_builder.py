"""Compatibility wrapper for the legacy SpecBuilder API."""

from __future__ import annotations

from typing import Any

from ...serializers import SpecDeserializer
from ...spec import YadsSpec


class SpecBuilder:
    """Thin wrapper to support the existing loader imports during refactor."""

    def __init__(self, data: dict[str, Any]):
        self.data = data

    def build(self) -> YadsSpec:
        return SpecDeserializer().deserialize(self.data)
