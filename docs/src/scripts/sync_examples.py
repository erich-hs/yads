from __future__ import annotations

if __name__ == "__main__" and (__package__ is None or __package__ == ""):
    raise SystemExit(
        "Run as a module: `python -m docs.src.scripts.sync_examples <FILES...>`"
    )

import argparse
import importlib
import inspect
import io
from pathlib import Path
import pkgutil
import re
import textwrap
from contextlib import redirect_stdout
from dataclasses import dataclass
from typing import Mapping

from ..examples import ExampleBlockRequest, ExampleDefinition, ExampleCallable

MARKER_PATTERN = re.compile(
    r"<!-- BEGIN:example (?P<example_id>[\w\-]+) (?P<slug>[\w\-]+) -->\n?"
    r"(?P<body>.*?)"
    r"<!-- END:example (?P=example_id) (?P=slug) -->",
    re.DOTALL,
)


@dataclass(frozen=True)
class RenderedBlock:
    example_id: str
    slug: str
    language: str
    content: str


class ExampleRunner:
    def __init__(self, definition: ExampleDefinition) -> None:
        self.definition: ExampleDefinition = definition
        self._stdout: dict[ExampleCallable, str] = {}

    def render(self, request: ExampleBlockRequest) -> RenderedBlock:
        content = self._render_content(request)
        return RenderedBlock(
            example_id=self.definition.example_id,
            slug=request.slug,
            language=request.language,
            content=content.rstrip(),
        )

    def _render_content(self, request: ExampleBlockRequest) -> str:
        if request.source == "callable":
            func = self._require_callable(request)
            return self._callable_source(func)
        if request.source == "stdout":
            func = self._require_callable(request)
            return self._stdout_output(func)
        if request.source == "literal":
            if request.text is None:
                msg = (
                    f"Example '{self.definition.example_id}' block '{request.slug}' "
                    "requires literal text."
                )
                raise ValueError(msg)
            return request.text
        msg = f"Unsupported example block source: {request.source}"
        raise ValueError(msg)

    def _stdout_output(self, func: ExampleCallable) -> str:
        if func not in self._stdout:
            buffer = io.StringIO()
            try:
                with redirect_stdout(buffer):
                    func()
            except Exception as exc:  # pragma: no cover - surfaced to user
                example_id = self.definition.example_id
                raise RuntimeError(f"Failed to execute example '{example_id}'.") from exc
            self._stdout[func] = buffer.getvalue().rstrip()
        return self._stdout[func]

    @staticmethod
    def _callable_source(func: ExampleCallable) -> str:
        source = inspect.getsource(func)
        dedented = textwrap.dedent(source)
        try:
            _, body = dedented.split("\n", 1)
        except ValueError:
            raise ValueError("Example callable must contain a body.") from None
        return textwrap.dedent(body).rstrip()

    def _require_callable(self, request: ExampleBlockRequest) -> ExampleCallable:
        func = request.callable
        if func is None:
            msg = (
                f"Example '{self.definition.example_id}' block '{request.slug}' "
                "requires a callable."
            )
            raise ValueError(msg)
        return func


def discover_examples() -> Mapping[str, ExampleDefinition]:
    discovered: dict[str, ExampleDefinition] = {}
    package = importlib.import_module("docs.src.examples")
    for module_info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        module_name = module_info.name
        last = module_name.rsplit(".", 1)[-1]
        if last in {"base", "__init__"}:
            continue
        module = importlib.import_module(module_name)
        definition = getattr(module, "EXAMPLE", None)
        if definition is None:
            continue
        if definition.example_id in discovered:
            msg = f"Duplicate example id: {definition.example_id}"
            raise ValueError(msg)
        discovered[definition.example_id] = definition
    return discovered


def render_blocks() -> dict[tuple[str, str], RenderedBlock]:
    blocks: dict[tuple[str, str], RenderedBlock] = {}
    for definition in discover_examples().values():
        runner = ExampleRunner(definition)
        for request in definition.blocks:
            rendered = runner.render(request)
            key = (rendered.example_id, rendered.slug)
            blocks[key] = rendered
    return blocks


def format_block(block: RenderedBlock) -> str:
    content = block.content
    return (
        f"<!-- BEGIN:example {block.example_id} {block.slug} -->\n"
        f"```{block.language}\n{content}\n```\n"
        f"<!-- END:example {block.example_id} {block.slug} -->"
    )


def update_file(path: Path, blocks: Mapping[tuple[str, str], RenderedBlock]) -> bool:
    text = path.read_text()

    def _replace(match: re.Match[str]) -> str:
        example_id = match.group("example_id")
        slug = match.group("slug")
        key = (example_id, slug)
        if key not in blocks:
            msg = f"No rendered block for example '{example_id}' slug '{slug}'"
            raise KeyError(msg)
        replacement = format_block(blocks[key])
        return replacement

    updated, count = MARKER_PATTERN.subn(_replace, text)
    if count == 0:
        return False
    if updated != text:
        path.write_text(updated)
        return True
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync README snippets from examples")
    parser.add_argument("files", nargs="+", type=Path, help="Files to update")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    blocks = render_blocks()
    changed = False
    for path in args.files:
        if update_file(path, blocks):
            changed = True
            print(f"Updated {path}")
    if not changed:
        print("No updates required.")


if __name__ == "__main__":
    main()
