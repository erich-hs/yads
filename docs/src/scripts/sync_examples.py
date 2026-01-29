from __future__ import annotations

if __name__ == "__main__" and (__package__ is None or __package__ == ""):
    raise SystemExit(
        "Run as a module: `python -m docs.src.scripts.sync_examples <FILES...>`"
    )

import argparse
import ast
import importlib
import importlib.util
import inspect
import io
from pathlib import Path
import pkgutil
import re
import textwrap
from contextlib import redirect_stdout
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

from ..examples import ExampleBlockRequest, ExampleDefinition, ExampleCallable

MARKER_PATTERN = re.compile(
    r"(?P<indent>[ \t]*)<!-- BEGIN:example (?P<example_id>[\w\-]+) (?P<slug>[\w\-]+) -->\n?"
    r"(?P<body>.*?)"
    r"(?P=indent)<!-- END:example (?P=example_id) (?P=slug) -->",
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


def discover_examples(
    requested_example_ids: Iterable[str] | None = None,
) -> Mapping[str, ExampleDefinition]:
    index = _build_example_index()
    available = set(index.keys())
    required = set(requested_example_ids or available)
    missing = required - available
    if missing:
        missing_joined = ", ".join(sorted(missing))
        msg = f"Unknown example id(s) referenced in docs: {missing_joined}"
        raise KeyError(msg)

    discovered: dict[str, ExampleDefinition] = {}
    for example_id in required:
        module_name = index[example_id]
        module = importlib.import_module(module_name)
        definition = getattr(module, "EXAMPLE", None)
        if definition is None:
            msg = f"Module '{module_name}' does not define EXAMPLE"
            raise AttributeError(msg)
        discovered[example_id] = definition
    return discovered


def render_blocks(
    requested_blocks: Sequence[tuple[str, str]],
) -> dict[tuple[str, str], RenderedBlock]:
    if not requested_blocks:
        return {}

    grouped: dict[str, set[str]] = {}
    for example_id, slug in requested_blocks:
        grouped.setdefault(example_id, set()).add(slug)

    definitions = discover_examples(grouped.keys())
    blocks: dict[tuple[str, str], RenderedBlock] = {}
    for example_id, slugs in grouped.items():
        definition = definitions[example_id]
        requests_by_slug = {request.slug: request for request in definition.blocks}
        runner = ExampleRunner(definition)
        missing: list[str] = []
        for slug in slugs:
            request = requests_by_slug.get(slug)
            if request is None:
                missing.append(slug)
                continue
            rendered = runner.render(request)
            key = (rendered.example_id, rendered.slug)
            blocks[key] = rendered
        if missing:
            missing_slugs = ", ".join(sorted(missing))
            msg = f"Example '{example_id}' does not define block(s): {missing_slugs}"
            raise KeyError(msg)
    return blocks


def format_block(block: RenderedBlock, indent: str = "") -> str:
    content = block.content
    rendered = (
        f"<!-- BEGIN:example {block.example_id} {block.slug} -->\n"
        f"```{block.language}\n{content}\n```\n"
        f"<!-- END:example {block.example_id} {block.slug} -->"
    )
    if not indent:
        return rendered
    return textwrap.indent(rendered, prefix=indent, predicate=lambda _line: True)


def update_file(path: Path, blocks: Mapping[tuple[str, str], RenderedBlock]) -> bool:
    text = path.read_text()

    def _replace(match: re.Match[str]) -> str:
        example_id = match.group("example_id")
        slug = match.group("slug")
        indent = match.group("indent") or ""
        key = (example_id, slug)
        if key not in blocks:
            msg = f"No rendered block for example '{example_id}' slug '{slug}'"
            raise KeyError(msg)
        replacement = format_block(blocks[key], indent=indent)
        return replacement

    updated, count = MARKER_PATTERN.subn(_replace, text)
    if count == 0:
        return False
    if updated != text:
        path.write_text(updated)
        return True
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync documentation snippets from registered examples",
    )
    parser.add_argument("files", nargs="*", type=Path, help="Files or folders to sync")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Update all markdown files under --docs-root",
    )
    parser.add_argument(
        "--docs-root",
        action="append",
        type=Path,
        dest="docs_roots",
        help=(
            "Docs root(s) scanned when using --all (default: docs plus README.md "
            "when present)"
        ),
    )
    parser.add_argument(
        "--ignore",
        action="append",
        type=Path,
        help="Paths to skip when syncing (defaults skip docs/develop/contributing.md).",
    )
    return parser.parse_args()


MARKDOWN_SUFFIXES = {".md", ".mdx"}
DEFAULT_IGNORES = [Path("docs/develop/contributing.md")]


def _iter_example_module_paths() -> list[tuple[str, Path]]:
    package = importlib.import_module("docs.src.examples")
    modules: list[tuple[str, Path]] = []
    for module_info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
        module_name = module_info.name
        last = module_name.rsplit(".", 1)[-1]
        if last in {"base", "__init__"}:
            continue
        spec = importlib.util.find_spec(module_name)
        if spec is None or spec.origin is None:
            continue
        modules.append((module_name, Path(spec.origin)))
    return modules


def _extract_example_ids(path: Path) -> list[str]:
    try:
        tree = ast.parse(path.read_text())
    except OSError as exc:  # pragma: no cover - surfaced to user
        raise RuntimeError(f"Failed to parse example module: {path}") from exc

    example_ids: list[str] = []

    def _maybe_collect(call: ast.Call) -> None:
        if not isinstance(call.func, ast.Name) or call.func.id != "ExampleDefinition":
            return
        for keyword in call.keywords:
            if keyword.arg == "example_id":
                try:
                    value = ast.literal_eval(keyword.value)
                except (ValueError, SyntaxError):
                    return
                if isinstance(value, str):
                    example_ids.append(value)
                return

    for node in tree.body:
        value = None
        if isinstance(node, ast.Assign):
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            value = node.value
        if isinstance(value, ast.Call):
            _maybe_collect(value)

    return example_ids


def _build_example_index() -> dict[str, str]:
    index: dict[str, str] = {}
    for module_name, path in _iter_example_module_paths():
        for example_id in _extract_example_ids(path):
            if example_id in index:
                msg = f"Duplicate example id: {example_id}"
                raise ValueError(msg)
            index[example_id] = module_name
    return index


def _iter_markdown_files(path: Path) -> Iterable[Path]:
    if path.is_file() and path.suffix.lower() in MARKDOWN_SUFFIXES:
        yield path
        return
    if path.is_dir():
        for pattern in ("*.md", "*.mdx"):
            yield from path.rglob(pattern)


def _expand_targets(paths: Iterable[Path]) -> list[Path]:
    expanded: list[Path] = []
    for in_path in paths:
        resolved = in_path
        if not resolved.exists():
            msg = f"Path does not exist: {resolved}"
            raise FileNotFoundError(msg)
        matches = sorted(_iter_markdown_files(resolved))
        if resolved.is_file() and not matches:
            suffix_label = resolved.suffix or "<none>"
            msg = (
                "Unsupported file for syncing examples: "
                f"{resolved} (suffix '{suffix_label}')"
            )
            raise ValueError(msg)
        expanded.extend(matches)
    return expanded


def _deduplicate_paths(paths: Iterable[Path]) -> list[Path]:
    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in paths:
        key = candidate.resolve()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _collect_requested_blocks(paths: Iterable[Path]) -> list[tuple[str, str]]:
    requested: list[tuple[str, str]] = []
    for path in paths:
        text = path.read_text()
        for match in MARKER_PATTERN.finditer(text):
            requested.append((match.group("example_id"), match.group("slug")))
    return requested


def _filter_ignored_paths(paths: Iterable[Path], ignored: Iterable[Path]) -> list[Path]:
    ignored_resolved = {path.resolve() for path in ignored}
    return [path for path in paths if path.resolve() not in ignored_resolved]


def main() -> None:
    args = parse_args()
    targets: list[Path] = []
    if args.files:
        targets.extend(_expand_targets(args.files))
    docs_roots = list(args.docs_roots or [Path("docs")])
    if args.docs_roots is None:
        readme = Path("README.md")
        if readme.exists():
            docs_roots.append(readme)

    if args.all:
        for root in docs_roots:
            if not root.exists():
                msg = f"Docs root does not exist: {root}"
                raise FileNotFoundError(msg)
            targets.extend(_expand_targets([root]))
    targets = _deduplicate_paths(targets)
    ignored_paths = list(DEFAULT_IGNORES)
    if args.ignore:
        ignored_paths.extend(args.ignore)
    targets = _filter_ignored_paths(targets, ignored_paths)
    if not targets:
        raise SystemExit("Provide at least one file/directory or pass --all.")

    requested_blocks = _collect_requested_blocks(targets)
    blocks = render_blocks(requested_blocks)

    changed = False
    for path in targets:
        if update_file(path, blocks):
            changed = True
            print(f"Updated {path}")
    if not changed:
        print("No updates required.")


if __name__ == "__main__":
    main()
