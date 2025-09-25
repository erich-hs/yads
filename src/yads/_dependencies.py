"""Lightweight, cached dependency checks and decorator utilities."""

from __future__ import annotations

from functools import lru_cache, wraps
import importlib
import importlib.metadata as md
from typing import Callable, ParamSpec, TypeVar

from .exceptions import (
    MissingDependencyError,
    DependencyVersionError,
)


P = ParamSpec("P")
R = TypeVar("R")


@lru_cache(maxsize=None)
def _get_installed_version(package_name: str) -> str | None:
    """Return installed version for `package_name` or `None` if missing."""

    try:
        return md.version(package_name)
    except md.PackageNotFoundError:
        return None


def _normalize_version(version: str) -> tuple[int, ...]:
    """Normalize a version string into a tuple of integers when possible.

    We keep this intentionally simple to avoid adding a runtime dependency on
    `packaging`. This is suitable for basic minimum version checks using
    dotted numeric versions such as "3.5.0" or "4.0.0".

    If non-numeric segments are present, they are ignored beyond the first
    non-numeric token to err on the side of conservative comparison.
    """

    parts: list[int] = []
    for token in version.split("."):
        if not token.isdigit():
            break
        parts.append(int(token))
    return tuple(parts)


def _meets_min_version(installed: str, minimum: str) -> bool:
    """Return True if `installed` >= `minimum` using numeric tuple compare.

    Falls back to string comparison if normalization yields empty tuples.
    """

    inst = _normalize_version(installed)
    minv = _normalize_version(minimum)
    if inst and minv:
        # Compare by padding shorter tuple with zeros
        length = max(len(inst), len(minv))
        inst_pad = inst + (0,) * (length - len(inst))
        minv_pad = minv + (0,) * (length - len(minv))
        return inst_pad >= minv_pad
    # Fallback: best-effort lexical compare
    return installed >= minimum


def _format_install_hint(package_name: str, min_version: str | None) -> str:
    constraint = f">= {min_version} " if min_version else ""
    return (
        f"Install with: 'pip install {package_name}{constraint}'. "
        f"Or using uv: 'uv add {package_name}{constraint}'."
    )


def ensure_dependency(package_name: str, min_version: str | None = None) -> None:
    """Ensure `package_name` is available and meets `min_version` if given.

    Raises:
        MissingDependencyError: When the required dependency is not available.
        DependencyVersionError: When the required dependency version is below the minimum.
    """

    installed = _get_installed_version(package_name)
    if installed is None:
        hint = _format_install_hint(package_name, min_version)
        needed = f" (>= {min_version})" if min_version else ""
        raise MissingDependencyError(
            f"Dependency '{package_name}'{needed} is required but not installed.\n{hint}"
        )

    if min_version and not _meets_min_version(installed, min_version):
        hint = _format_install_hint(package_name, min_version)
        raise DependencyVersionError(
            f"Dependency '{package_name}' must be >= {min_version}, "
            f"found {installed}.\n{hint}"
        )


def requires_dependency(
    package_name: str,
    min_version: str | None = None,
    *,
    import_name: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator to enforce an optional dependency at call time.

    Args:
        package_name: The name used to resolve the installed package version.
        min_version: Optional minimum version required.
        import_name: Optional fully-qualified module path to import lazily
            just before executing the wrapped function.
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            ensure_dependency(package_name, min_version)
            if import_name is not None:
                importlib.import_module(import_name)
            return func(*args, **kwargs)

        return wrapper

    return decorator
