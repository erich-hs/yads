"""Lightweight, cached dependency checks and decorator utilities."""

from __future__ import annotations

from functools import lru_cache, wraps
import importlib
import importlib.metadata as md
from typing import Any, Callable, ParamSpec, TypeVar

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
    constraint = f">={min_version}" if min_version else ""
    return (
        f"""Install with: 'pip install "{package_name}{constraint}"'. """
        f"""Or using uv: 'uv add {package_name}{constraint}'."""
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


def try_import_optional(
    module_name: str,
    *,
    required_import: str,
    package_name: str,
    min_version: str | None = None,
    context: str | None = None,
) -> tuple[Any | None, str | None]:
    """Attempt to import an optional feature without raising.

    This helper ensures an optional dependency and imports a specific attribute
    from a module. It never raises; instead, it returns a tuple of
    (imported_object, message). On success, message is None. On failure, the
    returned object is None and message contains a contextual, user-facing
    hint.

    Args:
        module_name: Fully qualified module path to import (e.g.,
            "pyspark.sql.types").
        required_import: Attribute name expected to be available in the module
            (e.g., "VariantType").
        package_name: Package name used for version resolution and install hints
            (e.g., "pyspark").
        min_version: Optional minimum version required for the feature.
        context: Optional short context message describing where this feature is
            needed (e.g., "Variant type for field 'col'").

    Returns:
        A tuple of (obj, message). If the import succeeds, obj is the imported
        attribute and message is None. Otherwise, obj is None and message is a
        human-readable explanation including an install hint.
    """

    # Ensure the base package (and minimum version) is available first.
    try:
        ensure_dependency(package_name, min_version)
    except (MissingDependencyError, DependencyVersionError) as e:
        msg = str(e)
        if context:
            msg = f"While handling {context}, the following error occurred: {msg}"
        return None, msg

    # Import the module and retrieve the required attribute.
    try:
        module = importlib.import_module(module_name)
    except Exception:  # pragma: no cover - defensive
        hint = _format_install_hint(package_name, min_version)
        msg = (
            f"Failed to import module '{module_name}' for optional feature '{required_import}'.\n"
            f"{hint}"
        )
        if context:
            msg = f"While handling {context}, the following error occurred: {msg}"
        return None, msg

    try:
        obj = getattr(module, required_import)
    except AttributeError:
        hint = _format_install_hint(package_name, min_version)
        msg = f"Optional feature '{required_import}' is unavailable in module '{module_name}'."
        if min_version:
            msg += f" This feature may require {package_name} >= {min_version}."
        msg += f"\n{hint}"
        if context:
            msg = f"While handling {context}, the following error occurred: {msg}"
        return None, msg

    return obj, None
