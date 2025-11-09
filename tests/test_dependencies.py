"""Unit tests for yads._dependencies module.

This module tests the dependency checking and decorator functionality,
including version comparison, error handling, and caching behavior.
"""

import importlib.metadata as md
from unittest.mock import patch

import pytest

from yads._dependencies import (
    _get_installed_version,
    _normalize_version,
    _meets_min_version,
    _format_install_hint,
    ensure_dependency,
    requires_dependency,
    try_import_optional,
)
from yads.exceptions import MissingDependencyError, DependencyVersionError


class TestGetInstalledVersion:
    """Test the cached version checking function."""

    def test_get_installed_version_existing_package(self):
        """Test getting version for an existing package."""
        with patch("yads._dependencies.md.version") as mock_version:
            mock_version.return_value = "1.2.3"
            result = _get_installed_version("pytest")
            assert result == "1.2.3"
            mock_version.assert_called_once_with("pytest")

    def test_get_installed_version_missing_package(self):
        """Test getting version for a missing package."""
        with patch("yads._dependencies.md.version") as mock_version:
            mock_version.side_effect = md.PackageNotFoundError("Package not found")
            result = _get_installed_version("nonexistent-package")
            assert result is None
            mock_version.assert_called_once_with("nonexistent-package")

    def test_get_installed_version_caching(self):
        """Test that version checks are cached."""
        with patch("yads._dependencies.md.version") as mock_version:
            mock_version.return_value = "2.0.0"

            # First call
            result1 = _get_installed_version("test-package")
            assert result1 == "2.0.0"

            # Second call should use cache
            result2 = _get_installed_version("test-package")
            assert result2 == "2.0.0"

            # Should only be called once due to caching
            mock_version.assert_called_once_with("test-package")


class TestNormalizeVersion:
    """Test version string normalization."""

    def test_normalize_version_simple(self):
        """Test normalizing simple version strings."""
        assert _normalize_version("1.2.3") == (1, 2, 3)
        assert _normalize_version("0.0.1") == (0, 0, 1)
        assert _normalize_version("10.20.30") == (10, 20, 30)

    def test_normalize_version_different_lengths(self):
        """Test normalizing versions of different lengths."""
        assert _normalize_version("1") == (1,)
        assert _normalize_version("1.2") == (1, 2)
        assert _normalize_version("1.2.3.4") == (1, 2, 3, 4)

    def test_normalize_version_with_non_numeric(self):
        """Test normalizing versions with non-numeric parts."""
        # The implementation stops at the first non-numeric token
        assert _normalize_version("1.2.3-alpha") == (1, 2)  # stops at "3" before "-"
        assert _normalize_version("1.2.3rc1") == (1, 2)  # stops at "3" before "r"
        assert _normalize_version("1.2.3+build.1") == (1, 2)  # stops at "3" before "+"
        assert _normalize_version("1.2.3.dev0") == (
            1,
            2,
            3,
        )  # stops at "dev0" (not numeric)

    def test_normalize_version_empty_or_invalid(self):
        """Test normalizing empty or invalid version strings."""
        assert _normalize_version("") == ()
        assert _normalize_version("alpha") == ()
        assert _normalize_version("1.2.alpha") == (1, 2)


class TestMeetsMinVersion:
    """Test version comparison logic."""

    def test_meets_min_version_numeric_comparison(self):
        """Test numeric tuple comparison."""
        assert _meets_min_version("1.2.3", "1.2.2") is True
        assert _meets_min_version("1.2.3", "1.2.3") is True
        assert _meets_min_version("1.2.3", "1.2.4") is False
        assert _meets_min_version("2.0.0", "1.9.9") is True

    def test_meets_min_version_different_lengths(self):
        """Test comparison with different version lengths."""
        # "1.2" normalizes to (1, 2), gets padded to (1, 2, 0)
        # "1.2.0" normalizes to (1, 2, 0)
        assert _meets_min_version("1.2", "1.2.0") is True  # (1, 2, 0) >= (1, 2, 0)
        assert _meets_min_version("1.2.0", "1.2") is True  # (1, 2, 0) >= (1, 2, 0)
        assert _meets_min_version("1.1", "1.2.0") is False  # (1, 1, 0) < (1, 2, 0)

    def test_meets_min_version_fallback_to_string(self):
        """Test fallback to string comparison when normalization fails."""
        # These should fall back to string comparison
        assert _meets_min_version("1.0.0", "1.0.0") is True
        assert _meets_min_version("2.0.0", "1.0.0") is True
        assert _meets_min_version("0.9.0", "1.0.0") is False

    def test_meets_min_version_edge_cases(self):
        """Test edge cases in version comparison."""
        # Empty versions fall back to string comparison
        assert _meets_min_version("", "") is True
        assert _meets_min_version("1.0.0", "") is True
        assert _meets_min_version("", "1.0.0") is False


class TestFormatInstallHint:
    """Test installation hint formatting."""

    def test_format_install_hint_without_version(self):
        """Test formatting hint without minimum version."""
        hint = _format_install_hint("pyspark", None)
        assert 'pip install "pyspark"' in hint
        assert "uv add pyspark" in hint
        assert ">=" not in hint

    def test_format_install_hint_with_version(self):
        """Test formatting hint with minimum version."""
        hint = _format_install_hint("pyspark", "4.0.0")
        assert 'pip install "pyspark>=4.0.0"' in hint
        assert "uv add pyspark>=4.0.0" in hint


class TestEnsureDependency:
    """Test the ensure_dependency function."""

    def test_ensure_dependency_package_exists(self):
        """Test ensuring dependency when package exists."""
        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = "1.0.0"
            # Should not raise
            ensure_dependency("pyspark")
            mock_get_version.assert_called_once_with("pyspark")

    def test_ensure_dependency_package_missing(self):
        """Test ensuring dependency when package is missing."""
        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = None

            with pytest.raises(MissingDependencyError) as exc_info:
                ensure_dependency("nonexistent-package")

            error_msg = str(exc_info.value)
            assert "nonexistent-package" in error_msg
            assert 'pip install "nonexistent-package"' in error_msg
            assert "uv add nonexistent-package" in error_msg

    def test_ensure_dependency_package_missing_with_version(self):
        """Test ensuring dependency when package is missing with version requirement."""
        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = None

            with pytest.raises(MissingDependencyError) as exc_info:
                ensure_dependency("pyspark", "4.0.0")

            error_msg = str(exc_info.value)
            assert (
                "Dependency 'pyspark' (>= 4.0.0) is required but not installed"
                in error_msg
            )
            assert 'pip install "pyspark>=4.0.0"' in error_msg

    def test_ensure_dependency_version_too_old(self):
        """Test ensuring dependency when version is too old."""
        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = "3.0.0"

            with pytest.raises(DependencyVersionError) as exc_info:
                ensure_dependency("pyspark", "4.0.0")

            error_msg = str(exc_info.value)
            assert "Dependency 'pyspark' must be >= 4.0.0, found 3.0.0" in error_msg
            assert 'pip install "pyspark>=4.0.0"' in error_msg

    def test_ensure_dependency_version_satisfied(self):
        """Test ensuring dependency when version requirement is satisfied."""
        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = "4.1.0"
            # Should not raise
            ensure_dependency("pyspark", "4.0.0")
            mock_get_version.assert_called_once_with("pyspark")

    def test_ensure_dependency_exact_version_match(self):
        """Test ensuring dependency with exact version match."""
        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = "4.0.0"
            # Should not raise
            ensure_dependency("pyspark", "4.0.0")
            mock_get_version.assert_called_once_with("pyspark")


class TestRequiresDependency:
    """Test the requires_dependency decorator."""

    def test_requires_dependency_basic_functionality(self):
        """Test basic decorator functionality."""

        @requires_dependency("pytest")
        def test_function() -> str:
            return "success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            result = test_function()
            assert result == "success"
            mock_ensure.assert_called_once_with("pytest", None)

    def test_requires_dependency_with_version(self):
        """Test decorator with version requirement."""

        @requires_dependency("pyspark", "4.0.0")
        def test_function() -> str:
            return "success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            result = test_function()
            assert result == "success"
            mock_ensure.assert_called_once_with("pyspark", "4.0.0")

    def test_requires_dependency_with_import_name(self):
        """Test decorator with import_name parameter."""

        @requires_dependency("pyspark", import_name="pyspark.sql.types")
        def test_function() -> str:
            return "success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            with patch("importlib.import_module") as mock_import:
                result = test_function()
                assert result == "success"
                mock_ensure.assert_called_once_with("pyspark", None)
                mock_import.assert_called_once_with("pyspark.sql.types")

    def test_requires_dependency_with_version_and_import(self):
        """Test decorator with both version and import_name."""

        @requires_dependency("pyspark", "4.0.0", import_name="pyspark.sql.types")
        def test_function() -> str:
            return "success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            with patch("importlib.import_module") as mock_import:
                result = test_function()
                assert result == "success"
                mock_ensure.assert_called_once_with("pyspark", "4.0.0")
                mock_import.assert_called_once_with("pyspark.sql.types")

    def test_requires_dependency_preserves_function_metadata(self):
        """Test that decorator preserves function metadata."""

        @requires_dependency("pytest")
        def test_function() -> str:
            """Test function docstring."""
            return "success"

        assert test_function.__name__ == "test_function"
        assert test_function.__doc__ == "Test function docstring."

    def test_requires_dependency_with_arguments(self):
        """Test decorator with function that has arguments."""

        @requires_dependency("pytest")
        def test_function(arg1: str, arg2: int = 42) -> str:
            return f"{arg1}_{arg2}"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            result = test_function("test", arg2=100)
            assert result == "test_100"
            mock_ensure.assert_called_once_with("pytest", None)

    def test_requires_dependency_raises_on_missing_dependency(self):
        """Test that decorator raises when dependency is missing."""

        @requires_dependency("nonexistent-package")
        def test_function() -> str:
            return "success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            mock_ensure.side_effect = MissingDependencyError("Package not found")

            with pytest.raises(MissingDependencyError):
                test_function()

            mock_ensure.assert_called_once_with("nonexistent-package", None)

    def test_requires_dependency_raises_on_version_error(self):
        """Test that decorator raises when version requirement not met."""

        @requires_dependency("pyspark", "4.0.0")
        def test_function() -> str:
            return "success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            mock_ensure.side_effect = DependencyVersionError("Version too old")

            with pytest.raises(DependencyVersionError):
                test_function()

            mock_ensure.assert_called_once_with("pyspark", "4.0.0")

    def test_requires_dependency_import_error_propagates(self):
        """Test that import errors in import_name are propagated."""

        @requires_dependency("pyspark", import_name="nonexistent.module")
        def test_function() -> str:
            return "success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            with patch("importlib.import_module") as mock_import:
                mock_import.side_effect = ImportError("Module not found")

                with pytest.raises(ImportError, match="Module not found"):
                    test_function()

                mock_ensure.assert_called_once_with("pyspark", None)
                mock_import.assert_called_once_with("nonexistent.module")


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_pyspark_converter_scenario(self):
        """Test a realistic PySpark converter scenario."""

        @requires_dependency("pyspark", "3.1.0", import_name="pyspark.sql.types")
        def convert_to_pyspark() -> str:
            from pyspark.sql.types import StringType  # type: ignore[import-untyped]

            return f"PySpark {StringType()} conversion"

        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = "3.5.0"

            with patch("importlib.import_module") as mock_import:
                result = convert_to_pyspark()
                assert "PySpark" in result
                mock_get_version.assert_called_once_with("pyspark")
                mock_import.assert_called_once_with("pyspark.sql.types")

    def test_pyarrow_converter_scenario(self):
        """Test a realistic PyArrow converter scenario."""

        @requires_dependency("pyarrow", "10.0.0")
        def convert_to_pyarrow() -> str:
            import pyarrow as pa  # type: ignore[import-untyped]

            return f"PyArrow {pa.__version__} conversion"

        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = "12.0.0"

            result = convert_to_pyarrow()
            assert "PyArrow" in result
            mock_get_version.assert_called_once_with("pyarrow")

    def test_version_specific_feature_scenario(self):
        """Test version-specific feature gating."""

        @requires_dependency("pyspark", "4.0.0", import_name="pyspark.sql.types")
        def use_variant_type() -> str:
            from pyspark.sql.types import VariantType  # type: ignore[import-untyped]

            return f"VariantType: {VariantType()}"

        with patch("yads._dependencies._get_installed_version") as mock_get_version:
            mock_get_version.return_value = "3.5.0"

            with pytest.raises(DependencyVersionError) as exc_info:
                use_variant_type()

            error_msg = str(exc_info.value)
            assert "Dependency 'pyspark' must be >= 4.0.0, found 3.5.0" in error_msg


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_package_name(self):
        """Test behavior with empty package name."""
        # Empty string causes ValueError from importlib.metadata
        with pytest.raises(ValueError, match="A distribution name is required"):
            ensure_dependency("")

    def test_none_package_name(self):
        """Test behavior with None package name."""
        # None causes ValueError from importlib.metadata
        with pytest.raises(ValueError, match="A distribution name is required"):
            ensure_dependency(None)  # type: ignore[arg-type]

    def test_very_long_version_strings(self):
        """Test behavior with very long version strings."""
        long_version = "1." + "0." * 100 + "0"
        assert _normalize_version(long_version) == (1,) + (0,) * 100 + (0,)


class TestRequiresDependencyWithMethods:
    """Test requires_dependency decorator with class and static methods."""

    def test_decorator_with_class_method(self):
        """Test decorator with class methods."""

        class TestClass:
            @requires_dependency("pytest")
            def test_method(self) -> str:
                return "method success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            instance = TestClass()
            result = instance.test_method()
            assert result == "method success"
            mock_ensure.assert_called_once_with("pytest", None)

    def test_decorator_with_static_method(self):
        """Test decorator with static methods."""

        class TestClass:
            @staticmethod
            @requires_dependency("pytest")
            def test_static_method() -> str:
                return "static success"

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            result = TestClass.test_static_method()
            assert result == "static success"
            mock_ensure.assert_called_once_with("pytest", None)


class TestTryImportOptional:
    """Tests for try_import_optional helper."""

    def test_try_import_optional_success(self):
        """Returns object and None message when dependency and attribute exist."""

        class DummyModule:
            Feature = object()

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            with patch("importlib.import_module") as mock_import:
                mock_ensure.return_value = None
                mock_import.return_value = DummyModule()

                obj, msg = try_import_optional(
                    "dummy.module",
                    required_import="Feature",
                    package_name="dummy",
                    min_version="1.0.0",
                    context="Feature X for 'field'",
                )

                assert msg is None
                assert obj is DummyModule.Feature
                mock_ensure.assert_called_once_with("dummy", "1.0.0")
                mock_import.assert_called_once_with("dummy.module")

    def test_try_import_optional_missing_dependency(self):
        """Returns (None, message) when dependency check fails, includes context."""

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            mock_ensure.side_effect = MissingDependencyError(
                "Dependency 'pkg' is required but not installed.\n"
                "Install with: 'pip install \"pkg\"'. Or using uv: 'uv add pkg'."
            )

            obj, msg = try_import_optional(
                "pkg.module",
                required_import="Feat",
                package_name="pkg",
                min_version=None,
                context="Using Feat for 'col'",
            )

            assert obj is None
            assert msg is not None
            assert "Dependency 'pkg'" in msg
            assert 'pip install "pkg"' in msg
            assert (
                "While handling Using Feat for 'col', the following error occurred:"
                in msg
            )

    def test_try_import_optional_module_import_error(self):
        """Returns (None, message) when module import fails."""

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            with patch("importlib.import_module") as mock_import:
                mock_ensure.return_value = None
                mock_import.side_effect = ImportError("No module named 'x'")

                obj, msg = try_import_optional(
                    "pkg.missing",
                    required_import="Feat",
                    package_name="pkg",
                    min_version="2.0.0",
                    context="Using Feat",
                )

                assert obj is None
                assert msg is not None
                assert (
                    "Failed to import module 'pkg.missing' for optional feature 'Feat'."
                    in msg
                )
                assert 'pip install "pkg>=2.0.0"' in msg
                assert "While handling Using Feat, the following error occurred:" in msg

    def test_try_import_optional_missing_attribute(self):
        """Returns (None, message) when attribute is missing, includes version hint."""

        class DummyModule:
            pass

        with patch("yads._dependencies.ensure_dependency") as mock_ensure:
            with patch("importlib.import_module") as mock_import:
                mock_ensure.return_value = None
                mock_import.return_value = DummyModule()

                obj, msg = try_import_optional(
                    "pkg.module",
                    required_import="MissingFeature",
                    package_name="pkg",
                    min_version="3.4.5",
                    context="Need MissingFeature",
                )

                assert obj is None
                assert msg is not None
                assert (
                    "Optional feature 'MissingFeature' is unavailable in module 'pkg.module'."
                    in msg
                )
                assert "pkg >= 3.4.5" in msg
                assert 'pip install "pkg>=3.4.5"' in msg
                assert (
                    "While handling Need MissingFeature, the following error occurred:"
                    in msg
                )
