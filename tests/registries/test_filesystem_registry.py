"""Tests for FileSystemRegistry."""

import logging
import pytest
import tempfile
from pathlib import Path

from yads.registries import FileSystemRegistry
from yads.exceptions import (
    DuplicateSpecWarning,
    InvalidSpecNameError,
    RegistryConnectionError,
    SpecNotFoundError,
)
from yads.spec import YadsSpec, Column
import yads.types as ytypes


@pytest.fixture
def temp_registry_path():
    """Create a temporary directory for registry tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def registry(temp_registry_path):
    """Create a FileSystemRegistry instance."""
    return FileSystemRegistry(temp_registry_path)


@pytest.fixture
def sample_spec():
    """Create a sample spec for testing."""
    return YadsSpec(
        name="catalog.db.test_table",
        version=1,
        columns=[
            Column(name="id", type=ytypes.Integer(bits=64)),
            Column(name="name", type=ytypes.String()),
        ],
    )


@pytest.fixture
def modified_spec(sample_spec):
    """Create a modified version of the sample spec."""
    return YadsSpec(
        name=sample_spec.name,
        version=1,
        columns=[
            Column(name="id", type=ytypes.Integer(bits=64)),
            Column(name="name", type=ytypes.String()),
            Column(name="email", type=ytypes.String()),  # Added column
        ],
    )


class TestFileSystemRegistryInit:
    """Test registry initialization."""

    def test_init_local_filesystem(self, temp_registry_path):
        """Test initialization with local filesystem path."""
        registry = FileSystemRegistry(temp_registry_path)
        assert registry.base_path == temp_registry_path
        assert registry.fs is not None
        assert registry.logger is not None

    def test_init_with_custom_logger(self, temp_registry_path):
        """Test initialization with custom logger."""
        custom_logger = logging.getLogger("test.registry")
        registry = FileSystemRegistry(temp_registry_path, logger=custom_logger)
        assert registry.logger is custom_logger

    def test_init_validates_base_path(self):
        """Test that invalid remote paths raise RegistryConnectionError."""
        # Note: Local filesystem paths don't fail immediately with fsspec
        # Test with an invalid protocol instead
        with pytest.raises(RegistryConnectionError):
            FileSystemRegistry("invalid-protocol://path/to/registry")


class TestRegister:
    """Test spec registration."""

    def test_register_new_spec(self, registry, sample_spec):
        """Test registering a new spec assigns version 1."""
        version = registry.register(sample_spec)
        assert version == 1

    def test_register_increments_version(self, registry, sample_spec, modified_spec):
        """Test that registering twice increments version."""
        v1 = registry.register(sample_spec)
        v2 = registry.register(modified_spec)
        assert v1 == 1
        assert v2 == 2

    def test_register_duplicate_content_warns(self, registry, sample_spec):
        """Test that registering same content warns and returns existing version."""
        v1 = registry.register(sample_spec)

        # Register again with same content
        with pytest.warns(DuplicateSpecWarning, match="identical to version 1"):
            v2 = registry.register(sample_spec)

        assert v1 == v2 == 1

    def test_register_invalid_spec_name_slash(self, sample_spec):
        """Test that spec names with '/' raise InvalidSpecNameError."""
        invalid_spec = YadsSpec(
            name="catalog/db/table",  # Invalid: contains /
            version=1,
            columns=sample_spec.columns,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            registry = FileSystemRegistry(tmpdir)
            with pytest.raises(InvalidSpecNameError, match="invalid characters"):
                registry.register(invalid_spec)

    def test_register_invalid_spec_name_special_chars(self, sample_spec):
        """Test that spec names with special chars raise InvalidSpecNameError."""
        for char in ["\\", ":", "*", "?", "<", ">", "|"]:
            invalid_spec = YadsSpec(
                name=f"table{char}name",
                version=1,
                columns=sample_spec.columns,
            )

            with tempfile.TemporaryDirectory() as tmpdir:
                registry = FileSystemRegistry(tmpdir)
                with pytest.raises(InvalidSpecNameError):
                    registry.register(invalid_spec)

    def test_register_valid_spec_name_with_dots(self, registry):
        """Test that spec names with dots work."""
        spec = YadsSpec(
            name="catalog.database.schema.table",
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
        )
        version = registry.register(spec)
        assert version == 1


class TestGet:
    """Test spec retrieval."""

    def test_get_latest_version(self, registry, sample_spec, modified_spec):
        """Test retrieving latest version without specifying version."""
        registry.register(sample_spec)
        registry.register(modified_spec)

        latest = registry.get(sample_spec.name)
        assert latest.version == 2
        assert len(latest.columns) == 3  # modified_spec has 3 columns

    def test_get_specific_version(self, registry, sample_spec, modified_spec):
        """Test retrieving a specific older version."""
        registry.register(sample_spec)
        registry.register(modified_spec)

        v1 = registry.get(sample_spec.name, version=1)
        assert v1.version == 1
        assert len(v1.columns) == 2  # sample_spec has 2 columns

    def test_get_nonexistent_spec(self, registry):
        """Test that getting nonexistent spec raises SpecNotFoundError."""
        with pytest.raises(SpecNotFoundError, match="not found"):
            registry.get("nonexistent.spec")

    def test_get_nonexistent_version(self, registry, sample_spec):
        """Test that getting nonexistent version raises SpecNotFoundError."""
        registry.register(sample_spec)

        with pytest.raises(SpecNotFoundError, match="version 999 not found"):
            registry.get(sample_spec.name, version=999)


class TestListVersions:
    """Test version listing."""

    def test_list_versions(self, registry, sample_spec, modified_spec):
        """Test listing versions returns sorted list."""
        registry.register(sample_spec)
        registry.register(modified_spec)

        # Register third version
        spec3 = YadsSpec(
            name=sample_spec.name,
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
        )
        registry.register(spec3)

        versions = registry.list_versions(sample_spec.name)
        assert versions == [1, 2, 3]

    def test_list_versions_empty(self, registry):
        """Test listing versions for nonexistent spec returns empty list."""
        versions = registry.list_versions("nonexistent.spec")
        assert versions == []


class TestExists:
    """Test spec existence checking."""

    def test_exists_true(self, registry, sample_spec):
        """Test that exists returns True for registered spec."""
        registry.register(sample_spec)
        assert registry.exists(sample_spec.name) is True

    def test_exists_false(self, registry):
        """Test that exists returns False for nonexistent spec."""
        assert registry.exists("nonexistent.spec") is False


class TestSpecComparison:
    """Test spec equality comparison logic."""

    def test_specs_equal_ignores_version(self, registry):
        """Test that specs with different versions but same content are equal."""
        spec1 = YadsSpec(
            name="test.table",
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
        )
        spec2 = YadsSpec(
            name="test.table",
            version=999,  # Different version
            columns=[Column(name="id", type=ytypes.Integer())],
        )

        assert registry._specs_equal(spec1, spec2) is True

    def test_specs_not_equal_different_columns(self, registry):
        """Test that specs with different columns are not equal."""
        spec1 = YadsSpec(
            name="test.table",
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
        )
        spec2 = YadsSpec(
            name="test.table",
            version=1,
            columns=[Column(name="name", type=ytypes.String())],
        )

        assert registry._specs_equal(spec1, spec2) is False

    def test_specs_not_equal_different_metadata(self, registry):
        """Test that specs with different metadata are not equal."""
        spec1 = YadsSpec(
            name="test.table",
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
            metadata={"owner": "team_a"},
        )
        spec2 = YadsSpec(
            name="test.table",
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
            metadata={"owner": "team_b"},
        )

        assert registry._specs_equal(spec1, spec2) is False


class TestFilesystemOperations:
    """Test filesystem operations."""

    def test_creates_directory_structure(self, temp_registry_path, sample_spec):
        """Test that registry creates proper directory structure."""
        registry = FileSystemRegistry(temp_registry_path)
        registry.register(sample_spec)

        # Check directory structure
        import urllib.parse

        encoded_name = urllib.parse.quote(sample_spec.name, safe="")
        versions_dir = Path(temp_registry_path) / encoded_name / "versions"

        assert versions_dir.exists()
        assert (versions_dir / "1.yaml").exists()

    def test_yaml_serialization_roundtrip(self, registry, sample_spec):
        """Test that spec can be registered and retrieved without data loss."""
        registry.register(sample_spec)
        retrieved = registry.get(sample_spec.name)

        # Check key properties are preserved
        assert retrieved.name == sample_spec.name
        assert len(retrieved.columns) == len(sample_spec.columns)
        assert retrieved.columns[0].name == sample_spec.columns[0].name
        assert retrieved.columns[1].name == sample_spec.columns[1].name

    def test_url_encoding_in_paths(self, temp_registry_path):
        """Test that spec names are properly URL-encoded in filesystem."""
        spec = YadsSpec(
            name="catalog.db.table",  # Contains dots
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
        )

        registry = FileSystemRegistry(temp_registry_path)
        registry.register(spec)

        # Check that directory uses encoded name
        import urllib.parse

        encoded_name = urllib.parse.quote(spec.name, safe="")
        encoded_dir = Path(temp_registry_path) / encoded_name

        assert encoded_dir.exists()
        assert encoded_name == "catalog.db.table"  # dots don't need encoding


class TestIntegrationWithMemoryFS:
    """Integration tests using fsspec memory filesystem."""

    def test_memory_filesystem(self):
        """Test registry works with fsspec memory filesystem."""
        registry = FileSystemRegistry("memory://test-registry")

        spec = YadsSpec(
            name="test.spec",
            version=1,
            columns=[Column(name="id", type=ytypes.Integer())],
        )

        v1 = registry.register(spec)
        assert v1 == 1

        retrieved = registry.get("test.spec")
        assert retrieved.name == "test.spec"
        assert retrieved.version == 1

        versions = registry.list_versions("test.spec")
        assert versions == [1]

        assert registry.exists("test.spec") is True


class TestVersionFieldHandling:
    """Test that version field gets updated correctly."""

    def test_version_field_gets_updated(self, registry):
        """Test that returned spec has correct version number."""
        spec = YadsSpec(
            name="test.table",
            version=1,  # Initial version
            columns=[Column(name="id", type=ytypes.Integer())],
        )

        v1 = registry.register(spec)
        assert v1 == 1

        # Register modified version
        spec2 = YadsSpec(
            name="test.table",
            version=1,  # Still version 1 in the object
            columns=[
                Column(name="id", type=ytypes.Integer()),
                Column(name="name", type=ytypes.String()),
            ],
        )

        v2 = registry.register(spec2)
        assert v2 == 2

        # Retrieve and check version is correct
        retrieved = registry.get("test.table", version=2)
        assert retrieved.version == 2
