import io
import pytest
import yaml
from pathlib import Path

from yads.exceptions import SpecParsingError
from yads.loaders import (
    from_yaml,
    from_yaml_path,
    from_yaml_string,
    from_yaml_stream,
)
from yads.spec import YadsSpec

VALID_SPEC_DIR = Path(__file__).parent.parent / "fixtures" / "spec" / "valid"


# %% Fixtures
# Get all valid spec files
valid_spec_files = list(VALID_SPEC_DIR.glob("*.yaml"))


@pytest.fixture(params=valid_spec_files, ids=[f.name for f in valid_spec_files])
def valid_spec_path(request):
    return request.param


@pytest.fixture
def valid_spec_content(valid_spec_path):
    return valid_spec_path.read_text()


@pytest.fixture
def valid_spec_dict(valid_spec_content):
    return yaml.safe_load(valid_spec_content)


# %% From YAML helpers
class TestFromYaml:
    def test_with_valid_path(self, valid_spec_path, valid_spec_dict):
        spec = from_yaml_path(valid_spec_path)
        assert isinstance(spec, YadsSpec)
        assert spec.name == valid_spec_dict["name"]

    def test_with_valid_string_content(self, valid_spec_content, valid_spec_dict):
        spec = from_yaml_string(valid_spec_content)
        assert isinstance(spec, YadsSpec)
        assert spec.name == valid_spec_dict["name"]

    def test_with_text_stream(self, valid_spec_content, valid_spec_dict):
        stream = io.StringIO(valid_spec_content)
        spec = from_yaml_stream(stream)
        assert isinstance(spec, YadsSpec)
        assert spec.name == valid_spec_dict["name"]

    def test_with_binary_stream(self, valid_spec_content, valid_spec_dict):
        stream = io.BytesIO(valid_spec_content.encode("utf-8"))
        spec = from_yaml_stream(stream)
        assert isinstance(spec, YadsSpec)
        assert spec.name == valid_spec_dict["name"]

    def test_from_yaml_convenience_with_path(self, valid_spec_path, valid_spec_dict):
        spec = from_yaml(valid_spec_path)
        assert isinstance(spec, YadsSpec)
        assert spec.name == valid_spec_dict["name"]

    def test_from_yaml_convenience_with_text_stream(
        self, valid_spec_content, valid_spec_dict
    ):
        stream = io.StringIO(valid_spec_content)
        spec = from_yaml(stream)
        assert isinstance(spec, YadsSpec)
        assert spec.name == valid_spec_dict["name"]

    def test_from_yaml_convenience_with_binary_stream(
        self, valid_spec_content, valid_spec_dict
    ):
        stream = io.BytesIO(valid_spec_content.encode("utf-8"))
        spec = from_yaml(stream)
        assert isinstance(spec, YadsSpec)
        assert spec.name == valid_spec_dict["name"]

    def test_from_yaml_path_missing_file_raises(self):
        with pytest.raises(FileNotFoundError):
            from_yaml_path("non_existent/spec.yaml")

    def test_invalid_yaml_content_raises_error(self):
        content = "- item1\n- item2"
        with pytest.raises(
            SpecParsingError, match="Loaded YAML content did not parse to a dictionary"
        ):
            from_yaml_string(content)
