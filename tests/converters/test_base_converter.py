from yads.converters.base import BaseConverter, BaseConverterConfig
from yads.exceptions import ConverterConfigError
from yads.spec import Column, Field, YadsSpec
from yads.types import Integer, String
import pytest
from dataclasses import FrozenInstanceError
from types import MappingProxyType


# %% BaseConverterConfig validations
class TestBaseConverterConfig:
    def test_config_mode_invalid(self):
        with pytest.raises(
            ConverterConfigError, match="mode must be one of 'raise' or 'coerce'."
        ):
            BaseConverterConfig(mode="invalid")

    def test_config_ignore_and_include_overlap(self):
        with pytest.raises(
            ConverterConfigError, match="Columns cannot be both ignored and included"
        ):
            BaseConverterConfig(ignore_columns={"col1"}, include_columns={"col1"})

    def test_config_coerces_mutable_inputs_to_immutable(self):
        def override_fn(field, converter):
            return "x"

        cfg = BaseConverterConfig(
            ignore_columns=["a", "b"],
            include_columns=("c", "d"),
            column_overrides={"a": override_fn},
        )

        assert isinstance(cfg.ignore_columns, frozenset)
        assert isinstance(cfg.include_columns, frozenset)
        assert isinstance(cfg.column_overrides, MappingProxyType)
        assert cfg.ignore_columns == frozenset({"a", "b"})
        assert cfg.include_columns == frozenset({"c", "d"})
        assert cfg.column_overrides["a"] is override_fn

    def test_config_immutable_attributes_and_mappings(self):
        cfg = BaseConverterConfig(
            ignore_columns={"a"}, include_columns=set(), column_overrides={}
        )

        # Dataclass is frozen: attribute reassignment not allowed
        with pytest.raises(FrozenInstanceError):
            cfg.ignore_columns = frozenset()
        with pytest.raises(FrozenInstanceError):
            cfg.include_columns = None
        with pytest.raises(FrozenInstanceError):
            cfg.column_overrides = {}

        # Containers themselves are immutable
        with pytest.raises(AttributeError):
            cfg.ignore_columns.add("z")  # frozenset has no add
        if cfg.include_columns is not None:
            with pytest.raises(AttributeError):
                cfg.include_columns.add("z")
        with pytest.raises(TypeError):
            cfg.column_overrides["x"] = lambda f, c: None

    def test_config_detaches_from_external_mutations(self):
        def f1(field, converter):
            return "f1"

        def f2(field, converter):
            return "f2"

        ignore_mut = ["a", "b"]
        include_mut = {"c"}
        overrides_mut = {"a": f1}

        cfg = BaseConverterConfig(
            ignore_columns=ignore_mut,
            include_columns=include_mut,
            column_overrides=overrides_mut,
        )

        # mutate inputs after construction
        ignore_mut.append("x")
        include_mut.add("y")
        overrides_mut["a"] = f2
        overrides_mut["b"] = f2

        # config remains unchanged
        assert cfg.ignore_columns == frozenset({"a", "b"})
        assert cfg.include_columns == frozenset({"c"})
        assert cfg.column_overrides == MappingProxyType({"a": f1})

    def test_config_include_columns_none_and_empty(self):
        # None remains None
        cfg_none = BaseConverterConfig()
        assert cfg_none.include_columns is None

        # empty iterables become an empty frozenset
        cfg_empty = BaseConverterConfig(include_columns=[])
        assert isinstance(cfg_empty.include_columns, frozenset)
        assert len(cfg_empty.include_columns) == 0

    def test_config_accepts_generators(self):
        def gen():
            for x in ["a", "b", "c"]:
                yield x

        cfg = BaseConverterConfig(ignore_columns=gen())
        assert cfg.ignore_columns == frozenset({"a", "b", "c"})


# %% BaseConverter context manager
class TestBaseConverterContextManager:
    def test_mode_override_and_restore(self):
        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = BaseConverterConfig(mode="raise")
        c = DummyConverter(config)
        # initial mode is raise
        assert c.config.mode == "raise"
        with c.conversion_context(mode="coerce"):
            # temporary coerce
            assert c.config.mode == "coerce"
        # restored to raise
        assert c.config.mode == "raise"

    def test_field_context_override_and_restore(self):
        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        c = DummyConverter()
        assert getattr(c, "_current_field_name") is None
        with c.conversion_context(field="colA"):
            assert getattr(c, "_current_field_name") == "colA"
        assert getattr(c, "_current_field_name") is None


# %% BaseConverter column filtering
class TestBaseConverterColumnFiltering:
    def test_filter_columns_no_filters(self):
        """Test _filter_columns with no ignore/include filters."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="col1", type=String()),
                Column(name="col2", type=Integer()),
                Column(name="col3", type=String()),
            ],
        )
        converter = DummyConverter()
        filtered = list(converter._filter_columns(spec))

        assert len(filtered) == 3
        assert [col.name for col in filtered] == ["col1", "col2", "col3"]

    def test_filter_columns_ignore_columns(self):
        """Test _filter_columns with ignore_columns set."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="col1", type=String()),
                Column(name="col2", type=Integer()),
                Column(name="col3", type=String()),
            ],
        )
        config = BaseConverterConfig(ignore_columns={"col2"})
        converter = DummyConverter(config)
        filtered = list(converter._filter_columns(spec))

        assert len(filtered) == 2
        assert [col.name for col in filtered] == ["col1", "col3"]

    def test_filter_columns_include_columns(self):
        """Test _filter_columns with include_columns set."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="col1", type=String()),
                Column(name="col2", type=Integer()),
                Column(name="col3", type=String()),
            ],
        )
        config = BaseConverterConfig(include_columns={"col1", "col3"})
        converter = DummyConverter(config)
        filtered = list(converter._filter_columns(spec))

        assert len(filtered) == 2
        assert [col.name for col in filtered] == ["col1", "col3"]

    def test_filter_columns_empty_include_columns(self):
        """Test _filter_columns with empty include_columns results in no columns."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="col1", type=String()),
                Column(name="col2", type=Integer()),
            ],
        )
        config = BaseConverterConfig(include_columns=set())
        converter = DummyConverter(config)
        filtered = list(converter._filter_columns(spec))

        assert len(filtered) == 0

    def test_validate_column_filters_valid(self):
        """Test _validate_column_filters with valid column names."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="col1", type=String()),
                Column(name="col2", type=Integer()),
                Column(name="col3", type=String()),
            ],
        )
        config = BaseConverterConfig(
            ignore_columns={"col1"}, include_columns={"col2", "col3"}
        )
        converter = DummyConverter(config)

        # Should not raise any exception
        converter._validate_column_filters(spec)

    def test_validate_column_filters_unknown_ignored(self):
        """Test _validate_column_filters with unknown ignored columns."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="col1", type=String()),
                Column(name="col2", type=Integer()),
            ],
        )
        config = BaseConverterConfig(ignore_columns={"col1", "unknown_col"})
        converter = DummyConverter(config)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in ignore_columns: unknown_col"
        ):
            converter._validate_column_filters(spec)

    def test_validate_column_filters_unknown_included(self):
        """Test _validate_column_filters with unknown included columns."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="col1", type=String()),
                Column(name="col2", type=Integer()),
            ],
        )
        config = BaseConverterConfig(include_columns={"col1", "unknown_col"})
        converter = DummyConverter(config)

        with pytest.raises(
            ConverterConfigError, match="Unknown columns in include_columns: unknown_col"
        ):
            converter._validate_column_filters(spec)

    def test_validate_column_filters_both_unknown(self):
        """Test _validate_column_filters with both unknown ignored and included columns."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        spec = YadsSpec(
            name="test",
            version="1.0.0",
            columns=[
                Column(name="col1", type=String()),
            ],
        )
        config = BaseConverterConfig(
            ignore_columns={"unknown1", "unknown2"}, include_columns={"unknown3"}
        )
        converter = DummyConverter(config)

        with pytest.raises(ConverterConfigError) as exc_info:
            converter._validate_column_filters(spec)

        error_msg = str(exc_info.value)
        assert "Unknown columns in ignore_columns: unknown1, unknown2" in error_msg
        assert "Unknown columns in include_columns: unknown3" in error_msg


# %% BaseConverter column overrides
class TestBaseConverterColumnOverrides:
    def test_has_column_override_true(self):
        """Test _has_column_override returns True when override exists."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = BaseConverterConfig(
            column_overrides={"col1": lambda field, converter: "custom"}
        )
        converter = DummyConverter(config)

        assert converter._has_column_override("col1") is True

    def test_has_column_override_false(self):
        """Test _has_column_override returns False when override does not exist."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = BaseConverterConfig(
            column_overrides={"col1": lambda field, converter: "custom"}
        )
        converter = DummyConverter(config)

        assert converter._has_column_override("col2") is False

    def test_apply_column_override(self):
        """Test _apply_column_override calls the override function correctly."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        def custom_override(field, converter):
            return f"custom_{field.name}"

        config = BaseConverterConfig(column_overrides={"col1": custom_override})
        converter = DummyConverter(config)

        field = Field(name="col1", type=String())
        result = converter._apply_column_override(field)

        assert result == "custom_col1"

    def test_convert_field_with_overrides_uses_override(self):
        """Test _convert_field_with_overrides uses override when available."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

            def _convert_field_default(self, field):
                return f"default_{field.name}"

        def custom_override(field, converter):
            return f"override_{field.name}"

        config = BaseConverterConfig(column_overrides={"col1": custom_override})
        converter = DummyConverter(config)

        field = Field(name="col1", type=String())
        result = converter._convert_field_with_overrides(field)

        assert result == "override_col1"

    def test_convert_field_with_overrides_uses_default(self):
        """Test _convert_field_with_overrides uses default when no override."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

            def _convert_field_default(self, field):
                return f"default_{field.name}"

        config = BaseConverterConfig(column_overrides={"col2": lambda f, c: "override"})
        converter = DummyConverter(config)

        field = Field(name="col1", type=String())
        result = converter._convert_field_with_overrides(field)

        assert result == "default_col1"

    def test_convert_field_default_not_implemented_error(self):
        """Test _convert_field_default raises NotImplementedError by default."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        converter = DummyConverter()
        field = Field(name="col1", type=String())

        with pytest.raises(
            NotImplementedError,
            match="DummyConverter must implement _convert_field_default",
        ):
            converter._convert_field_with_overrides(field)
