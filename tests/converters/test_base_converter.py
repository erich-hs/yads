from yads.converters.base import BaseConverter, BaseConverterConfig
from yads.exceptions import ConverterConfigError, UnsupportedFeatureError
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


# %% BaseConverter raise_or_coerce method
class TestBaseConverterRaiseOrCoerce:
    def test_raise_or_coerce_is_public(self):
        """Test that raise_or_coerce is a public method."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class DummyConfig(BaseConverterConfig):
            fallback_type: str = "fallback_string"

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        converter = DummyConverter(DummyConfig(mode="coerce"))

        # Should be accessible as a public method
        assert hasattr(converter, "raise_or_coerce")
        assert callable(converter.raise_or_coerce)
        # Should not have underscore prefix
        assert not hasattr(converter, "_raise_or_coerce")

    def test_raise_or_coerce_raises_in_raise_mode(self):
        """Test raise_or_coerce raises UnsupportedFeatureError in raise mode."""
        from yads.exceptions import UnsupportedFeatureError

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = BaseConverterConfig(mode="raise")
        converter = DummyConverter(config)

        with pytest.raises(UnsupportedFeatureError) as exc_info:
            converter.raise_or_coerce(
                "UnsupportedType",
                coerce_type="StringType",  # Explicit coerce_type
                error_msg="Custom error message",
            )

        assert "Custom error message" in str(exc_info.value)

    def test_raise_or_coerce_raises_with_default_message(self):
        """Test raise_or_coerce generates default error message."""
        from yads.exceptions import UnsupportedFeatureError

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = BaseConverterConfig(mode="raise")
        converter = DummyConverter(config)
        converter._current_field_name = "test_field"

        with pytest.raises(UnsupportedFeatureError) as exc_info:
            converter.raise_or_coerce("MyType", coerce_type="StringType")

        error_msg = str(exc_info.value)
        assert "DummyConverter does not support type: MyType" in error_msg
        assert "for 'test_field'" in error_msg

    def test_raise_or_coerce_returns_fallback_in_coerce_mode(self):
        """Test raise_or_coerce returns fallback type in coerce mode."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class DummyConfig(BaseConverterConfig):
            fallback_type: str = "StringFallback"

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = DummyConfig(mode="coerce")
        converter = DummyConverter(config)

        with pytest.warns(UserWarning) as warning_list:
            result = converter.raise_or_coerce("UnsupportedType")

        assert result == "StringFallback"
        assert len(warning_list) == 1

    def test_raise_or_coerce_returns_explicit_coerce_type(self):
        """Test raise_or_coerce returns explicit coerce_type over fallback."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class DummyConfig(BaseConverterConfig):
            fallback_type: str = "StringFallback"

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = DummyConfig(mode="coerce")
        converter = DummyConverter(config)

        with pytest.warns(UserWarning):
            result = converter.raise_or_coerce(
                "UnsupportedType", coerce_type="CustomCoerceType"
            )

        assert result == "CustomCoerceType"

    def test_raise_or_coerce_warning_includes_type_and_coercion(self):
        """Test warning message includes both error and coercion info."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class DummyConfig(BaseConverterConfig):
            fallback_type: str = "StringFallback"

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = DummyConfig(mode="coerce")
        converter = DummyConverter(config)

        with pytest.warns(UserWarning) as warning_list:
            converter.raise_or_coerce("UnsupportedType", error_msg="Type not supported")

        warning_msg = str(warning_list[0].message)
        assert "Type not supported" in warning_msg
        assert "will be coerced to" in warning_msg
        assert "StringFallback" in warning_msg

    def test_raise_or_coerce_uses_field_context(self):
        """Test raise_or_coerce includes field context in messages."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class DummyConfig(BaseConverterConfig):
            fallback_type: str = "fallback"

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = DummyConfig(mode="coerce")
        converter = DummyConverter(config)
        converter._current_field_name = "my_column"

        with pytest.warns(UserWarning) as warning_list:
            converter.raise_or_coerce("UnsupportedType")

        warning_msg = str(warning_list[0].message)
        assert "my_column" in warning_msg

    def test_raise_or_coerce_missing_fallback_type_error(self):
        """Test raise_or_coerce raises UnsupportedFeatureError with hint when fallback_type is None in coerce mode."""

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        # Config without fallback_type attribute
        config = BaseConverterConfig(mode="coerce")
        converter = DummyConverter(config)

        with pytest.raises(UnsupportedFeatureError) as exc_info:
            converter.raise_or_coerce("UnsupportedType")

        assert "Specify a fallback_type" in str(exc_info.value)

    def test_raise_or_coerce_format_type_for_display_hook(self):
        """Test _format_type_for_display hook is used."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class CustomConfig(BaseConverterConfig):
            fallback_type: str = "fallback"

        class CustomConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

            def _format_type_for_display(self, type_obj):
                return f"CUSTOM[{type_obj}]"

        config = CustomConfig(mode="coerce")
        converter = CustomConverter(config)

        with pytest.warns(UserWarning) as warning_list:
            converter.raise_or_coerce("UnsupportedType", error_msg="Test error")

        warning_msg = str(warning_list[0].message)
        assert "CUSTOM[fallback]" in warning_msg

    def test_raise_or_coerce_with_none_yads_type_and_error_msg(self):
        """Test raise_or_coerce accepts None for yads_type when error_msg is provided."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class DummyConfig(BaseConverterConfig):
            fallback_type: str = "fallback"

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = DummyConfig(mode="coerce")
        converter = DummyConverter(config)

        # Should work with yads_type=None when error_msg is explicit
        with pytest.warns(UserWarning) as warning_list:
            result = converter.raise_or_coerce(
                yads_type=None, error_msg="Custom error without yads_type"
            )

        assert result == "fallback"
        assert "Custom error without yads_type" in str(warning_list[0].message)

    def test_raise_or_coerce_raises_when_both_none(self):
        """Test raise_or_coerce raises ValueError when both yads_type and error_msg are None."""
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class DummyConfig(BaseConverterConfig):
            fallback_type: str = "fallback"

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = DummyConfig(mode="coerce")
        converter = DummyConverter(config)

        # Should raise ValueError when both are None
        with pytest.raises(ValueError) as exc_info:
            converter.raise_or_coerce(yads_type=None, error_msg=None)

        assert "Either yads_type or error_msg must be provided" in str(exc_info.value)

    def test_raise_or_coerce_with_context_mode_override(self):
        """Test raise_or_coerce respects conversion_context mode override."""
        from yads.exceptions import UnsupportedFeatureError
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class DummyConfig(BaseConverterConfig):
            fallback_type: str = "fallback"

        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        config = DummyConfig(mode="coerce")
        converter = DummyConverter(config)

        # Override to raise mode
        with converter.conversion_context(mode="raise"):
            with pytest.raises(UnsupportedFeatureError):
                converter.raise_or_coerce("UnsupportedType", error_msg="Should raise")

        # Back to coerce mode
        with pytest.warns(UserWarning):
            result = converter.raise_or_coerce("UnsupportedType")
            assert result == "fallback"
