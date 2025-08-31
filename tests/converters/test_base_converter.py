from yads.converters.base import BaseConverter, BaseConverterConfig


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
