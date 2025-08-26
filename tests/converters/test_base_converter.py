from yads.converters.base import BaseConverter


# %% BaseConverter context manager
class TestBaseConverterContextManager:
    def test_mode_override_and_restore(self):
        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        c = DummyConverter(mode="raise")
        # initial mode is raise
        assert getattr(c, "_mode") == "raise"
        with c.conversion_context(mode="coerce"):
            # temporary coerce
            assert getattr(c, "_mode") == "coerce"
        # restored to raise
        assert getattr(c, "_mode") == "raise"

    def test_field_context_override_and_restore(self):
        class DummyConverter(BaseConverter):
            def convert(self, spec, **kwargs):
                return None

        c = DummyConverter()
        assert getattr(c, "_current_field_name") is None
        with c.conversion_context(field="colA"):
            assert getattr(c, "_current_field_name") == "colA"
        assert getattr(c, "_current_field_name") is None
