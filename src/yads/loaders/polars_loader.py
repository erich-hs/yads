"""Load a `YadsSpec` from a `polars.Schema`.

This loader converts Polars schemas to yads specifications by building a
normalized dictionary representation and delegating spec construction and
validation to `SpecBuilder`. It preserves column-level nullability where
possible and handles Polars-specific type features.

Example:
    >>> import polars as pl
    >>> from yads.loaders import PolarsLoader
    >>> schema = pl.Schema({
    ...     "id": pl.Int64,
    ...     "name": pl.String,
    ... })
    >>> loader = PolarsLoader()
    >>> spec = loader.load(schema, name="test.table", version="1.0.0")
    >>> spec.name
    'test.table'
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from .. import types as ytypes
from ..exceptions import LoaderConfigError, UnsupportedFeatureError, validation_warning
from .._dependencies import ensure_dependency
from .base import BaseLoaderConfig, ConfigurableLoader
from .common import SpecBuilder

ensure_dependency("polars", min_version="1.0.0")

import polars as pl  # type: ignore[import-untyped] # noqa: E402

if TYPE_CHECKING:
    from ..spec import YadsSpec


@dataclass(frozen=True)
class PolarsLoaderConfig(BaseLoaderConfig):
    """Configuration for PolarsLoader.

    Args:
        mode: Loading mode. "raise" will raise exceptions on unsupported
            features. "coerce" will attempt to coerce unsupported features to
            supported ones with warnings. Defaults to "coerce".
        fallback_type: A yads type to use as fallback when an unsupported
            Polars type is encountered. Only used when mode is "coerce".
            Must be either String or Binary, or None. Defaults to None.
    """

    fallback_type: ytypes.YadsType | None = None

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        super().__post_init__()
        if self.fallback_type is not None:
            if not isinstance(self.fallback_type, (ytypes.String, ytypes.Binary)):
                raise LoaderConfigError(
                    "fallback_type must be either String or Binary type, or None."
                )


class PolarsLoader(ConfigurableLoader):
    """Load a `YadsSpec` from a `polars.Schema`.

    The loader converts Polars schemas to yads specifications by building a
    normalized dictionary representation and delegating spec construction and
    validation to `SpecBuilder`.

    In "raise" mode, incompatible Polars types raise `UnsupportedFeatureError`.
    In "coerce" mode, the loader attempts to coerce unsupported types to
    compatible fallback types (String or Binary) with warnings.

    Notes:
        - Polars Schema doesn't track nullability at the schema level, so all
          fields are treated as nullable by default.
    """

    def __init__(self, config: PolarsLoaderConfig | None = None) -> None:
        """Initialize the PolarsLoader.

        Args:
            config: Configuration object. If None, uses default PolarsLoaderConfig.
        """
        self.config: PolarsLoaderConfig = config or PolarsLoaderConfig()
        super().__init__(self.config)

    def load(
        self,
        schema: pl.Schema,
        *,
        name: str,
        version: str,
        description: str | None = None,
        mode: Literal["raise", "coerce"] | None = None,
    ) -> YadsSpec:
        """Convert the Polars schema to `YadsSpec`.

        Args:
            schema: Source Polars schema.
            name: Fully-qualified spec name to assign.
            version: Spec version string.
            description: Optional human-readable description.
            mode: Optional override for the loading mode. When not provided, the
                loader's configured mode is used. If provided:
                - "raise": Raise on any unsupported features.
                - "coerce": Apply adjustments to produce a valid spec and emit warnings.

        Returns:
            A validated immutable `YadsSpec` instance.
        """
        with self.load_context(mode=mode):
            columns: list[dict[str, Any]] = []
            for field_name, dtype in schema.items():
                with self.load_context(field=field_name):
                    column_def = self._convert_field(field_name, dtype)
                    columns.append(column_def)

            data: dict[str, Any] = {
                "name": name,
                "version": version,
                "columns": columns,
            }

            if description:
                data["description"] = description

            return SpecBuilder(data).build()

    # %% ---- Field and type conversion -----------------------------------------------
    def _convert_field(self, field_name: str, dtype: pl.DataType) -> dict[str, Any]:
        """Convert a Polars field to a normalized column definition.

        Args:
            field_name: Name of the field.
            dtype: Polars data type for the field.

        Returns:
            Dictionary representation of the column definition.
        """
        col: dict[str, Any] = {"name": field_name}

        type_def = self._convert_type(dtype)
        col.update(type_def)

        # Polars Schema doesn't track nullability, so all fields are nullable
        # by default. No not_null constraint is added.

        return col

    def _convert_type(self, dtype: pl.DataType) -> dict[str, Any]:
        """Convert a Polars data type to a normalized type definition.

        Maps Polars types to yads types according to the type mapping strategy.

        Currently unsupported:
            - pl.Categorical
            - pl.Enum
        """
        # Primitive types - check against type classes
        if dtype is pl.Null or isinstance(dtype, pl.Null):
            return {"type": "void"}
        if dtype is pl.Boolean or isinstance(dtype, pl.Boolean):
            return {"type": "boolean"}
        if dtype is pl.Int8 or isinstance(dtype, pl.Int8):
            return {"type": "integer", "params": {"bits": 8, "signed": True}}
        if dtype is pl.Int16 or isinstance(dtype, pl.Int16):
            return {"type": "integer", "params": {"bits": 16, "signed": True}}
        if dtype is pl.Int32 or isinstance(dtype, pl.Int32):
            return {"type": "integer", "params": {"bits": 32, "signed": True}}
        if dtype is pl.Int64 or isinstance(dtype, pl.Int64):
            return {"type": "integer", "params": {"bits": 64, "signed": True}}
        if dtype is pl.UInt8 or isinstance(dtype, pl.UInt8):
            return {"type": "integer", "params": {"bits": 8, "signed": False}}
        if dtype is pl.UInt16 or isinstance(dtype, pl.UInt16):
            return {"type": "integer", "params": {"bits": 16, "signed": False}}
        if dtype is pl.UInt32 or isinstance(dtype, pl.UInt32):
            return {"type": "integer", "params": {"bits": 32, "signed": False}}
        if dtype is pl.UInt64 or isinstance(dtype, pl.UInt64):
            return {"type": "integer", "params": {"bits": 64, "signed": False}}
        if dtype is pl.Float32 or isinstance(dtype, pl.Float32):
            return {"type": "float", "params": {"bits": 32}}
        if dtype is pl.Float64 or isinstance(dtype, pl.Float64):
            return {"type": "float", "params": {"bits": 64}}
        if (
            dtype is pl.String
            or dtype is pl.Utf8
            or isinstance(dtype, (pl.String, pl.Utf8))
        ):
            return {"type": "string"}
        if dtype is pl.Binary or isinstance(dtype, pl.Binary):
            return {"type": "binary"}
        if dtype is pl.Date or isinstance(dtype, pl.Date):
            return {"type": "date", "params": {"bits": 32}}
        if dtype is pl.Time or isinstance(dtype, pl.Time):
            # Polars Time is always nanoseconds
            return {"type": "time", "params": {"unit": "ns", "bits": 64}}

        # Check for parameterized types using isinstance
        if isinstance(dtype, pl.Duration):
            time_unit = self._extract_duration_unit(dtype)
            return {"type": "duration", "params": {"unit": time_unit}}

        if isinstance(dtype, pl.Datetime):
            time_unit = self._extract_datetime_unit(dtype)
            time_zone = self._extract_datetime_timezone(dtype)

            if time_zone is None:
                return {"type": "timestamp", "params": {"unit": time_unit}}
            return {"type": "timestamptz", "params": {"unit": time_unit, "tz": time_zone}}

        if isinstance(dtype, pl.Decimal):
            precision = getattr(dtype, "precision", None)
            scale = getattr(dtype, "scale", None)
            params: dict[str, Any] = {}
            if precision is not None:
                params["precision"] = precision
            if scale is not None:
                params["scale"] = scale
            return {"type": "decimal", "params": params}

        if isinstance(dtype, pl.List):
            inner_type = getattr(dtype, "inner", None)
            if inner_type is None:
                raise UnsupportedFeatureError(
                    f"Cannot extract inner type from List type: {dtype} "
                    f"for '{self._current_field_name or '<unknown>'}'"
                )

            with self.load_context(field="<array_element>"):
                elem_def = self._convert_type(inner_type)
            return {"type": "array", "element": elem_def}

        if isinstance(dtype, pl.Array):
            inner_type = getattr(dtype, "inner", None)
            shape = getattr(dtype, "shape", None)

            if inner_type is None:
                raise UnsupportedFeatureError(
                    f"Cannot extract inner type from Array type: {dtype} "
                    f"for '{self._current_field_name or '<unknown>'}'"
                )

            if shape is None:
                # No shape information, treat as variable-length array
                with self.load_context(field="<array_element>"):
                    elem_def = self._convert_type(inner_type)
                return {"type": "array", "element": elem_def}

            # Convert shape to tuple if it's not already
            if isinstance(shape, (list, tuple)):
                shape_tuple = tuple(shape)
            else:
                shape_tuple = (shape,)

            # For multi-dimensional arrays, extract the base element type
            # by unwrapping nested Array types
            base_inner_type = inner_type
            while isinstance(base_inner_type, pl.Array):
                base_inner_type = getattr(base_inner_type, "inner", None)
                if base_inner_type is None:
                    raise UnsupportedFeatureError(
                        f"Cannot extract base inner type from nested Array type: {dtype} "
                        f"for '{self._current_field_name or '<unknown>'}'"
                    )

            with self.load_context(field="<array_element>"):
                elem_def = self._convert_type(base_inner_type)

            # If shape is 1D, use Array with size parameter
            if len(shape_tuple) == 1:
                return {
                    "type": "array",
                    "element": elem_def,
                    "params": {"size": shape_tuple[0]},
                }

            # Multi-dimensional shape -> Tensor
            return {
                "type": "tensor",
                "element": elem_def,
                "params": {"shape": shape_tuple},
            }

        if isinstance(dtype, pl.Struct):
            fields_list = getattr(dtype, "fields", None)
            if fields_list is None:
                raise UnsupportedFeatureError(
                    f"Cannot extract fields from Struct type: {dtype} "
                    f"for '{self._current_field_name or '<unknown>'}'"
                )

            fields: list[dict[str, Any]] = []
            for pl_field in fields_list:
                field_name = getattr(pl_field, "name", None)
                field_dtype = getattr(pl_field, "dtype", None)

                if field_name is None or field_dtype is None:
                    raise UnsupportedFeatureError(
                        f"Cannot extract field name or dtype from Struct field: "
                        f"{pl_field} for '{self._current_field_name or '<unknown>'}'"
                    )

                with self.load_context(field=field_name):
                    field_def = self._convert_field(field_name, field_dtype)
                    fields.append(field_def)

            return {"type": "struct", "fields": fields}

        if hasattr(pl, "Object"):
            if dtype is pl.Object or isinstance(dtype, pl.Object):
                return {"type": "variant"}

        if hasattr(pl, "Categorical") and isinstance(dtype, pl.Categorical):
            error_msg = (
                f"PolarsLoader does not support Polars type: '{dtype}' "
                f"({type(dtype).__name__}) for "
                f"'{self._current_field_name or '<unknown>'}'"
            )
            return self._handle_unsupported_type(error_msg)

        if hasattr(pl, "Enum") and isinstance(dtype, pl.Enum):
            error_msg = (
                f"PolarsLoader does not support Polars type: '{dtype}' "
                f"({type(dtype).__name__}) for "
                f"'{self._current_field_name or '<unknown>'}'"
            )
            return self._handle_unsupported_type(error_msg)

        error_msg = (
            f"PolarsLoader does not support Polars type: '{dtype}' "
            f"({type(dtype).__name__}) for "
            f"'{self._current_field_name or '<unknown>'}'"
        )
        return self._handle_unsupported_type(error_msg)

    def _handle_unsupported_type(self, error_msg: str) -> dict[str, Any]:
        """Handle unsupported types based on mode configuration."""
        if self.config.mode == "coerce":
            if self.config.fallback_type is None:
                raise UnsupportedFeatureError(
                    f"{error_msg}. Specify a fallback_type to enable coercion "
                    "of unsupported types."
                )
            validation_warning(
                message=(
                    f"{error_msg}. The data type will be coerced to "
                    f"{self.config.fallback_type}."
                ),
                filename="yads.loaders.polars_loader",
                module=__name__,
            )
            return self._get_fallback_type_definition()

        raise UnsupportedFeatureError(f"{error_msg}.")

    def _get_fallback_type_definition(self) -> dict[str, Any]:
        """Get the fallback type definition based on config."""
        if isinstance(self.config.fallback_type, ytypes.Binary):
            binary_type = self.config.fallback_type
            if binary_type.length is not None:
                return {"type": "binary", "params": {"length": binary_type.length}}
            return {"type": "binary"}
        else:  # String (default)
            string_type = self.config.fallback_type
            assert isinstance(string_type, ytypes.String)  # Validated in __post_init__
            if string_type.length is not None:
                return {"type": "string", "params": {"length": string_type.length}}
            return {"type": "string"}

    # %% ---- Helpers -----------------------------------------------------------------
    @staticmethod
    def _extract_duration_unit(dtype: pl.DataType) -> str:
        time_unit = getattr(dtype, "time_unit", None)
        if time_unit is None:
            return "ns"
        return str(time_unit)

    @staticmethod
    def _extract_datetime_unit(dtype: pl.DataType) -> str:
        time_unit = getattr(dtype, "time_unit", None)
        if time_unit is None:
            return "ns"
        return str(time_unit)

    @staticmethod
    def _extract_datetime_timezone(dtype: pl.DataType) -> str | None:
        time_zone = getattr(dtype, "time_zone", None)
        if time_zone is None:
            return None
        return str(time_zone)
