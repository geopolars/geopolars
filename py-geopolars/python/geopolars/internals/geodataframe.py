from __future__ import annotations

import json
from typing import TYPE_CHECKING

import polars as pl
from arro3.core import Field, Schema, Table

from geopolars.internals.enums import GeoArrowExtensionName
from geopolars.internals.geoseries import GeoSeries

if TYPE_CHECKING:
    import geopandas
    from polars._typing import FrameInitTypes


class Subclass(pl.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        print("__arrow_c_stream__")
        x = Table.from_arrow(super())
        return x.__arrow_c_stream__(requested_schema)
        # return super().__arrow_c_stream__(requested_schema)


df = pl.DataFrame({"a": [1, 2, 3, 4]})
sdf = Subclass(df)
t = Table.from_arrow(sdf)


class GeoDataFrame(pl.DataFrame):
    column_types: dict[str, GeoArrowExtensionName]
    column_metadata: dict[str, dict[str, str]]

    def __init__(
        self,
        data: FrameInitTypes | None = None,
        *args,
        ga_col_types: dict[str, GeoArrowExtensionName] | None = None,
        ga_col_meta: dict[str, dict[str, str]] | None = None,
        **kwargs,
    ):
        if hasattr(data, "__arrow_c_stream__"):
            table = Table.from_arrow(data)
            extracted_column_types, extracted_column_metadata = (
                _extract_geoarrow_metadata(table)
            )
            data = _remove_field_metadata(table)

        else:
            extracted_column_types = None
            extracted_column_metadata = None

        column_types = (
            ga_col_types if ga_col_types is not None else extracted_column_types
        )
        if column_types is None:
            raise ValueError("Must pass `ga_col_types` for non-geoarrow input.")

        self.column_types = column_types
        self.column_metadata = (
            ga_col_meta if ga_col_meta is not None else extracted_column_metadata or {}
        )

        super().__init__(data, *args, **kwargs)

        # TODO: Validate input for the given types
        # (at least when passed in by user)

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        table = Table.from_arrow(super())

        new_fields: list[Field] = []
        schema = table.schema
        for field_idx in range(len(schema)):
            field = schema.field(field_idx)
            if field.name not in self.column_types:
                new_fields.append(field)
                continue

            metadata: dict[str, str] = {
                "ARROW:extension:name": self.column_types[field.name],
            }

            if field.name in self.column_metadata:
                metadata["ARROW:extension:metadata"] = json.dumps(
                    self.column_metadata[field.name]
                )

            new_fields.append(field.with_metadata(metadata))

        new_schema = Schema(new_fields, metadata=schema.metadata)
        return table.with_schema(new_schema).__arrow_c_stream__(requested_schema)

    def geometry(self, name: str | None = None):
        if name is None:
            if len(self.column_types) == 0:
                raise ValueError("No geometry columns")
            elif len(self.column_types) == 1:
                name = next(iter(self.column_types.keys()))
            else:
                raise ValueError(
                    "Must pass `name` when multiple geometry columns exist in table"
                )

        if name not in self.column_types:
            raise ValueError(
                f"{name=} is not a geometry column (does not have GeoArrow metadata)"
            )

        s = self.get_column(name)
        ga_type = self.column_types[name]
        ga_meta = self.column_metadata.get(name)

        return GeoSeries(s, ga_type=ga_type, ga_meta=ga_meta)

    @classmethod
    def from_geopandas(cls, data: geopandas.GeoDataFrame):
        if not hasattr(data, "to_arrow"):
            raise ValueError("geopandas 1.0 or higher required")

        return cls(data.to_arrow(geometry_encoding="geoarrow"))

    def to_geopandas(self):
        try:
            import geopandas
        except ImportError:
            raise ImportError("Geopandas is required when using to_geopandas().")

        if not hasattr(geopandas.GeoDataFrame, "from_arrow"):
            raise ValueError("geopandas 1.0 or higher required")

        return geopandas.GeoDataFrame.from_arrow(self)


def _extract_geoarrow_metadata(
    table: Table,
) -> tuple[dict[str, GeoArrowExtensionName] | None, dict[str, dict[str, str]] | None]:
    column_types = {}
    column_metadata = {}

    geoarrow_names = {e.value for e in GeoArrowExtensionName}

    schema = table.schema
    for field_idx in range(len(schema)):
        field = schema.field(field_idx)
        field_meta = field.metadata_str

        if "ARROW:extension:name" not in field_meta:
            continue

        ext_name = field_meta["ARROW:extension:name"]
        if ext_name not in geoarrow_names:
            continue

        column_types[field.name] = GeoArrowExtensionName(ext_name)
        column_metadata[field.name] = {}

        if "ARROW:extension:metadata" not in field_meta:
            continue

        column_metadata[field.name] = json.loads(field_meta["ARROW:extension:metadata"])

    if column_types:
        return column_types, column_metadata
    else:
        return None, None


def _remove_field_metadata(table: Table) -> Table:
    schema = table.schema
    fields: list[Field] = []

    for field_idx in range(len(schema)):
        field = schema.field(field_idx)
        fields.append(field.with_metadata({}))

    new_schema = Schema(fields, metadata=schema.metadata)
    return table.with_schema(new_schema)
