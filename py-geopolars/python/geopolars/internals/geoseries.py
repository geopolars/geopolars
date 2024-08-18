from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import polars as pl
from arro3.core import Array, ChunkedArray, Field, Table

from geopolars.internals.base import SeriesWrapper
from geopolars.internals.enums import GeoArrowExtensionName
from geopolars.internals.georust.geoseries import GeoRustSeries
from geopolars.proj.reproject import reproject_column

if TYPE_CHECKING:
    import geopandas
    import pyproj
    from polars.series.series import ArrayLike

import geoarrow.pyarrow
import numpy as np
import pyarrow as pa

geoarrow.pyarrow.register_extension_types()

x = np.arange(0, 4, dtype=np.float64)
y = np.arange(4, 8, dtype=np.float64)
arr = pa.StructArray.from_arrays([x, y], ["x", "y"])
s = pl.Series(arr)
arr2 = pa.array(s)
arr2.type


class GeoSeries(pl.Series):
    """Extension of `polars.Series` to handle geospatial vector data."""

    geoarrow_type: GeoArrowExtensionName
    geoarrow_metadata: dict[str, str]

    def __init__(
        self,
        name: str | ArrayLike | None = None,
        values: ArrayLike | None = None,
        *args,
        ga_type: GeoArrowExtensionName | None = None,
        ga_meta: dict[str, str] | None = None,
        **kwargs,
    ):
        # The GeoSeries constructor has a bunch of overloads. In particular, either name
        # or value can hold the actual data input.

        # First check for __arrow_c_stream__ and then check for __arrow_c_array__ to
        # make absolutely sure that the source is not rechunking to a contiguous array
        if hasattr(name, "__arrow_c_stream__"):
            name = ChunkedArray.from_arrow(name)
            extracted_ga_type, extracted_ga_meta = _extract_geoarrow_metadata(
                name.field.metadata_str
            )
            name = _remove_chunked_array_field_metadata(name)

        elif hasattr(values, "__arrow_c_stream__"):
            values = ChunkedArray.from_arrow(values)
            extracted_ga_type, extracted_ga_meta = _extract_geoarrow_metadata(
                values.field.metadata_str
            )
            values = _remove_chunked_array_field_metadata(values)

        elif hasattr(name, "__arrow_c_array__"):
            raise NotImplementedError()
            name = Array.from_arrow(name)
            extracted_ga_type, extracted_ga_meta = _extract_geoarrow_metadata(
                name.field.metadata_str
            )

        elif hasattr(values, "__arrow_c_array__"):
            raise NotImplementedError()
        else:
            extracted_ga_type = None
            extracted_ga_meta = None

        geoarrow_type = ga_type if ga_type is not None else extracted_ga_type
        if geoarrow_type is None:
            raise ValueError("Must pass `ga_type` for non-geoarrow input.")
        self.geoarrow_type = geoarrow_type
        self.geoarrow_metadata = (
            ga_meta if ga_meta is not None else extracted_ga_meta or {}
        )

        super().__init__(name, values, *args, **kwargs)

        # TODO: Validate input for the given types
        # (at least when passed in by user)

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """
        Export a Series via the Arrow PyCapsule Interface.

        https://arrow.apache.org/docs/dev/format/CDataInterface/PyCapsuleInterface.html
        """
        sw = SeriesWrapper(
            s=self,
            geoarrow_type=self.geoarrow_type,
            geoarrow_metadata=self.geoarrow_metadata,
        )
        return sw.__arrow_c_stream__(requested_schema)

    @classmethod
    def from_geopandas(cls, data: geopandas.GeoSeries):
        if not hasattr(data, "to_arrow"):
            raise ValueError("geopandas 1.0 or higher required")

        arrow_array = data.to_arrow(geometry_encoding="geoarrow")
        return cls(arrow_array)

    def to_geopandas(self) -> geopandas.GeoSeries:
        """Converts this `GeoSeries` to a [`geopandas.GeoSeries`][geopandas.GeoSeries].

        This operation clones data. This requires that `geopandas` and
        `pyarrow` are installed.

        Returns:

            This `GeoSeries` as a `geopandas.GeoSeries`.
        """
        try:
            import geopandas
        except ImportError:
            raise ImportError("Geopandas is required when using to_geopandas().")

        if not hasattr(geopandas.GeoSeries, "from_arrow"):
            raise ValueError("geopandas 1.0 or higher required")

        return geopandas.GeoSeries.from_arrow(self)

    def crs(self) -> pyproj.CRS | None:
        crs_obj = self.geoarrow_metadata.get("crs")
        return pyproj.CRS.from_user_input(crs_obj) if crs_obj is not None else None

    @property
    def geo(self) -> GeoRustSeries:
        sw = SeriesWrapper(
            s=self,
            geoarrow_type=self.geoarrow_type,
            geoarrow_metadata=self.geoarrow_metadata,
        )
        return GeoRustSeries(s=sw)

    # @property
    # def geos(self) -> GEOSSeriesOperations:
    #     return GEOSSeriesOperations(series=self)

    def set_crs(
        self,
        crs: Any | None = None,
        epsg: int | None = None,
    ) -> GeoSeries:
        from pyproj import CRS

        if isinstance(crs, pyproj.CRS):
            crs_obj = crs
        elif crs is not None:
            crs_obj = CRS.from_user_input(crs)
        elif epsg is not None:
            crs_obj = CRS.from_epsg(epsg)
        else:
            raise ValueError("Either crs or epsg must be provided")

        naive_series = pl.Series._from_pyseries(self._s)

        ga_meta = self.geoarrow_metadata.copy()
        ga_meta["crs"] = crs_obj.to_json()

        return GeoSeries(naive_series, ga_type=self.geoarrow_type, ga_meta=ga_meta)

    def to_crs(self, to_crs: Any | pyproj.CRS) -> GeoSeries:
        """
        Transform all geometries in a GeoSeries to a different coordinate
        reference system.

        This method will transform all points in all objects.  It has no notion
        of projecting entire geometries.  All segments joining points are
        assumed to be lines in the current projection, not geodesics.  Objects
        crossing the dateline (or other projection boundary) will have
        undesirable behavior.

        Parameters:

            to_crs: Destination coordinate system. The value can be anything accepted
                by [`pyproj.CRS.from_user_input()`][pyproj.crs.CRS.from_user_input],
                such as an authority string (eg "EPSG:4326") or a WKT string.

        Returns:

            A `GeoSeries` with all geometries transformed to a new coordinate reference
                system.
        """
        from pyproj import CRS

        to_crs_obj = CRS.from_user_input(to_crs)
        ca = ChunkedArray.from_arrow(self)
        out_field, out_ca = reproject_column(
            field=ca.field, column=ca, to_crs=to_crs_obj
        )
        return GeoSeries(ChunkedArray(out_ca.chunks, out_field))


def _extract_geoarrow_metadata(
    meta: dict[str, str],
) -> tuple[GeoArrowExtensionName | None, dict[str, str] | None]:
    ext_name = meta.get("ARROW:extension:name")
    ga_type = GeoArrowExtensionName(ext_name) if ext_name is not None else None
    ga_meta_str = meta.get("ARROW:extension:metadata")
    ga_meta = json.loads(ga_meta_str) if ga_meta_str is not None else None
    return ga_type, ga_meta


def _remove_chunked_array_field_metadata(ca: ChunkedArray) -> ChunkedArray:
    if "ARROW:extension:name" not in ca.field.metadata_str:
        return ca

    # Workaround for
    # PanicException: assertion failed: !self.name.is_null()
    field = Field("", ca.type)
    return ChunkedArray(ca.chunks, field)


def _remove_array_field_metadata(arr: Array) -> Array:
    if "ARROW:extension:name" not in arr.field.metadata_str:
        return arr

    # Workaround for
    # PanicException: assertion failed: !self.name.is_null()
    field = Field("", ca.type)
    return Array(arr, field)


name = s.to_arrow()
self = GeoSeries(s, ga_type=GeoArrowExtensionName.POINT)
ca = ChunkedArray.from_arrow(self)
GeoSeries(ca)

self[0]
