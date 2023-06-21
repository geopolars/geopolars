from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from geopolars import _geopolars
from geopolars.enums import GeometryType
from geopolars.internals.georust import GeoRustSeries
from geopolars.internals.geos import GEOSSeriesOperations
from geopolars.proj import PROJ_DATA_PATH

try:
    import geopandas
except ImportError:
    geopandas = None

try:
    import pyarrow
except ImportError:
    pyarrow = None

try:
    import shapely
except ImportError:
    shapely = None

if TYPE_CHECKING:
    import geopandas
    import pyproj


class GeoSeries(pl.Series):
    """Extension of `polars.Series` to handle geospatial vector data."""

    _geom_type: GeometryType | None

    def __init__(self, *args, _geom_type: GeometryType | None = None, **kwargs):
        self._geom_type = _geom_type

        if isinstance(args[0], pl.Series):
            self._s = args[0]._s
            return

        super().__init__(*args, **kwargs)

    # TODO: these are named too similarly
    @property
    def geo(self) -> GeoRustSeries:
        return GeoRustSeries(series=self)

    @property
    def geos(self) -> GEOSSeriesOperations:
        return GEOSSeriesOperations(series=self)

    @classmethod
    def _from_geopandas(cls, geoseries: geopandas.GeoSeries, force_wkb: bool):
        if geopandas is None:
            raise ImportError("Geopandas is required when using from_geopandas().")

        if pyarrow is None:
            raise ImportError("Pyarrow is required when using from_geopandas().")

        if shapely is None or shapely.__version__[0] != "2":
            raise ImportError(
                "Shapely version 2 is required when using from_geopandas()."
            )

        import numpy as np

        if len(np.unique(shapely.get_type_id(geoseries))) != 1:
            print("Multiple geometry types: falling back to WKB encoding")
            force_wkb = True

        if force_wkb:
            wkb_arrow_array = pyarrow.Array.from_pandas(geoseries.to_wkb())
            polars_series = pl.Series._from_arrow(
                geoseries.name or "geometry", wkb_arrow_array
            )
            return cls(polars_series, _geom_type=GeometryType.MISSING)

        geom_type, coords, offsets = shapely.to_ragged_array(geoseries, include_z=False)

        # From https://github.com/jorisvandenbossche/python-geoarrow/blob/80b76e74e0492a8f0914ed5331155154d0776593/src/geoarrow/extension_types.py#LL135-L172 # noqa E501
        # In the future restore extension array type?
        if geom_type == shapely.GeometryType.POINT:
            parr = pyarrow.StructArray.from_arrays(
                [coords[:, 0], coords[:, 1]], ["x", "y"]
            )
            return cls(parr, _geom_type=GeometryType.POINT)

        elif geom_type == shapely.GeometryType.LINESTRING:
            offsets1 = offsets[0]
            _parr = pyarrow.StructArray.from_arrays(
                [coords[:, 0], coords[:, 1]], ["x", "y"]
            )
            parr = pyarrow.ListArray.from_arrays(pyarrow.array(offsets1), _parr)
            return cls(parr, _geom_type=GeometryType.LINESTRING)

        elif geom_type == shapely.GeometryType.POLYGON:
            offsets1, offsets2 = offsets
            _parr = pyarrow.StructArray.from_arrays(
                [coords[:, 0], coords[:, 1]], ["x", "y"]
            )
            _parr1 = pyarrow.ListArray.from_arrays(pyarrow.array(offsets1), _parr)
            parr = pyarrow.ListArray.from_arrays(pyarrow.array(offsets2), _parr1)
            return cls(parr, _geom_type=GeometryType.POLYGON)

        elif geom_type == shapely.GeometryType.MULTIPOINT:
            raise NotImplementedError("Multi types not yet supported")

            _parr = pyarrow.StructArray.from_arrays(
                [coords[:, 0], coords[:, 1]], ["x", "y"]
            )
            parr = pyarrow.ListArray.from_arrays(pyarrow.array(offsets), _parr)
            return geom_type, parr

        elif geom_type == shapely.GeometryType.MULTILINESTRING:
            raise NotImplementedError("Multi types not yet supported")
            offsets1, offsets2 = offsets
            _parr = pyarrow.StructArray.from_arrays(
                [coords[:, 0], coords[:, 1]], ["x", "y"]
            )
            _parr1 = pyarrow.ListArray.from_arrays(pyarrow.array(offsets1), _parr)
            parr = pyarrow.ListArray.from_arrays(pyarrow.array(offsets2), _parr1)
            return geom_type, parr

        elif geom_type == shapely.GeometryType.MULTIPOLYGON:
            raise NotImplementedError("Multi types not yet supported")

            offsets1, offsets2, offsets3 = offsets
            _parr = pyarrow.StructArray.from_arrays(
                [coords[:, 0], coords[:, 1]], ["x", "y"]
            )
            _parr1 = pyarrow.ListArray.from_arrays(pyarrow.array(offsets1), _parr)
            _parr2 = pyarrow.ListArray.from_arrays(pyarrow.array(offsets2), _parr1)
            parr = pyarrow.ListArray.from_arrays(pyarrow.array(offsets3), _parr2)
            return geom_type, parr
        else:
            raise ValueError("wrong type ", geom_type)

    def to_geopandas(self) -> geopandas.GeoSeries:
        """Converts this `GeoSeries` to a [`geopandas.GeoSeries`][geopandas.GeoSeries].

        This operation clones data. This requires that `geopandas` and
        `pyarrow` are installed.

        Returns:

            This `GeoSeries` as a `geopandas.GeoSeries`.
        """
        if geopandas is None:
            raise ImportError("Geopandas is required when using to_geopandas().")

        import numpy as np

        pyarrow_array = self.to_arrow()
        if not self._geom_type or pyarrow_array.type == pyarrow.binary():
            numpy_array = pyarrow_array.to_numpy(zero_copy_only=False)
            # Ideally we shouldn't need the cast to numpy, but the pyarrow BinaryScalar
            # hasn't implemented len()
            return geopandas.GeoSeries(geopandas.array.from_wkb(numpy_array))

        def geoarrow_coords_to_numpy(struct_array: pyarrow.StructArray):
            x_coords = struct_array.field("x").to_numpy()
            y_coords = struct_array.field("y").to_numpy()
            return np.vstack([x_coords, y_coords]).T

        # Assume it's a geoarrow column
        if self._geom_type == GeometryType.POINT:
            coords = geoarrow_coords_to_numpy(pyarrow_array)
            return shapely.from_ragged_array(shapely.GeometryType.POINT, coords, None)

        elif self._geom_type == GeometryType.LINESTRING:
            coords = geoarrow_coords_to_numpy(pyarrow_array.values)
            offsets = np.asarray(pyarrow_array.offsets)
            return shapely.from_ragged_array(
                shapely.GeometryType.LINESTRING, coords, offsets
            )

        elif self._geom_type == GeometryType.POLYGON:
            coords = geoarrow_coords_to_numpy(pyarrow_array.values.values)
            offsets2 = np.asarray(pyarrow_array.offsets)
            offsets1 = np.asarray(pyarrow_array.values.offsets)
            offsets = (offsets1, offsets2)  # type: ignore
            return shapely.from_ragged_array(
                shapely.GeometryType.POLYGON, coords, offsets
            )

        elif self._geom_type == GeometryType.MULTIPOINT:
            coords = geoarrow_coords_to_numpy(pyarrow_array.values)
            offsets = np.asarray(pyarrow_array.offsets)
            return shapely.from_ragged_array(
                shapely.GeometryType.MULTIPOINT, coords, offsets
            )

        elif self._geom_type == GeometryType.MULTILINESTRING:
            coords = geoarrow_coords_to_numpy(pyarrow_array.values.values)
            offsets2 = np.asarray(pyarrow_array.offsets)
            offsets1 = np.asarray(pyarrow_array.values.offsets)
            offsets = (offsets1, offsets2)  # type: ignore
            return shapely.from_ragged_array(
                shapely.GeometryType.MULTILINESTRING, coords, offsets
            )

        elif self._geom_type == GeometryType.MULTIPOLYGON:
            coords = geoarrow_coords_to_numpy(pyarrow_array.values.values.values)
            offsets3 = np.asarray(pyarrow_array.offsets)
            offsets2 = np.asarray(pyarrow_array.values.offsets)
            offsets1 = np.asarray(pyarrow_array.values.values.offsets)
            offsets = (offsets1, offsets2, offsets3)  # type: ignore
            return shapely.from_ragged_array(
                shapely.GeometryType.MULTIPOLYGON, coords, offsets
            )

        raise ValueError()

    def to_crs(
        self, from_crs: str | pyproj.crs.CRS, to_crs: str | pyproj.crs.CRS
    ) -> GeoSeries:
        """
        Transform all geometries in a GeoSeries to a different coordinate
        reference system.

        For now, you must pass in both ``from_crs`` and ``to_crs``. In the future, we'll
        handle the current CRS automatically.

        This method will transform all points in all objects.  It has no notion
        of projecting entire geometries.  All segments joining points are
        assumed to be lines in the current projection, not geodesics.  Objects
        crossing the dateline (or other projection boundary) will have
        undesirable behavior.

        Parameters:

            from_crs: Origin coordinate system. The value can be anything accepted
                by [`pyproj.CRS.from_user_input()`][pyproj.crs.CRS.from_user_input],
                such as an authority string (eg "EPSG:4326") or a WKT string.
            to_crs: Destination coordinate system. The value can be anything accepted
                by [`pyproj.CRS.from_user_input()`][pyproj.crs.CRS.from_user_input],
                such as an authority string (eg "EPSG:4326") or a WKT string.

        Returns:

            A `GeoSeries` with all geometries transformed to a new coordinate reference
                system.
        """

        if not hasattr(_geopolars.proj, "to_crs"):
            # TODO: use a custom geopolars exception class here
            raise ValueError("Geopolars not built with proj support")

        if not PROJ_DATA_PATH:
            raise ValueError("PROJ_DATA could not be found.")

        # If pyproj.CRS objects are passed in, serialize them to PROJJSON
        if not isinstance(from_crs, str) and hasattr(from_crs, "to_json"):
            from_crs = from_crs.to_json()

        if not isinstance(to_crs, str) and hasattr(to_crs, "to_json"):
            to_crs = to_crs.to_json()

        return _geopolars.proj.to_crs(self, from_crs, to_crs, PROJ_DATA_PATH)
