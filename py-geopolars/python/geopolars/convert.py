from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
from polars import DataFrame, Series

from geopolars.enums import GeometryType
from geopolars.internals.geodataframe import GeoDataFrame
from geopolars.internals.geoseries import GeoSeries

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


if TYPE_CHECKING:  # pragma: no cover
    import pandas
    import pyarrow as pa


def from_arrow(a: pa.Table | pa.Array | pa.ChunkedArray) -> GeoDataFrame | GeoSeries:
    """
    Construct a GeoPolars :class:`GeoDataFrame` or :class:`GeoSeries` from an
    Arrow Table or Array.

    This operation will be zero copy for the most part. Types that are not
    supported by Polars may be cast to the closest supported type.

    Parameters
    ----------
    a : :class:`pyarrow.Table` or :class:`pyarrow.Array`
        Data represented as Arrow Table or Array.

    Returns
    -------

    :class:`GeoDataFrame` or :class:`GeoSeries`
    """
    # TODO: this should probably have a check that the data is indeed geographic?
    # And return a bare Series/DataFrame if it isn't?
    output = pl.from_arrow(a)
    if isinstance(output, Series):
        return GeoSeries(output)

    assert isinstance(
        output, DataFrame
    ), "Output of polars.from_arrow expected to be Series or DataFrame"
    return GeoDataFrame(output)


def from_geopandas(
    gdf: geopandas.GeoDataFrame
    | geopandas.GeoSeries
    | pandas.DataFrame
    | pandas.Series,
) -> GeoDataFrame | GeoSeries | DataFrame | Series:
    """
    Construct a GeoPolars :class:`GeoDataFrame` or :class:`GeoSeries` from a
    :class:`geopandas.GeoDataFrame` or :class:`geopandas.GeoSeries`.

    This operation clones data.

    This requires that :mod:`geopandas` and :mod:`pyarrow` are installed.

    Parameters
    ----------
    gdf : :class:`geopandas.GeoDataFrame` or :class:`geopandas.GeoSeries`
        Data represented as a geopandas GeoDataFrame or GeoSeries

    Returns
    -------

    `GeoDataFrame` or `GeoSeries`
    """
    if geopandas is None:
        raise ImportError("Geopandas is required when using from_geopandas().")

    import pandas

    if isinstance(gdf, geopandas.GeoSeries):
        return geopandas_geoseries_to_geopolars(gdf)
    elif isinstance(gdf, geopandas.GeoDataFrame):
        # TODO: update
        return GeoDataFrame._from_geopandas(gdf)
    elif isinstance(gdf, (pandas.DataFrame, pandas.Series)):
        return pl.from_pandas(gdf)
    else:
        raise ValueError(
            f"Expected geopandas GeoDataFrame or GeoSeries, got {type(gdf)}."
        )


def geopandas_geoseries_to_geopolars(geoseries: geopandas.GeoSeries):
    """Convert GeoPandas GeoSeries to GeoArrow

    This prefers converting to a GeoArrow struct encoding when possible, falling back to WKB for mixed geometries.
    """
    if pyarrow is None:
        raise ImportError("Pyarrow is required when using from_geopandas().")

    if shapely is None or shapely.__version__[0] != "2":
        raise ImportError("Shapely version 2 is required when using from_geopandas().")

    import numpy as np

    if len(np.unique(shapely.get_type_id(geoseries))) != 1:
        print("Multiple geometry types: falling back to WKB encoding")
        wkb_arrow_array = pyarrow.Array.from_pandas(geoseries.to_wkb())
        return GeoSeries(wkb_arrow_array, _geom_type=GeometryType.MISSING)

    geom_type, coords, offsets = shapely.to_ragged_array(geoseries, include_z=False)

    # From https://github.com/jorisvandenbossche/python-geoarrow/blob/80b76e74e0492a8f0914ed5331155154d0776593/src/geoarrow/extension_types.py#LL135-L172
    # In the future restore extension array type?
    if geom_type == shapely.GeometryType.POINT:
        parr = pyarrow.StructArray.from_arrays([coords[:, 0], coords[:, 1]], ["x", "y"])
        return GeoSeries(parr, _geom_type=geom_type)

    elif geom_type == shapely.GeometryType.LINESTRING:
        _parr = pyarrow.StructArray.from_arrays(
            [coords[:, 0], coords[:, 1]], ["x", "y"]
        )
        parr = pyarrow.ListArray.from_arrays(pyarrow.array(offsets), _parr)
        return GeoSeries(parr, _geom_type=geom_type)

    elif geom_type == shapely.GeometryType.POLYGON:
        offsets1, offsets2 = offsets
        _parr = pyarrow.StructArray.from_arrays(
            [coords[:, 0], coords[:, 1]], ["x", "y"]
        )
        _parr1 = pyarrow.ListArray.from_arrays(pyarrow.array(offsets1), _parr)
        parr = pyarrow.ListArray.from_arrays(pyarrow.array(offsets2), _parr1)
        return GeoSeries(parr, _geom_type=geom_type)

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
