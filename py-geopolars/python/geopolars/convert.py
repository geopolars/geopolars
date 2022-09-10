from __future__ import annotations

from typing import TYPE_CHECKING, overload

import polars as pl
from polars import DataFrame, Series

from geopolars.internals.geodataframe import GeoDataFrame
from geopolars.internals.geoseries import GeoSeries

try:
    import geopandas
except ImportError:
    geopandas = None

if TYPE_CHECKING:  # pragma: no cover
    import pandas
    import pyarrow as pa


def from_arrow(a: pa.Table | pa.Array | pa.ChunkedArray) -> GeoDataFrame | GeoSeries:
    """Convert from Arrow data to GeoSeries or GeoDataFrame"""
    # TODO: this should probably have a check that the data is indeed geographic?
    # And return a bare Series/DataFrame if it isn't?
    output = pl.from_arrow(a)
    if isinstance(output, Series):
        return GeoSeries(output)

    assert isinstance(
        output, DataFrame
    ), "Output of polars.from_arrow expected to be Series or DataFrame"
    return GeoDataFrame(output)


@overload
def from_geopandas(
    gdf: geopandas.GeoDataFrame,
) -> GeoDataFrame:
    ...


@overload
def from_geopandas(
    gdf: geopandas.GeoSeries,
) -> GeoSeries:
    ...


def from_geopandas(
    gdf: geopandas.GeoDataFrame
    | geopandas.GeoSeries
    | pandas.DataFrame
    | pandas.Series,
) -> GeoDataFrame | GeoSeries | pl.DataFrame | pl.Series:
    """
    Construct a GeoPolars GeoDataFrame or GeoSeries from a geopandas
    GeoDataFrame or GeoSeries.

    This operation clones data.

    This requires that geopandas and pyarrow are installed.

    Parameters
    ----------
    gdf : geopandas GeoDataFrame or GeoSeries
        Data represented as a geopandas GeoDataFrame or GeoSeries

    Returns
    -------

    GeoDataFrame or GeoSeries
    """
    if geopandas is None:
        raise ImportError("Geopandas is required when using from_geopandas().")

    import pandas

    if isinstance(gdf, geopandas.GeoSeries):
        # This should probably use a _from_pygeos method?
        raise NotImplementedError("GeoSeries from geopandas is not yet implemented")
        # return GeoSeries._from_geopandas(gdf)
    elif isinstance(gdf, geopandas.GeoDataFrame):
        return GeoDataFrame._from_geopandas(gdf)
    elif isinstance(gdf, (pandas.DataFrame, pandas.Series)):
        return pl.from_pandas(gdf)
    else:
        raise ValueError(
            f"Expected geopandas GeoDataFrame or GeoSeries, got {type(gdf)}."
        )
