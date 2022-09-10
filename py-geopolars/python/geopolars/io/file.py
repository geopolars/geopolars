from __future__ import annotations

import polars as pl

from geopolars.internals.geodataframe import GeoDataFrame

try:
    import geopandas
except ImportError:
    geopandas = None


def read_file(filename, *args, **kwargs) -> pl.DataFrame | GeoDataFrame:
    """Returns a GeoDataFrame from a file or URL.

    .. versionadded:: 0.7.0 mask, rows

    Parameters
    ----------
    filename : str, path object or file-like object
        Either the absolute or relative path to the file or URL to
        be opened, or any object with a read() method (such as an open file
        or StringIO)
    bbox : tuple | GeoDataFrame or GeoSeries | shapely Geometry, default None
        Filter features by given bounding box, GeoSeries, GeoDataFrame or a shapely
        geometry. With engine="fiona", CRS mis-matches are resolved if given a GeoSeries
        or GeoDataFrame. With engine="pyogrio", bbox must be in the same CRS as the
        dataset. Tuple is (minx, miny, maxx, maxy) to match the bounds property of
        shapely geometry objects. Cannot be used with mask.
    mask : dict | GeoDataFrame or GeoSeries | shapely Geometry, default None
        Filter for features that intersect with the given dict-like geojson
        geometry, GeoSeries, GeoDataFrame or shapely geometry.
        CRS mis-matches are resolved if given a GeoSeries or GeoDataFrame.
        Cannot be used with bbox.
    rows : int or slice, default None
        Load in specific rows by passing an integer (first `n` rows) or a
        slice() object.
    engine : str, "fiona" or "pyogrio"
        The underlying library that is used to read the file. Currently, the
        supported options are "fiona" and "pyogrio". Defaults to "fiona" if
        installed, otherwise tries "pyogrio".
    **kwargs :
        Keyword args to be passed to the engine. In case of the "fiona" engine,
        the keyword arguments are passed to :func:`fiona.open` or
        :class:`fiona.collection.BytesCollection` when opening the file.
        For more information on possible keywords, type:
        ``import fiona; help(fiona.open)``. In case of the "pyogrio" engine,
        the keyword arguments are passed to :func:`pyogrio.read_dataframe`.

    Examples
    --------
    >>> df = geopandas.read_file("nybb.shp")  # doctest: +SKIP

    Specifying layer of GPKG:

    >>> df = geopandas.read_file("file.gpkg", layer='cities')  # doctest: +SKIP

    Reading only first 10 rows:

    >>> df = geopandas.read_file("nybb.shp", rows=10)  # doctest: +SKIP

    Reading only geometries intersecting ``mask``:

    >>> df = geopandas.read_file("nybb.shp", mask=polygon)  # doctest: +SKIP

    Reading only geometries intersecting ``bbox``:

    >>> df = geopandas.read_file("nybb.shp", bbox=(0, 0, 10, 20))  # doctest: +SKIP

    Returns
    -------
    :obj:`geopandas.GeoDataFrame` or :obj:`pandas.DataFrame` :

        If `ignore_geometry=True` a :obj:`pandas.DataFrame` will be returned.

    Notes
    -----
    The format drivers will attempt to detect the encoding of your data, but
    may fail. In this case, the proper encoding can be specified explicitly
    by using the encoding keyword parameter, e.g. ``encoding='utf-8'``.
    """
    if geopandas is None:
        raise ImportError(
            "Geopandas is currently required for the read_file method. "
            "Install it with `pip install geopandas`."
        )

    import pandas

    geopandas_gdf = geopandas.read_file(filename=filename, *args, **kwargs)

    if isinstance(geopandas_gdf, geopandas.GeoDataFrame):
        return GeoDataFrame._from_geopandas(geopandas_gdf)

    if isinstance(geopandas_gdf, pandas.DataFrame):
        return pl.from_pandas(geopandas_gdf)

    raise ValueError(
        "Expected geopandas.read_file to return a GeoDataFrame, "
        f"got {type(geopandas_gdf)}"
    )
