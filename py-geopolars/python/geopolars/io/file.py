from __future__ import annotations

from typing import TYPE_CHECKING, cast

import polars as pl
from polars import DataFrame
from pyogrio.raw import read_arrow as _read_arrow

from geopolars.internals.geodataframe import GeoDataFrame

if TYPE_CHECKING:
    from pathlib import Path


def read_file(
    path_or_buffer: Path | str | bytes,
    /,
    layer: int | str | None = None,
    encoding: str | None = None,
    columns=None,
    read_geometry: bool = True,
    force_2d: bool = False,
    skip_features: int = 0,
    max_features: int | None = None,
    where: str | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    fids=None,
    sql=None,
    sql_dialect=None,
    return_fids=False,
) -> DataFrame | GeoDataFrame:
    """Read OGR data source into numpy arrays.

    IMPORTANT: non-linear geometry types (e.g., MultiSurface) are converted
    to their linear approximations.

    Parameters:

        path_or_buffer: A dataset path or URI, or raw buffer.
        layer:
            If an integer is provided, it corresponds to the index of the layer
            with the data source.  If a string is provided, it must match the name
            of the layer in the data source.  Defaults to first layer in data source.
        encoding:
            If present, will be used as the encoding for reading string values from
            the data source, unless encoding can be inferred directly from the data
            source.
        columns: list-like, optional (default: all columns)
            List of column names to import from the data source.  Column names must
            exactly match the names in the data source, and will be returned in
            the order they occur in the data source.  To avoid reading any columns,
            pass an empty list-like.
        read_geometry:
            If True, will read geometry into WKB. If False, geometry will be None.
            Defaults to True.
        force_2d:
            If the geometry has Z values, setting this to True will cause those to
            be ignored and 2D geometries to be returned. Defaults to False.
        skip_features:
            Number of features to skip from the beginning of the file before returning
            features.  Must be less than the total number of features in the file.
        max_features : int, optional (default: None)
            Number of features to read from the file.  Must be less than the total
            number of features in the file minus skip_features (if used).
        where:
            Where clause to filter features in layer by attribute values.  Uses a
            restricted form of SQL WHERE clause, defined [here](http://ogdi.sourceforge.net/prop/6.2.CapabilitiesMetadata.html).

            Examples:

            - `"ISO_A3 = 'CAN'"`
            - `"POP_EST > 10000000 AND POP_EST < 100000000"`
        bbox:
            If present, will be used to filter records whose geometry intersects this
            box.  This must be in the same CRS as the dataset.  If GEOS is present
            and used by GDAL, only geometries that intersect this bbox will be
            returned; if GEOS is not available or not used by GDAL, all geometries
            with bounding boxes that intersect this bbox will be returned.
        fids : array-like, optional (default: None)
            Array of integer feature id (FID) values to select. Cannot be combined
            with other keywords to select a subset (`skip_features`, `max_features`,
            `where` or `bbox`). Note that the starting index is driver and file
            specific (e.g. typically 0 for Shapefile and 1 for GeoPackage, but can
            still depend on the specific file). The performance of reading a large
            number of features usings FIDs is also driver specific.
        return_fids : bool, optional (default: False)
            If True, will return the FIDs of the feature that were read.

    Returns:

        A GeoPolars GeoDataFrame or Polars DataFrame
    """
    metadata, table = _read_arrow(
        path_or_buffer,
        layer=layer,
        encoding=encoding,
        columns=columns,
        read_geometry=read_geometry,
        force_2d=force_2d,
        skip_features=skip_features,
        max_features=max_features,
        where=where,
        bbox=bbox,
        fids=fids,
        sql=sql,
        sql_dialect=sql_dialect,
        return_fids=return_fids,
    )
    # TODO: check for metadata['geometry_type'] not Unknown for whether to cast to
    # geoarrow

    geometry_name = metadata["geometry_name"] or "wkb_geometry"
    # Note: we're passing in a pyarrow.Table so the result will always be a
    # DataFrame, not series
    df = cast(DataFrame, pl.from_arrow(table))
    if geometry_name not in table.column_names:
        return df

    return GeoDataFrame(df)
