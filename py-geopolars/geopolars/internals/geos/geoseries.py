from dataclasses import dataclass

import polars as pl


@dataclass
class GEOSSeriesOperations:
    """Operations to be done via GEOS

    Note that geometries are always stored in GeoArrow format, and to use GEOS
    operations, serialization to/from GEOS happens on the fly.
    """

    series: pl.Series

    pass
