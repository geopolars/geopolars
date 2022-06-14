from __future__ import annotations

import polars
from geopolars import geopolars


class GeoSeries(polars.Series):
    """Extension of polars Series to interpret geometric data"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def centroid(self) -> GeoSeries:
        return GeoSeries(geopolars.centroid(self))

    def convex_hull(self) -> GeoSeries:
        return GeoSeries(geopolars.convex_hull(self))
