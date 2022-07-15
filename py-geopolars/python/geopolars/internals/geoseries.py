from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import polars
from geopolars import geopolars as _geopolars

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa


class GeoSeries(polars.Series):
    """Extension of polars Series for geospatial data"""

    crs: Optional[str]

    def __init__(self, *args, crs: Optional[str] = None, **kwargs):
        self.crs = crs
        super().__init__(*args, **kwargs)

    @classmethod
    def _from_arrow(
        cls, name: str, values: pa.Array, rechunk: bool = True, *, crs: Optional[str]
    ) -> GeoSeries:
        series = super()._from_arrow(name=name, values=values, rechunk=rechunk)
        return cls(series, crs=crs)

    def centroid(self) -> GeoSeries:
        return _geopolars.centroid(self)

    def convex_hull(self) -> GeoSeries:
        return _geopolars.convex_hull(self)
