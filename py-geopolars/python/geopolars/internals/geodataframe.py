from __future__ import annotations

from polars import DataFrame

from geopolars import geopolars as core  # type: ignore
from geopolars.internals.geoseries import GeoSeries

DEFAULT_GEO_COLUMN_NAME = "geometry"


class GeoDataFrame(DataFrame):

    _geometry_column_name = DEFAULT_GEO_COLUMN_NAME

    def __init__(self, data=None, columns=None, orient=None, *, geometry=None):

        # Wrap an existing polars DataFrame
        if isinstance(data, DataFrame):
            self._df = data._df
            return

        super().__init__(data, columns, orient)

    @property
    def geometry(self):
        return GeoSeries(self.get_column(self._geometry_column_name))
