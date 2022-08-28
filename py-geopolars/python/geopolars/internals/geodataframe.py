import polars

from geopolars.internals.geoseries import GeoSeries

DEFAULT_GEO_COLUMN_NAME = "geometry"


class GeoDataFrame(polars.DataFrame):

    _geometry_column_name = DEFAULT_GEO_COLUMN_NAME

    def __init__(self, data=None, columns=None, orient=None, *, geometry=None):

        # Wrap an existing polars DataFrame
        if isinstance(data, polars.DataFrame):
            self._df = data._df
            return

        super().__init__(data, columns, orient)

    @property
    def geometry(self):
        return GeoSeries(self.get_column(self._geometry_column_name))
