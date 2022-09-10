from __future__ import annotations

import polars as pl

from geopolars.internals.geoseries import GeoSeries

try:
    import geopandas
except ImportError:
    geopandas = None

DEFAULT_GEO_COLUMN_NAME = "geometry"


class GeoDataFrame(pl.DataFrame):

    _geometry_column_name = DEFAULT_GEO_COLUMN_NAME

    def __init__(self, data=None, columns=None, orient=None, *, geometry=None):

        # Wrap an existing polars DataFrame
        if isinstance(data, pl.DataFrame):
            self._df = data._df
            return

        super().__init__(data, columns, orient)

    def get_column(self, name: str) -> pl.Series | GeoSeries:
        """
        Get a single column as Series or GeoSeries by name.

        Return GeoSeries if requested column is geometry column.

        Parameters
        ----------
        name : str
            Name of the column to retrieve.

        Examples
        --------
        >>> df = pl.DataFrame({"foo": [1, 2, 3], "bar": [4, 5, 6]})
        >>> df.get_column("foo")
        shape: (3,)
        Series: 'foo' [i64]
        [
                1
                2
                3
        ]

        """
        series = super().get_column(name)
        if name == self._geometry_column_name:
            series = GeoSeries(series)

        return series

    @property
    def geometry(self):
        return GeoSeries(self.get_column(self._geometry_column_name))

    @classmethod
    def _from_geopandas(cls, geodataframe):
        from geopandas.io.arrow import _geopandas_to_arrow

        arrow_table = _geopandas_to_arrow(geodataframe)
        polars_df = pl.from_arrow(arrow_table)
        return cls(polars_df)

    def to_geopandas(self):
        if geopandas is None:
            raise ImportError("Geopandas is required when using from_geopandas().")

        pandas_df = self.select(
            [col for col in self.columns if col != self._geometry_column_name]
        ).to_pandas()
        return geopandas.GeoDataFrame(pandas_df, geometry=self.geometry.to_geopandas())
