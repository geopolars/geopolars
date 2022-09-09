from __future__ import annotations

import json

import polars as pl

from geopolars import geopolars as core  # type: ignore
from geopolars.internals.geoseries import GeoSeries

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
        Get a single column as Series or GeoSeries by name. Return GeoSeries if requested column is geometry column.

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
    def from_geopandas(cls, geodataframe):
        from geopandas.io.arrow import _geopandas_to_arrow

        arrow_table = _geopandas_to_arrow(geodataframe)
        polars_df = polars.from_arrow(arrow_table)
        return cls(polars_df)

    def to_geopandas(self):
        from geopandas.io.arrow import _arrow_to_geopandas

        geoarrow_metadata = {
            "columns": {
                self._geometry_column_name: {
                    "encoding": "WKB",
                    "geometry_type": "Unknown",
                    "crs": None,
                    "edges": "planar",
                }
            },
            "primary_column": self._geometry_column_name,
            "version": "0.4.0",
        }
        arrow_table = self.to_arrow()
        return _arrow_to_geopandas(
            arrow_table, metadata={b"geo": json.dumps(geoarrow_metadata).encode()}
        )
