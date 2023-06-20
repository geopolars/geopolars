from geopolars._geopolars import version
from geopolars.convert import from_arrow, from_geopandas
from geopolars.internals.geodataframe import GeoDataFrame
from geopolars.internals.georust import GeoRustSeries
from geopolars.internals.geoseries import GeoSeries
from geopolars.io.file import read_file

from . import datasets

__all__ = [
    # geopolars.io.file
    "read_file",
    # geopolars.convert
    "from_arrow",
    "from_geopandas",
    # geopolars.internals
    "GeoDataFrame",
    "GeoSeries",
    "GeoRustSeries",
]

__version__: str = version()
