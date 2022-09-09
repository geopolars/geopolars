from geopolars.internals.geodataframe import GeoDataFrame

try:
    import geopandas
except ImportError:
    geopandas = None


def read_file(*args, **kwargs):
    if geopandas is None:
        raise ImportError(
            "Geopandas is currently required for the read_file method. "
            "Install it with `pip install geopandas`."
        )

    geopandas_gdf = geopandas.read_file(*args, **kwargs)
    return GeoDataFrame.from_geopandas(geopandas_gdf)
