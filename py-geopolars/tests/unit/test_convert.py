import geopandas
import geopandas.datasets

import geopolars as gpl


class TestFromGeoPandas:
    def test_gdf_from_geopandas(self, ne_cities_gdf: gpl.GeoDataFrame):
        geopandas_gdf = geopandas.read_file(
            geopandas.datasets.get_path("naturalearth_cities")
        )
        gdf = gpl.from_geopandas(geopandas_gdf)
        assert isinstance(gdf, gpl.GeoDataFrame)
        assert gdf == ne_cities_gdf

    def test_geoseries_from_geopandas(self, ne_cities_gdf: gpl.GeoDataFrame):
        geopandas_gdf = geopandas.read_file(
            geopandas.datasets.get_path("naturalearth_cities")
        )
        geoseries = gpl.from_geopandas(geopandas_gdf.geometry)
        assert geoseries == ne_cities_gdf.get_column("geometry")
