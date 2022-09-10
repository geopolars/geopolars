import geopandas
import geopandas.datasets

import geopolars as gpl


class TestFromGeoPandas:
    def test_from_geopandas_ne_cities(self, ne_cities_gdf: gpl.GeoDataFrame):
        geopandas_gdf = geopandas.read_file(
            geopandas.datasets.get_path("naturalearth_cities")
        )
        gdf = gpl.from_geopandas(geopandas_gdf)
        assert gdf.select("geometry") == ne_cities_gdf.select("geometry")
