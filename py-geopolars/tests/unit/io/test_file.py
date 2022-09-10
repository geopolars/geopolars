import geopandas
import geopandas.datasets

import geopolars as gpl


class TestReadFile:
    def test_read_file_geopandas_ne_cities(self, ne_cities_gdf: gpl.GeoDataFrame):
        path = geopandas.datasets.get_path("naturalearth_cities")
        gdf = gpl.read_file(path)
        assert gdf.select("geometry") == ne_cities_gdf.select("geometry")
