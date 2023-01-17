import geopandas
import geopandas.datasets

import geopolars as gpl


class TestReadFile:
    def test_read_file_geopandas_ne_cities(self):
        path = geopandas.datasets.get_path("naturalearth_cities")
        gdf = gpl.read_file(path)
        # dataset size depends on version of geopandas
        assert len(gdf) > 200
        assert isinstance(gdf, gpl.GeoDataFrame)
