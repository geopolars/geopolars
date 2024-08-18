import geodatasets

import geopolars as gpl


class TestReadFile:
    def test_read_file_geopandas_ne_cities(self):
        path = geodatasets.get_path("nybb")
        gdf = gpl.read_file(path)
        assert len(gdf) == 5
        assert isinstance(gdf, gpl.GeoDataFrame)
