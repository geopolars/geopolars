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
        assert gdf.frame_equal(ne_cities_gdf)

    def test_geoseries_from_geopandas(self, ne_cities_gdf: gpl.GeoDataFrame):
        geopandas_gdf = geopandas.read_file(
            geopandas.datasets.get_path("naturalearth_cities")
        )
        geoseries = gpl.from_geopandas(geopandas_gdf.geometry)
        assert isinstance(geoseries, gpl.GeoSeries)
        assert geoseries.series_equal(ne_cities_gdf.get_column("geometry"))


class TestToGeoPandas:
    def test_gdf_to_geopandas(self, ne_cities_gdf: gpl.GeoDataFrame):
        geopandas_gdf = ne_cities_gdf.to_geopandas()
        assert isinstance(geopandas_gdf, geopandas.GeoDataFrame)

    def test_geoseries_to_geopandas(self, ne_cities_gdf: gpl.GeoDataFrame):
        geopandas_geoseries = ne_cities_gdf.geometry.to_geopandas()
        assert isinstance(geopandas_geoseries, geopandas.GeoSeries)


class TestRoundTripGeoPandas:
    def test_gdf_round_trip(self, ne_cities_gdf: gpl.GeoDataFrame):
        geopandas_gdf = ne_cities_gdf.to_geopandas()
        new_gdf = gpl.GeoDataFrame._from_geopandas(geopandas_gdf)
        assert new_gdf.frame_equal(ne_cities_gdf)

    def test_geoseries_round_trip(self, ne_cities_gdf: gpl.GeoDataFrame):
        geoseries = ne_cities_gdf.geometry
        geopandas_geoseries = geoseries.to_geopandas()
        new_geoseries = gpl.GeoSeries._from_geopandas(geopandas_geoseries)
        assert new_geoseries.series_equal(geoseries)
