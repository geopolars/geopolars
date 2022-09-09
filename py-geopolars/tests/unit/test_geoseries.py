import geopolars as gpl


def test_hello_world(ne_cities_gdf: gpl.GeoDataFrame):
    assert len(ne_cities_gdf) == 202
