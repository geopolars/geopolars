from __future__ import annotations

import pytest

import geopolars as gpl

NE_CITIES_GDF = gpl.datasets.read_dataset("naturalearth_cities")


@pytest.fixture
def ne_cities_gdf() -> gpl.GeoDataFrame:
    return NE_CITIES_GDF
