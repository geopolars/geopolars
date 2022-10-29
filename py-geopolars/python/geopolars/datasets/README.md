These datasets are derived from those included with `geopandas`:

https://github.com/geopandas/geopandas/tree/b7ddddb109832304c066dc3982147b669e8f4868/geopandas/datasets

```py
import geopandas as gpd
import pandas as pd

gdf = gpd.read_file(gpd.datasets.get_path("nybb"))
gdf["BoroCode"] = pd.to_numeric(gdf["BoroCode"], downcast="unsigned")
gdf["Shape_Leng"] = pd.to_numeric(gdf["Shape_Leng"], downcast="float")
gdf["Shape_Area"] = pd.to_numeric(gdf["Shape_Area"], downcast="float")
gdf.to_feather("nybb.arrow")

gdf = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
gdf["pop_est"] = pd.to_numeric(gdf["pop_est"], downcast="unsigned")
gdf["gdp_md_est"] = pd.to_numeric(gdf["gdp_md_est"], downcast="float")
gdf.to_feather("naturalearth_lowres.arrow")

gdf = gpd.read_file(gpd.datasets.get_path("naturalearth_cities"))
gdf.to_feather("naturalearth_cities.arrow")
```
