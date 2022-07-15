These datasets are derived from those included with `geopandas`:

https://github.com/geopandas/geopandas/blob/f01bcedf72d6a89a5c2f71b782db660c4ac5a414/geopandas/datasets/README.md

Run with `osgeo/gdal:latest` on 2022-07-15
```
> docker run --rm -it -v $(pwd):/data osgeo/gdal:latest ogrinfo --version
GDAL 3.5.0dev-4b77cd8111ce6c6b79cb0c75101394367b9e135e, released 2022/05/03
```

```
docker run --rm -it -v $(pwd):/data osgeo/gdal:latest ogr2ogr -lco GEOMETRY_ENCODING=WKB /data/naturalearth_cities.arrow /data/naturalearth_cities
docker run --rm -it -v $(pwd):/data osgeo/gdal:latest ogr2ogr -lco GEOMETRY_ENCODING=WKB /data/naturalearth_lowres.arrow /data/naturalearth_lowres
docker run --rm -it -v $(pwd):/data osgeo/gdal:latest ogr2ogr -lco GEOMETRY_ENCODING=WKB /data/nybb.arrow /vsizip/data/nybb_16a.zip
```
