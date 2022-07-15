import geopolars as gpl
import polars as pl
import pyarrow as pa

reader = pa.ipc.open_file("../geopolars/cities.arrow")
table = reader.read_all()

df = pl.from_arrow(table)
geom = df.get_column("geometry")
s = gpl.GeoSeries(geom, crs="epsg:4326")

s_centroid = s.centroid()
print(s_centroid.crs)
