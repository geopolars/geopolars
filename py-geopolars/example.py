import polars as pl
from geopolars import centroid
import pyarrow as pa


reader = pa.ipc.open_file("../cities.arrow")
table = reader.read_all()

df = pl.from_arrow(table)
geom = df.get_column("geometry")
out = centroid(geom)
print(out)
