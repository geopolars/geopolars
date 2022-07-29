import polars as pl
import pyarrow as pa

from geopolars import centroid

reader = pa.ipc.open_file("../cities.arrow")
table = reader.read_all()

df = pl.from_arrow(table)
geom = df.get_column("geometry")
out = centroid(geom)
print(out)
