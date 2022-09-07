import pyarrow as pa
import geopolars as gpl

reader = pa.ipc.open_file("../data/cities.arrow")
table = reader.read_all()

df = gpl.from_arrow(table)
out = df.geometry.centroid()
print(out)
