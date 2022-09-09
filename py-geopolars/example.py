import pyarrow as pa

import geopolars as gpl

reader = pa.ipc.open_file("../data/cities.arrow")
table = reader.read_all()

df = gpl.from_arrow(table)
geom = df.get_column("geometry")
out = geom.centroid()
print(out)
