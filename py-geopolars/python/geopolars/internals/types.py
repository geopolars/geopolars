import sys
from typing import Literal, Tuple, Union

GeodesicMethod = Literal["geodesic", "haversine", "vincenty"]
TransformOrigin = Union[Literal["centroid", "center"], Tuple[float, float]]
