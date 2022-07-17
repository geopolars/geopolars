import sys
from typing import Tuple, Union

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

GeodesicMethod = Literal["geodesic", "haversine", "vincenty"]
TransformOrigin = Union[Literal["centroid"], Literal["center"], Tuple[float, float]]
