from __future__ import annotations

from geopolars import geopolars as core
from geopolars.internals.types import TransformOrigin
from polars import Series


class GeoSeries(Series):
    """Extension of polars Series to interpret geometric data"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def affine_transform(self, matrix) -> GeoSeries:
        """Transform the geometries of the GeoSeries using an affine transformation matrix

        Args:
            matrix (_type_): _description_

        Returns:
            GeoSeries: transformed geometries
        """
        # TODO: check if transform is an instance of Affine? Or add a test? Since Affine is a
        # namedtuple, will it *just work*?
        return core.affine_transform(self, matrix)

    def area(self) -> Series:
        return core.area(self)

    def centroid(self) -> GeoSeries:
        return core.centroid(self)

    def convex_hull(self) -> GeoSeries:
        return core.convex_hull(self)

    def envelope(self) -> GeoSeries:
        return core.envelope(self)

    def euclidean_length(self) -> Series:
        return core.euclidean_length(self)

    def exterior(self) -> GeoSeries:
        return core.exterior(self)

    def geodesic_length(self, method: str) -> Series:
        return core.geodesic_length(self, method)

    def geom_type(self) -> Series:
        return core.geom_type(self)

    def is_empty(self) -> Series:
        return core.is_empty(self)

    def is_ring(self) -> Series:
        return core.is_ring(self)

    def rotate(self, angle: float, origin: TransformOrigin) -> GeoSeries:
        return core.rotate(self, angle, origin)

    def scale(self, xfact: float, yfact: float, origin: TransformOrigin) -> GeoSeries:
        return core.scale(self, xfact, yfact, origin)

    def skew(self, xs: float, ys: float, origin: TransformOrigin) -> GeoSeries:
        return core.skew(self, xs, ys, origin)

    def to_crs(self, from_crs: str, to_crs: str) -> GeoSeries:
        return core.to_crs(self, from_crs, to_crs)

    def translate(self, x: float, y: float) -> GeoSeries:
        return core.translate(self, x, y)

    def x(self) -> Series:
        return core.x(self)

    def y(self) -> Series:
        return core.y(self)
