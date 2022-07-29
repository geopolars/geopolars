from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from geopolars import geopolars as core
from geopolars.internals.types import GeodesicMethod, TransformOrigin
from polars import Series

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow as pa


class GeoSeries(Series):
    """Extension of polars Series to handle geospatial vector data"""

    crs: Optional[str]

    def __init__(self, *args, crs: Optional[str] = None, **kwargs):
        self.crs = crs
        super().__init__(*args, **kwargs)

    @classmethod
    def _from_arrow(
        cls, name: str, values: pa.Array, rechunk: bool = True, *, crs: Optional[str]
    ) -> GeoSeries:
        series = super()._from_arrow(name=name, values=values, rechunk=rechunk)
        return cls(series, crs=crs)

    def affine_transform(self, matrix) -> GeoSeries:
        """Return a ``GeoSeries`` with translated geometries

        Parameters
        ----------
        matrix: List or tuple
            The 6 parameter matrix is ``[a, b, d, e, xoff, yoff]``
        """
        # TODO: check if transform is an instance of Affine? Or add a test? Since Affine is a
        # namedtuple, will it *just work*?
        return core.affine_transform(self, matrix)

    @property
    def area(self) -> Series:
        """Returns a ``Series`` containing the area of each geometry in the
        ``GeoSeries`` expressed in the units of the CRS.

        Notes
        -----
        Area may be invalid for a geographic CRS using degrees as units;
        use :meth:`GeoSeries.to_crs` to project geometries to a planar
        CRS before using this function.
        """
        return core.area(self)

    def centroid(self) -> GeoSeries:
        return core.centroid(self)

    def convex_hull(self) -> GeoSeries:
        return core.convex_hull(self)

    def envelope(self) -> GeoSeries:
        return core.envelope(self)

    def euclidean_length(self) -> Series:
        """Returns a ``Series`` containing the length of each geometry
        expressed in the units of the CRS.

        See also
        --------
        GeoSeries.area : measure area of a polygon

        Notes
        -----
        Length may be invalid for a geographic CRS using degrees as units;
        use :meth:`GeoSeries.to_crs` to project geometries to a planar
        CRS before using this function.
        """
        return core.euclidean_length(self)

    def exterior(self) -> GeoSeries:
        return core.exterior(self)

    def geodesic_length(self, method: GeodesicMethod = "geodesic") -> Series:
        return core.geodesic_length(self, method)

    @property
    def geom_type(self) -> Series:
        """Returns a ``Series`` of strings specifying the `Geometry Type` of each
        object.
        """
        return core.geom_type(self)

    def is_empty(self) -> Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        empty geometries.
        """
        return core.is_empty(self)

    def is_ring(self) -> Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        features that are closed.
        """
        return core.is_ring(self)

    def rotate(self, angle: float, origin: TransformOrigin = "center") -> GeoSeries:
        """Returns a ``GeoSeries`` with rotated geometries.

        Parameters
        ----------
        angle : float
            The angle of rotation in degrees. Positive angles are
            counter-clockwise and negative are clockwise rotations.
        origin : string or tuple (x, y)
            The point of origin can be a keyword 'center' for the bounding box
            center (default), 'centroid' for the geometry's centroid, or a
            coordinate tuple (x, y).
        """
        return core.rotate(self, angle, origin)

    def scale(
        self, xfact: float = 1.0, yfact: float = 1.0, origin: TransformOrigin = "center"
    ) -> GeoSeries:
        """Returns a ``GeoSeries`` with scaled geometries.

        The geometries can be scaled by different factors along each
        dimension. Negative scale factors will mirror or reflect coordinates.

        Parameters
        ----------
        xfact, yfact : float, float
            Scaling factors for the x and y dimensions respectively.
        origin : string or tuple (x, y)
            The point of origin can be a keyword 'center' for the 2D bounding
            box center (default), 'centroid' for the geometry's 2D centroid
            or a coordinate tuple (x, y).
        """
        return core.scale(self, xfact, yfact, origin)

    def skew(
        self, xs: float = 0.0, ys: float = 0.0, origin: TransformOrigin = "center"
    ) -> GeoSeries:
        """Returns a ``GeoSeries`` with skewed geometries.

        The geometries are sheared by angles along the x and y dimensions.

        Parameters
        ----------
        xs, ys : float, float
            The shear angle(s) for the x and y axes respectively in degrees.
        origin : string or tuple (x, y)
            The point of origin can be a keyword 'center' for the bounding box
            center (default), 'centroid' for the geometry's centroid or a
            coordinate tuple (x, y).
        """

        return core.skew(self, xs, ys, origin)

    def to_crs(self, from_crs: str, to_crs: str) -> GeoSeries:
        return core.to_crs(self, from_crs, to_crs)

    def translate(self, xoff: float = 0.0, yoff: float = 0.0) -> GeoSeries:
        """Returns a ``GeoSeries`` with translated geometries.

        Parameters
        ----------
        xoff, yoff : float, float
            Amount of offset along each dimension.
            xoff and yoff for translation along the x and y
            dimensions respectively.
        """
        return core.translate(self, xoff, yoff)

    @property
    def x(self) -> Series:
        """Return the x location of point geometries in a GeoSeries

        Returns
        -------
        polars.Series

        See Also
        --------
        GeoSeries.y
        """
        return core.x(self)

    @property
    def y(self) -> Series:
        """Return the y location of point geometries in a GeoSeries

        Returns
        -------
        polars.Series

        See Also
        --------
        GeoSeries.x
        """
        return core.y(self)
