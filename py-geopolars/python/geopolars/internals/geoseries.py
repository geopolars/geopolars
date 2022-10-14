from __future__ import annotations

import polars as pl

from geopolars import geopolars as core  # type: ignore
from geopolars.internals.types import GeodesicMethod, TransformOrigin

try:
    import geopandas
except ImportError:
    geopandas = None

try:
    import pyarrow
except ImportError:
    pyarrow = None


class GeoSeries(pl.Series):
    """Extension of polars Series to handle geospatial vector data"""

    def __init__(self, *args, **kwargs):
        if isinstance(args[0], pl.Series):
            self._s = args[0]._s
            return

        super().__init__(*args, **kwargs)

    @classmethod
    def _from_geopandas(cls, geoseries: geopandas.GeoSeries):
        if geopandas is None:
            raise ImportError("Geopandas is required when using from_geopandas().")

        if pyarrow is None:
            raise ImportError("Pyarrow is required when using from_geopandas().")

        wkb_arrow_array = pyarrow.Array.from_pandas(geoseries.to_wkb())
        polars_series = pl.Series._from_arrow(
            geoseries.name or "geometry", wkb_arrow_array
        )
        return cls(polars_series)

    def to_geopandas(self) -> geopandas.GeoSeries:
        if geopandas is None:
            raise ImportError("Geopandas is required when using to_geopandas().")

        pyarrow_array = self.to_arrow()

        # This is kinda ugly, but necessary because polars stores binary data as
        # List<u8> which geopandas doesn't know how to accept, and pyarrow hasn't
        # implemented a cast for List<u8> to the binary type
        return geopandas.GeoSeries(
            geopandas.array.from_wkb(
                [row.values.to_numpy().tobytes() for row in pyarrow_array]
            )
        )

    def affine_transform(self, matrix) -> GeoSeries:
        """Return a ``GeoSeries`` with translated geometries

        Parameters
        ----------
        matrix: List or tuple
            The 6 parameter matrix is ``[a, b, d, e, xoff, yoff]``
        """
        # TODO: check if transform is an instance of Affine? Or add a test?
        # Since Affine is a namedtuple, will it *just work*?
        return core.affine_transform(self, matrix)

    @property
    def area(self) -> pl.Series:
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

    def euclidean_length(self) -> pl.Series:
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

    def geodesic_length(self, method: GeodesicMethod = "geodesic") -> pl.Series:
        return core.geodesic_length(self, method)

    @property
    def geom_type(self) -> pl.Series:
        """Returns a ``Series`` of strings specifying the `Geometry Type` of each
        object.
        """
        return core.geom_type(self)

    # Note: Polars defines an is_empty method
    def is_geom_empty(self) -> pl.Series:
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        empty geometries.
        """
        return core.is_empty(self)

    def is_ring(self) -> pl.Series:
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

    # Note: polars defines a `skew` method
    def geom_skew(
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

    def distance(self, other: GeoSeries) -> GeoSeries:
        """Returns a Series containing the distance to aligned other.

        Distance is cartesian distance in 2D space, and the units of the output
        are in terms of the CRS of the two input series. The operation works
        on a 1-to-1 row-wise manner.

        Parameters
        ----------
        other : Geoseries
            The series to which calculate distance in 1-to-1 row-wise manner.
        """

        return core.distance(self, other)

    # def to_crs(self, from_crs: str, to_crs: str) -> GeoSeries:
    #     return core.to_crs(self, from_crs, to_crs)

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
    def x(self) -> pl.Series:
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
    def y(self) -> pl.Series:
        """Return the y location of point geometries in a GeoSeries

        Returns
        -------
        polars.Series

        See Also
        --------
        GeoSeries.x
        """
        return core.y(self)
