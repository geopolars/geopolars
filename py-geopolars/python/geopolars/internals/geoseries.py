from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from geopolars import geopolars as core
from geopolars.internals.types import GeodesicMethod, TransformOrigin
from geopolars.proj import PROJ_DATA_PATH

try:
    import geopandas
except ImportError:
    geopandas = None

try:
    import pyarrow
except ImportError:
    pyarrow = None

if TYPE_CHECKING:
    import pyproj


class GeoSeries(pl.Series):
    """Extension of :class:`polars.Series` to handle geospatial vector data."""

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
        """Converts this ``GeoSeries`` to a :class:`geopandas.GeoSeries`.

        This operation clones data. This requires that :mod:`geopandas` and
        :mod:`pyarrow` are installed.

        Returns
        -------

        :class:`geopandas.GeoSeries`
        """
        if geopandas is None:
            raise ImportError("Geopandas is required when using to_geopandas().")

        pyarrow_array = self.to_arrow()
        numpy_array = pyarrow_array.to_numpy(zero_copy_only=False)
        # Ideally we shouldn't need the cast to numpy, but the pyarrow BinaryScalar
        # hasn't implemented len()
        return geopandas.GeoSeries(geopandas.array.from_wkb(numpy_array))

    def affine_transform(self, matrix) -> GeoSeries:
        """Returns a ``GeoSeries`` with translated geometries.

        See http://shapely.readthedocs.io/en/stable/manual.html#shapely.affinity.affine_transform
        or https://docs.rs/geo/latest/geo/algorithm/affine_ops/trait.AffineOps.html for details.

        Parameters
        ----------
        matrix: List or tuple
            The 6 parameter matrix is ``[a, b, d, e, xoff, yoff]``
        """  # noqa (E501 link is longer than max line length)
        # TODO: check if transform is an instance of Affine? Or add a test?
        # Since Affine is a namedtuple, will it *just work*?
        return core.affine_transform(self, matrix)

    @property
    def area(self) -> pl.Series:
        """Returns a ``Series`` containing the area of each geometry in the
        ``GeoSeries`` expressed in the units of the CRS.

        See also
        --------
        GeoSeries.euclidean_length : measure euclidean length
        GeoSeries.geodesic_length : measure geodesic length

        Notes
        -----
        Area may be invalid for a geographic CRS using degrees as units;
        use :meth:`GeoSeries.to_crs` to project geometries to a planar
        CRS before using this function.
        """
        return core.area(self)

    @property
    def centroid(self) -> GeoSeries:
        """Returns a ``GeoSeries`` of points representing the centroid of each
        geometry.

        Note that centroid does not have to be on or within original geometry.
        """
        return core.centroid(self)

    def convex_hull(self) -> GeoSeries:
        """Returns a ``GeoSeries`` of geometries representing the convex hull
        of each geometry.

        The convex hull of a geometry is the smallest convex `Polygon`
        containing all the points in each geometry, unless the number of points
        in the geometric object is less than three. For two points, the convex
        hull collapses to a `LineString`; for 1, a `Point`.

        See also
        --------
        GeoSeries.envelope : bounding rectangle geometry

        """
        return core.convex_hull(self)

    def envelope(self) -> GeoSeries:
        """Returns a ``GeoSeries`` of geometries representing the envelope of
        each geometry.

        The envelope of a geometry is the bounding rectangle. That is, the
        point or smallest rectangular polygon (with sides parallel to the
        coordinate axes) that contains the geometry.

        See also
        --------
        GeoSeries.convex_hull : convex hull geometry
        """
        return core.envelope(self)

    def euclidean_length(self) -> pl.Series:
        """Returns a ``Series`` containing the euclidean length of each geometry
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
        """Returns a ``GeoSeries`` of LinearRings representing the outer
        boundary of each polygon in the GeoSeries.
        """
        return core.exterior(self)

    def geodesic_length(self, method: GeodesicMethod = "geodesic") -> pl.Series:
        """Returns a ``Series`` containing the geodesic length of each geometry
        expressed in meters.

        Parameters
        ----------
        method : str
            Method for calculating length: one of ``'geodesic'``, ``'haversine'``, or
            ``'vincenty'``.

            ``'geodesic'`` uses the geodesic measurement methods given by
            `Karney (2013)`_. As opposed to older methods like Vincenty, this method is
            accurate to a few nanometers and always converges. ``'vincenty'`` uses
            `Vincenty's formulae`_. ``'haversine'`` uses the `haversine formula`_.

            .. _Karney (2013): https://arxiv.org/pdf/1109.4448.pdf
            .. _Vincenty's formulae: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
            .. _haversine formula: https://en.wikipedia.org/wiki/Haversine_formula

        See also
        --------
        GeoSeries.area : measure area of a polygon

        Notes
        -----
        This method is only meaningful for input data as longitude/latitude coordinates
        on the WGS84 ellipsoid (i.e. EPSG:4326).

        Length may be invalid for a geographic CRS using degrees as units;
        use :meth:`GeoSeries.to_crs` to project geometries to a planar
        CRS before using this function.
        """
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

        See http://shapely.readthedocs.io/en/latest/manual.html#shapely.affinity.rotate
        or https://docs.rs/geo/latest/geo/algorithm/rotate/trait.Rotate.html
        for details.

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

        See http://shapely.readthedocs.io/en/latest/manual.html#shapely.affinity.scale
        or https://docs.rs/geo/latest/geo/algorithm/scale/trait.Scale.html
        for details.

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

        See http://shapely.readthedocs.io/en/latest/manual.html#shapely.affinity.skew
        or https://docs.rs/geo/latest/geo/algorithm/skew/trait.Skew.html
        for details.

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

    def to_crs(self, from_crs: str | pyproj.CRS, to_crs: str | pyproj.CRS) -> GeoSeries:
        """Returns a ``GeoSeries`` with all geometries transformed to a new
        coordinate reference system.

        Transform all geometries in a GeoSeries to a different coordinate
        reference system.

        For now, you must pass in both ``from_crs`` and ``to_crs``. In the future, we'll
        handle the current CRS automatically.

        This method will transform all points in all objects.  It has no notion
        of projecting entire geometries.  All segments joining points are
        assumed to be lines in the current projection, not geodesics.  Objects
        crossing the dateline (or other projection boundary) will have
        undesirable behavior.

        Parameters
        ----------
        from_crs : :class:`pyproj.CRS <pyproj.crs.CRS>` or str
            Origin coordinate system. The value can be anything accepted
            by :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.
        to_crs : :class:`pyproj.CRS <pyproj.crs.CRS>` or str
            Destination coordinate system. The value can be anything accepted
            by :meth:`pyproj.CRS.from_user_input() <pyproj.crs.CRS.from_user_input>`,
            such as an authority string (eg "EPSG:4326") or a WKT string.

        Returns
        -------
        GeoSeries
        """

        if not hasattr(core, "to_crs"):
            # TODO: use a custom geopolars exception class here
            raise ValueError("Geopolars not built with proj support")

        if not PROJ_DATA_PATH:
            raise ValueError("PROJ_DATA could not be found.")

        # If pyproj.CRS objects are passed in, serialize them to PROJJSON
        if not isinstance(from_crs, str) and hasattr(from_crs, "to_json"):
            from_crs = from_crs.to_json()

        if not isinstance(to_crs, str) and hasattr(to_crs, "to_json"):
            to_crs = to_crs.to_json()

        return core.to_crs(self, from_crs, to_crs, PROJ_DATA_PATH)

    def translate(self, xoff: float = 0.0, yoff: float = 0.0) -> GeoSeries:
        """Returns a ``GeoSeries`` with translated geometries.

        See http://shapely.readthedocs.io/en/latest/manual.html#shapely.affinity.translate
        or https://docs.rs/geo/latest/geo/algorithm/translate/trait.Translate.html
        for details.

        Parameters
        ----------
        xoff, yoff : float, float
            Amount of offset along each dimension.
            xoff and yoff for translation along the x and y
            dimensions respectively.
        """  # noqa (E501 link is longer than max line length)
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
