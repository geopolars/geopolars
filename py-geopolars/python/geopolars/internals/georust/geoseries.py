from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars
import polars as pl

from geopolars._geopolars import geo as georust
from geopolars.internals.types import AffineTransform, GeodesicMethod, TransformOrigin

if TYPE_CHECKING:
    from geopolars import GeoSeries


@dataclass
class GeoRustSeries:
    """Operations to be done via GeoRust native algorithms"""

    series: pl.Series

    def affine_transform(self, matrix: list[float] | AffineTransform) -> GeoSeries:
        """Returns a `GeoSeries` with translated geometries.

        See Shapely's [`affine_transform`][shapely_docs] or Rust's
        [`AffineOps`][rust_docs] for details.

        [shapely_docs]: https://shapely.readthedocs.io/en/stable/manual.html#shapely.affinity.affine_transform
        [georust_docs]: https://docs.rs/geo/latest/geo/algorithm/affine_ops/trait.AffineOps.html

        Parameters:

            matrix: The 6 parameter matrix is `[a, b, d, e, xoff, yoff]`

        Returns:

            New `GeoSeries` with translated geometries.
        """
        # TODO: check if transform is an instance of Affine? Or add a test?
        # Since Affine is a namedtuple, will it *just work*?
        return georust.affine_transform(self, matrix)

    @property
    def area(self) -> pl.Series:
        """Returns a `Series` containing the area of each geometry in the
        `GeoSeries` expressed in the units of the CRS.

        ## See also

        - [`euclidean_length`][geopolars.GeoRustSeries.euclidean_length]: measure
          euclidean length
        - [`geodesic_length`][geopolars.GeoRustSeries.geodesic_length]: measure geodesic
          length

        ## Notes

        Area may be invalid for a geographic CRS using degrees as units; use
        [`to_crs`][geopolars.GeoSeries.to_crs] to project geometries to a planar CRS
        before using this function.
        """
        return georust.area(self)

    @property
    def centroid(self) -> GeoSeries:
        """Returns a `GeoSeries` of points representing the centroid of each
        geometry.

        Note that centroid does not have to be on or within original geometry.

        Returns:

            New `GeoSeries` with centroids.
        """
        return georust.centroid(self)

    def convex_hull(self) -> GeoSeries:
        """Returns a `GeoSeries` of geometries representing the convex hull
        of each geometry.

        The convex hull of a geometry is the smallest convex `Polygon`
        containing all the points in each geometry, unless the number of points
        in the geometric object is less than three. For two points, the convex
        hull collapses to a `LineString`; for 1, a `Point`.

        ## See also

        - [`envelope`][geopolars.GeoRustSeries.envelope]: bounding rectangle geometry

        """
        return georust.convex_hull(self)

    def envelope(self) -> GeoSeries:
        """Returns a `GeoSeries` of geometries representing the envelope of
        each geometry.

        The envelope of a geometry is the bounding rectangle. That is, the
        point or smallest rectangular polygon (with sides parallel to the
        coordinate axes) that contains the geometry.

        ## See also

        [`convex_hull`][geopolars.GeoRustSeries.convex_hull]: convex hull geometry
        """
        return georust.envelope(self)

    def euclidean_length(self) -> pl.Series:
        """Returns a `Series` containing the euclidean length of each geometry
        expressed in the units of the CRS.

        ## See also

        [`area`][geopolars.GeoRustSeries.area]: measure area of a polygon

        ## Notes

        Length may be invalid for a geographic CRS using degrees as units;
        use [`GeoSeries.to_crs`][geopolars.GeoSeries.to_crs] to project geometries to a
        planar CRS before using this function.
        """
        return georust.euclidean_length(self)

    def exterior(self) -> GeoSeries:
        """Returns a `GeoSeries` of LinearRings representing the outer
        boundary of each polygon in the GeoSeries.
        """
        return georust.exterior(self)

    def geodesic_length(self, method: GeodesicMethod = "geodesic") -> polars.Series:
        """Returns a `Series` containing the geodesic length of each geometry
        expressed in meters.

        Parameters:

            method:
                Method for calculating length: one of `'geodesic'`, `'haversine'`, or
                `'vincenty'`.

                `'geodesic'` uses the geodesic measurement methods given by
                [`Karney (2013)`][Karney]. As opposed to older methods like Vincenty,
                this method is accurate to a few nanometers and always converges.
                `'vincenty'` uses [`Vincenty's formulae`][Vincenty]. `'haversine'` uses
                the [`haversine formula`][Haversine].

                [Karney]: https://arxiv.org/pdf/1109.4448.pdf
                [Vincenty]: https://en.wikipedia.org/wiki/Vincenty%27s_formulae
                [Haversine]: https://en.wikipedia.org/wiki/Haversine_formula

        Returns:

            [`Series`][polars.Series] containing the geodesic length of each geometry
        expressed in meters.

        ## See also

        [`area`][geopolars.GeoRustSeries.area]: measure area of a polygon

        ## Notes

        This method is only meaningful for input data as longitude/latitude coordinates
        on the WGS84 ellipsoid (i.e. EPSG:4326).

        Length may be invalid for a geographic CRS using degrees as units;
        use [`GeoSeries.to_crs`][geopolars.GeoSeries.to_crs] to project geometries to a
        planar CRS before using this function.
        """
        return georust.geodesic_length(self, method)

    @property
    def geom_type(self) -> pl.Series:
        """Returns a `Series` of strings specifying the `Geometry Type` of each
        object.
        """
        return georust.geom_type(self)

    def is_empty(self) -> pl.Series:
        """Returns a `Series` of `dtype('bool')` with value `True` for
        empty geometries.
        """
        return georust.is_empty(self)

    def is_ring(self) -> pl.Series:
        """Returns a `Series` of `dtype('bool')` with value `True` for
        features that are closed.
        """
        return georust.is_ring(self)

    def rotate(self, angle: float, origin: TransformOrigin = "center") -> GeoSeries:
        """Returns a `GeoSeries` with rotated geometries.

        See Shapely's [`rotate`][shapely_docs] or Rust's [`Rotate`][rust_docs] for
        details.

        [shapely_docs]: https://shapely.readthedocs.io/en/latest/manual.html#shapely.affinity.rotate
        [georust_docs]: https://docs.rs/geo/latest/geo/algorithm/rotate/trait.Rotate.html

        Parameters:

            angle: float
                The angle of rotation in degrees. Positive angles are
                counter-clockwise and negative are clockwise rotations.
            origin: string or tuple (x, y)
                The point of origin can be a keyword 'center' for the bounding box
                center (default), 'centroid' for the geometry's centroid, or a
                coordinate tuple (x, y).
        """
        return georust.rotate(self, angle, origin)

    def scale(
        self, xfact: float = 1.0, yfact: float = 1.0, origin: TransformOrigin = "center"
    ) -> GeoSeries:
        """Returns a `GeoSeries` with scaled geometries.

        The geometries can be scaled by different factors along each
        dimension. Negative scale factors will mirror or reflect coordinates.

        See Shapely's [`scale`][shapely_docs] or Rust's [`Scale`][rust_docs] for
        details.

        [shapely_docs]: https://shapely.readthedocs.io/en/latest/manual.html#shapely.affinity.scale
        [georust_docs]: https://docs.rs/geo/latest/geo/algorithm/scale/trait.Scale.html

        Parameters:

        xfact: Scaling factors for the x dimension.
        yfact: Scaling factors for the y dimension.
        origin: The point of origin can be a keyword 'center' for the 2D bounding
            box center (default), 'centroid' for the geometry's 2D centroid
            or a coordinate tuple (x, y).
        """
        return georust.scale(self, xfact, yfact, origin)

    def skew(
        self, xs: float = 0.0, ys: float = 0.0, origin: TransformOrigin = "center"
    ) -> GeoSeries:
        """Returns a `GeoSeries` with skewed geometries.

        The geometries are sheared by angles along the x and y dimensions.

        See Shapely's [`skew`][shapely_docs] or Rust's [`Skew`][rust_docs] for details.

        [shapely_docs]: https://shapely.readthedocs.io/en/latest/manual.html#shapely.affinity.skew
        [georust_docs]: https://docs.rs/geo/latest/geo/algorithm/skew/trait.Skew.html

        Parameters:

        xs: The shear angle for the x axis in degrees.
        ys: The shear angle for the y axis in degrees.
        origin: The point of origin can be a keyword `'center'` for the bounding box
            center (default), `'centroid'` for the geometry's centroid or a
            coordinate tuple `(x, y)`.

        Returns:

            `GeoSeries` with skewed geometries.
        """

        return georust.skew(self, xs, ys, origin)

    def distance(self, other: GeoSeries) -> GeoSeries:
        """Returns a Series containing the distance to aligned other.

        Distance is cartesian distance in 2D space, and the units of the output
        are in terms of the CRS of the two input series. The operation works
        on a 1-to-1 row-wise manner.

        Parameters:

            other: The series to which calculate distance in 1-to-1 row-wise manner.

        Returns:

            GeoSeries containing the distance from each element to the element in
            `other`.
        """

        return georust.distance(self, other)

    def translate(self, xoff: float = 0.0, yoff: float = 0.0) -> GeoSeries:
        """Returns a `GeoSeries` with translated geometries.

        See Shapely's [`translate`][shapely_docs] or Rust's [`Translate`][rust_docs]
        for details.

        [shapely_docs]: https://shapely.readthedocs.io/en/latest/manual.html#shapely.affinity.translate
        [rust_docs]: https://docs.rs/geo/latest/geo/algorithm/translate/trait.Translate.html

        Parameters:

            xoff: Amount of offset along the x dimension.
            yoff: Amount of offset along the y dimension.
        """
        return georust.translate(self, xoff, yoff)

    @property
    def x(self) -> pl.Series:
        """Return the x location of point geometries in a GeoSeries

        ## See Also

        [`y`][geopolars.GeoRustSeries.y]

        Returns:

            Series with x values
        """
        return georust.x(self)

    @property
    def y(self) -> pl.Series:
        """Return the y location of point geometries in a GeoSeries

        ## See Also

        [`x`][geopolars.GeoRustSeries.x]

        Returns:

            Series with y values
        """
        return georust.y(self)
