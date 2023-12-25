use crate::error::Result;
#[cfg(feature = "proj")]
use crate::ops::proj::ProjOptions;
use geo::algorithm::affine_ops::AffineTransform;
use polars::prelude::Series;
use std::convert::Into;

pub struct Placeholder;

pub trait GeoSeries {
    /// Apply an affine transform to the geoseries and return a geoseries of the tranformed geometries;
    fn affine_transform(&self, matrix: impl Into<AffineTransform<f64>>) -> Result<Series>;

    /// Returns a Series containing the area of each geometry in the GeoSeries expressed in the
    /// units of the CRS.
    fn area(&self) -> Result<Series>;

    /// Returns a GeoSeries of points representing the centroid of each geometry.
    ///
    /// Note that centroid does not have to be on or within original geometry.
    fn centroid(&self) -> Result<Series>;

    /// Returns a GeoSeries of geometries representing the convex hull of each geometry.
    ///
    /// The convex hull of a geometry is the smallest convex Polygon containing all the points in each geometry
    fn convex_hull(&self) -> Result<Series>;

    /// Returns a GeoSeries of geometries representing the envelope of each geometry.
    ///
    /// The envelope of a geometry is the bounding rectangle. That is, the point or smallest
    /// rectangular polygon (with sides parallel to the coordinate axes) that contains the
    /// geometry.
    fn envelope(&self) -> Result<Series>;

    /// Returns a Series with the value of the euclidean length of each geometry
    ///
    /// Calculates the euclidean length of each geometry in the series and returns it as a series.
    /// Not valid for Point or MultiPoint geometries. For Polygon it's the
    /// length of the exterior ring of the exterior ring of the Polygon and for MultiPolygon
    /// it returns the
    fn euclidean_length(&self) -> Result<Series>;

    /// Returns a GeoSeries of LinearRings representing the outer boundary of each polygon in the
    /// GeoSeries.
    ///
    /// Applies to GeoSeries containing only Polygons. Returns `None` for other geometry types.
    fn exterior(&self) -> Result<Series>;

    /// Explodes multi-part geometries into multiple single geometries.
    fn explode(&self) -> Result<Series>;

    /// Returns a Series with the value of the geodesic length of each geometry
    ///
    /// Calculates the geodesic length of each geometry in the series and returns it as a series.
    /// Not valid for Point or MultiPoint geometries. For Polygon it's the
    /// length of the exterior ring of the exterior ring of the Polygon and for MultiPolygon
    /// it returns the
    fn geodesic_length(&self, method: Placeholder) -> Result<Series>;

    /// Returns the type ids of each geometry
    /// This mimics the pygeos implementation
    /// <https://pygeos.readthedocs.io/en/latest/geometry.html?highlight=id#pygeos.geometry.get_type_id>
    ///
    /// None (missing) is -1
    /// POINT is 0
    /// LINESTRING is 1
    /// LINEARRING is 2
    /// POLYGON is 3
    /// MULTIPOINT is 4
    /// MULTILINESTRING is 5
    /// MULTIPOLYGON is 6
    /// GEOMETRYCOLLECTION is 7
    fn geom_type(&self) -> Result<Series>;

    /// Returns a boolean Series with value True for empty geometries
    fn is_empty(&self) -> Result<Series>;

    /// Returns a boolean Series with value True for features that are closed.
    ///
    /// When constructing a LinearRing, the sequence of coordinates may be explicitly closed by
    /// passing identical values in the first and last indices. Otherwise, the sequence will be
    /// implicitly closed by copying the first tuple to the last index.
    fn is_ring(&self) -> Result<Series>;

    /// Returns a GeoSeries with each of the geometries rotated by a fixed x and y ammount around
    /// some origin.
    ///
    /// # Arguments
    ///
    /// * `angle` - The angle to rotate specified in degrees
    ///
    /// * `origin` - The origin around which to rotate the geometry
    fn rotate(&self, angle: f64, origin: Placeholder) -> Result<Series>;

    /// Returns a GeoSeries with each of the geometries skewd by a fixed x and y amount around a
    /// given origin
    ///
    /// # Arguments
    ///
    /// * `xfact` The amount to scale the geometry in the x direction. Units are the units of the
    /// geometry crs.
    ///
    /// * `yfact` The amount to scale the geometry in the y direction. Units are the units of the
    /// geometry crs.
    ///
    /// * `origin` - The origin around which to scale the geometry
    fn scale(&self, xfact: f64, yfact: f64, origin: Placeholder) -> Result<Series>;

    /// Returns a GeoSeries containing a simplified representation of each geometry.
    ///
    /// The algorithm (Douglas-Peucker) recursively splits the original line into smaller parts and
    /// connects these parts’ endpoints by a straight line. Then, it removes all points whose
    /// distance to the straight line is smaller than tolerance. It does not move any points and it
    /// always preserves endpoints of the original line or polygon. See
    /// <https://docs.rs/geo/latest/geo/algorithm/simplify/trait.Simplify.html> for details
    fn simplify(&self, tolerance: f64) -> Result<Series>;

    /// Returns a GeoSeries with each of the geometries skewed by a fixed x and y amount around a
    /// given origin
    ///
    /// # Arguments
    ///
    /// * `xs` The angle to skew the geometry in the x direction in units of degrees
    ///
    /// * `ys` The angle to skey the geometry in the y direction in units of degrees
    ///
    /// * `origin` - The origin around which to scale the geometry
    ///
    /// The transform that is applied is
    ///
    /// ```ignore
    /// [[1, tan(x), xoff],
    /// [tan(y), 1, yoff],
    /// [0, 0, 1]]
    ///
    /// xoff = -origin.y * tan(xs)
    /// yoff = -origin.x * tan(ys)
    /// ```
    fn skew(&self, xs: f64, ys: f64, origin: Placeholder) -> Result<Series>;

    /// Returns a Series containing the distance to aligned other. Distance is cartesian distance in 2D space, and the units of the output are in terms of the CRS of the two input series. The operation works on a 1-to-1 row-wise manner.
    ///
    /// # Arguments
    ///
    /// * `other` - The Geoseries (elementwise) to find the distance to.
    fn distance(&self, other: &Series) -> Result<Series>;

    // Note: Ideally we wouldn't have both `from` and `to` here, where the series would include the
    // current CRS, but that would require polars to support extension types.
    #[cfg(feature = "proj")]
    fn to_crs(&self, from: &str, to: &str) -> Result<Series>;

    // Note: Ideally we wouldn't have both `from` and `to` here, where the series would include the
    // current CRS, but that would require polars to support extension types.
    #[cfg(feature = "proj")]
    fn to_crs_with_options(
        &self,
        from: &str,
        to: &str,
        proj_options: ProjOptions,
    ) -> Result<Series>;

    /// Returns a GeoSeries with each of the geometries translated by a fixed x and y amount
    ///
    /// # Arguments
    ///
    /// * `x` The amount to translate the geometry in the x direction. Units are the units of the
    /// geometry crs.
    ///
    /// * `y` The amount to translate the geometry in the y direction. Units are the units of the
    /// geometry crs.
    ///
    /// * `origin` - The origin around which to scale the geometry
    fn translate(&self, x: f64, y: f64) -> Result<Series>;

    /// Return the x location of point geometries in a GeoSeries
    fn x(&self) -> Result<Series>;

    /// Return the y location of point geometries in a GeoSeries
    fn y(&self) -> Result<Series>;
}

impl GeoSeries for Series {
    fn affine_transform(&self, _matrix: impl Into<AffineTransform<f64>>) -> Result<Series> {
        todo!()
    }

    fn area(&self) -> Result<Series> {
        todo!()
    }

    fn centroid(&self) -> Result<Series> {
        todo!()
    }

    fn convex_hull(&self) -> Result<Series> {
        todo!()
    }

    fn envelope(&self) -> Result<Series> {
        todo!()
    }

    fn euclidean_length(&self) -> Result<Series> {
        todo!()
    }

    fn explode(&self) -> Result<Series> {
        todo!()
    }

    fn exterior(&self) -> Result<Series> {
        todo!()
    }

    fn geodesic_length(&self, _method: Placeholder) -> Result<Series> {
        todo!()
    }

    fn geom_type(&self) -> Result<Series> {
        todo!()
    }

    fn is_empty(&self) -> Result<Series> {
        todo!()
    }

    fn is_ring(&self) -> Result<Series> {
        todo!()
    }

    fn rotate(&self, _angle: f64, _origin: Placeholder) -> Result<Series> {
        todo!()
    }

    fn scale(&self, _xfact: f64, _yfact: f64, _origin: Placeholder) -> Result<Series> {
        todo!()
    }

    fn simplify(&self, _tolerance: f64) -> Result<Series> {
        todo!()
    }

    fn skew(&self, _xs: f64, _ys: f64, _origin: Placeholder) -> Result<Series> {
        todo!()
    }

    fn distance(&self, _other: &Series) -> Result<Series> {
        todo!()
        // crate::ops::distance::euclidean_distance(self, other)
    }

    #[cfg(feature = "proj")]
    fn to_crs(&self, from: &str, to: &str) -> Result<Series> {
        crate::ops::proj::to_crs(self, from, to)
    }

    #[cfg(feature = "proj")]
    fn to_crs_with_options(
        &self,
        from: &str,
        to: &str,
        proj_options: ProjOptions,
    ) -> Result<Series> {
        crate::ops::proj::to_crs_with_options(self, from, to, proj_options)
    }

    fn translate(&self, _x: f64, _y: f64) -> Result<Series> {
        todo!()
    }

    fn x(&self) -> Result<Series> {
        todo!()
    }

    fn y(&self) -> Result<Series> {
        todo!()
    }
}
