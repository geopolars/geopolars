use crate::error::Result;
use crate::ops::affine::TransformOrigin;
use crate::ops::length::GeodesicLengthMethod;
#[cfg(feature = "proj")]
use crate::ops::proj::ProjOptions;
use geo::algorithm::affine_ops::AffineTransform;
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use polars::error::ErrString;
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::{PolarsError, Series};
use std::convert::Into;

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

    /// Create a Series from a vector of geometries
    fn from_geom_vec(geoms: &[Geometry<f64>]) -> Result<Series>;

    /// Returns a Series with the value of the geodesic length of each geometry
    ///
    /// Calculates the geodesic length of each geometry in the series and returns it as a series.
    /// Not valid for Point or MultiPoint geometries. For Polygon it's the
    /// length of the exterior ring of the exterior ring of the Polygon and for MultiPolygon
    /// it returns the
    fn geodesic_length(&self, method: GeodesicLengthMethod) -> Result<Series>;

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
    fn rotate(&self, angle: f64, origin: TransformOrigin) -> Result<Series>;

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
    fn scale(&self, xfact: f64, yfact: f64, origin: TransformOrigin) -> Result<Series>;

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
    fn skew(&self, xs: f64, ys: f64, origin: TransformOrigin) -> Result<Series>;

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
    fn affine_transform(&self, matrix: impl Into<AffineTransform<f64>>) -> Result<Series> {
        crate::ops::affine::affine_transform(self, matrix)
    }

    fn area(&self) -> Result<Series> {
        crate::ops::area::area(self)
    }

    fn centroid(&self) -> Result<Series> {
        crate::ops::centroid::centroid(self)
    }

    fn convex_hull(&self) -> Result<Series> {
        crate::ops::convex_hull::convex_hull(self)
    }

    fn envelope(&self) -> Result<Series> {
        crate::ops::envelope::envelope(self)
    }

    fn euclidean_length(&self) -> Result<Series> {
        crate::ops::length::euclidean_length(self)
    }

    fn explode(&self) -> Result<Series> {
        crate::ops::explode::explode(self)
    }

    fn exterior(&self) -> Result<Series> {
        crate::ops::exterior::exterior(self)
    }

    fn from_geom_vec(geoms: &[Geometry<f64>]) -> Result<Self> {
        let mut wkb_array = MutableBinaryArray::<i32>::with_capacity(geoms.len());

        for geom in geoms {
            let wkb = geom.to_wkb(CoordDimensions::xy()).map_err(|_| {
                PolarsError::ComputeError(ErrString::from(
                    "Failed to convert geom vec to GeoSeries",
                ))
            })?;
            wkb_array.push(Some(wkb));
        }
        let array: BinaryArray<i32> = wkb_array.into();

        let series = Series::try_from(("geometry", Box::new(array) as Box<dyn Array>))?;
        Ok(series)
    }

    fn geodesic_length(&self, method: GeodesicLengthMethod) -> Result<Series> {
        crate::ops::length::geodesic_length(self, method)
    }

    fn geom_type(&self) -> Result<Series> {
        crate::ops::geom_type::geom_type(self)
    }

    fn is_empty(&self) -> Result<Series> {
        crate::ops::is_empty::is_empty(self)
    }

    fn is_ring(&self) -> Result<Series> {
        crate::ops::is_ring::is_ring(self)
    }

    fn rotate(&self, angle: f64, origin: TransformOrigin) -> Result<Series> {
        crate::ops::affine::rotate(self, angle, origin)
    }

    fn scale(&self, xfact: f64, yfact: f64, origin: TransformOrigin) -> Result<Series> {
        crate::ops::affine::scale(self, xfact, yfact, origin)
    }

    fn simplify(&self, tolerance: f64) -> Result<Series> {
        crate::ops::simplify::simplify(self, tolerance)
    }

    fn skew(&self, xs: f64, ys: f64, origin: TransformOrigin) -> Result<Series> {
        crate::ops::affine::skew(self, xs, ys, origin)
    }

    fn distance(&self, other: &Series) -> Result<Series> {
        crate::ops::distance::euclidean_distance(self, other)
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

    fn translate(&self, x: f64, y: f64) -> Result<Series> {
        crate::ops::affine::translate(self, x, y)
    }

    fn x(&self) -> Result<Series> {
        crate::ops::point::x(self)
    }

    fn y(&self) -> Result<Series> {
        crate::ops::point::y(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        geoseries::{GeoSeries, GeodesicLengthMethod},
        util::iter_geom,
    };
    use polars::export::arrow::array::Array;
    use polars::prelude::Series;

    use geo::{line_string, polygon, CoordsIter, Geometry, LineString, MultiPoint, Point};
    use geozero::{CoordDimensions, ToWkb};
    use polars::export::arrow::array::{BinaryArray, MutableBinaryArray};

    use super::TransformOrigin;

    #[test]
    fn convex_hull_for_multipoint() {
        let mut test_data = MutableBinaryArray::<i32>::with_capacity(1);

        // Values borrowed from this test in geo crate: https://docs.rs/geo/0.14.2/src/geo/algorithm/convexhull.rs.html#323
        let v = vec![
            Point::new(0.0, 10.0),
            Point::new(1.0, 1.0),
            Point::new(10.0, 0.0),
            Point::new(1.0, -1.0),
            Point::new(0.0, -10.0),
            Point::new(-1.0, -1.0),
            Point::new(-10.0, 0.0),
            Point::new(-1.0, 1.0),
            Point::new(0.0, 10.0),
        ];
        let mp = MultiPoint(v);

        let correct_poly: Geometry<f64> = polygon![
            (x:0.0, y: -10.0),
            (x:10.0, y: 0.0),
            (x:0.0, y:10.0),
            (x:-10.0, y:0.0),
            (x:0.0, y:-10.0),
        ]
        .into();

        let test_geom: Geometry<f64> = mp.into();
        let test_wkb = test_geom.to_wkb(CoordDimensions::xy()).unwrap();
        test_data.push(Some(test_wkb));

        let test_array: BinaryArray<i32> = test_data.into();

        let series =
            Series::try_from(("geometry", Box::new(test_array) as Box<dyn Array>)).unwrap();
        let convex_res = series.convex_hull();

        assert!(
            convex_res.is_ok(),
            "Should get a valid result back from convex hull"
        );
        let convex_res = convex_res.unwrap();

        assert_eq!(
            convex_res.len(),
            1,
            "Should get a single result back from the series"
        );
        let mut geom_iter = iter_geom(&convex_res);
        let result = geom_iter.next().unwrap();

        assert_eq!(result, correct_poly, "Should get the correct convex hull");
    }

    #[test]
    fn skew() {
        let geo_series = Series::from_geom_vec(&[Geometry::Polygon(polygon!(
        (x: 0.0,y:0.0),
        (x: 0.0,y:1.0),
        (x: 1.0,y: 1.0),
        (x: 1.0,y: 0.0)
        ))])
        .unwrap();

        let result: Geometry<f64> = polygon!(
            (x:-0.008727532464108793,y:-0.017460384745873865),
            (x:0.008727532464108793,y:0.9825396152541261),
            (x:1.008727532464109, y:1.0174603847458739),
            (x:0.9912724675358912, y:0.017460384745873865)
        )
        .into();

        let skewed_series = geo_series.skew(1.0, 2.0, TransformOrigin::Center);
        assert!(skewed_series.is_ok(), "To get a series back");

        let geom = iter_geom(&skewed_series.unwrap()).next().unwrap();

        assert_eq!(geom, result, "the polygon should be transformed correctly");

        for (p1, p2) in geom.coords_iter().zip(result.coords_iter()) {
            assert!(
                (p1.x - p2.x).abs() < 0.00000001,
                "The geometries x coords to be correct to within some tollerenace"
            );
            assert!(
                (p1.y - p2.y).abs() < 0.00000001,
                "The geometries y coords to be correct to within some tollerenace"
            );
        }
    }

    #[test]
    fn distance() {
        let geo_series = Series::from_geom_vec(&[
            Geometry::Point(Point::new(0.0, 0.0)),
            Geometry::Point(Point::new(0.0, 0.0)),
            Geometry::Point(Point::new(1.0, 1.0)),
            Geometry::LineString(LineString::<f64>::from(vec![(0.0, 0.0), (0.0, 4.0)])),
        ])
        .unwrap();

        let other_geo_series = Series::from_geom_vec(&[
            Geometry::Point(Point::new(0.0, 1.0)),
            Geometry::Point(Point::new(1.0, 1.0)),
            Geometry::Point(Point::new(4.0, 5.0)),
            Geometry::Point(Point::new(2.0, 2.0)),
        ])
        .unwrap();
        let results = vec![1.0_f64, 2.0_f64.sqrt(), 5.0_f64, 2.0_f64];

        let distance_series = geo_series.distance(&other_geo_series);
        assert!(distance_series.is_ok(), "To get a series back");

        let distance_series = distance_series.unwrap();
        let distance_vec: Vec<f64> = distance_series.f64().unwrap().into_no_null_iter().collect();

        for (d1, d2) in distance_vec.iter().zip(results.iter()) {
            assert_eq!(d1, d2, "Distances differ, should be the same");
        }
    }

    #[test]
    fn rotate() {
        let geo_series = Series::from_geom_vec(&[Geometry::Polygon(polygon!(
        (x: 0.0,y:0.0),
        (x: 0.0,y:1.0),
        (x: 1.0,y: 1.0),
        (x: 1.0,y: 0.0)
        ))])
        .unwrap();

        let result: Geometry<f64> = polygon!(
        (x:0.0,y:0.0),
        (x:-1.0,y:0.0),
        (x:-1.0, y:1.0),
        (x:0.0, y:1.0)
        )
        .into();

        let rotated_series = geo_series.rotate(90.0, TransformOrigin::Point(Point::new(0.0, 0.0)));
        assert!(rotated_series.is_ok(), "To get a series back");

        let geom = iter_geom(&rotated_series.unwrap()).next().unwrap();
        for (p1, p2) in geom.coords_iter().zip(result.coords_iter()) {
            assert!(
                (p1.x - p2.x).abs() < 0.00000001,
                "The geometries x coords to be correct to within some tollerenace"
            );
            assert!(
                (p1.y - p2.y).abs() < 0.00000001,
                "The geometries y coords to be correct to within some tollerenace"
            );
        }
    }

    #[test]
    fn translate() {
        let geo_series = Series::from_geom_vec(&[Geometry::Polygon(polygon!(
        (x: 0.0,y:0.0),
        (x: 0.0,y:1.0),
        (x: 1.0,y: 1.0),
        (x: 1.0,y: 0.0)
        ))])
        .unwrap();

        let result: Geometry<f64> = polygon!(
        (x:1.0,y:1.0),
        (x:1.0,y:2.0),
        (x:2.0, y:2.0),
        (x:2.0, y:1.0)
        )
        .into();

        let translated_series = geo_series.translate(1.0, 1.0);
        assert!(translated_series.is_ok(), "To get a series back");

        let geom = iter_geom(&translated_series.unwrap()).next().unwrap();
        assert_eq!(geom, result, "The geom to be approprietly translated");
    }

    #[test]
    fn scale() {
        let geo_series = Series::from_geom_vec(&[Geometry::Polygon(polygon!(
        (x: 0.0,y:0.0),
        (x: 0.0,y:1.0),
        (x: 1.0,y: 1.0),
        (x: 1.0,y: 0.0)
        ))])
        .unwrap();

        let result_center: Geometry<f64> = polygon!(
        (x:-0.5,y:-0.5),
        (x:-0.5,y:1.5),
        (x:1.5, y:1.5),
        (x:1.5, y:-0.5)
        )
        .into();

        let result_point: Geometry<f64> = polygon!(
        (x:0.0,y:0.0),
        (x:0.0,y:2.0),
        (x:2.0, y:2.0),
        (x:2.0, y:0.0)
        )
        .into();

        let scaled_series = geo_series.scale(2.0, 2.0, TransformOrigin::Center);
        assert!(scaled_series.is_ok(), "To get a series back");

        let geom = iter_geom(&scaled_series.unwrap()).next().unwrap();
        assert_eq!(
            geom, result_center,
            "The geom to be approprietly scaled about it's center"
        );

        let scaled_series =
            geo_series.scale(2.0, 2.0, TransformOrigin::Point(Point::new(0.0, 0.0)));
        let geom = iter_geom(&scaled_series.unwrap()).next().unwrap();
        assert_eq!(
            geom, result_point,
            "The geom to be approprietly scaled about the point 0,0"
        );
    }

    #[test]
    fn euclidean_length() {
        let mut test_data = MutableBinaryArray::<i32>::with_capacity(1);

        let line_string: Geometry<f64> = line_string![
            (x: 1., y: 1.),
            (x: 7., y: 1.),
            (x: 8., y: 1.),
            (x: 9., y: 1.),
            (x: 10., y: 1.),
            (x: 11., y: 1.)
        ]
        .into();

        let test_wkb = line_string.to_wkb(CoordDimensions::xy()).unwrap();
        test_data.push(Some(test_wkb));

        let test_array: BinaryArray<i32> = test_data.into();

        let series =
            Series::try_from(("geometry", Box::new(test_array) as Box<dyn Array>)).unwrap();
        let lengths = series.euclidean_length().unwrap();
        let as_vec: Vec<f64> = lengths.f64().unwrap().into_no_null_iter().collect();

        assert_eq!(10.0_f64, as_vec[0]);
    }
    #[test]
    fn explode() {
        let point_0 = Point::new(0., 0.);
        let point_1 = Point::new(1., 1.);
        let point_2 = Point::new(2., 2.);
        let point_3 = Point::new(3., 3.);
        let point_4 = Point::new(4., 4.);

        let expected_series = Series::from_geom_vec(&[
            Geometry::Point(point_0),
            Geometry::Point(point_1),
            Geometry::Point(point_2),
            Geometry::Point(point_3),
            Geometry::Point(point_4),
        ])
        .unwrap();

        let multipoint_0 = MultiPoint::new(vec![point_0, point_1]);
        let multipoint_1 = MultiPoint::new(vec![point_2, point_3, point_4]);

        let input_series = Series::from_geom_vec(&[
            Geometry::MultiPoint(multipoint_0),
            Geometry::MultiPoint(multipoint_1),
        ])
        .unwrap();

        let output_series = GeoSeries::explode(&input_series).unwrap();

        assert_eq!(output_series, expected_series);
    }
    #[test]
    fn haversine_length() {
        let mut test_data = MutableBinaryArray::<i32>::with_capacity(1);

        let line_string: Geometry<f64> = LineString::<f64>::from(vec![
            // New York City
            (-74.006, 40.7128),
            // London
            (-0.1278, 51.5074),
        ])
        .into();

        let test_wkb = line_string.to_wkb(CoordDimensions::xy()).unwrap();
        test_data.push(Some(test_wkb));

        let test_array: BinaryArray<i32> = test_data.into();

        let series =
            Series::try_from(("geometry", Box::new(test_array) as Box<dyn Array>)).unwrap();
        let lengths = series
            .geodesic_length(GeodesicLengthMethod::Haversine)
            .unwrap();
        let as_vec: Vec<f64> = lengths.f64().unwrap().into_no_null_iter().collect();

        assert_eq!(
            5_570_230., // meters
            as_vec[0].round()
        );
    }
    #[test]
    fn vincenty_length() {
        let mut test_data = MutableBinaryArray::<i32>::with_capacity(1);

        let line_string: Geometry<f64> = LineString::<f64>::from(vec![
            // New York City
            (-74.006, 40.7128),
            // London
            (-0.1278, 51.5074),
        ])
        .into();

        let test_wkb = line_string.to_wkb(CoordDimensions::xy()).unwrap();
        test_data.push(Some(test_wkb));

        let test_array: BinaryArray<i32> = test_data.into();

        let series =
            Series::try_from(("geometry", Box::new(test_array) as Box<dyn Array>)).unwrap();
        let lengths = series
            .geodesic_length(GeodesicLengthMethod::Vincenty)
            .unwrap();
        let as_vec: Vec<f64> = lengths.f64().unwrap().into_no_null_iter().collect();

        assert_eq!(
            5585234., // meters
            as_vec[0].round()
        );
    }

    #[test]
    fn geodesic_length() {
        let mut test_data = MutableBinaryArray::<i32>::with_capacity(1);

        let line_string: Geometry<f64> = LineString::<f64>::from(vec![
            // New York City
            (-74.006, 40.7128),
            // London
            (-0.1278, 51.5074),
            // Osaka
            (135.5244559, 34.687455),
        ])
        .into();

        let test_wkb = line_string.to_wkb(CoordDimensions::xy()).unwrap();
        test_data.push(Some(test_wkb));

        let test_array: BinaryArray<i32> = test_data.into();

        let series =
            Series::try_from(("geometry", Box::new(test_array) as Box<dyn Array>)).unwrap();
        let lengths = series
            .geodesic_length(GeodesicLengthMethod::Geodesic)
            .unwrap();
        let as_vec: Vec<f64> = lengths.f64().unwrap().into_no_null_iter().collect();

        assert_eq!(
            15_109_158., // meters
            as_vec[0].round()
        );
    }
}
