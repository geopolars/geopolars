use std::sync::Arc;

use crate::util::iter_geom;
use arrow2::array::{
    ArrayRef, BinaryArray, BooleanArray, MutableBinaryArray, MutableBooleanArray,
    MutablePrimitiveArray, PrimitiveArray,
};

#[cfg(feature = "affine-transform")]
use geo::algorithm::affine_ops::AffineTransform;
use geo::{map_coords::MapCoords, Geometry, Point};
use geozero::{CoordDimensions, ToWkb};
use polars::prelude::{PolarsError, Result, Series};
use std::convert::Into;

pub enum GeodesicLengthMethod {
    Haversine,
    Geodesic,
    Vincenty,
}

/// Used to express the origin for a given transform. Can be specified either be with reference to
/// the geometry being transformed (Centroid, Center) or some arbitrary point.
///
/// - Centroid: Use the centriod of each geometry in the series as the transform origin.
/// - Center: Use the center of each geometry in the series as the transform origin. The center is
///   defined as the center of the bounding box of the geometry
/// - Point: Define a single point to transform each geometry in the series about.
pub enum TransformOrigin {
    Centroid,
    Center,
    Point(Point),
}

pub trait GeoSeries {
    /// Apply an affine transform to the geoseries and return a geoseries of the tranformed geometries;
    #[cfg(feature = "affine-transform")]
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
    #[cfg(feature = "affine-transform")]
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
    #[cfg(feature = "affine-transform")]
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
    #[cfg(feature = "affine-transform")]
    fn skew(&self, xs: f64, ys: f64, origin: TransformOrigin) -> Result<Series>;

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
    #[cfg(feature = "affine-transform")]
    fn translate(&self, x: f64, y: f64) -> Result<Series>;

    /// Return the x location of point geometries in a GeoSeries
    fn x(&self) -> Result<Series>;

    /// Return the y location of point geometries in a GeoSeries
    fn y(&self) -> Result<Series>;
}

impl GeoSeries for Series {
    #[cfg(feature = "affine-transform")]
    fn affine_transform(&self, matrix: impl Into<AffineTransform<f64>>) -> Result<Series> {
        let transform: AffineTransform<f64> = matrix.into();
        let output_vec: Vec<Geometry> = iter_geom(self)
            .map(|geom| geom.map_coords(|c| transform.apply(c)))
            .collect();

        Series::from_geom_vec(&output_vec)
    }

    fn area(&self) -> Result<Series> {
        use geo::prelude::Area;

        let output_series: Series = iter_geom(self).map(|geom| geom.unsigned_area()).collect();

        Ok(output_series)
    }

    fn centroid(&self) -> Result<Series> {
        use geo::algorithm::centroid::Centroid;

        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let value: Geometry<f64> = geom.centroid().expect("could not create centroid").into();
            let wkb = value
                .to_wkb(CoordDimensions::xy())
                .expect("Unable to create wkb");

            output_array.push(Some(wkb));
        }

        let result: BinaryArray<i32> = output_array.into();

        Series::try_from(("geometry", Arc::new(result) as ArrayRef))
    }

    fn convex_hull(&self) -> Result<Series> {
        use geo::algorithm::convex_hull::ConvexHull;
        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let hull = match geom {
                Geometry::Polygon(polygon) => Ok(polygon.convex_hull()),
                Geometry::MultiPolygon(multi_poly) => Ok(multi_poly.convex_hull()),
                Geometry::MultiPoint(points) => Ok(points.convex_hull()),
                Geometry::LineString(line_string) => Ok(line_string.convex_hull()),
                Geometry::MultiLineString(multi_line_string) => Ok(multi_line_string.convex_hull()),
                _ => Err(PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                    "ConvexHull not supported for this geometry type",
                ))),
            }?;
            let hull: Geometry<f64> = hull.into();
            let hull_wkb = hull.to_wkb(CoordDimensions::xy()).unwrap();

            output_array.push(Some(hull_wkb));
        }

        let result: BinaryArray<i32> = output_array.into();
        Series::try_from(("geometry", Arc::new(result) as ArrayRef))
    }

    fn envelope(&self) -> Result<Series> {
        use geo::algorithm::bounding_rect::BoundingRect;

        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let value: Geometry<f64> = geom.bounding_rect().unwrap().into();
            let wkb = value
                .to_wkb(CoordDimensions::xy())
                .expect("Unable to create wkb");

            output_array.push(Some(wkb));
        }

        let result: BinaryArray<i32> = output_array.into();

        Series::try_from(("geometry", Arc::new(result) as ArrayRef))
    }

    fn euclidean_length(&self) -> Result<Series> {
        use geo::algorithm::euclidean_length::EuclideanLength;
        let mut result = MutablePrimitiveArray::<f64>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let length: f64 = match geom {
                Geometry::Point(_) => Ok(0.0),
                Geometry::Line(line) => Ok(line.euclidean_length()),
                Geometry::LineString(line_string) => Ok(line_string.euclidean_length()),
                Geometry::Polygon(polygon) => Ok(polygon.exterior().euclidean_length()),
                Geometry::MultiPoint(_) => Ok(0.0),
                Geometry::MultiLineString(multi_line_string) => {
                    Ok(multi_line_string.euclidean_length())
                }
                Geometry::MultiPolygon(mutli_polygon) => Ok(mutli_polygon
                    .iter()
                    .map(|poly| poly.exterior().euclidean_length())
                    .sum()),
                Geometry::GeometryCollection(_) => {
                    Err(PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                        "Length methods are not implemented for geometry collection",
                    )))
                }
                Geometry::Rect(rec) => Ok(rec.to_polygon().exterior().euclidean_length()),
                Geometry::Triangle(triangle) => {
                    Ok(triangle.to_polygon().exterior().euclidean_length())
                }
            }?;
            result.push(Some(length));
        }

        let result: PrimitiveArray<f64> = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }

    fn exterior(&self) -> Result<Series> {
        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let maybe_exterior = match geom {
                Geometry::Polygon(polygon) => {
                    let exterior: Geometry<f64> = polygon.exterior().clone().into();
                    Some(exterior.to_wkb(CoordDimensions::xy()).unwrap())
                }
                _ => None,
            };
            output_array.push(maybe_exterior);
        }

        let result: BinaryArray<i32> = output_array.into();

        Series::try_from(("geometry", Arc::new(result) as ArrayRef))
    }

    fn from_geom_vec(geoms: &[Geometry<f64>]) -> Result<Self> {
        let mut wkb_array = MutableBinaryArray::<i32>::with_capacity(geoms.len());

        for geom in geoms {
            let wkb = geom.to_wkb(CoordDimensions::xy()).map_err(|_| {
                PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                    "Failed to convert geom vec to GeoSeries",
                ))
            })?;
            wkb_array.push(Some(wkb));
        }
        let array: BinaryArray<i32> = wkb_array.into();

        let series = Series::try_from(("geometry", Arc::new(array) as ArrayRef)).unwrap();
        Ok(series)
    }

    fn geodesic_length(&self, method: GeodesicLengthMethod) -> Result<Series> {
        use geo::algorithm::{
            geodesic_length::GeodesicLength, haversine_length::HaversineLength,
            vincenty_length::VincentyLength,
        };
        let mut result = MutablePrimitiveArray::<f64>::with_capacity(self.len());

        let map_vincenty_error = |_| {
            PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                "Failed to calculate vincenty length",
            ))
        };

        for geom in iter_geom(self) {
            let length: f64 = match (&method, geom) {
                (_, Geometry::Point(_)) => Ok(0.0),

                (GeodesicLengthMethod::Haversine, Geometry::Line(line)) => {
                    Ok(line.haversine_length())
                }
                (GeodesicLengthMethod::Geodesic, Geometry::Line(line)) => {
                    Ok(line.geodesic_length())
                }
                (GeodesicLengthMethod::Vincenty, Geometry::Line(line)) => {
                    line.vincenty_length().map_err(map_vincenty_error)
                }

                (GeodesicLengthMethod::Haversine, Geometry::LineString(line_string)) => {
                    Ok(line_string.haversine_length())
                }
                (GeodesicLengthMethod::Geodesic, Geometry::LineString(line_string)) => {
                    Ok(line_string.geodesic_length())
                }
                (GeodesicLengthMethod::Vincenty, Geometry::LineString(line_string)) => {
                    line_string.vincenty_length().map_err(map_vincenty_error)
                }

                (GeodesicLengthMethod::Haversine, Geometry::Polygon(polygon)) => {
                    Ok(polygon.exterior().haversine_length())
                }
                (GeodesicLengthMethod::Geodesic, Geometry::Polygon(polygon)) => {
                    Ok(polygon.exterior().geodesic_length())
                }
                (GeodesicLengthMethod::Vincenty, Geometry::Polygon(polygon)) => polygon
                    .exterior()
                    .vincenty_length()
                    .map_err(map_vincenty_error),

                (_, Geometry::MultiPoint(_)) => Ok(0.0),

                (GeodesicLengthMethod::Haversine, Geometry::MultiLineString(multi_line_string)) => {
                    Ok(multi_line_string.haversine_length())
                }

                (GeodesicLengthMethod::Geodesic, Geometry::MultiLineString(multi_line_string)) => {
                    Ok(multi_line_string.geodesic_length())
                }
                (GeodesicLengthMethod::Vincenty, Geometry::MultiLineString(multi_line_string)) => {
                    multi_line_string
                        .vincenty_length()
                        .map_err(map_vincenty_error)
                }
                (GeodesicLengthMethod::Haversine, Geometry::MultiPolygon(mutli_polygon)) => {
                    Ok(mutli_polygon
                        .iter()
                        .map(|poly| poly.exterior().haversine_length())
                        .sum())
                }
                (GeodesicLengthMethod::Geodesic, Geometry::MultiPolygon(mutli_polygon)) => {
                    Ok(mutli_polygon
                        .iter()
                        .map(|poly| poly.exterior().geodesic_length())
                        .sum())
                }

                (GeodesicLengthMethod::Vincenty, Geometry::MultiPolygon(mutli_polygon)) => {
                    let result: std::result::Result<Vec<f64>, _> = mutli_polygon
                        .iter()
                        .map(|poly| poly.exterior().vincenty_length())
                        .collect();
                    result.map(|v| v.iter().sum()).map_err(map_vincenty_error)
                }
                (_, Geometry::GeometryCollection(_)) => {
                    Err(PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                        "Length methods are not implemented for geometry collection",
                    )))
                }
                (GeodesicLengthMethod::Haversine, Geometry::Rect(rec)) => {
                    Ok(rec.to_polygon().exterior().haversine_length())
                }
                (GeodesicLengthMethod::Geodesic, Geometry::Rect(rec)) => {
                    Ok(rec.to_polygon().exterior().geodesic_length())
                }
                (GeodesicLengthMethod::Vincenty, Geometry::Rect(rec)) => rec
                    .to_polygon()
                    .exterior()
                    .vincenty_length()
                    .map_err(map_vincenty_error),
                (GeodesicLengthMethod::Haversine, Geometry::Triangle(triangle)) => {
                    Ok(triangle.to_polygon().exterior().haversine_length())
                }
                (GeodesicLengthMethod::Geodesic, Geometry::Triangle(triangle)) => {
                    Ok(triangle.to_polygon().exterior().geodesic_length())
                }
                (GeodesicLengthMethod::Vincenty, Geometry::Triangle(triangle)) => triangle
                    .to_polygon()
                    .exterior()
                    .vincenty_length()
                    .map_err(map_vincenty_error),
            }?;
            result.push(Some(length));
        }

        let result: PrimitiveArray<f64> = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }

    fn geom_type(&self) -> Result<Series> {
        let mut result = MutablePrimitiveArray::<i8>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let type_id: i8 = match geom {
                Geometry::Point(_) => 0,
                Geometry::Line(_) => 1,
                Geometry::LineString(_) => 1,
                Geometry::Polygon(_) => 3,
                Geometry::MultiPoint(_) => 4,
                Geometry::MultiLineString(_) => 5,
                Geometry::MultiPolygon(_) => 6,
                Geometry::GeometryCollection(_) => 7,
                // Should these still call themselves polygon?
                Geometry::Rect(_) => 3,
                Geometry::Triangle(_) => 3,
            };
            result.push(Some(type_id));
        }

        let result: PrimitiveArray<i8> = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }

    fn is_empty(&self) -> Result<Series> {
        use geo::dimensions::HasDimensions;

        let mut result = MutableBooleanArray::with_capacity(self.len());

        for geom in iter_geom(self) {
            result.push(Some(geom.is_empty()));
        }

        let result: BooleanArray = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }

    fn is_ring(&self) -> Result<Series> {
        let mut result = MutableBooleanArray::with_capacity(self.len());

        for geom in iter_geom(self) {
            let value = match geom {
                Geometry::LineString(g) => Some(g.is_closed()),
                Geometry::MultiLineString(g) => Some(g.is_closed()),
                _ => None,
            };
            result.push(value);
        }

        let result: BooleanArray = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }

    #[cfg(feature = "affine-transform")]
    fn rotate(&self, angle: f64, origin: TransformOrigin) -> Result<Series> {
        use geo::algorithm::bounding_rect::BoundingRect;
        use geo::algorithm::centroid::Centroid;
        match origin {
            TransformOrigin::Centroid => {
                let rotated_geoms: Vec<Geometry<f64>> = iter_geom(self)
                    .map(|geom| {
                        let centroid = geom.centroid().unwrap();
                        let transform = AffineTransform::rotate(angle, centroid);
                        geom.map_coords(|c| transform.apply(c))
                    })
                    .collect();
                Series::from_geom_vec(&rotated_geoms)
            }
            TransformOrigin::Center => {
                let rotated_geoms: Vec<Geometry<f64>> = iter_geom(self)
                    .map(|geom| {
                        let center = geom.bounding_rect().unwrap().center();
                        let transform = AffineTransform::rotate(angle, center.into());
                        geom.map_coords(|c| transform.apply(c))
                    })
                    .collect();
                Series::from_geom_vec(&rotated_geoms)
            }
            TransformOrigin::Point(point) => {
                let transform = AffineTransform::rotate(angle, point);
                self.affine_transform(transform)
            }
        }
    }

    #[cfg(feature = "affine-transform")]
    fn scale(&self, xfact: f64, yfact: f64, origin: TransformOrigin) -> Result<Series> {
        use geo::algorithm::bounding_rect::BoundingRect;
        use geo::algorithm::centroid::Centroid;
        match origin {
            TransformOrigin::Centroid => {
                let rotated_geoms: Vec<Geometry<f64>> = iter_geom(self)
                    .map(|geom| {
                        let centroid = geom.centroid().unwrap();
                        let transform = AffineTransform::scale(xfact, yfact, centroid);
                        geom.map_coords(|c| transform.apply(c))
                    })
                    .collect();
                Series::from_geom_vec(&rotated_geoms)
            }
            TransformOrigin::Center => {
                let rotated_geoms: Vec<Geometry<f64>> = iter_geom(self)
                    .map(|geom| {
                        let center = geom.bounding_rect().unwrap().center();
                        let transform = AffineTransform::scale(xfact, yfact, center.into());
                        geom.map_coords(|c| transform.apply(c))
                    })
                    .collect();
                Series::from_geom_vec(&rotated_geoms)
            }
            TransformOrigin::Point(point) => {
                let transform = AffineTransform::scale(xfact, yfact, point);
                self.affine_transform(transform)
            }
        }
    }

    fn simplify(&self, tolerance: f64) -> Result<Series> {
        use geo::algorithm::simplify::Simplify;

        let mut output_array = MutableBinaryArray::<i32>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let value = match geom {
                Geometry::Point(g) => Geometry::Point(g),
                Geometry::MultiPoint(g) => Geometry::MultiPoint(g),
                Geometry::LineString(g) => Geometry::LineString(g.simplify(&tolerance)),
                Geometry::MultiLineString(g) => Geometry::MultiLineString(g.simplify(&tolerance)),
                Geometry::Polygon(g) => Geometry::Polygon(g.simplify(&tolerance)),
                Geometry::MultiPolygon(g) => Geometry::MultiPolygon(g.simplify(&tolerance)),
                _ => unimplemented!(),
            };

            let wkb = value
                .to_wkb(CoordDimensions::xy())
                .expect("Unable to create wkb");

            output_array.push(Some(wkb));
        }

        let result: BinaryArray<i32> = output_array.into();

        Series::try_from(("geometry", Arc::new(result) as ArrayRef))
    }

    #[cfg(feature = "affine-transform")]
    fn skew(&self, xs: f64, ys: f64, origin: TransformOrigin) -> Result<Series> {
        use geo::algorithm::bounding_rect::BoundingRect;
        use geo::algorithm::centroid::Centroid;
        match origin {
            TransformOrigin::Centroid => {
                let rotated_geoms: Vec<Geometry<f64>> = iter_geom(self)
                    .map(|geom| {
                        let centroid = geom.centroid().unwrap();
                        let transform = AffineTransform::skew(xs, ys, centroid);
                        geom.map_coords(|c| transform.apply(c))
                    })
                    .collect();
                Series::from_geom_vec(&rotated_geoms)
            }
            TransformOrigin::Center => {
                let rotated_geoms: Vec<Geometry<f64>> = iter_geom(self)
                    .map(|geom| {
                        let center = geom.bounding_rect().unwrap().center();
                        let transform = AffineTransform::skew(xs, ys, center.into());
                        geom.map_coords(|c| transform.apply(c))
                    })
                    .collect();
                Series::from_geom_vec(&rotated_geoms)
            }
            TransformOrigin::Point(point) => {
                let transform = AffineTransform::skew(xs, ys, point);
                self.affine_transform(transform)
            }
        }
    }

    #[cfg(feature = "affine-transform")]
    fn translate(&self, x: f64, y: f64) -> Result<Series> {
        let transform = AffineTransform::translate(x, y);
        self.affine_transform(transform)
    }

    fn x(&self) -> Result<Series> {
        let mut result = MutablePrimitiveArray::<f64>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let point: Point<f64> = match geom.try_into() {
                Ok(point) => point,
                Err(_) => {
                    return Err(PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                        "Not a point geometry",
                    )))
                }
            };
            result.push(Some(point.x()));
        }

        let result: PrimitiveArray<f64> = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }

    fn y(&self) -> Result<Series> {
        let mut result = MutablePrimitiveArray::<f64>::with_capacity(self.len());

        for geom in iter_geom(self) {
            let point: Point<f64> = match geom.try_into() {
                Ok(point) => point,
                Err(_) => {
                    return Err(PolarsError::ComputeError(std::borrow::Cow::Borrowed(
                        "Not a point geometry",
                    )))
                }
            };
            result.push(Some(point.y()));
        }

        let result: PrimitiveArray<f64> = result.into();
        Series::try_from(("result", Arc::new(result) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        geoseries::{GeoSeries, GeodesicLengthMethod},
        util::iter_geom,
    };
    use polars::prelude::Series;
    use std::sync::Arc;

    use arrow2::array::{ArrayRef, BinaryArray, MutableBinaryArray};
    use geo::{line_string, polygon, CoordsIter, Geometry, LineString, MultiPoint, Point};
    use geozero::{CoordDimensions, ToWkb};

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

        let series = Series::try_from(("geometry", Arc::new(test_array) as ArrayRef)).unwrap();
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

    #[cfg(feature = "affine-transform")]
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

    #[cfg(feature = "affine-transform")]
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

    #[cfg(feature = "affine-transform")]
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

    #[cfg(feature = "affine-transform")]
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

        let series = Series::try_from(("geometry", Arc::new(test_array) as ArrayRef)).unwrap();
        let lengths = series.euclidean_length().unwrap();
        let as_vec: Vec<f64> = lengths.f64().unwrap().into_no_null_iter().collect();

        assert_eq!(10.0_f64, as_vec[0]);
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

        let series = Series::try_from(("geometry", Arc::new(test_array) as ArrayRef)).unwrap();
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

        let series = Series::try_from(("geometry", Arc::new(test_array) as ArrayRef)).unwrap();
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

        let series = Series::try_from(("geometry", Arc::new(test_array) as ArrayRef)).unwrap();
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
