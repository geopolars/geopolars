use std::sync::Arc;

use crate::util::iter_geom;
use arrow2::array::{
    ArrayRef, BinaryArray, BooleanArray, MutableBinaryArray, MutableBooleanArray,
    MutablePrimitiveArray, PrimitiveArray,
};
use geo::{Geometry, Point};
use geozero::{CoordDimensions, ToWkb};
use polars::prelude::{PolarsError, Result, Series};

pub trait GeoSeries {
    /// Returns a Series containing the area of each geometry in the GeoSeries expressed in the
    /// units of the CRS.
    fn area(&self) -> Result<Series>;

    /// Returns a GeoSeries of points representing the centroid of each geometry.
    ///
    /// Note that centroid does not have to be on or within original geometry.
    fn centroid(&self) -> Result<Series>;

    /// Returns a GeoSeries of geometries representing the envelope of each geometry.
    ///
    /// The envelope of a geometry is the bounding rectangle. That is, the point or smallest
    /// rectangular polygon (with sides parallel to the coordinate axes) that contains the
    /// geometry.
    fn envelope(&self) -> Result<Series>;

    /// Returns a GeoSeries of LinearRings representing the outer boundary of each polygon in the
    /// GeoSeries.
    ///
    /// Applies to GeoSeries containing only Polygons. Returns `None` for other geometry types.
    fn exterior(&self) -> Result<Series>;

    /// Returns the type ids of each geometry
    /// This mimics the pygeos implementation
    /// https://pygeos.readthedocs.io/en/latest/geometry.html?highlight=id#pygeos.geometry.get_type_id
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

    /// Returns a GeoSeries containing a simplified representation of each geometry.
    ///
    /// The algorithm (Douglas-Peucker) recursively splits the original line into smaller parts and
    /// connects these parts’ endpoints by a straight line. Then, it removes all points whose
    /// distance to the straight line is smaller than tolerance. It does not move any points and it
    /// always preserves endpoints of the original line or polygon. See
    /// https://docs.rs/geo/latest/geo/algorithm/simplify/trait.Simplify.html for details
    fn simplify(&self, tolerance: f64) -> Result<Series>;

    /// Return the x location of point geometries in a GeoSeries
    fn x(&self) -> Result<Series>;

    /// Return the y location of point geometries in a GeoSeries
    fn y(&self) -> Result<Series>;
}

impl GeoSeries for Series {
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
