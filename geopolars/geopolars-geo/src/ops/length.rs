use crate::error::Result;
use geo::algorithm::euclidean_length::EuclideanLength;
use geo::algorithm::geodesic_length::GeodesicLength;
use geo::algorithm::haversine_length::HaversineLength;
use geo::algorithm::vincenty_length::VincentyLength;
use geo::Geometry;
use geoarrow::{GeometryArray, GeometryArrayTrait};
use polars::error::ErrString;
use polars::export::arrow::array::{MutablePrimitiveArray, PrimitiveArray};
use polars::export::arrow::bitmap::Bitmap;
use polars::export::arrow::datatypes::DataType as ArrowDataType;
use polars::prelude::PolarsError;

pub enum GeodesicLengthMethod {
    Haversine,
    Geodesic,
    Vincenty,
}

pub(crate) fn euclidean_length(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_euclidean_length)));
        }
        GeometryArray::Point(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.euclidean_length())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.exterior().euclidean_length()))
            });
        }
        GeometryArray::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.euclidean_length())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| {
                    g.iter()
                        .map(|poly| poly.exterior().euclidean_length())
                        .sum()
                }))
            });
        }
    }

    Ok(output_array.into())
}

pub(crate) fn geodesic_length(
    array: GeometryArray,
    method: &GeodesicLengthMethod,
) -> Result<PrimitiveArray<f64>> {
    match method {
        GeodesicLengthMethod::Geodesic => _geodesic_length(array),
        GeodesicLengthMethod::Haversine => haversine_length(array),
        GeodesicLengthMethod::Vincenty => vincenty_length(array),
    }
}

fn _geodesic_length(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_geodesic_length)));
        }
        GeometryArray::Point(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.geodesic_length())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.exterior().geodesic_length()))
            });
        }
        GeometryArray::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.geodesic_length())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(
                    maybe_g.map(|g| g.iter().map(|poly| poly.exterior().geodesic_length()).sum()),
                )
            });
        }
    }

    Ok(output_array.into())
}

fn haversine_length(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_haversine_length)));
        }
        GeometryArray::Point(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.haversine_length())));
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.exterior().haversine_length()))
            });
        }
        GeometryArray::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.haversine_length())));
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| {
                    g.iter()
                        .map(|poly| poly.exterior().haversine_length())
                        .sum()
                }))
            });
        }
    }

    Ok(output_array.into())
}

fn vincenty_length(array: GeometryArray) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());
    let map_vincenty_error =
        |_| PolarsError::ComputeError(ErrString::from("Failed to calculate vincenty length"));

    match array {
        GeometryArray::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_vincenty_length)));
        }
        GeometryArray::Point(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::LineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array
                    .push(maybe_g.map(|g| g.vincenty_length().map_err(map_vincenty_error).unwrap()))
            });
        }
        GeometryArray::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| {
                    g.exterior()
                        .vincenty_length()
                        .map_err(map_vincenty_error)
                        .unwrap()
                }))
            });
        }
        GeometryArray::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArray::MultiLineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array
                    .push(maybe_g.map(|g| g.vincenty_length().map_err(map_vincenty_error).unwrap()))
            });
        }
        GeometryArray::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| {
                    g.iter()
                        .map(|poly| {
                            poly.exterior()
                                .vincenty_length()
                                .map_err(map_vincenty_error)
                                .unwrap()
                        })
                        .sum()
                }))
            });
        }
    }

    Ok(output_array.into())
}

/// Create a Float64Array with given length and validity
fn zero_arr(len: usize, validity: Option<&Bitmap>) -> PrimitiveArray<f64> {
    PrimitiveArray::<f64>::new(
        ArrowDataType::Float64,
        vec![0.; len].into(),
        validity.cloned(),
    )
}

fn geometry_euclidean_length(geom: Geometry) -> f64 {
    match geom {
        Geometry::Point(_) => 0.0,
        Geometry::Line(line) => line.euclidean_length(),
        Geometry::LineString(line_string) => line_string.euclidean_length(),
        Geometry::Polygon(polygon) => polygon.exterior().euclidean_length(),
        Geometry::MultiPoint(_) => 0.0,
        Geometry::MultiLineString(multi_line_string) => multi_line_string.euclidean_length(),
        Geometry::MultiPolygon(mutli_polygon) => mutli_polygon
            .iter()
            .map(|poly| poly.exterior().euclidean_length())
            .sum(),
        Geometry::GeometryCollection(_) => {
            panic!("Length methods are not implemented for geometry collection")
        }
        Geometry::Rect(rec) => rec.to_polygon().exterior().euclidean_length(),
        Geometry::Triangle(triangle) => triangle.to_polygon().exterior().euclidean_length(),
    }
}

fn geometry_geodesic_length(geom: Geometry) -> f64 {
    match geom {
        Geometry::Point(_) => 0.0,
        Geometry::Line(line) => line.geodesic_length(),
        Geometry::LineString(line_string) => line_string.geodesic_length(),
        Geometry::Polygon(polygon) => polygon.exterior().geodesic_length(),
        Geometry::MultiPoint(_) => 0.0,
        Geometry::MultiLineString(multi_line_string) => multi_line_string.geodesic_length(),
        Geometry::MultiPolygon(mutli_polygon) => mutli_polygon
            .iter()
            .map(|poly| poly.exterior().geodesic_length())
            .sum(),
        Geometry::GeometryCollection(_) => {
            panic!("Length methods are not implemented for geometry collection")
        }
        Geometry::Rect(rec) => rec.to_polygon().exterior().geodesic_length(),
        Geometry::Triangle(triangle) => triangle.to_polygon().exterior().geodesic_length(),
    }
}

fn geometry_haversine_length(geom: Geometry) -> f64 {
    match geom {
        Geometry::Point(_) => 0.0,
        Geometry::Line(line) => line.haversine_length(),
        Geometry::LineString(line_string) => line_string.haversine_length(),
        Geometry::Polygon(polygon) => polygon.exterior().haversine_length(),
        Geometry::MultiPoint(_) => 0.0,
        Geometry::MultiLineString(multi_line_string) => multi_line_string.haversine_length(),
        Geometry::MultiPolygon(mutli_polygon) => mutli_polygon
            .iter()
            .map(|poly| poly.exterior().haversine_length())
            .sum(),
        Geometry::GeometryCollection(_) => {
            panic!("Length methods are not implemented for geometry collection")
        }
        Geometry::Rect(rec) => rec.to_polygon().exterior().haversine_length(),
        Geometry::Triangle(triangle) => triangle.to_polygon().exterior().haversine_length(),
    }
}

fn geometry_vincenty_length(geom: Geometry) -> f64 {
    let map_vincenty_error =
        |_| PolarsError::ComputeError(ErrString::from("Failed to calculate vincenty length"));

    match geom {
        Geometry::Point(_) => 0.0,
        Geometry::Line(line) => line.vincenty_length().map_err(map_vincenty_error).unwrap(),
        Geometry::LineString(line_string) => line_string
            .vincenty_length()
            .map_err(map_vincenty_error)
            .unwrap(),
        Geometry::Polygon(polygon) => polygon
            .exterior()
            .vincenty_length()
            .map_err(map_vincenty_error)
            .unwrap(),
        Geometry::MultiPoint(_) => 0.0,
        Geometry::MultiLineString(multi_line_string) => multi_line_string
            .vincenty_length()
            .map_err(map_vincenty_error)
            .unwrap(),
        Geometry::MultiPolygon(mutli_polygon) => mutli_polygon
            .iter()
            .map(|poly| {
                poly.exterior()
                    .vincenty_length()
                    .map_err(map_vincenty_error)
                    .unwrap()
            })
            .sum(),
        Geometry::GeometryCollection(_) => {
            panic!("Length methods are not implemented for geometry collection")
        }
        Geometry::Rect(rec) => rec
            .to_polygon()
            .exterior()
            .vincenty_length()
            .map_err(map_vincenty_error)
            .unwrap(),
        Geometry::Triangle(triangle) => triangle
            .to_polygon()
            .exterior()
            .vincenty_length()
            .map_err(map_vincenty_error)
            .unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::{euclidean_length, geodesic_length, GeodesicLengthMethod};
    use geo::{line_string, Geometry};
    use geoarrow::{GeometryArray, LineStringArray, WKBArray};
    use polars::export::arrow::array::Array;

    #[test]
    fn euclidean_length_wkb() {
        let input_geom: Geometry<f64> = line_string![
            (x: 1., y: 1.),
            (x: 7., y: 1.),
            (x: 8., y: 1.),
            (x: 9., y: 1.),
            (x: 10., y: 1.),
            (x: 11., y: 1.)
        ]
        .into();
        let input_array: WKBArray = vec![Some(input_geom)].into();
        let result_array = euclidean_length(GeometryArray::WKB(input_array)).unwrap();

        let expected = 10.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }

    #[test]
    fn euclidean_length_geoarrow_linestring() {
        let input_geom = line_string![
            (x: 1., y: 1.),
            (x: 7., y: 1.),
            (x: 8., y: 1.),
            (x: 9., y: 1.),
            (x: 10., y: 1.),
            (x: 11., y: 1.)
        ];
        let input_array: LineStringArray = vec![input_geom].into();
        let result_array = euclidean_length(GeometryArray::LineString(input_array)).unwrap();

        let expected = 10.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }

    #[test]
    fn haversine_length_wkb() {
        let input_geom: Geometry = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ]
        .into();
        let input_array: WKBArray = vec![Some(input_geom)].into();
        let result_array = geodesic_length(
            GeometryArray::WKB(input_array),
            &GeodesicLengthMethod::Haversine,
        )
        .unwrap();

        // Meters
        let expected = 5_570_230.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }

    #[test]
    fn haversine_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ];
        let input_array: LineStringArray = vec![input_geom].into();
        let result_array = geodesic_length(
            GeometryArray::LineString(input_array),
            &GeodesicLengthMethod::Haversine,
        )
        .unwrap();

        // Meters
        let expected = 5_570_230.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }

    #[test]
    fn vincenty_length_wkb() {
        let input_geom: Geometry = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ]
        .into();
        let input_array: WKBArray = vec![Some(input_geom)].into();
        let result_array = geodesic_length(
            GeometryArray::WKB(input_array),
            &GeodesicLengthMethod::Vincenty,
        )
        .unwrap();

        // Meters
        let expected = 5585234.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }

    #[test]
    fn vincenty_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ];
        let input_array: LineStringArray = vec![input_geom].into();
        let result_array = geodesic_length(
            GeometryArray::LineString(input_array),
            &GeodesicLengthMethod::Vincenty,
        )
        .unwrap();

        // Meters
        let expected = 5585234.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }

    #[test]
    fn geodesic_length_wkb() {
        let input_geom: Geometry = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
            // Osaka
            (x: 135.5244559, y: 34.687455),
        ]
        .into();
        let input_array: WKBArray = vec![Some(input_geom)].into();
        let result_array = geodesic_length(
            GeometryArray::WKB(input_array),
            &GeodesicLengthMethod::Geodesic,
        )
        .unwrap();

        // Meters
        let expected = 15_109_158.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }

    #[test]
    fn geodesic_length_geoarrow() {
        let input_geom = line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
            // Osaka
            (x: 135.5244559, y: 34.687455),
        ];
        let input_array: LineStringArray = vec![input_geom].into();
        let result_array = geodesic_length(
            GeometryArray::LineString(input_array),
            &GeodesicLengthMethod::Geodesic,
        )
        .unwrap();

        // Meters
        let expected = 15_109_158.0_f64;
        assert_eq!(expected, result_array.value(0).round());
        assert!(result_array.is_valid(0));
    }
}
