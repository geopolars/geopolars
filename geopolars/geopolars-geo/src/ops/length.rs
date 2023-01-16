use crate::error::Result;
use geo::algorithm::euclidean_length::EuclideanLength;
use geo::algorithm::geodesic_length::GeodesicLength;
use geo::algorithm::haversine_length::HaversineLength;
use geo::algorithm::vincenty_length::VincentyLength;
use geo::Geometry;
use geopolars_arrow::GeometryArrayEnum;
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

pub(crate) fn euclidean_length(array: GeometryArrayEnum) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArrayEnum::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_euclidean_length)));
        }
        GeometryArrayEnum::Point(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArrayEnum::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.euclidean_length())));
        }
        GeometryArrayEnum::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.exterior().euclidean_length()))
            });
        }
        GeometryArrayEnum::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.euclidean_length())));
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
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
    array: GeometryArrayEnum,
    method: &GeodesicLengthMethod,
) -> Result<PrimitiveArray<f64>> {
    match method {
        GeodesicLengthMethod::Geodesic => _geodesic_length(array),
        GeodesicLengthMethod::Haversine => haversine_length(array),
        GeodesicLengthMethod::Vincenty => vincenty_length(array),
    }
}

fn _geodesic_length(array: GeometryArrayEnum) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArrayEnum::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_geodesic_length)));
        }
        GeometryArrayEnum::Point(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArrayEnum::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.geodesic_length())));
        }
        GeometryArrayEnum::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.exterior().geodesic_length()))
            });
        }
        GeometryArrayEnum::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.geodesic_length())));
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(
                    maybe_g.map(|g| g.iter().map(|poly| poly.exterior().geodesic_length()).sum()),
                )
            });
        }
    }

    Ok(output_array.into())
}

fn haversine_length(array: GeometryArrayEnum) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());

    match array {
        GeometryArrayEnum::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_haversine_length)));
        }
        GeometryArrayEnum::Point(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArrayEnum::LineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.haversine_length())));
        }
        GeometryArrayEnum::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| g.exterior().haversine_length()))
            });
        }
        GeometryArrayEnum::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(|g| g.haversine_length())));
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
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

fn vincenty_length(array: GeometryArrayEnum) -> Result<PrimitiveArray<f64>> {
    let mut output_array = MutablePrimitiveArray::<f64>::with_capacity(array.len());
    let map_vincenty_error =
        |_| PolarsError::ComputeError(ErrString::from("Failed to calculate vincenty length"));

    match array {
        GeometryArrayEnum::WKB(arr) => {
            arr.iter_geo()
                .for_each(|maybe_g| output_array.push(maybe_g.map(geometry_vincenty_length)));
        }
        GeometryArrayEnum::Point(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArrayEnum::LineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array
                    .push(maybe_g.map(|g| g.vincenty_length().map_err(map_vincenty_error).unwrap()))
            });
        }
        GeometryArrayEnum::Polygon(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array.push(maybe_g.map(|g| {
                    g.exterior()
                        .vincenty_length()
                        .map_err(map_vincenty_error)
                        .unwrap()
                }))
            });
        }
        GeometryArrayEnum::MultiPoint(arr) => {
            return Ok(zero_arr(arr.len(), arr.validity()));
        }
        GeometryArrayEnum::MultiLineString(arr) => {
            arr.iter_geo().for_each(|maybe_g| {
                output_array
                    .push(maybe_g.map(|g| g.vincenty_length().map_err(map_vincenty_error).unwrap()))
            });
        }
        GeometryArrayEnum::MultiPolygon(arr) => {
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
    use super::GeodesicLengthMethod;
    use crate::geoseries::GeoSeries;
    use geo::{line_string, Geometry, LineString};
    use geopolars_arrow::linestring::MutableLineStringArray;
    use geozero::{CoordDimensions, ToWkb};
    use polars::export::arrow::array::{Array, BinaryArray, ListArray, MutableBinaryArray};
    use polars::prelude::Series;

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
    fn euclidean_length_geoarrow_linestring() {
        let line_strings = vec![line_string![
            (x: 1., y: 1.),
            (x: 7., y: 1.),
            (x: 8., y: 1.),
            (x: 9., y: 1.),
            (x: 10., y: 1.),
            (x: 11., y: 1.)
        ]];
        let mut_line_string_arr: MutableLineStringArray = line_strings.into();
        let line_string_arr: ListArray<i64> = mut_line_string_arr.into();
        let series =
            Series::try_from(("geometry", Box::new(line_string_arr) as Box<dyn Array>)).unwrap();

        let actual = series.euclidean_length().unwrap();
        let actual_ca = actual.f64().unwrap();
        assert_eq!(actual_ca.into_iter().next().unwrap().unwrap(), 10.0_f64);
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
    fn haversine_length_geoarrow() {
        let line_strings = vec![line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ]];
        let mut_line_string_arr: MutableLineStringArray = line_strings.into();
        let line_string_arr: ListArray<i64> = mut_line_string_arr.into();
        let series =
            Series::try_from(("geometry", Box::new(line_string_arr) as Box<dyn Array>)).unwrap();

        let actual = series
            .geodesic_length(GeodesicLengthMethod::Haversine)
            .unwrap();
        let actual_ca = actual.f64().unwrap();
        assert_eq!(
            actual_ca.into_iter().next().unwrap().unwrap().round(),
            5_570_230.
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
    fn vincenty_length_geoarrow() {
        let line_strings = vec![line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
        ]];
        let mut_line_string_arr: MutableLineStringArray = line_strings.into();
        let line_string_arr: ListArray<i64> = mut_line_string_arr.into();
        let series =
            Series::try_from(("geometry", Box::new(line_string_arr) as Box<dyn Array>)).unwrap();

        let actual = series
            .geodesic_length(GeodesicLengthMethod::Vincenty)
            .unwrap();
        let actual_ca = actual.f64().unwrap();
        assert_eq!(
            actual_ca.into_iter().next().unwrap().unwrap().round(),
            5585234.
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

    #[test]
    fn geodesic_length_geoarrow() {
        let line_strings = vec![line_string![
            // New York City
            (x: -74.006, y: 40.7128),
            // London
            (x: -0.1278, y: 51.5074),
            // Osaka
            (x: 135.5244559, y: 34.687455),
        ]];
        let mut_line_string_arr: MutableLineStringArray = line_strings.into();
        let line_string_arr: ListArray<i64> = mut_line_string_arr.into();
        let series =
            Series::try_from(("geometry", Box::new(line_string_arr) as Box<dyn Array>)).unwrap();

        let actual = series
            .geodesic_length(GeodesicLengthMethod::Geodesic)
            .unwrap();
        let actual_ca = actual.f64().unwrap();
        assert_eq!(
            actual_ca.into_iter().next().unwrap().unwrap().round(),
            15_109_158.
        );
    }
}
