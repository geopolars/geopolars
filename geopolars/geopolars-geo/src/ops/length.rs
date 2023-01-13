use crate::error::Result;
use crate::util::{
    iter_geom, map_linestring_series_to_float_series, map_polygon_series_to_float_series,
};
use geo::algorithm::euclidean_length::EuclideanLength;
use geo::algorithm::geodesic_length::GeodesicLength;
use geo::algorithm::haversine_length::HaversineLength;
use geo::algorithm::vincenty_length::VincentyLength;
use geo::Geometry;
use geopolars_arrow::util::{get_geoarrow_type, GeoArrowType};
use polars::error::ErrString;
use polars::export::arrow::array::{Array, MutablePrimitiveArray, PrimitiveArray};
use polars::prelude::{PolarsError, Series};

pub enum GeodesicLengthMethod {
    Haversine,
    Geodesic,
    Vincenty,
}

pub(crate) fn euclidean_length(series: &Series) -> Result<Series> {
    match get_geoarrow_type(series) {
        GeoArrowType::WKB => euclidean_length_wkb(series),
        GeoArrowType::Point => length_geoarrow_point(series),
        GeoArrowType::LineString => {
            map_linestring_series_to_float_series(series, |ls| ls.euclidean_length())
        }
        _ => panic!("Unexpected geometry type for operation euclidean_length"),
    }
}

pub(crate) fn geodesic_length(series: &Series, method: GeodesicLengthMethod) -> Result<Series> {
    let map_vincenty_error =
        |_| PolarsError::ComputeError(ErrString::from("Failed to calculate vincenty length"));

    match (&method, get_geoarrow_type(series)) {
        (_, GeoArrowType::Point) => length_geoarrow_point(series),

        (GeodesicLengthMethod::Geodesic, GeoArrowType::LineString) => {
            map_linestring_series_to_float_series(series, |ls| ls.geodesic_length())
        }
        (GeodesicLengthMethod::Haversine, GeoArrowType::LineString) => {
            map_linestring_series_to_float_series(series, |ls| ls.haversine_length())
        }
        (GeodesicLengthMethod::Vincenty, GeoArrowType::LineString) => {
            map_linestring_series_to_float_series(series, |ls| {
                ls.vincenty_length().map_err(map_vincenty_error).unwrap()
            })
        }

        (GeodesicLengthMethod::Geodesic, GeoArrowType::Polygon) => {
            map_polygon_series_to_float_series(series, |p| p.exterior().geodesic_length())
        }
        (GeodesicLengthMethod::Haversine, GeoArrowType::Polygon) => {
            map_polygon_series_to_float_series(series, |p| p.exterior().haversine_length())
        }
        (GeodesicLengthMethod::Vincenty, GeoArrowType::Polygon) => {
            map_polygon_series_to_float_series(series, |p| {
                p.exterior()
                    .vincenty_length()
                    .map_err(map_vincenty_error)
                    .unwrap()
            })
        }

        (_, GeoArrowType::WKB) => geodesic_length_wkb(series, method),
    }
}

fn euclidean_length_wkb(series: &Series) -> Result<Series> {
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(series.len());

    for geom in iter_geom(series) {
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
            Geometry::GeometryCollection(_) => Err(PolarsError::ComputeError(ErrString::from(
                "Length methods are not implemented for geometry collection",
            ))),
            Geometry::Rect(rec) => Ok(rec.to_polygon().exterior().euclidean_length()),
            Geometry::Triangle(triangle) => Ok(triangle.to_polygon().exterior().euclidean_length()),
        }?;
        result.push(Some(length));
    }

    let result: PrimitiveArray<f64> = result.into();
    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}

fn length_geoarrow_point(series: &Series) -> Result<Series> {
    // Length of point geometries is always 0
    // TODO: correct validity
    let result: Vec<f64> = vec![0.0; series.len()];
    let series = Series::try_from((
        "geometry",
        Box::new(PrimitiveArray::from_vec(result)) as Box<dyn Array>,
    ))?;
    Ok(series)
}

fn geodesic_length_wkb(series: &Series, method: GeodesicLengthMethod) -> Result<Series> {
    let mut result = MutablePrimitiveArray::<f64>::with_capacity(series.len());

    let map_vincenty_error =
        |_| PolarsError::ComputeError(ErrString::from("Failed to calculate vincenty length"));

    for geom in iter_geom(series) {
        let length: f64 = match (&method, geom) {
            (_, Geometry::Point(_)) => Ok(0.0),

            (GeodesicLengthMethod::Haversine, Geometry::Line(line)) => Ok(line.haversine_length()),
            (GeodesicLengthMethod::Geodesic, Geometry::Line(line)) => Ok(line.geodesic_length()),
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
            (_, Geometry::GeometryCollection(_)) => Err(PolarsError::ComputeError(
                ErrString::from("Length methods are not implemented for geometry collection"),
            )),
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
    let series = Series::try_from(("result", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
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
