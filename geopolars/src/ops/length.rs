use crate::error::Result;
use geo::algorithm::euclidean_length::EuclideanLength;
use geo::algorithm::geodesic_length::GeodesicLength;
use geo::algorithm::haversine_length::HaversineLength;
use geo::algorithm::vincenty_length::VincentyLength;
use geo::Geometry;
use polars::error::ErrString;
use polars::export::arrow::array::{Array, MutablePrimitiveArray, PrimitiveArray};
use polars::prelude::{PolarsError, Series};

use crate::util::iter_geom;

pub enum GeodesicLengthMethod {
    Haversine,
    Geodesic,
    Vincenty,
}

pub(crate) fn euclidean_length(series: &Series) -> Result<Series> {
    euclidean_length_wkb(series)
}

pub(crate) fn geodesic_length(series: &Series, method: GeodesicLengthMethod) -> Result<Series> {
    geodesic_length_wkb(series, method)
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
