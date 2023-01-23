use crate::error::Result;
use geo::Geometry;
use geoarrow::{GeometryArray, WKBArray};

pub(crate) fn explode(array: GeometryArray) -> Result<GeometryArray> {
    match array {
        GeometryArray::WKB(arr) => Ok(GeometryArray::WKB(explode_wkb(arr)?)),
        GeometryArray::Point(arr) => Ok(GeometryArray::Point(arr)),
        GeometryArray::LineString(arr) => Ok(GeometryArray::LineString(arr)),
        GeometryArray::Polygon(arr) => Ok(GeometryArray::Polygon(arr)),
        _ => todo!(),
    }
}

fn explode_wkb(array: WKBArray) -> Result<WKBArray> {
    let mut exploded_vector = Vec::new();

    for geometry in array.iter_geo().flatten() {
        match geometry {
            Geometry::Point(geometry) => {
                let point = Geometry::Point(geometry);
                exploded_vector.push(Some(point))
            }
            Geometry::MultiPoint(geometry) => {
                for geom in geometry.into_iter() {
                    let point = Geometry::Point(geom);
                    exploded_vector.push(Some(point))
                }
            }
            Geometry::Line(geometry) => {
                let line = Geometry::Line(geometry);
                exploded_vector.push(Some(line))
            }
            Geometry::LineString(geometry) => {
                let line_string = Geometry::LineString(geometry);
                exploded_vector.push(Some(line_string))
            }
            Geometry::MultiLineString(geometry) => {
                for geom in geometry.into_iter() {
                    let line_string = Geometry::LineString(geom);
                    exploded_vector.push(Some(line_string))
                }
            }
            Geometry::Polygon(geometry) => {
                let polygon = Geometry::Polygon(geometry);
                exploded_vector.push(Some(polygon))
            }
            Geometry::MultiPolygon(geometry) => {
                for geom in geometry.into_iter() {
                    let polygon = Geometry::Polygon(geom);
                    exploded_vector.push(Some(polygon))
                }
            }
            Geometry::Rect(geometry) => {
                let rectangle = Geometry::Rect(geometry);
                exploded_vector.push(Some(rectangle))
            }
            Geometry::Triangle(geometry) => {
                let triangle = Geometry::Triangle(geometry);
                exploded_vector.push(Some(triangle))
            }
            _ => unimplemented!(),
        };
    }

    Ok(exploded_vector.into())
}

#[cfg(test)]
mod tests {
    use crate::geoseries::GeoSeries;
    use crate::util::from_geom_vec;
    use geo::{Geometry, MultiPoint, Point};

    #[test]
    fn explode() {
        let point_0 = Point::new(0., 0.);
        let point_1 = Point::new(1., 1.);
        let point_2 = Point::new(2., 2.);
        let point_3 = Point::new(3., 3.);
        let point_4 = Point::new(4., 4.);

        let expected_series = from_geom_vec(&[
            Geometry::Point(point_0),
            Geometry::Point(point_1),
            Geometry::Point(point_2),
            Geometry::Point(point_3),
            Geometry::Point(point_4),
        ])
        .unwrap();

        let multipoint_0 = MultiPoint::new(vec![point_0, point_1]);
        let multipoint_1 = MultiPoint::new(vec![point_2, point_3, point_4]);

        let input_series = from_geom_vec(&[
            Geometry::MultiPoint(multipoint_0),
            Geometry::MultiPoint(multipoint_1),
        ])
        .unwrap();

        let output_series = GeoSeries::explode(&input_series).unwrap();

        assert_eq!(output_series, expected_series);
    }
}
