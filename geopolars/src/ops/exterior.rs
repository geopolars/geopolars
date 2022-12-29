use crate::error::Result;
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::Series;

use crate::util::{get_geoarrow_type, iter_geom, GeoArrowType};

pub(crate) fn exterior(series: &Series) -> Result<Series> {
    match get_geoarrow_type(series) {
        GeoArrowType::WKB => exterior_wkb(series),
        GeoArrowType::Polygon => exterior_geoarrow_polygon(series),
        _ => panic!("Unexpected geometry type for operation exterior"),
    }
}

fn exterior_wkb(series: &Series) -> Result<Series> {
    let mut output_array = MutableBinaryArray::<i32>::with_capacity(series.len());

    for geom in iter_geom(series) {
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

    Ok(Series::try_from((
        "geometry",
        Box::new(result) as Box<dyn Array>,
    ))?)
}

fn exterior_geoarrow_polygon(series: &Series) -> Result<Series> {
    todo!()
}
