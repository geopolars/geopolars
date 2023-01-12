use crate::error::Result;
use crate::util::{iter_geom, map_polygon_series_to_float_series};
use geo::prelude::Area;
use geopolars_arrow::util::{get_geoarrow_type, GeoArrowType};
use polars::prelude::Series;

pub(crate) fn area(series: &Series) -> Result<Series> {
    match get_geoarrow_type(series) {
        GeoArrowType::WKB => area_wkb(series),
        GeoArrowType::Polygon => map_polygon_series_to_float_series(series, |p| p.unsigned_area()),
        _ => panic!("Unexpected geometry type for operation area"),
    }
}

fn area_wkb(series: &Series) -> Result<Series> {
    let output_series: Series = iter_geom(series).map(|geom| geom.unsigned_area()).collect();

    Ok(output_series)
}
