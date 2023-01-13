use crate::error::Result;
use crate::util::iter_geom;
use geo::algorithm::centroid::Centroid;
use geo::Geometry;
use geopolars_arrow::linestring::LineStringSeries;
use geopolars_arrow::point::MutablePointArray;
use geopolars_arrow::polygon::PolygonSeries;
use geopolars_arrow::util::{get_geoarrow_type, GeoArrowType};
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::Series;

pub(crate) fn centroid(series: &Series) -> Result<Series> {
    match get_geoarrow_type(series) {
        GeoArrowType::WKB => centroid_wkb(series),
        GeoArrowType::Point => Ok(series.clone()),
        GeoArrowType::LineString => centroid_geoarrow_linestring(series),
        GeoArrowType::Polygon => centroid_geoarrow_polygon(series),
    }
}

fn centroid_wkb(series: &Series) -> Result<Series> {
    let mut output_array = MutableBinaryArray::<i32>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let value: Geometry<f64> = geom.centroid().expect("could not create centroid").into();
        let wkb = value
            .to_wkb(CoordDimensions::xy())
            .expect("Unable to create wkb");

        output_array.push(Some(wkb));
    }

    let result: BinaryArray<i32> = output_array.into();

    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}

fn centroid_geoarrow_linestring(series: &Series) -> Result<Series> {
    let mut result = MutablePointArray::with_capacity(series.len());

    for chunk in LineStringSeries(series).chunks() {
        let parts = chunk.parts();
        for i in 0..parts.len() {
            let new_pt = parts.get_as_geo(i).and_then(|ls| ls.centroid());
            result.push(new_pt);
        }
    }

    let series = Series::try_from(("geometry", Box::new(result.into_arrow()) as Box<dyn Array>))?;
    Ok(series)
}

fn centroid_geoarrow_polygon(series: &Series) -> Result<Series> {
    let mut result = MutablePointArray::with_capacity(series.len());

    for chunk in PolygonSeries(series).chunks() {
        let parts = chunk.parts();
        for i in 0..parts.len() {
            let new_pt = parts.get_as_geo(i).and_then(|ls| ls.centroid());
            result.push(new_pt);
        }
    }

    let series = Series::try_from(("geometry", Box::new(result.into_arrow()) as Box<dyn Array>))?;
    Ok(series)
}
