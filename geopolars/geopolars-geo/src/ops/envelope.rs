use crate::error::Result;
use crate::util::iter_geom;
use geo::algorithm::bounding_rect::BoundingRect;
use geo::Geometry;
use geopolars_arrow::linestring::LineStringSeries;
use geopolars_arrow::polygon::MutablePolygonArray;
use geopolars_arrow::util::{get_geoarrow_type, GeoArrowType};
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{Array, BinaryArray, MutableBinaryArray};
use polars::prelude::{ListChunked, Series};
use polars::series::IntoSeries;

pub(crate) fn envelope(series: &Series) -> Result<Series> {
    match get_geoarrow_type(series) {
        GeoArrowType::WKB => envelope_wkb(series),
        GeoArrowType::LineString => envelope_geoarrow_linestring(series),
        GeoArrowType::Polygon => todo!(),
        _ => panic!("envelope not supported for this geometry type"),
    }
}

fn envelope_wkb(series: &Series) -> Result<Series> {
    let mut output_array = MutableBinaryArray::<i32>::with_capacity(series.len());

    for geom in iter_geom(series) {
        let value: Geometry<f64> = geom.bounding_rect().unwrap().into();
        let wkb = value
            .to_wkb(CoordDimensions::xy())
            .expect("Unable to create wkb");

        output_array.push(Some(wkb));
    }

    let result: BinaryArray<i32> = output_array.into();

    let series = Series::try_from(("geometry", Box::new(result) as Box<dyn Array>))?;
    Ok(series)
}

fn envelope_geoarrow_linestring(series: &Series) -> Result<Series> {
    let mut output_chunks: Vec<Box<dyn Array>> = vec![];
    for chunk in LineStringSeries(series).chunks() {
        let parts = chunk.parts();

        // Each envelope has 5 coordinates (closed box)
        let mut output_x: Vec<f64> = Vec::with_capacity(series.len() * 5);
        let mut output_y: Vec<f64> = Vec::with_capacity(series.len() * 5);

        // Indexes from each geometry into the ring array.
        // Because every geometry has only exterior rings, this is a monotonically increasing array
        // with step 1
        let geom_offsets: Vec<i64> = (0_i64..(series.len() + 1) as i64).collect();

        // Indexes from each ring into the coordinates array.
        // Because every geometry has exactly 5 coordinates, this is a monotonically increasing array
        // with step 5
        let ring_offsets: Vec<i64> = (0_i64..((series.len() + 1) * 5) as i64)
            .step_by(5)
            .collect();
        println!("ring_offsets: {:?}", ring_offsets);

        let validity = parts.validity.cloned().map(|b| b.make_mut());

        parts.iter_geo().for_each(|g| {
            let out = g.and_then(|g| g.bounding_rect());
            // TODO: check this is the winding order we want
            if let Some(out) = out {
                output_x.push(out.min().x);
                output_y.push(out.min().y);

                output_x.push(out.min().x);
                output_y.push(out.max().y);

                output_x.push(out.max().x);
                output_y.push(out.max().y);

                output_x.push(out.max().x);
                output_y.push(out.min().y);

                output_x.push(out.min().x);
                output_y.push(out.min().y);
            } else {
                // TODO: correct validity for valid input geometry with null envelope
                (0..5).for_each(|_| {
                    output_x.push(0.);
                    output_y.push(0.)
                });
            }
        });

        let mut_arr = MutablePolygonArray {
            x: output_x,
            y: output_y,
            ring_offsets,
            geom_offsets,
            validity,
        };
        println!("MutablePolygonArray {:?}", mut_arr);
        output_chunks.push(Box::new(mut_arr.into_arrow()) as Box<dyn Array>);
    }

    Ok(ListChunked::from_chunks("result", output_chunks).into_series())
}

#[cfg(test)]
mod tests {
    use geo::{coord, line_string, Rect};
    use geopolars_arrow::linestring::MutableLineStringArray;
    use geopolars_arrow::polygon::PolygonSeries;
    use polars::export::arrow::array::{Array, ListArray};
    use polars::series::Series;

    use crate::geoseries::GeoSeries;

    #[test]
    fn linestring_test() {
        let line_strings = vec![line_string![
            (x: 1., y: 1.),
            (x: 2., y: -2.),
            (x: -3., y: -3.),
            (x: -4., y: 4.)
        ]];
        let mut_line_string_arr: MutableLineStringArray = line_strings.into();
        let line_string_arr: ListArray<i64> = mut_line_string_arr.into();
        let series =
            Series::try_from(("geometry", Box::new(line_string_arr) as Box<dyn Array>)).unwrap();

        let actual = series.envelope().unwrap();
        let actual_geo = PolygonSeries(&actual).get_as_geo(0).unwrap();

        let expected = Rect::new(coord! { x: -4., y: -3. }, coord! { x: 2., y: 4. }).to_polygon();
        assert_eq!(actual_geo, expected);
    }
}
