use crate::error::Result;
use crate::geoarrow::polygon::array::{PolygonArrayParts, PolygonSeries};
use crate::util::{get_geoarrow_type, iter_geom, GeoArrowType};
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use polars::export::arrow::array::{
    Array, BinaryArray, MutableBinaryArray, MutablePrimitiveArray, PrimitiveArray,
};
use polars::prelude::Series;

// pub(crate) fn exterior(series: &Series) -> Result<Series> {
//     match get_geoarrow_type(series) {
//         GeoArrowType::WKB => exterior_wkb(series),
//         GeoArrowType::Polygon => exterior_geoarrow_polygon(series),
//         _ => panic!("Unexpected geometry type for operation exterior"),
//     }
// }

pub(crate) fn exterior(series: &Series) -> Result<Series> {
    exterior_wkb(series)
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

// fn exterior_geoarrow_polygon(series: &Series) -> Result<Series> {
//     let ps = PolygonSeries(series);
//     let chunks: Vec<PolygonArrayParts> = ps.chunks().iter().map(|chunk| chunk.parts()).collect();

//     let (coord_length, offsets_length) = get_polygon_output_lengths(chunks);

//     let offsets_buffer = vec![0_i64; offsets_length];

//     let x_coord_buffer = MutablePrimitiveArray::<f64>::with_capacity(coord_length);
//     x_coord_buffer.s
//     // let x_coord_buffer = Vec::<f64>::with_capacity(coord_length);
//     let y_coord_buffer = Vec::<f64>::with_capacity(coord_length);

//     for chunk in chunks {
//         for geom_offset in chunk.geom_offsets.as_slice() {
//             let (ext_ring_start, ext_ring_end) = chunk.ring_offsets.start_end(*geom_offset as usize);
//             let x_ext = chunk.x.slice(ext_ring_start, ext_ring_end - ext_ring_start);
//             let y_ext = chunk.y.slice(ext_ring_start, ext_ring_end - ext_ring_start);

//             // TODO: copy these slices into the x_coord_buffer,
//             // Update offsets buffer
//         }
//         chunk.x.slice(0, 6).set_values(values)
//     }

//     todo!()
// }

fn get_polygon_output_lengths(chunks: Vec<PolygonArrayParts>) -> (usize, usize) {
    // The length of the coordinates buffer
    let mut coord_length: usize = 0;

    // The length of the output LineString's offsets buffer
    // This should be equal to the length of the input Polygon's geom_offsets buffer, since every
    // polygon has one and only one exterior.
    let mut offsets_length: usize = 0;

    for chunk in chunks {
        offsets_length += chunk.geom_offsets.len();

        // Only care about the first geom_offset since we only care about the exterior ring
        for geom_offset in chunk.geom_offsets.as_slice() {
            let (ext_ring_start, ext_ring_end) =
                chunk.ring_offsets.start_end(*geom_offset as usize);
            coord_length += ext_ring_end - ext_ring_start;
        }
    }

    (coord_length, offsets_length)
}
