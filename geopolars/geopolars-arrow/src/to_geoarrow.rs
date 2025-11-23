use std::sync::Arc;

use geoarrow_array::{GeoArrowArray, WrapArray};
use geoarrow_schema::GeoArrowType;

use crate::to_arrow::polars_to_arrow;

/// Convert a polars-arrow Array to a GeoArrow array
pub fn polars_to_geoarrow(array: polars_arrow::array::ArrayRef) -> Arc<dyn GeoArrowArray> {
    let (arrow_array, arrow_field) = polars_to_arrow(array);

    let geoarrow_data_type =
        GeoArrowType::from_arrow_field(&arrow_field).expect("import to geoarrow");
    geoarrow_data_type
        .wrap_array(&arrow_array)
        .expect("import to geoarrow")
}
