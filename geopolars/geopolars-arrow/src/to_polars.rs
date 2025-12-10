use std::mem::transmute;

/// Convert a GeoArrow array to a polars-arrow ArrayRef
pub fn geoarrow_array_to_polars(
    array: &dyn geoarrow_array::GeoArrowArray,
) -> polars_arrow::array::ArrayRef {
    let field = array.data_type().to_field("", true);
    let array = array.to_array_ref();
    arrow_array_to_polars(&array, &field)
}

/// Convert an arrow-rs Array to a polars-arrow ArrayRef
///
/// It is possible to implement safe conversion between arrow-rs and polars-arrow by manually
/// implementing the conversion for each array type. See
/// https://github.com/jorgecarleitao/arrow2/pull/1446. However, that is a lot more code than going
/// through FFI. So for now, I'll choose the "unsafe" route, assuming that both arrow-rs and
/// polars-arrow have correctly implemented the Arrow FFI interfaces.
pub fn arrow_array_to_polars(
    array: &dyn arrow_array::Array,
    field: &arrow_schema::Field,
) -> polars_arrow::array::ArrayRef {
    let data = array.to_data();
    let ffi_array = arrow_data::ffi::FFI_ArrowArray::new(&data);
    // TODO: implement error handling
    let ffi_schema =
        arrow_schema::ffi::FFI_ArrowSchema::try_from(field).expect("Field to be valid");

    unsafe {
        // Transmute arrow-rs FFI types to polars-arrow FFI types
        //
        // # Safety
        // Both arrow-rs and polars-arrow implement the Arrow C Data Interface
        let polars_ffi_array: polars_arrow::ffi::ArrowArray = transmute(ffi_array);
        let polars_ffi_schema: polars_arrow::ffi::ArrowSchema = transmute(ffi_schema);

        // TODO: implement error handling
        let polars_field = polars_arrow::ffi::import_field_from_c(&polars_ffi_schema)
            .expect("import field to polars");

        // TODO: implement error handling
        polars_arrow::ffi::import_array_from_c(polars_ffi_array, polars_field.dtype)
            .expect("import array to polars")
    }
}

pub fn geoarrow_type_to_polars(
    datatype: &geoarrow_schema::GeoArrowType,
) -> polars_arrow::datatypes::Field {
    let field = datatype.to_field("", true);
    arrow_field_to_polars(&field)
}

pub fn arrow_field_to_polars(field: &arrow_schema::Field) -> polars_arrow::datatypes::Field {
    // TODO: implement error handling
    let ffi_schema =
        arrow_schema::ffi::FFI_ArrowSchema::try_from(field).expect("Field to be valid");

    unsafe {
        // Transmute arrow-rs FFI types to polars-arrow FFI types
        //
        // # Safety
        // Both arrow-rs and polars-arrow implement the Arrow C Data Interface
        let polars_ffi_schema: polars_arrow::ffi::ArrowSchema = transmute(ffi_schema);

        // TODO: implement error handling
        polars_arrow::ffi::import_field_from_c(&polars_ffi_schema).expect("import field to polars")
    }
}
