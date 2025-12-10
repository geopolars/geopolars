use std::mem::transmute;
use std::sync::Arc;

pub fn polars_field_to_arrow(field: &polars_arrow::datatypes::Field) -> arrow_schema::FieldRef {
    let polars_ffi_schema = polars_arrow::ffi::export_field_to_c(field);

    unsafe {
        // Transmute polars-arrow FFI types to arrow-rs FFI types
        //
        // # Safety
        // Both arrow-rs and polars-arrow implement the Arrow C Data Interface
        let arrow_ffi_schema: arrow_schema::ffi::FFI_ArrowSchema = transmute(polars_ffi_schema);

        // TODO: implement error handling
        let arrow_field =
            arrow_schema::Field::try_from(&arrow_ffi_schema).expect("import field to arrow-rs");

        Arc::new(arrow_field)
    }
}

/// Convert a polars-arrow ArrayRef to an arrow-rs ArrayRef and FieldRef
///
/// We need to return both an [arrow_array::ArrayRef] and an [arrow_schema::FieldRef]
/// because arrow-rs' ArrayRef does not maintain extension information.
pub fn polars_array_to_arrow(
    array: polars_arrow::array::ArrayRef,
) -> (arrow_array::ArrayRef, arrow_schema::FieldRef) {
    let polars_field = polars_arrow::datatypes::Field::new("".into(), array.dtype().clone(), true);
    let polars_ffi_schema = polars_arrow::ffi::export_field_to_c(&polars_field);
    let polars_ffi_array = polars_arrow::ffi::export_array_to_c(array);

    unsafe {
        // Transmute polars-arrow FFI types to arrow-rs FFI types
        //
        // # Safety
        // Both arrow-rs and polars-arrow implement the Arrow C Data Interface
        let arrow_ffi_array: arrow_data::ffi::FFI_ArrowArray = transmute(polars_ffi_array);
        let arrow_ffi_schema: arrow_schema::ffi::FFI_ArrowSchema = transmute(polars_ffi_schema);

        // TODO: implement error handling
        let arrow_field =
            arrow_schema::Field::try_from(&arrow_ffi_schema).expect("import field to arrow-rs");

        // TODO: implement error handling
        let arrow_array_data = arrow_array::ffi::from_ffi_and_data_type(
            arrow_ffi_array,
            arrow_field.data_type().clone(),
        )
        .expect("import array to arrow-rs");

        (
            arrow_array::make_array(arrow_array_data),
            Arc::new(arrow_field),
        )
    }
}
