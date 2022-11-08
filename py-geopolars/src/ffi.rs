use polars::export::arrow::array::Array;
use polars::export::arrow::ffi;
use polars::prelude::{ArrowField, Series};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{ffi::Py_uintptr_t, PyAny, PyObject, PyResult};

#[cfg(feature = "proj")]
use std::path::{Path, PathBuf};

pub type ArrayRef = Box<dyn Array>;

/// Take an arrow array from python and convert it to a rust arrow array.
/// This operation does not copy data.
fn array_to_rust(arrow_array: &PyAny) -> PyResult<ArrayRef> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(ffi::ArrowArray::empty());
    let schema = Box::new(ffi::ArrowSchema::empty());

    let array_ptr = &*array as *const ffi::ArrowArray;
    let schema_ptr = &*schema as *const ffi::ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    arrow_array.call_method1(
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    unsafe {
        let field = ffi::import_field_from_c(schema.as_ref()).unwrap();
        let array = ffi::import_array_from_c(*array, field.data_type).unwrap();
        Ok(array)
    }
}

/// Arrow array to Python.
pub(crate) fn to_py_array(py: Python, pyarrow: &PyModule, array: ArrayRef) -> PyResult<PyObject> {
    let schema = Box::new(ffi::export_field_to_c(&ArrowField::new(
        "",
        array.data_type().clone(),
        true,
    )));
    let array = Box::new(ffi::export_array_to_c(array));

    let schema_ptr: *const ffi::ArrowSchema = &*schema;
    let array_ptr: *const ffi::ArrowArray = &*array;

    let array = pyarrow.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    Ok(array.to_object(py))
}

pub fn py_series_to_rust_series(series: &PyAny) -> PyResult<Series> {
    // rechunk series so that they have a single arrow array
    let series = series.call_method0("rechunk")?;

    let name = series.getattr("name")?.extract::<String>()?;

    // retrieve pyarrow array
    let array = series.call_method0("to_arrow")?;

    // retrieve rust arrow array
    let array = array_to_rust(array)?;

    Series::try_from((name.as_str(), array)).map_err(|e| PyValueError::new_err(format!("{}", e)))
}

// Allow dead code because this function will be used later
#[allow(dead_code)]
pub fn rust_series_to_py_series(series: &Series) -> PyResult<PyObject> {
    // ensure we have a single chunk
    let series = series.rechunk();
    let array = series.to_arrow(0);

    // acquire the gil
    Python::with_gil(|py| {
        // import pyarrow
        let pyarrow = py.import("pyarrow")?;

        // pyarrow array
        let pyarrow_array = to_py_array(py, pyarrow, array)?;

        // import polars
        let polars = py.import("polars")?;
        let out = polars.call_method1("from_arrow", (pyarrow_array,))?;
        Ok(out.to_object(py))
    })
}

pub fn rust_series_to_py_geoseries(series: &Series) -> PyResult<PyObject> {
    // ensure we have a single chunk
    let series = series.rechunk();
    let array = series.to_arrow(0);

    // acquire the gil
    Python::with_gil(|py| {
        // import pyarrow
        let pyarrow = py.import("pyarrow")?;

        // pyarrow array
        let pyarrow_array = to_py_array(py, pyarrow, array)?;

        // import geopolars
        let geopolars = py.import("geopolars")?;
        let out = geopolars.call_method1("from_arrow", (pyarrow_array,))?;
        Ok(out.to_object(py))
    })
}

#[cfg(feature = "proj")]
pub fn proj_data_directory() -> PyResult<PathBuf> {
    use pyo3::exceptions::PyIOError;

    Python::with_gil(|py| {
        let geopolars = py.import("geopolars")?;
        let file_location: String = geopolars.getattr("__file__")?.extract()?;
        let mut pb = Path::new(&file_location)
            .parent()
            .ok_or_else(|| PyIOError::new_err("can not resolve bundled proj_data directory"))?
            .to_path_buf();
        pb.push("proj_data");
        Ok(pb)
    })
}
