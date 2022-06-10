mod ffi;

use geopolars::geoseries::GeoSeries;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
fn centroid(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series
        .centroid()
        .map_err(|e| PyValueError::new_err(format!("Something went wrong: {:?}", e)))?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
fn convex_hull(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series
        .convex_hull()
        .map_err(|e| PyValueError::new_err(format!("Something went wrong: {:?}", e)))?;
    ffi::rust_series_to_py_series(&out)
}

#[pymodule]
fn geopolars(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(centroid)).unwrap();
    m.add_wrapped(wrap_pyfunction!(convex_hull)).unwrap();
    Ok(())
}
