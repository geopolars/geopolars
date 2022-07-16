mod error;
mod ffi;

use crate::error::PyGeopolarsError;
use geopolars::geoseries::{GeoSeries, GeodesicLengthMethod};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
fn centroid(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.centroid().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
fn convex_hull(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.convex_hull().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
fn euclidean_length(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.euclidean_length().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction(args = "(method=\"geodesic\")")]
fn geodesic_length(series: &PyAny, method: &str) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;

    let rust_method: GeodesicLengthMethod = match method {
        "geodesic" => Ok(GeodesicLengthMethod::Geodesic),
        "haversine" => Ok(GeodesicLengthMethod::Haversine),
        "vincenty" => Ok(GeodesicLengthMethod::Vincenty),
        _ => Err(PyValueError::new_err(
            "Geodesic calculation method not valid. Use one of geodesic, haversine or vincenty",
        )),
    }?;

    let out = series
        .geodesic_length(rust_method)
        .map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
fn to_crs(series: &PyAny, from: &str, to: &str) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.to_crs(from, to).map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pymodule]
fn geopolars(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(centroid))?;
    m.add_wrapped(wrap_pyfunction!(convex_hull))?;
    m.add_wrapped(wrap_pyfunction!(euclidean_length))?;
    m.add_wrapped(wrap_pyfunction!(geodesic_length))?;
    m.add_wrapped(wrap_pyfunction!(to_crs))?;
    Ok(())
}
