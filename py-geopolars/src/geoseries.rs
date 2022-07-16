use crate::error::PyGeopolarsError;
use crate::ffi;
use geopolars::geoseries::{GeoSeries, GeodesicLengthMethod};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[pyfunction]
pub(crate) fn centroid(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.centroid().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn convex_hull(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.convex_hull().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn euclidean_length(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.euclidean_length().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction(args = "(method=\"geodesic\")")]
pub(crate) fn geodesic_length(series: &PyAny, method: &str) -> PyResult<PyObject> {
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
pub(crate) fn to_crs(series: &PyAny, from: &str, to: &str) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.to_crs(from, to).map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}
