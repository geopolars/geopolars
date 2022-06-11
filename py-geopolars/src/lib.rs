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

#[pyfunction]
fn euclidean_distance(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series
        .euclidean_distance()
        .map_err(|e| PyValueError::new_err(format!("Something went wrong: {:?}", e)))?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
fn geodesic_distance(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series
        .geodesic_distance()
        .map_err(|e| PyValueError::new_err(format!("Something went wrong: {:?}", e)))?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
fn haversine_distance(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series
        .haversine_distance()
        .map_err(|e| PyValueError::new_err(format!("Something went wrong: {:?}", e)))?;
    ffi::rust_series_to_py_series(&out)
}

#[pymodule]
fn geopolars(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(centroid)).unwrap();
    m.add_wrapped(wrap_pyfunction!(convex_hull)).unwrap();
    m.add_wrapped(wrap_pyfunction!(euclidean_distance).unwrap());
    m.add_wrapped(wrap_pyfunction!(geodesic_distance).unwrap());
    m.add_wrapped(wrap_pyfunction!(haversine_distance).unwrap());
    Ok(())
}
