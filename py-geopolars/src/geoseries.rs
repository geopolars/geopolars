use crate::error::PyGeopolarsError;
use crate::ffi;
use crate::utils::PythonTransformOrigin;
use geo::AffineTransform;
use geopolars::geoseries::{GeoSeries, GeodesicLengthMethod};
use pyo3::prelude::*;

/// Apply an affine transform to the geoseries and return a geoseries of the tranformed geometries;
#[pyfunction]
pub(crate) fn affine_transform(series: &PyAny, transform: [f64; 6]) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let transform = AffineTransform::try_from(transform)?;
    let out = series
        .affine_transform(transform)
        .map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn area(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.area().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

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
pub(crate) fn envelope(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.envelope().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn euclidean_length(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.euclidean_length().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
pub(crate) fn exterior(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.exterior().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn geodesic_length(series: &PyAny, method: &str) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;

    let rust_method: GeodesicLengthMethod = match method.to_lowercase().as_str() {
        "geodesic" => Ok(GeodesicLengthMethod::Geodesic),
        "haversine" => Ok(GeodesicLengthMethod::Haversine),
        "vincenty" => Ok(GeodesicLengthMethod::Vincenty),
        _ => Err(PyGeopolarsError::Other(
            "Geodesic calculation method not valid. Use one of geodesic, haversine or vincenty"
                .to_string(),
        )),
    }?;

    let out = series
        .geodesic_length(rust_method)
        .map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
pub(crate) fn geom_type(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.geom_type().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
pub(crate) fn is_empty(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.is_empty().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
pub(crate) fn is_ring(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.is_ring().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

// TODO: implement API for TransformOrigin
#[pyfunction]
pub(crate) fn rotate(
    series: &PyAny,
    angle: f64,
    origin: PythonTransformOrigin,
) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series
        .rotate(angle, origin.try_into()?)
        .map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn scale(
    series: &PyAny,
    xfact: f64,
    yfact: f64,
    origin: PythonTransformOrigin,
) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series
        .scale(xfact, yfact, origin.try_into()?)
        .map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn skew(
    series: &PyAny,
    xs: f64,
    ys: f64,
    origin: PythonTransformOrigin,
) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series
        .skew(xs, ys, origin.try_into()?)
        .map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn distance(series: &PyAny, other: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let other = ffi::py_series_to_rust_series(other)?;
    let out = series.distance(&other).map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

// #[pyfunction]
// pub(crate) fn to_crs(series: &PyAny, from: &str, to: &str) -> PyResult<PyObject> {
//     let series = ffi::py_series_to_rust_series(series)?;
//     let out = series.to_crs(from, to).map_err(PyGeopolarsError::from)?;
//     ffi::rust_series_to_py_series(&out)
// }

#[pyfunction]
pub(crate) fn translate(series: &PyAny, x: f64, y: f64) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.translate(x, y).map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}

#[pyfunction]
pub(crate) fn x(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.x().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}

#[pyfunction]
pub(crate) fn y(series: &PyAny) -> PyResult<PyObject> {
    let series = ffi::py_series_to_rust_series(series)?;
    let out = series.y().map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_series(&out)
}
