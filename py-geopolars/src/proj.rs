use crate::error::PyGeopolarsError;
use crate::ffi;
use geopolars::geopolars_geo::geoseries::GeoSeries;
use pyo3::prelude::*;
use std::path::PathBuf;

use geopolars::geopolars_geo::ops::proj::ProjOptions;

#[pyfunction]
pub(crate) fn to_crs(
    series: &PyAny,
    from: &str,
    to: &str,
    proj_data_dir: PathBuf,
) -> PyResult<PyObject> {
    let proj_options = ProjOptions {
        search_paths: Some(vec![proj_data_dir]),
    };

    let series = ffi::py_series_to_rust_series(series)?;

    let out = series
        .to_crs_with_options(from, to, proj_options)
        .map_err(PyGeopolarsError::from)?;
    ffi::rust_series_to_py_geoseries(&out)
}
