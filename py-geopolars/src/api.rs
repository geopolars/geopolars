use crate::geo;
#[cfg(feature = "proj")]
use crate::proj;
use pyo3::prelude::*;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pyfunction]
fn version() -> &'static str {
    VERSION
}

fn register_geo_module(py: Python<'_>, parent_module: &PyModule) -> PyResult<()> {
    let child_module = PyModule::new(py, "geo")?;

    child_module.add_wrapped(wrap_pyfunction!(geo::affine_transform))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::area))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::centroid))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::convex_hull))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::envelope))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::euclidean_length))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::exterior))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::geodesic_length))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::geom_type))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::is_empty))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::is_ring))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::rotate))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::scale))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::skew))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::distance))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::translate))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::x))?;
    child_module.add_wrapped(wrap_pyfunction!(geo::y))?;

    parent_module.add_submodule(child_module)?;
    Ok(())
}

fn register_proj_module(py: Python<'_>, parent_module: &PyModule) -> PyResult<()> {
    let child_module = PyModule::new(py, "proj")?;

    #[cfg(feature = "proj")]
    child_module.add_wrapped(wrap_pyfunction!(proj::to_crs))?;

    parent_module.add_submodule(child_module)?;
    Ok(())
}

fn register_geos_module(py: Python<'_>, parent_module: &PyModule) -> PyResult<()> {
    let child_module = PyModule::new(py, "geos")?;

    parent_module.add_submodule(child_module)?;
    Ok(())
}

#[pymodule]
pub fn _geopolars(_py: Python, m: &PyModule) -> PyResult<()> {
    register_geo_module(_py, m)?;

    register_proj_module(_py, m)?;

    register_geos_module(_py, m)?;

    m.add_wrapped(wrap_pyfunction!(version))?;
    Ok(())
}
