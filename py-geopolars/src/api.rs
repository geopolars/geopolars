use crate::geoseries;
use pyo3::prelude::*;

#[pymodule]
pub fn geopolars(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(geoseries::affine_transform))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::area))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::centroid))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::convex_hull))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::envelope))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::euclidean_length))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::exterior))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::geodesic_length))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::geom_type))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::is_empty))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::is_ring))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::rotate))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::scale))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::skew))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::distance))?;
    // m.add_wrapped(wrap_pyfunction!(geoseries::to_crs))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::translate))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::x))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::y))?;
    Ok(())
}
