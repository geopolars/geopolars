use crate::geoseries;
use pyo3::prelude::*;

#[pymodule]
pub fn geopolars(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(geoseries::centroid))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::convex_hull))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::euclidean_length))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::geodesic_length))?;
    m.add_wrapped(wrap_pyfunction!(geoseries::to_crs))?;
    Ok(())
}
