use crate::error::PyGeopolarsError;
use geopolars::geoseries::TransformOrigin;
use pyo3::prelude::*;

#[derive(FromPyObject, Debug)]
pub(crate) enum PythonTransformOrigin {
    String(String),                // input is a string
    Tuple(f64, f64),               // input is a 2-tuple of floats
    Coordinate { x: f64, y: f64 }, // input is a dict of x and y
}

impl std::convert::TryFrom<PythonTransformOrigin> for TransformOrigin {
    type Error = PyGeopolarsError;

    fn try_from(value: PythonTransformOrigin) -> Result<Self, Self::Error> {
        let transform_origin = match value {
            PythonTransformOrigin::String(s) => match s.to_lowercase().as_str() {
                "centroid" => TransformOrigin::Centroid,
                "center" => TransformOrigin::Center,
                _ => return Err(PyGeopolarsError::Other("Invalid argument".to_string())),
            },
            PythonTransformOrigin::Coordinate { x, y } => TransformOrigin::Point((x, y).into()),
            PythonTransformOrigin::Tuple(x, y) => TransformOrigin::Point((x, y).into()),
        };
        Ok(transform_origin)
    }
}
