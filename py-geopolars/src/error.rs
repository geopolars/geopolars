use geopolars::error::GeopolarsError;
use polars::export::arrow::error::Error as ArrowError;
use polars::prelude::PolarsError;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::{
    create_exception,
    exceptions::{PyException, PyRuntimeError},
    prelude::*,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PyGeopolarsError {
    #[error(transparent)]
    Polars(#[from] PolarsError),

    #[error("{0}")]
    Other(String),

    #[error(transparent)]
    Arrow(#[from] ArrowError),

    #[error(transparent)]
    GeopolarsError(#[from] GeopolarsError),
}

impl std::convert::From<PyGeopolarsError> for PyErr {
    fn from(err: PyGeopolarsError) -> PyErr {
        let default = || PyRuntimeError::new_err(format!("{:?}", &err));

        match &err {
            PyGeopolarsError::Polars(err) => match err {
                PolarsError::NotFound(name) => NotFoundError::new_err(name.to_string()),
                PolarsError::ComputeError(err) => ComputeError::new_err(err.to_string()),
                PolarsError::NoData(err) => NoDataError::new_err(err.to_string()),
                PolarsError::ShapeMisMatch(err) => ShapeError::new_err(err.to_string()),
                PolarsError::SchemaMisMatch(err) => SchemaError::new_err(err.to_string()),
                PolarsError::Io(err) => PyIOError::new_err(err.to_string()),
                PolarsError::InvalidOperation(err) => PyValueError::new_err(err.to_string()),
                PolarsError::ArrowError(err) => ArrowErrorException::new_err(format!("{:?}", err)),
                PolarsError::Duplicate(err) => DuplicateError::new_err(err.to_string()),
            },
            PyGeopolarsError::Arrow(err) => ArrowErrorException::new_err(format!("{:?}", err)),
            PyGeopolarsError::GeopolarsError(err) => {
                GeopolarsErrorException::new_err(format!("{:?}", err))
            }
            _ => default(),
        }
    }
}

// TODO: we can probably remove some of these that are taken from underlying polars?
create_exception!(exceptions, ArrowErrorException, PyException);
create_exception!(exceptions, ComputeError, PyException);
create_exception!(exceptions, DuplicateError, PyException);
create_exception!(exceptions, GeopolarsErrorException, PyException);
create_exception!(exceptions, NoDataError, PyException);
create_exception!(exceptions, NotFoundError, PyException);
create_exception!(exceptions, SchemaError, PyException);
create_exception!(exceptions, ShapeError, PyException);
