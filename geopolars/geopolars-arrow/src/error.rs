//! Defines [`Error`], representing all errors returned by this crate.
use std::fmt::Debug;
use thiserror::Error;

/// Enum with all errors in this crate.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum GeoArrowError {
    /// Returned when functionality is not yet available.
    #[error("Not yet implemented: {0}")]
    NotYetImplemented(String),

    #[error("General error: {0}")]
    General(String),

    /// Wrapper for an error triggered by a dependency
    #[error(transparent)]
    External(#[from] anyhow::Error),

    /// Whenever pushing to a container fails because it does not support more entries.
    /// The solution is usually to use a higher-capacity container-backing type.
    #[error("Overflow")]
    Overflow,
}
