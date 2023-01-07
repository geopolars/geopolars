use crate::error::Result;
use crate::util::from_geom_vec;
use crate::util::iter_geom;
use geo::Geometry;
use polars::prelude::Series;
use proj::Transform;
use std::path::PathBuf;

use proj::ProjBuilder;

/// Options to be passed to ProjBuilder
/// We use a custom ProjOptions struct instead of accepting ProjBuilder so that we can more easily
/// use multithreading in the future.
#[derive(Default, Clone)]
pub struct ProjOptions {
    /// Search paths to set through PROJ
    pub search_paths: Option<Vec<PathBuf>>,
}

impl ProjOptions {
    pub fn to_proj_builder(&self) -> Result<ProjBuilder> {
        let mut builder = ProjBuilder::new();

        if let Some(search_paths) = &self.search_paths {
            for search_path in search_paths {
                builder.set_search_paths(search_path)?;
            }
        }
        Ok(builder)
    }
}

pub(crate) fn to_crs(series: &Series, from: &str, to: &str) -> Result<Series> {
    to_crs_with_options(series, from, to, ProjOptions::default())
}

pub(crate) fn to_crs_with_options(
    series: &Series,
    from: &str,
    to: &str,
    proj_options: ProjOptions,
) -> Result<Series> {
    to_crs_with_options_wkb(series, from, to, proj_options)
}

fn to_crs_with_options_wkb(
    series: &Series,
    from: &str,
    to: &str,
    proj_options: ProjOptions,
) -> Result<Series> {
    let proj = proj_options
        .to_proj_builder()?
        .proj_known_crs(from, to, None)?;

    // Specify literal Result<> to propagate error from within closure
    // https://stackoverflow.com/a/26370894
    let output_vec: Result<Vec<Geometry>> = iter_geom(series)
        .map(|mut geom| {
            // geom.tranform modifies `geom` in place.
            // Note that this doesn't modify the _original series_ because iter_geom makes a
            // copy
            // https://docs.rs/proj/latest/proj/#integration-with-geo-types
            geom.transform(&proj)?;
            Ok(geom)
        })
        .collect();

    from_geom_vec(&output_vec?)
}
