use std::path::PathBuf;

use proj::ProjBuilder;

use crate::error::Result;

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
