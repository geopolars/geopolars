use crate::geoseries::GeoSeries;
use polars::prelude::{DataFrame, Result, Series};

pub trait GeoDataFrame {
    fn centroid(&self) -> Result<Series>;
}

impl GeoDataFrame for DataFrame {
    fn centroid(&self) -> Result<Series> {
        let geom_column = self.column("geometry")?;
        geom_column.centroid()
    }
}
