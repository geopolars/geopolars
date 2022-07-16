use crate::error::Result;
use crate::geoseries::GeoSeries;
use polars::prelude::{DataFrame, Series};

pub trait GeoDataFrame {
    fn centroid(&self) -> Result<Series>;
    fn convex_hull(&self) -> Result<Series>;
}

impl GeoDataFrame for DataFrame {
    fn centroid(&self) -> Result<Series> {
        let geom_column = self.column("geometry")?;
        geom_column.centroid()
    }

    fn convex_hull(&self) -> Result<Series> {
        let geom_column = self.column("geometry")?;
        geom_column.convex_hull()
    }
}
