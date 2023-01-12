pub mod error;
pub mod geodataframe;
pub mod geoseries;
pub mod ops;
pub mod spatial_index;
pub mod util;

pub use geopolars_arrow;
pub use geopolars_geo;
#[cfg(feature = "geos")]
pub use geopolars_geos;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
