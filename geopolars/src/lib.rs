pub mod ops;
pub mod spatial_index;
pub mod util;

pub use geoarrow;
pub use geopolars_geo;
#[cfg(feature = "geos")]
pub use geopolars_geos;

pub use geopolars_geo::error;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
