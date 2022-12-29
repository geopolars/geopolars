pub mod error;
pub mod geoarrow;
pub mod geodataframe;
pub mod geoseries;
pub mod ops;
pub mod spatial_index;
pub mod util;

#[cfg(feature = "proj")]
pub mod proj;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
