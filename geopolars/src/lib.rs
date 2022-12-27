pub mod error;
pub mod geodataframe;
pub mod geoseries;
pub mod spatial_index;
pub mod util;
pub mod geoarrow;

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
