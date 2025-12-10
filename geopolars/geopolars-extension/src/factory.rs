use polars::prelude::DataType;
use polars::prelude::extension::{ExtensionTypeFactory, ExtensionTypeImpl};

pub struct GeoArrowExtensionTypeFactory;

impl ExtensionTypeFactory for GeoArrowExtensionTypeFactory {
    fn create_type_instance(
        &self,
        name: &str,
        storage: &DataType,
        metadata: Option<&str>,
    ) -> Box<dyn ExtensionTypeImpl> {
        todo!()
    }
}
