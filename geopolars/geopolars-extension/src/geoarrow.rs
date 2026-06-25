//! Definitions of GeoArrow extension types for the Polars extension registry.
//!
//! Each of the types contained in this module is a thin wrapper around the corresponding type in
//! [`geoarrow_schema`] to implement Polars' [`ExtensionTypeImpl`].

use std::any::Any;
use std::borrow::Cow;
use std::hash::{BuildHasher, Hash};

use arrow_schema::extension::ExtensionType;
use polars::prelude::PlFixedStateQuality;
use polars_core::prelude::extension::ExtensionTypeImpl;

macro_rules! define_basic_type {
    (
        $(#[$($attrss:meta)*])*
        $struct_name:ident
    ) => {
        $(#[$($attrss)*])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub struct $struct_name(Result<geoarrow_schema::$struct_name, String>);

        impl $struct_name {
            pub fn new(inner: geoarrow_schema::$struct_name) -> Self {
                Self(Ok(inner))
            }

            pub fn invalid(error: impl std::fmt::Display) -> Self {
                Self(Err(error.to_string()))
            }

            fn format_inner(&self) -> String {
                match &self.0 {
                    Ok(inner) => format!("{:?}", inner),
                    Err(e) => format!("InvalidExtensionType({})", e),
                }
            }
        }

        impl ExtensionTypeImpl for $struct_name {
            fn name(&self) -> Cow<'_, str> {
                Cow::Borrowed(geoarrow_schema::$struct_name::NAME)
            }

            fn serialize_metadata(&self) -> Option<Cow<'_, str>> {
                self.0.as_ref().ok()?.serialize_metadata().map(Cow::Owned)
            }

            fn dyn_clone(&self) -> Box<dyn ExtensionTypeImpl> {
                Box::new(self.clone())
            }

            fn dyn_eq(&self, other: &dyn ExtensionTypeImpl) -> bool {
                let Some(other) = (other as &dyn Any).downcast_ref::<Self>() else {
                    return false;
                };

                self == other
            }

            fn dyn_hash(&self) -> u64 {
                PlFixedStateQuality::default().hash_one(self)
            }

            fn dyn_display(&self) -> Cow<'_, str> {
                Cow::Owned(self.format_inner())
            }

            fn dyn_debug(&self) -> Cow<'_, str> {
                Cow::Owned(self.format_inner())
            }
        }

        impl From<geoarrow_schema::$struct_name> for $struct_name {
            fn from(value: geoarrow_schema::$struct_name) -> Self {
                Self::new(value)
            }
        }

        impl TryFrom<$struct_name> for geoarrow_schema::$struct_name {
            type Error = String;

            fn try_from(value: $struct_name) -> Result<Self, Self::Error> {
                value.0
            }
        }
    }
}

define_basic_type!(
    /// A GeoPolars Point extension type.
    PointType
);
define_basic_type!(
    /// A GeoPolars LineString extension type.
    LineStringType
);
define_basic_type!(
    /// A GeoPolars Polygon extension type.
    PolygonType
);
define_basic_type!(
    /// A GeoPolars MultiPoint extension type.
    MultiPointType
);
define_basic_type!(
    /// A GeoPolars MultiLineString extension type.
    MultiLineStringType
);
define_basic_type!(
    /// A GeoPolars MultiPolygon extension type.
    MultiPolygonType
);
define_basic_type!(
    /// A GeoPolars GeometryCollection extension type.
    GeometryCollectionType
);
define_basic_type!(
    /// A GeoPolars Geometry extension type.
    GeometryType
);
define_basic_type!(
    /// A GeoPolars Box extension type.
    BoxType
);
define_basic_type!(
    /// A GeoPolars Wkb extension type.
    WkbType
);
define_basic_type!(
    /// A GeoPolars Wkt extension type.
    WktType
);
