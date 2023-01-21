use crate::enum_::GeometryType;
use crate::error::GeoArrowError;
use crate::trait_::GeometryArray;
use crate::MutableWKBArray;
use arrow2::array::{Array, BinaryArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use geo::Geometry;
use geozero::{wkb::Wkb, ToGeo};

/// A [`GeometryArray`] semantically equivalent to `Vec<Option<Geometry>>` using Arrow's
/// in-memory representation.
#[derive(Debug, Clone)]
pub struct WKBArray(BinaryArray<i64>);

// Implement geometry accessors
impl WKBArray {
    /// Create a new WKBArray from a BinaryArray
    pub fn new(arr: BinaryArray<i64>) -> Self {
        Self(arr)
    }

    /// Returns the number of geometries in this array
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the value at slot `i` as a geo object.
    pub fn value_as_geo(&self, i: usize) -> Geometry {
        let buf = self.0.value(i);
        Wkb(buf.to_vec())
            .to_geo()
            .expect("unable to convert to geo")
    }

    /// Gets the value at slot `i` as a geo object, additionally checking the validity bitmap
    pub fn get_as_geo(&self, i: usize) -> Option<Geometry> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        let buf = self.0.value(i);
        geos::Geometry::new_from_wkb(buf).expect("Unable to parse WKB")
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        let buf = self.0.value(i);
        Some(geos::Geometry::new_from_wkb(buf).expect("Unable to parse WKB"))
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<Geometry, impl Iterator<Item = Geometry> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.validity())
    }

    /// Iterator over GEOS geometry objects
    #[cfg(feature = "geos")]
    pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geos(i))
    }

    /// Iterator over GEOS geometry objects, taking validity into account
    #[cfg(feature = "geos")]
    pub fn iter_geos(
        &self,
    ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geos_values(), self.validity())
    }

    pub fn into_arrow(self) -> BinaryArray<i64> {
        self.0
    }
}

impl From<BinaryArray<i64>> for WKBArray {
    fn from(other: BinaryArray<i64>) -> Self {
        Self(other)
    }
}

impl TryFrom<Box<dyn Array>> for WKBArray {
    type Error = GeoArrowError;

    fn try_from(value: Box<dyn Array>) -> Result<Self, Self::Error> {
        let arr = value.as_any().downcast_ref::<BinaryArray<i64>>().unwrap();
        Ok(arr.clone().into())
    }
}

impl GeometryArray for WKBArray {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    fn geometry_type(&self) -> GeometryType {
        GeometryType::WKB
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.0.validity()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn GeometryArray> {
        Box::new(WKBArray::new(self.0.slice(offset, length)))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn GeometryArray> {
        Box::new(WKBArray::new(self.0.slice_unchecked(offset, length)))
    }

    // fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn GeometryArray> {
    //     Box::new(WKBArray::new(self.0.clone().with_validity(validity)))
    // }

    fn to_boxed(&self) -> Box<dyn GeometryArray> {
        Box::new(WKBArray::new(self.0.clone()))
    }
}

impl From<Vec<Option<Geometry>>> for WKBArray {
    fn from(other: Vec<Option<Geometry>>) -> Self {
        let mut_arr: MutableWKBArray = other.into();
        mut_arr.into()
    }
}
