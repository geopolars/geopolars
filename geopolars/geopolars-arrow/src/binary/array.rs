use crate::error::GeoArrowError;
use crate::{GeometryArrayTrait, MutableWKBArray, WKB};
use arrow2::array::{Array, BinaryArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use rstar::RTree;

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

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn with_validity(&self, validity: Option<Bitmap>) -> Self {
        WKBArray::new(self.0.clone().with_validity(validity))
    }
}

impl<'a> GeometryArrayTrait<'a> for WKBArray {
    type Scalar = WKB<'a>;
    type ScalarGeo = geo::Geometry;
    type ArrowArray = BinaryArray<i64>;

    fn value(&'a self, i: usize) -> Self::Scalar {
        crate::WKB {
            arr: &self.0,
            geom_index: i,
        }
    }

    fn into_arrow(self) -> BinaryArray<i64> {
        self.0
    }

    /// Build a spatial index containing this array's geometries
    fn rstar_tree(&'a self) -> RTree<Self::Scalar> {
        let mut tree = RTree::new();
        self.iter().flatten().for_each(|geom| tree.insert(geom));
        tree
    }

    /// Returns the number of geometries in this array
    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns the optional validity.
    fn validity(&self) -> Option<&Bitmap> {
        self.0.validity()
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Examples
    /// ```
    /// use arrow2::array::PrimitiveArray;
    ///
    /// let array = PrimitiveArray::from_vec(vec![1, 2, 3]);
    /// assert_eq!(format!("{:?}", array), "Int32[1, 2, 3]");
    /// let sliced = array.slice(1, 1);
    /// assert_eq!(format!("{:?}", sliced), "Int32[2]");
    /// // note: `sliced` and `array` share the same memory region.
    /// ```
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    #[inline]
    #[must_use]
    fn slice(&self, offset: usize, length: usize) -> Self {
        WKBArray(self.0.slice(offset, length))
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    #[must_use]
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        WKBArray(self.0.slice_unchecked(offset, length))
    }

    fn to_boxed(&self) -> Box<Self> {
        Box::new(self.clone())
    }
}

impl WKBArray {
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
    pub fn iter_geo_values(&self) -> impl Iterator<Item = geo::Geometry> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<geo::Geometry, impl Iterator<Item = geo::Geometry> + '_, BitmapIter> {
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

impl From<Vec<Option<geo::Geometry>>> for WKBArray {
    fn from(other: Vec<Option<geo::Geometry>>) -> Self {
        let mut_arr: MutableWKBArray = other.into();
        mut_arr.into()
    }
}
