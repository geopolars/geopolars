use arrow2::bitmap::{Bitmap, MutableBitmap};
use rstar::{RTree, RTreeObject};
use std::any::Any;

pub trait GeometryArrayTrait<'a> {
    type Scalar: RTreeObject;
    type ScalarGeo: From<Self::Scalar>;
    type ArrowArray;

    fn value(&'a self, i: usize) -> Self::Scalar;

    fn get(&'a self, i: usize) -> Option<Self::Scalar> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value(i))
    }

    fn value_as_geo(&'a self, i: usize) -> Self::ScalarGeo {
        self.value(i).into()
    }

    /// Gets the value at slot `i` as a geo object, additionally checking the validity bitmap
    fn get_as_geo(&'a self, i: usize) -> Option<Self::ScalarGeo> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    fn into_arrow(self) -> Self::ArrowArray;

    /// Build a spatial index containing this array's geometries
    fn rstar_tree(&'a self) -> RTree<Self::Scalar>;

    /// The length of the [`GeometryArray`]. Every array has a length corresponding to the number of
    /// elements (slots).
    fn len(&self) -> usize;

    /// whether the array is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The validity of the [`GeometryArray`]: every array has an optional [`Bitmap`] that, when available
    /// specifies whether the array slot is valid or not (null).
    /// When the validity is [`None`], all slots are valid.
    fn validity(&self) -> Option<&Bitmap>;

    /// The number of null slots on this [`GeometryArray`].
    /// # Implementation
    /// This is `O(1)` since the number of null elements is pre-computed.
    #[inline]
    fn null_count(&self) -> usize {
        self.validity()
            .as_ref()
            .map(|x| x.unset_bits())
            .unwrap_or(0)
    }

    /// Returns whether slot `i` is null.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_null(&self, i: usize) -> bool {
        self.validity()
            .as_ref()
            .map(|x| !x.get_bit(i))
            .unwrap_or(false)
    }

    /// Returns whether slot `i` is valid.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    /// Slices the [`GeometryArray`], returning a new `Box<dyn GeometryArray>`.
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Panic
    /// This function panics iff `offset + length > self.len()`.
    fn slice(&self, offset: usize, length: usize) -> Self;

    /// Slices the [`GeometryArray`], returning a new `Box<dyn GeometryArray>`.
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self;

    // /// Clones this [`GeometryArray`] with a new new assigned bitmap.
    // /// # Panic
    // /// This function panics iff `validity.len() != self.len()`.
    // fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn GeometryArray>;

    /// Clone a `&dyn GeometryArray` to an owned `Box<dyn GeometryArray>`.
    fn to_boxed(&self) -> Box<Self>;
}

/// A trait describing a mutable geometry array; i.e. an array whose values can be changed.
/// Mutable arrays cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`GeometryArray`], concrete arrays (such as [`MutablePointArray`]) implement how they are mutated.
pub trait MutableGeometryArray: std::fmt::Debug + Send + Sync {
    /// The length of the array.
    fn len(&self) -> usize;

    /// Whether the array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The optional validity of the array.
    fn validity(&self) -> Option<&MutableBitmap>;

    // /// Convert itself to an (immutable) [`GeometryArray`].
    // fn as_box(&mut self) -> Box<GeometryArrayTrait>;

    // /// Convert itself to an (immutable) atomically reference counted [`GeometryArray`].
    // // This provided implementation has an extra allocation as it first
    // // boxes `self`, then converts the box into an `Arc`. Implementors may wish
    // // to avoid an allocation by skipping the box completely.
    // fn as_arc(&mut self) -> std::sync::Arc<GeometryArrayTrait> {
    //     self.as_box().into()
    // }

    /// Convert to `Any`, to enable dynamic casting.
    fn as_any(&self) -> &dyn Any;

    /// Convert to mutable `Any`, to enable dynamic casting.
    fn as_mut_any(&mut self) -> &mut dyn Any;

    // /// Adds a new null element to the array.
    // fn push_null(&mut self);

    /// Whether `index` is valid / set.
    /// # Panic
    /// Panics if `index >= self.len()`.
    #[inline]
    fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .as_ref()
            .map(|x| x.get(index))
            .unwrap_or(true)
    }

    // /// Reserves additional slots to its capacity.
    // fn reserve(&mut self, additional: usize);

    // /// Shrink the array to fit its length.
    // fn shrink_to_fit(&mut self);
}
