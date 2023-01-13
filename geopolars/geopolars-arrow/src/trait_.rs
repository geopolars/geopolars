use crate::enum_::GeometryType;
use polars::export::arrow::bitmap::{Bitmap, MutableBitmap};
use std::any::Any;

/// A trait representing an immutable Arrow geometry array. Arrow arrays are trait objects
/// that are infallibly downcasted to concrete types according to the [`GeometryArray::data_type`].
pub trait GeometryArray: Send + Sync + dyn_clone::DynClone + 'static {
    /// Converts itself to a reference of [`Any`], which enables downcasting to concrete types.
    fn as_any(&self) -> &dyn Any;

    /// Converts itself to a mutable reference of [`Any`], which enables mutable downcasting to concrete types.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// The length of the [`GeometryArray`]. Every array has a length corresponding to the number of
    /// elements (slots).
    fn len(&self) -> usize;

    /// whether the array is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The [`GeometryType`] of the [`GeometryArray`]. In combination with
    /// [`GeometryArray::as_any`], this can be used to downcast trait objects (`dyn GeometryArray`)
    /// to concrete arrays.
    fn geometry_type(&self) -> GeometryType;

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
    fn slice(&self, offset: usize, length: usize) -> Box<dyn GeometryArray>;

    /// Slices the [`GeometryArray`], returning a new `Box<dyn GeometryArray>`.
    /// # Implementation
    /// This operation is `O(1)` over `len`, as it amounts to increase two ref counts
    /// and moving the struct to the heap.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`
    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn GeometryArray>;

    // /// Clones this [`GeometryArray`] with a new new assigned bitmap.
    // /// # Panic
    // /// This function panics iff `validity.len() != self.len()`.
    // fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn GeometryArray>;

    /// Clone a `&dyn GeometryArray` to an owned `Box<dyn GeometryArray>`.
    fn to_boxed(&self) -> Box<dyn GeometryArray>;
}

dyn_clone::clone_trait_object!(GeometryArray);

/// A trait describing a mutable geometry array; i.e. an array whose values can be changed.
/// Mutable arrays cannot be cloned but can be mutated in place,
/// thereby making them useful to perform numeric operations without allocations.
/// As in [`GeometryArray`], concrete arrays (such as [`MutablePointArray`]) implement how they are mutated.
pub trait MutableGeometryArray: std::fmt::Debug + Send + Sync {
    /// The [`GeometryType`] of the array.
    fn geometry_type(&self) -> GeometryType;

    /// The length of the array.
    fn len(&self) -> usize;

    /// Whether the array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The optional validity of the array.
    fn validity(&self) -> Option<&MutableBitmap>;

    // /// Convert itself to an (immutable) [`GeometryArray`].
    // fn as_box(&mut self) -> Box<dyn GeometryArray>;

    // /// Convert itself to an (immutable) atomically reference counted [`GeometryArray`].
    // // This provided implementation has an extra allocation as it first
    // // boxes `self`, then converts the box into an `Arc`. Implementors may wish
    // // to avoid an allocation by skipping the box completely.
    // fn as_arc(&mut self) -> std::sync::Arc<dyn GeometryArray> {
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
