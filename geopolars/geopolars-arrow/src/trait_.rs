use crate::enum_::GeometryType;
use polars::export::arrow::bitmap::Bitmap;
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
