use crate::enum_::GeometryType;
use crate::error::GeoArrowError;
use crate::trait_::GeometryArray;
use crate::PolygonArray;
use arrow2::array::{Array, ListArray, PrimitiveArray, StructArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::offset::OffsetsBuffer;
use geo::{Coord, LineString, MultiLineString};

use super::MutableMultiLineStringArray;

/// A [`GeometryArray`] semantically equivalent to `Vec<Option<MultiLineString>>` using Arrow's
/// in-memory representation.
#[derive(Debug, Clone)]
pub struct MultiLineStringArray {
    /// Buffer of x coordinates
    x: Buffer<f64>,

    /// Buffer of y coordinates
    y: Buffer<f64>,

    /// Offsets into the ring array where each geometry starts
    geom_offsets: OffsetsBuffer<i64>,

    /// Offsets into the coordinate array where each ring starts
    ring_offsets: OffsetsBuffer<i64>,

    /// Validity bitmap
    validity: Option<Bitmap>,
}

pub(super) fn check(
    x: &[f64],
    y: &[f64],
    validity_len: Option<usize>,
    geom_offsets: &OffsetsBuffer<i64>,
) -> Result<(), GeoArrowError> {
    // TODO: check geom offsets and ring_offsets?
    if validity_len.map_or(false, |len| len != geom_offsets.len()) {
        return Err(GeoArrowError::General(
            "validity mask length must match the number of values".to_string(),
        ));
    }

    if x.len() != y.len() {
        return Err(GeoArrowError::General(
            "x and y arrays must have the same length".to_string(),
        ));
    }
    Ok(())
}

impl MultiLineStringArray {
    /// Create a new MultiLineStringArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn new(
        x: Buffer<f64>,
        y: Buffer<f64>,
        geom_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Self {
        check(&x, &y, validity.as_ref().map(|v| v.len()), &geom_offsets).unwrap();
        Self {
            x,
            y,
            geom_offsets,
            ring_offsets,
            validity,
        }
    }

    /// Create a new MultiLineStringArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn try_new(
        x: Buffer<f64>,
        y: Buffer<f64>,
        geom_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(&x, &y, validity.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            x,
            y,
            geom_offsets,
            ring_offsets,
            validity,
        })
    }

    /// Returns the number of geometries in this array
    #[inline]
    pub fn len(&self) -> usize {
        self.geom_offsets.len()
    }

    /// Returns true if the array is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the optional validity.
    #[inline]
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
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
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        assert!(
            offset + length <= self.len(),
            "offset + length may not exceed length of array"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a clone of this [`PrimitiveArray`] sliced by an offset and length.
    /// # Implementation
    /// This operation is `O(1)` as it amounts to increase two ref counts.
    /// # Safety
    /// The caller must ensure that `offset + length <= self.len()`.
    #[inline]
    #[must_use]
    pub unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Self {
        let validity = self
            .validity
            .clone()
            .map(|bitmap| bitmap.slice_unchecked(offset, length))
            .and_then(|bitmap| (bitmap.unset_bits() > 0).then_some(bitmap));
        Self {
            x: self.x.clone().slice_unchecked(offset, length),
            y: self.y.clone().slice_unchecked(offset, length),
            geom_offsets: self.geom_offsets.clone().slice_unchecked(offset, length),
            ring_offsets: self.ring_offsets.clone().slice_unchecked(offset, length),
            validity,
        }
    }
}

// Implement geometry accessors
impl MultiLineStringArray {
    pub fn get(&self, i: usize) -> Option<crate::MultiLineString> {
        if self.is_null(i) {
            return None;
        }

        Some(crate::MultiLineString {
            x: &self.x,
            y: &self.y,
            geom_offsets: &self.geom_offsets,
            ring_offsets: &self.ring_offsets,
            geom_index: i,
        })
    }

    /// Returns the value at slot `i` as a geo object.
    pub fn value_as_geo(&self, i: usize) -> MultiLineString {
        // Start and end indices into the ring_offsets buffer
        let (start_geom_idx, end_geom_idx) = self.geom_offsets.start_end(i);

        let mut line_strings: Vec<LineString> = Vec::with_capacity(end_geom_idx - start_geom_idx);

        for ring_idx in start_geom_idx..end_geom_idx {
            let (start_coord_idx, end_coord_idx) = self.ring_offsets.start_end(ring_idx);
            let mut ring: Vec<Coord> = Vec::with_capacity(end_coord_idx - start_coord_idx);
            for coord_idx in start_coord_idx..end_coord_idx {
                ring.push(Coord {
                    x: self.x[coord_idx],
                    y: self.y[coord_idx],
                })
            }
            line_strings.push(ring.into());
        }

        MultiLineString::new(line_strings)
    }

    /// Gets the value at slot `i` as a geo object, additionally checking the validity bitmap
    pub fn get_as_geo(&self, i: usize) -> Option<MultiLineString> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = MultiLineString> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<MultiLineString, impl Iterator<Item = MultiLineString> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.validity())
    }

    // GEOS from not implemented for MultiLineString I suppose
    //
    // /// Returns the value at slot `i` as a GEOS geometry.
    // #[cfg(feature = "geos")]
    // pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
    //     (&self.value_as_geo(i)).try_into().unwrap()
    // }

    // /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    // #[cfg(feature = "geos")]
    // pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
    //     if self.is_null(i) {
    //         return None;
    //     }

    //     self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
    // }

    // /// Iterator over GEOS geometry objects
    // #[cfg(feature = "geos")]
    // pub fn iter_geos_values(&self) -> impl Iterator<Item = geos::Geometry> + '_ {
    //     (0..self.len()).map(|i| self.value_as_geos(i))
    // }

    // /// Iterator over GEOS geometry objects, taking validity into account
    // #[cfg(feature = "geos")]
    // pub fn iter_geos(
    //     &self,
    // ) -> ZipValidity<geos::Geometry, impl Iterator<Item = geos::Geometry> + '_, BitmapIter> {
    //     ZipValidity::new_with_validity(self.iter_geos_values(), self.validity())
    // }

    pub fn into_arrow(self) -> ListArray<i64> {
        let polygon_array: PolygonArray = self.into();
        polygon_array.into_arrow()
    }
}

impl TryFrom<ListArray<i64>> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: ListArray<i64>) -> Result<Self, Self::Error> {
        let geom_offsets = value.offsets();
        let validity = value.validity();

        let inner_dyn_array = value.values();
        let inner_array = inner_dyn_array
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();

        let ring_offsets = inner_array.offsets();
        let coords_dyn_array = inner_array.values();
        let coords_array = coords_dyn_array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let x_array_values = coords_array.values()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = coords_array.values()[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        Ok(Self::new(
            x_array_values.values().clone(),
            y_array_values.values().clone(),
            geom_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
        ))
    }
}

impl TryFrom<Box<dyn Array>> for MultiLineStringArray {
    type Error = GeoArrowError;

    fn try_from(value: Box<dyn Array>) -> Result<Self, Self::Error> {
        let arr = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        arr.clone().try_into()
    }
}

impl GeometryArray for MultiLineStringArray {
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
        self.len()
    }

    #[inline]
    fn geometry_type(&self) -> GeometryType {
        GeometryType::WKB
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity()
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn GeometryArray> {
        Box::new(self.slice(offset, length))
    }

    unsafe fn slice_unchecked(&self, offset: usize, length: usize) -> Box<dyn GeometryArray> {
        Box::new(self.slice_unchecked(offset, length))
    }

    fn to_boxed(&self) -> Box<dyn GeometryArray> {
        Box::new(self.clone())
    }
}

impl From<Vec<Option<MultiLineString>>> for MultiLineStringArray {
    fn from(other: Vec<Option<MultiLineString>>) -> Self {
        let mut_arr: MutableMultiLineStringArray = other.into();
        mut_arr.into()
    }
}

impl From<Vec<MultiLineString>> for MultiLineStringArray {
    fn from(other: Vec<MultiLineString>) -> Self {
        let mut_arr: MutableMultiLineStringArray = other.into();
        mut_arr.into()
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl From<MultiLineStringArray> for PolygonArray {
    fn from(value: MultiLineStringArray) -> Self {
        Self::new(
            value.x,
            value.y,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use geo::{line_string, MultiLineString};

    fn ml0() -> MultiLineString {
        MultiLineString::new(vec![line_string![
            (x: -111., y: 45.),
            (x: -111., y: 41.),
            (x: -104., y: 41.),
            (x: -104., y: 45.),
        ]])
    }

    fn ml1() -> MultiLineString {
        MultiLineString::new(vec![
            line_string![
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            line_string![
                (x: -110., y: 44.),
                (x: -110., y: 42.),
                (x: -105., y: 42.),
                (x: -105., y: 44.),
            ],
        ])
    }

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: MultiLineStringArray = vec![ml0(), ml1()].into();
        assert_eq!(arr.value_as_geo(0), ml0());
        assert_eq!(arr.value_as_geo(1), ml1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiLineStringArray = vec![Some(ml0()), Some(ml1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(ml0()));
        assert_eq!(arr.get_as_geo(1), Some(ml1()));
        assert_eq!(arr.get_as_geo(2), None);
    }
}
