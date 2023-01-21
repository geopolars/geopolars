use crate::enum_::GeometryType;
use crate::error::GeoArrowError;
use crate::polygon::parse_polygon;
use crate::trait_::GeometryArray;
use arrow2::array::{Array, ListArray, PrimitiveArray, StructArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field};
use arrow2::offset::OffsetsBuffer;
use geo::{MultiPolygon, Polygon};

use super::MutableMultiPolygonArray;

/// A [`GeometryArray`] semantically equivalent to `Vec<Option<MultiPolygon>>` using Arrow's
/// in-memory representation.
#[derive(Debug, Clone)]
pub struct MultiPolygonArray {
    /// Buffer of x coordinates
    x: Buffer<f64>,

    /// Buffer of y coordinates
    y: Buffer<f64>,

    /// Offsets into the polygon array where each geometry starts
    geom_offsets: OffsetsBuffer<i64>,

    /// Offsets into the ring array where each polygon starts
    polygon_offsets: OffsetsBuffer<i64>,

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

impl MultiPolygonArray {
    /// Create a new MultiPolygonArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn new(
        x: Buffer<f64>,
        y: Buffer<f64>,
        geom_offsets: OffsetsBuffer<i64>,
        polygon_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Self {
        check(&x, &y, validity.as_ref().map(|v| v.len()), &geom_offsets).unwrap();
        Self {
            x,
            y,
            geom_offsets,
            polygon_offsets,
            ring_offsets,
            validity,
        }
    }

    /// Create a new MultiPolygonArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn try_new(
        x: Buffer<f64>,
        y: Buffer<f64>,
        geom_offsets: OffsetsBuffer<i64>,
        polygon_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(&x, &y, validity.as_ref().map(|v| v.len()), &geom_offsets)?;
        Ok(Self {
            x,
            y,
            geom_offsets,
            polygon_offsets,
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
            polygon_offsets: self.polygon_offsets.clone().slice_unchecked(offset, length),
            ring_offsets: self.ring_offsets.clone().slice_unchecked(offset, length),
            validity,
        }
    }
}

// Implement geometry accessors
impl MultiPolygonArray {
    pub fn get(&self, i: usize) -> Option<crate::MultiPolygon> {
        if self.is_null(i) {
            return None;
        }

        Some(crate::MultiPolygon {
            x: &self.x,
            y: &self.y,
            geom_offsets: &self.geom_offsets,
            polygon_offsets: &self.polygon_offsets,
            ring_offsets: &self.ring_offsets,
            geom_index: i,
        })
    }

    // TODO: Need to test this
    /// Returns the value at slot `i` as a geo object.
    pub fn value_as_geo(&self, i: usize) -> MultiPolygon {
        // Start and end indices into the polygon_offsets buffer
        let (start_geom_idx, end_geom_idx) = self.geom_offsets.start_end(i);

        let mut polygons: Vec<Polygon> = Vec::with_capacity(end_geom_idx - start_geom_idx);

        for geom_idx in start_geom_idx..end_geom_idx {
            let poly = parse_polygon(
                &self.x,
                &self.y,
                &self.polygon_offsets,
                &self.ring_offsets,
                geom_idx,
            );
            polygons.push(poly);
        }

        MultiPolygon::new(polygons)
    }

    /// Gets the value at slot `i` as a geo object, additionally checking the validity bitmap
    pub fn get_as_geo(&self, i: usize) -> Option<MultiPolygon> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = MultiPolygon> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<MultiPolygon, impl Iterator<Item = MultiPolygon> + '_, BitmapIter> {
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
        // Data type
        let coord_field_x = Field::new("x", DataType::Float64, false);
        let coord_field_y = Field::new("y", DataType::Float64, false);
        let struct_data_type = DataType::Struct(vec![coord_field_x, coord_field_y]);
        let inner_list_data_type = DataType::LargeList(Box::new(Field::new(
            "vertices",
            struct_data_type.clone(),
            false,
        )));
        let middle_list_data_type = DataType::LargeList(Box::new(Field::new(
            "rings",
            inner_list_data_type.clone(),
            false,
        )));
        let outer_list_data_type = DataType::LargeList(Box::new(Field::new(
            "polygons",
            middle_list_data_type.clone(),
            true,
        )));

        // Validity
        let validity: Option<Bitmap> = if let Some(validity) = self.validity {
            validity.into()
        } else {
            None
        };

        // Array data
        let array_x = PrimitiveArray::new(DataType::Float64, self.x, None).boxed();
        let array_y = PrimitiveArray::new(DataType::Float64, self.y, None).boxed();

        // Coord struct array
        let coord_array = StructArray::new(struct_data_type, vec![array_x, array_y], None).boxed();

        // Rings array
        let inner_list_array =
            ListArray::new(inner_list_data_type, self.ring_offsets, coord_array, None).boxed();

        // Polygons array
        let middle_list_array = ListArray::new(
            middle_list_data_type,
            self.polygon_offsets,
            inner_list_array,
            None,
        )
        .boxed();

        // Geometry array
        ListArray::new(
            outer_list_data_type,
            self.geom_offsets,
            middle_list_array,
            validity,
        )
    }
}

impl TryFrom<ListArray<i64>> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: ListArray<i64>) -> Result<Self, Self::Error> {
        let geom_offsets = value.offsets();
        let validity = value.validity();

        let first_level_dyn_array = value.values();
        let first_level_array = first_level_dyn_array
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();

        let polygon_offsets = first_level_array.offsets();
        let second_level_dyn_array = first_level_array.values();
        let second_level_array = second_level_dyn_array
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .unwrap();

        let ring_offsets = second_level_array.offsets();
        let coords_dyn_array = second_level_array.values();
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
            polygon_offsets.clone(),
            ring_offsets.clone(),
            validity.cloned(),
        ))
    }
}

impl TryFrom<Box<dyn Array>> for MultiPolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: Box<dyn Array>) -> Result<Self, Self::Error> {
        let arr = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        arr.clone().try_into()
    }
}

impl GeometryArray for MultiPolygonArray {
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

impl From<Vec<Option<MultiPolygon>>> for MultiPolygonArray {
    fn from(other: Vec<Option<MultiPolygon>>) -> Self {
        let mut_arr: MutableMultiPolygonArray = other.into();
        mut_arr.into()
    }
}

impl From<Vec<MultiPolygon>> for MultiPolygonArray {
    fn from(other: Vec<MultiPolygon>) -> Self {
        let mut_arr: MutableMultiPolygonArray = other.into();
        mut_arr.into()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use geo::{polygon, MultiPolygon};

    fn mp0() -> MultiPolygon {
        MultiPolygon::new(vec![
            polygon![
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            polygon!(
                exterior: [
                    (x: -111., y: 45.),
                    (x: -111., y: 41.),
                    (x: -104., y: 41.),
                    (x: -104., y: 45.),
                ],
                interiors: [
                    [
                        (x: -110., y: 44.),
                        (x: -110., y: 42.),
                        (x: -105., y: 42.),
                        (x: -105., y: 44.),
                    ],
                ],
            ),
        ])
    }

    fn mp1() -> MultiPolygon {
        MultiPolygon::new(vec![
            polygon![
                (x: -111., y: 45.),
                (x: -111., y: 41.),
                (x: -104., y: 41.),
                (x: -104., y: 45.),
            ],
            polygon![
                (x: -110., y: 44.),
                (x: -110., y: 42.),
                (x: -105., y: 42.),
                (x: -105., y: 44.),
            ],
        ])
    }

    #[test]
    fn geo_roundtrip_accurate() {
        let arr: MultiPolygonArray = vec![mp0(), mp1()].into();
        assert_eq!(arr.value_as_geo(0), mp0());
        assert_eq!(arr.value_as_geo(1), mp1());
    }

    #[test]
    fn geo_roundtrip_accurate_option_vec() {
        let arr: MultiPolygonArray = vec![Some(mp0()), Some(mp1()), None].into();
        assert_eq!(arr.get_as_geo(0), Some(mp0()));
        assert_eq!(arr.get_as_geo(1), Some(mp1()));
        assert_eq!(arr.get_as_geo(2), None);
    }
}
