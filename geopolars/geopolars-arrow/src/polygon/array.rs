use crate::enum_::GeometryType;
use crate::error::GeoArrowError;
use crate::trait_::GeometryArray;
use crate::MultiLineStringArray;
use arrow2::array::Array;
use arrow2::array::{ListArray, PrimitiveArray, StructArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::bitmap::Bitmap;
use arrow2::buffer::Buffer;
use arrow2::datatypes::{DataType, Field};
use arrow2::offset::OffsetsBuffer;
use geo::{Coord, LineString, Polygon};

use super::MutablePolygonArray;

/// A [`GeometryArray`] semantically equivalent to `Vec<Option<Polygon>>` using Arrow's
/// in-memory representation.
#[derive(Debug, Clone)]
pub struct PolygonArray {
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
) -> Result<(), GeoArrowError> {
    // TODO: check geom offsets and ring_offsets?
    if validity_len.map_or(false, |len| len != x.len()) {
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

impl PolygonArray {
    /// Create a new PolygonArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn new(
        x: Buffer<f64>,
        y: Buffer<f64>,
        geom_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Self {
        check(&x, &y, validity.as_ref().map(|v| v.len())).unwrap();
        Self {
            x,
            y,
            geom_offsets,
            ring_offsets,
            validity,
        }
    }

    /// Create a new PolygonArray from parts
    /// # Implementation
    /// This function is `O(1)`.
    pub fn try_new(
        x: Buffer<f64>,
        y: Buffer<f64>,
        geom_offsets: OffsetsBuffer<i64>,
        ring_offsets: OffsetsBuffer<i64>,
        validity: Option<Bitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(&x, &y, validity.as_ref().map(|v| v.len()))?;
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

pub(crate) fn parse_polygon(
    x: &Buffer<f64>,
    y: &Buffer<f64>,
    polygon_offsets: &OffsetsBuffer<i64>,
    ring_offsets: &OffsetsBuffer<i64>,
    i: usize,
) -> Polygon {
    // Start and end indices into the ring_offsets buffer
    let (start_geom_idx, end_geom_idx) = polygon_offsets.start_end(i);

    // Parse exterior ring first
    let (start_ext_ring_idx, end_ext_ring_idx) = ring_offsets.start_end(start_geom_idx);
    let mut exterior_coords: Vec<Coord> = Vec::with_capacity(end_ext_ring_idx - start_ext_ring_idx);

    for i in start_ext_ring_idx..end_ext_ring_idx {
        exterior_coords.push(Coord { x: x[i], y: y[i] })
    }
    let exterior_ring: LineString = exterior_coords.into();

    // Parse any interior rings
    // Note: need to check if interior rings exist otherwise the subtraction below can overflow
    let has_interior_rings = end_geom_idx - start_geom_idx > 1;
    let n_interior_rings = if has_interior_rings {
        end_geom_idx - start_geom_idx - 2
    } else {
        0
    };
    let mut interior_rings: Vec<LineString<f64>> = Vec::with_capacity(n_interior_rings);
    for ring_idx in start_geom_idx + 1..end_geom_idx {
        let (start_coord_idx, end_coord_idx) = ring_offsets.start_end(ring_idx);
        let mut ring: Vec<Coord> = Vec::with_capacity(end_coord_idx - start_coord_idx);
        for coord_idx in start_coord_idx..end_coord_idx {
            ring.push(Coord {
                x: x[coord_idx],
                y: y[coord_idx],
            })
        }
        interior_rings.push(ring.into());
    }

    Polygon::new(exterior_ring, interior_rings)
}

// Implement geometry accessors
impl PolygonArray {
    pub fn get(&self, i: usize) -> Option<crate::Polygon> {
        if self.is_null(i) {
            return None;
        }

        Some(crate::Polygon {
            x: &self.x,
            y: &self.y,
            geom_offsets: &self.geom_offsets,
            ring_offsets: &self.ring_offsets,
            geom_index: i,
        })
    }

    /// Returns the value at slot `i` as a geo object.
    pub fn value_as_geo(&self, i: usize) -> Polygon {
        parse_polygon(&self.x, &self.y, &self.geom_offsets, &self.ring_offsets, i)
    }

    /// Gets the value at slot `i` as a geo object, additionally checking the validity bitmap
    pub fn get_as_geo(&self, i: usize) -> Option<Polygon> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, not looking at validity
    pub fn iter_geo_values(&self) -> impl Iterator<Item = Polygon> + '_ {
        (0..self.len()).map(|i| self.value_as_geo(i))
    }

    /// Iterator over geo Geometry objects, taking into account validity
    pub fn iter_geo(&self) -> ZipValidity<Polygon, impl Iterator<Item = Polygon> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.iter_geo_values(), self.validity())
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    #[cfg(feature = "geos")]
    pub fn value_as_geos(&self, i: usize) -> geos::Geometry {
        (&self.value_as_geo(i)).try_into().unwrap()
    }

    /// Gets the value at slot `i` as a GEOS geometry, additionally checking the validity bitmap
    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        if self.is_null(i) {
            return None;
        }

        self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
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
        let outer_list_data_type = DataType::LargeList(Box::new(Field::new(
            "rings",
            inner_list_data_type.clone(),
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

        let coord_array = StructArray::new(struct_data_type, vec![array_x, array_y], None).boxed();

        let inner_list_array =
            ListArray::new(inner_list_data_type, self.ring_offsets, coord_array, None).boxed();

        ListArray::new(
            outer_list_data_type,
            self.geom_offsets,
            inner_list_array,
            validity,
        )
    }
}

impl TryFrom<ListArray<i64>> for PolygonArray {
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

impl TryFrom<Box<dyn Array>> for PolygonArray {
    type Error = GeoArrowError;

    fn try_from(value: Box<dyn Array>) -> Result<Self, Self::Error> {
        let arr = value.as_any().downcast_ref::<ListArray<i64>>().unwrap();
        arr.clone().try_into()
    }
}

impl GeometryArray for PolygonArray {
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

impl From<Vec<Option<Polygon>>> for PolygonArray {
    fn from(other: Vec<Option<Polygon>>) -> Self {
        let mut_arr: MutablePolygonArray = other.into();
        mut_arr.into()
    }
}

impl From<Vec<Polygon>> for PolygonArray {
    fn from(other: Vec<Polygon>) -> Self {
        let mut_arr: MutablePolygonArray = other.into();
        mut_arr.into()
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl From<PolygonArray> for MultiLineStringArray {
    fn from(value: PolygonArray) -> Self {
        Self::new(
            value.x,
            value.y,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
    }
}
