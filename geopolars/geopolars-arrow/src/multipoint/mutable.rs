use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::Offsets;
use arrow2::types::Index;
use geo::MultiPoint;

use crate::enum_::GeometryType;
use crate::error::GeoArrowError;
use crate::linestring::MutableLineStringArray;
use crate::trait_::MutableGeometryArray;

use super::array::MultiPointArray;

/// The Arrow equivalent to `Vec<Option<MultiPoint>>`.
/// Converting a [`MutableMultiPointArray`] into a [`MultiPointArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutableMultiPointArray {
    x: Vec<f64>,
    y: Vec<f64>,
    geom_offsets: Offsets<i64>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

// Many of the methods here use the From impl from MutableLineStringArray to MutableMultiPointArray
// to DRY

impl MutableMultiPointArray {
    /// Creates a new empty [`MutableMultiPointArray`].
    pub fn new() -> Self {
        MutableLineStringArray::new().into()
    }

    /// Creates a new [`MutableMultiPointArray`] with a capacity.
    pub fn with_capacities(coord_capacity: usize, geom_capacity: usize) -> Self {
        Self {
            x: Vec::with_capacity(coord_capacity),
            y: Vec::with_capacity(coord_capacity),
            geom_offsets: Offsets::<i64>::with_capacity(geom_capacity),
            validity: None,
        }
    }

    /// The canonical method to create a [`MutableMultiPointArray`] out of its internal components.
    /// # Implementation
    /// This function is `O(1)`.
    ///
    /// # Errors
    /// This function errors iff:
    /// * The validity is not `None` and its length is different from `values`'s length
    pub fn try_new(
        x: Vec<f64>,
        y: Vec<f64>,
        geom_offsets: Offsets<i64>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        MutableLineStringArray::try_new(x, y, geom_offsets, validity).map(|result| result.into())
    }

    /// Extract the low-level APIs from the [`MutableMultiPointArray`].
    pub fn into_inner(self) -> (Vec<f64>, Vec<f64>, Offsets<i64>, Option<MutableBitmap>) {
        (self.x, self.y, self.geom_offsets, self.validity)
    }

    pub fn into_arrow(self) -> ListArray<i64> {
        let arr: MultiPointArray = self.into();
        arr.into_arrow()
    }

    /// Adds a new value to the array.
    pub fn try_push_geo(&mut self, value: Option<MultiPoint>) -> Result<(), GeoArrowError> {
        if let Some(multipoint) = value {
            multipoint.0.iter().for_each(|point| {
                self.x.push(point.x());
                self.y.push(point.y());
            });
            self.try_push_valid()?;
        } else {
            self.push_null();
        }
        Ok(())
    }

    #[inline]
    /// Needs to be called when a valid value was extended to this array.
    /// This is a relatively low level function, prefer `try_push` when you can.
    pub fn try_push_valid(&mut self) -> Result<(), GeoArrowError> {
        let total_length = self.x.len();
        let offset = self.geom_offsets.last().to_usize();
        let length = total_length
            .checked_sub(offset)
            .ok_or(GeoArrowError::Overflow)?;

        // TODO: remove unwrap
        self.geom_offsets.try_push_usize(length).unwrap();
        if let Some(validity) = &mut self.validity {
            validity.push(true)
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) {
        self.geom_offsets.extend_constant(1);
        match &mut self.validity {
            Some(validity) => validity.push(false),
            None => self.init_validity(),
        }
    }

    fn init_validity(&mut self) {
        let len = self.geom_offsets.len_proxy();

        let mut validity = MutableBitmap::with_capacity(self.geom_offsets.capacity());
        validity.extend_constant(len, true);
        validity.set(len - 1, false);
        self.validity = Some(validity)
    }
}

impl Default for MutableMultiPointArray {
    fn default() -> Self {
        Self::new()
    }
}

impl MutableGeometryArray for MutableMultiPointArray {
    fn geometry_type(&self) -> GeometryType {
        GeometryType::Point
    }

    fn len(&self) -> usize {
        self.x.len()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl From<MutableMultiPointArray> for MultiPointArray {
    fn from(other: MutableMultiPointArray) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

        Self::new(
            other.x.into(),
            other.y.into(),
            other.geom_offsets.into(),
            validity,
        )
    }
}

impl From<MutableMultiPointArray> for ListArray<i64> {
    fn from(arr: MutableMultiPointArray) -> Self {
        arr.into_arrow()
    }
}

// TODO: in the future it would be useful to DRY the functions here and for LineString

/// Implement a converter that can be used for either Vec<LineString> or
/// Vec<MultiPoint>
pub(crate) fn line_string_from_geo_vec(geoms: Vec<MultiPoint>) -> MutableMultiPointArray {
    let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());

    for geom in &geoms {
        geom_offsets.try_push_usize(geom.0.len()).unwrap();
    }

    let mut x_arr = Vec::<f64>::with_capacity(geom_offsets.last().to_usize());
    let mut y_arr = Vec::<f64>::with_capacity(geom_offsets.last().to_usize());

    for geom in geoms {
        for point in geom.iter() {
            x_arr.push(point.x());
            y_arr.push(point.y());
        }
    }

    MutableMultiPointArray {
        x: x_arr,
        y: y_arr,
        geom_offsets,
        validity: None,
    }
}

/// Implement a converter that can be used for either Vec<Option<LineString>> or
/// Vec<Option<MultiPoint>>
pub(crate) fn line_string_from_geo_option_vec(
    geoms: Vec<Option<MultiPoint>>,
) -> MutableMultiPointArray {
    let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());
    let mut validity = MutableBitmap::with_capacity(geoms.len());

    for maybe_geom in &geoms {
        validity.push(maybe_geom.is_some());
        geom_offsets
            .try_push_usize(maybe_geom.as_ref().map_or(0, |geom| geom.0.len()))
            .unwrap();
    }

    let mut x_arr = Vec::<f64>::with_capacity(geom_offsets.last().to_usize());
    let mut y_arr = Vec::<f64>::with_capacity(geom_offsets.last().to_usize());

    for geom in geoms.into_iter().flatten() {
        for point in geom.iter() {
            x_arr.push(point.x());
            y_arr.push(point.y());
        }
    }

    MutableMultiPointArray {
        x: x_arr,
        y: y_arr,
        geom_offsets,
        validity: Some(validity),
    }
}

impl From<Vec<MultiPoint>> for MutableMultiPointArray {
    fn from(geoms: Vec<MultiPoint>) -> Self {
        line_string_from_geo_vec(geoms)
    }
}

impl From<Vec<Option<MultiPoint>>> for MutableMultiPointArray {
    fn from(geoms: Vec<Option<MultiPoint>>) -> Self {
        line_string_from_geo_option_vec(geoms)
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl From<MutableMultiPointArray> for MutableLineStringArray {
    fn from(value: MutableMultiPointArray) -> Self {
        Self::try_new(value.x, value.y, value.geom_offsets, value.validity).unwrap()
    }
}
