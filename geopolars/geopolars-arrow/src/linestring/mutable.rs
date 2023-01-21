use crate::error::GeoArrowError;
use crate::multipoint::MutableMultiPointArray;
use crate::LineStringArray;
use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::Offsets;
use arrow2::types::Index;
use geo::{CoordsIter, LineString};
use std::convert::From;

/// The Arrow equivalent to `Vec<Option<LineString>>`.
/// Converting a [`MutableLineStringArray`] into a [`LineStringArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutableLineStringArray {
    x: Vec<f64>,
    y: Vec<f64>,

    /// Offsets into the coordinate array where each geometry starts
    geom_offsets: Offsets<i64>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

impl MutableLineStringArray {
    /// Creates a new empty [`MutableLineStringArray`].
    pub fn new() -> Self {
        Self::with_capacities(0, 0)
    }

    /// Creates a new [`MutableLineStringArray`] with a capacity.
    pub fn with_capacities(coord_capacity: usize, geom_capacity: usize) -> Self {
        Self {
            x: Vec::with_capacity(coord_capacity),
            y: Vec::with_capacity(coord_capacity),
            geom_offsets: Offsets::<i64>::with_capacity(geom_capacity),
            validity: None,
        }
    }

    /// The canonical method to create a [`MutableLineStringArray`] out of its internal components.
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
        // Can't pass Offsets into the check, expected OffsetsBuffer
        // use crate::linestring::array::check;
        // check(&x, &y, validity.as_ref().map(|x| x.len()), &geom_offsets)?;
        Ok(Self {
            x,
            y,
            geom_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MutableLineStringArray`].
    pub fn into_inner(self) -> (Vec<f64>, Vec<f64>, Offsets<i64>, Option<MutableBitmap>) {
        (self.x, self.y, self.geom_offsets, self.validity)
    }

    /// Adds a new value to the array.
    pub fn try_push_geo(&mut self, value: Option<LineString>) -> Result<(), GeoArrowError> {
        if let Some(line_string) = value {
            line_string.coords_iter().for_each(|c| {
                self.x.push(c.x);
                self.y.push(c.y);
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

    pub fn into_arrow(self) -> ListArray<i64> {
        let linestring_arr: LineStringArray = self.into();
        linestring_arr.into_arrow()
    }
}

impl Default for MutableLineStringArray {
    fn default() -> Self {
        Self::new()
    }
}

impl From<MutableLineStringArray> for LineStringArray {
    fn from(other: MutableLineStringArray) -> Self {
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

impl From<MutableLineStringArray> for ListArray<i64> {
    fn from(arr: MutableLineStringArray) -> Self {
        arr.into_arrow()
    }
}

// TODO: in the future it would be useful to DRY the functions here and for MultiPoint

/// Implement a converter that can be used for either Vec<LineString> or
/// Vec<MultiPoint>
pub(crate) fn line_string_from_geo_vec(geoms: Vec<LineString>) -> MutableLineStringArray {
    let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());

    for geom in &geoms {
        geom_offsets.try_push_usize(geom.0.len()).unwrap();
    }

    let mut x_arr = Vec::<f64>::with_capacity(geom_offsets.last().to_usize());
    let mut y_arr = Vec::<f64>::with_capacity(geom_offsets.last().to_usize());

    for geom in geoms {
        for coord in geom.coords_iter() {
            x_arr.push(coord.x);
            y_arr.push(coord.y);
        }
    }

    MutableLineStringArray {
        x: x_arr,
        y: y_arr,
        geom_offsets,
        validity: None,
    }
}

/// Implement a converter that can be used for either Vec<Option<LineString>> or
/// Vec<Option<MultiPoint>>
pub(crate) fn line_string_from_geo_option_vec(
    geoms: Vec<Option<LineString>>,
) -> MutableLineStringArray {
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
        for coord in geom.coords_iter() {
            x_arr.push(coord.x);
            y_arr.push(coord.y);
        }
    }

    MutableLineStringArray {
        x: x_arr,
        y: y_arr,
        geom_offsets,
        validity: Some(validity),
    }
}

impl From<Vec<LineString>> for MutableLineStringArray {
    fn from(geoms: Vec<LineString>) -> Self {
        line_string_from_geo_vec(geoms)
    }
}

impl From<Vec<Option<LineString>>> for MutableLineStringArray {
    fn from(geoms: Vec<Option<LineString>>) -> Self {
        line_string_from_geo_option_vec(geoms)
    }
}

/// LineString and MultiPoint have the same layout, so enable conversions between the two to change
/// the semantic type
impl From<MutableLineStringArray> for MutableMultiPointArray {
    fn from(value: MutableLineStringArray) -> Self {
        Self::try_new(value.x, value.y, value.geom_offsets, value.validity).unwrap()
    }
}
