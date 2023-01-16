use geo::MultiLineString;
use polars::export::arrow::array::ListArray;
use polars::export::arrow::bitmap::{Bitmap, MutableBitmap};
use polars::export::arrow::offset::{Offsets, OffsetsBuffer};

use crate::error::GeoArrowError;
use crate::polygon::MutablePolygonArray;
use crate::MultiLineStringArray;

#[derive(Debug, Clone)]
pub struct MutableMultiLineStringArray {
    x: Vec<f64>,
    y: Vec<f64>,

    /// Offsets into the ring array where each geometry starts
    geom_offsets: Offsets<i64>,

    /// Offsets into the coordinate array where each ring starts
    ring_offsets: Offsets<i64>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

pub type MultiLineStringInner = (
    Vec<f64>,
    Vec<f64>,
    Offsets<i64>,
    Offsets<i64>,
    Option<MutableBitmap>,
);

impl MutableMultiLineStringArray {
    /// Creates a new empty [`MutableLineStringArray`].
    pub fn new() -> Self {
        MutablePolygonArray::new().into()
    }

    /// Creates a new [`MutableLineStringArray`] with a capacity.
    pub fn with_capacities(
        coord_capacity: usize,
        geom_capacity: usize,
        ring_capacity: usize,
    ) -> Self {
        MutablePolygonArray::with_capacities(coord_capacity, geom_capacity, ring_capacity).into()
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
        ring_offsets: Offsets<i64>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        MutablePolygonArray::try_new(x, y, geom_offsets, ring_offsets, validity)
            .map(|result| result.into())
    }

    /// Extract the low-level APIs from the [`MutableLineStringArray`].
    pub fn into_inner(self) -> MultiLineStringInner {
        (
            self.x,
            self.y,
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    pub fn into_arrow(self) -> ListArray<i64> {
        let arr: MultiLineStringArray = self.into();
        arr.into_arrow()
    }
}

impl Default for MutableMultiLineStringArray {
    fn default() -> Self {
        Self::new()
    }
}

impl From<MutableMultiLineStringArray> for MultiLineStringArray {
    fn from(other: MutableMultiLineStringArray) -> Self {
        let validity = other.validity.and_then(|x| {
            let bitmap: Bitmap = x.into();
            if bitmap.unset_bits() == 0 {
                None
            } else {
                Some(bitmap)
            }
        });

        let geom_offsets: OffsetsBuffer<i64> = other.geom_offsets.into();
        let ring_offsets: OffsetsBuffer<i64> = other.ring_offsets.into();

        Self::new(
            other.x.into(),
            other.y.into(),
            geom_offsets,
            ring_offsets,
            validity,
        )
    }
}

impl From<Vec<MultiLineString>> for MutableMultiLineStringArray {
    fn from(geoms: Vec<MultiLineString>) -> Self {
        use geo::coords_iter::CoordsIter;

        // Offset into ring indexes for each geometry
        let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each geometry has only a single
        // linestring
        let mut ring_offsets = Offsets::<i64>::with_capacity(geoms.len());

        // Current offset into ring array
        let mut current_geom_offset = 0;

        // Current offset into coord array
        let mut current_ring_offset = 0;

        for geom in &geoms {
            // Total number of linestrings in this multilinestring
            current_geom_offset += geom.0.len();
            geom_offsets.try_push_usize(current_geom_offset).unwrap();

            // Number of coords for each ring
            for linestring in geom.0.iter() {
                current_ring_offset += linestring.coords_count();
                ring_offsets.try_push_usize(current_ring_offset).unwrap();
            }
        }

        let mut x_arr = Vec::<f64>::with_capacity(current_ring_offset);
        let mut y_arr = Vec::<f64>::with_capacity(current_ring_offset);

        for geom in geoms {
            for coord in geom.coords_iter() {
                x_arr.push(coord.x);
                y_arr.push(coord.y);
            }
        }

        MutableMultiLineStringArray {
            x: x_arr,
            y: y_arr,
            geom_offsets,
            ring_offsets,
            validity: None,
        }
    }
}

impl From<Vec<Option<MultiLineString>>> for MutableMultiLineStringArray {
    fn from(geoms: Vec<Option<MultiLineString>>) -> Self {
        use geo::coords_iter::CoordsIter;

        let mut validity = MutableBitmap::with_capacity(geoms.len());

        // Offset into ring indexes for each geometry
        let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each geometry has only a single
        // linestring
        let mut ring_offsets = Offsets::<i64>::with_capacity(geoms.len());

        // Current offset into ring array
        let mut current_geom_offset = 0;

        // Current offset into coord array
        let mut current_ring_offset = 0;

        for geom in &geoms {
            if let Some(geom) = geom {
                validity.push(true);

                // Total number of linestrings in this multilinestring
                current_geom_offset += geom.0.len();
                geom_offsets.try_push_usize(current_geom_offset).unwrap();

                // Number of coords for each ring
                for linestring in geom.0.iter() {
                    current_ring_offset += linestring.coords_count();
                    ring_offsets.try_push_usize(current_ring_offset).unwrap();
                }
            } else {
                validity.push(false);
                geom_offsets.try_push_usize(current_geom_offset).unwrap();
            }
        }

        let mut x_arr = Vec::<f64>::with_capacity(current_ring_offset);
        let mut y_arr = Vec::<f64>::with_capacity(current_ring_offset);

        for geom in geoms.into_iter().flatten() {
            for coord in geom.coords_iter() {
                x_arr.push(coord.x);
                y_arr.push(coord.y);
            }
        }

        MutableMultiLineStringArray {
            x: x_arr,
            y: y_arr,
            geom_offsets,
            ring_offsets,
            validity: Some(validity),
        }
    }
}

/// Polygon and MultiLineString have the same layout, so enable conversions between the two to
/// change the semantic type
impl From<MutableMultiLineStringArray> for MutablePolygonArray {
    fn from(value: MutableMultiLineStringArray) -> Self {
        Self::try_new(
            value.x,
            value.y,
            value.geom_offsets,
            value.ring_offsets,
            value.validity,
        )
        .unwrap()
    }
}
