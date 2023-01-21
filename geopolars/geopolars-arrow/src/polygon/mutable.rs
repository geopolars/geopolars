use super::array::check;
use arrow2::array::ListArray;
use arrow2::bitmap::{Bitmap, MutableBitmap};
use arrow2::offset::{Offsets, OffsetsBuffer};
use geo::Polygon;

use crate::error::GeoArrowError;
use crate::multilinestring::MutableMultiLineStringArray;
use crate::PolygonArray;

pub type MutablePolygonParts = (
    Vec<f64>,
    Vec<f64>,
    Offsets<i64>,
    Offsets<i64>,
    Option<MutableBitmap>,
);

/// The Arrow equivalent to `Vec<Option<Polygon>>`.
/// Converting a [`MutablePolygonArray`] into a [`PolygonArray`] is `O(1)`.
#[derive(Debug, Clone)]
pub struct MutablePolygonArray {
    x: Vec<f64>,
    y: Vec<f64>,

    /// Offsets into the ring array where each geometry starts
    geom_offsets: Offsets<i64>,

    /// Offsets into the coordinate array where each ring starts
    ring_offsets: Offsets<i64>,

    /// Validity is only defined at the geometry level
    validity: Option<MutableBitmap>,
}

impl MutablePolygonArray {
    /// Creates a new empty [`MutableLineStringArray`].
    pub fn new() -> Self {
        Self::with_capacities(0, 0, 0)
    }

    /// Creates a new [`MutableLineStringArray`] with a capacity.
    pub fn with_capacities(
        coord_capacity: usize,
        geom_capacity: usize,
        ring_capacity: usize,
    ) -> Self {
        Self {
            x: Vec::with_capacity(coord_capacity),
            y: Vec::with_capacity(coord_capacity),
            geom_offsets: Offsets::<i64>::with_capacity(geom_capacity),
            ring_offsets: Offsets::<i64>::with_capacity(ring_capacity),
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
        ring_offsets: Offsets<i64>,
        validity: Option<MutableBitmap>,
    ) -> Result<Self, GeoArrowError> {
        check(&x, &y, validity.as_ref().map(|x| x.len()))?;
        Ok(Self {
            x,
            y,
            geom_offsets,
            ring_offsets,
            validity,
        })
    }

    /// Extract the low-level APIs from the [`MutableLineStringArray`].
    pub fn into_inner(self) -> MutablePolygonParts {
        (
            self.x,
            self.y,
            self.geom_offsets,
            self.ring_offsets,
            self.validity,
        )
    }

    pub fn into_arrow(self) -> ListArray<i64> {
        let polygon_array: PolygonArray = self.into();
        polygon_array.into_arrow()
    }
}

impl Default for MutablePolygonArray {
    fn default() -> Self {
        Self::new()
    }
}

impl From<MutablePolygonArray> for PolygonArray {
    fn from(other: MutablePolygonArray) -> Self {
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

impl From<Vec<Polygon>> for MutablePolygonArray {
    fn from(geoms: Vec<Polygon>) -> Self {
        use geo::coords_iter::CoordsIter;

        // Offset into ring indexes for each geometry
        let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each geometry has only a single ring
        let mut ring_offsets = Offsets::<i64>::with_capacity(geoms.len());

        // Current offset into ring array
        let mut current_geom_offset = 0;

        // Current offset into coord array
        let mut current_ring_offset = 0;

        for geom in &geoms {
            // Total number of rings in this polygon
            current_geom_offset += geom.interiors().len() + 1;
            geom_offsets.try_push_usize(current_geom_offset).unwrap();

            // Number of coords for each ring
            current_ring_offset += geom.exterior().coords_count();
            ring_offsets.try_push_usize(current_ring_offset).unwrap();

            for int_ring in geom.interiors() {
                current_ring_offset += int_ring.coords_count();
                ring_offsets.try_push_usize(current_ring_offset).unwrap();
            }
        }

        let mut x_arr = Vec::<f64>::with_capacity(current_ring_offset);
        let mut y_arr = Vec::<f64>::with_capacity(current_ring_offset);

        for geom in geoms {
            let ext_ring = geom.exterior();
            for coord in ext_ring.coords_iter() {
                x_arr.push(coord.x);
                y_arr.push(coord.y);
            }

            for int_ring in geom.interiors() {
                for coord in int_ring.coords_iter() {
                    x_arr.push(coord.x);
                    y_arr.push(coord.y);
                }
            }
        }

        MutablePolygonArray {
            x: x_arr,
            y: y_arr,
            geom_offsets,
            ring_offsets,
            validity: None,
        }
    }
}

impl From<Vec<Option<Polygon>>> for MutablePolygonArray {
    fn from(geoms: Vec<Option<Polygon>>) -> Self {
        use geo::coords_iter::CoordsIter;

        let mut validity = MutableBitmap::with_capacity(geoms.len());

        // Offset into ring indexes for each geometry
        let mut geom_offsets = Offsets::<i64>::with_capacity(geoms.len());

        // Offset into coordinates for each ring
        // This capacity will only be enough in the case where each geometry has only a single ring
        let mut ring_offsets = Offsets::<i64>::with_capacity(geoms.len());

        // Current offset into ring array
        let mut current_geom_offset = 0;

        // Current offset into coord array
        let mut current_ring_offset = 0;

        for geom in &geoms {
            if let Some(geom) = geom {
                validity.push(true);

                // Total number of rings in this polygon
                current_geom_offset += geom.interiors().len() + 1;
                geom_offsets.try_push_usize(current_geom_offset).unwrap();

                // Number of coords for each ring
                current_ring_offset += geom.exterior().coords_count();
                ring_offsets.try_push_usize(current_ring_offset).unwrap();

                for int_ring in geom.interiors() {
                    current_ring_offset += int_ring.coords_count();
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
            let ext_ring = geom.exterior();
            for coord in ext_ring.coords_iter() {
                x_arr.push(coord.x);
                y_arr.push(coord.y);
            }

            for int_ring in geom.interiors() {
                for coord in int_ring.coords_iter() {
                    x_arr.push(coord.x);
                    y_arr.push(coord.y);
                }
            }
        }

        MutablePolygonArray {
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
impl From<MutablePolygonArray> for MutableMultiLineStringArray {
    fn from(value: MutablePolygonArray) -> Self {
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
