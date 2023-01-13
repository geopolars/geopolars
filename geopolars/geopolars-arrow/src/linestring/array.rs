use geo::{Coord, LineString};
use polars::export::arrow::array::{Array, ListArray, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::utils::{BitmapIter, ZipValidity};
use polars::export::arrow::bitmap::Bitmap;
use polars::export::arrow::offset::OffsetsBuffer;
use polars::prelude::Series;

use crate::util::index_to_chunked_index;

/// A struct representing a non-null single LineString geometry
#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct LineStringScalar(pub StructArray);

impl LineStringScalar {
    pub fn into_geo(self) -> LineString {
        let struct_array_values = self.0.values();
        let x_arrow_array = &struct_array_values[0];
        let y_arrow_array = &struct_array_values[1];

        let x_array_values = x_arrow_array
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = y_arrow_array
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        let mut coords: Vec<Coord> = Vec::with_capacity(x_array_values.len());
        for i in 0..x_array_values.len() {
            coords.push(Coord {
                x: x_array_values.value(i),
                y: y_array_values.value(i),
            })
        }

        LineString::new(coords)
    }
}

/// Deconstructed LineStringArray
/// We define this as a separate struct so that we don't have to downcast on every row
#[derive(Debug, Clone)]
pub struct LineStringArrayParts<'a> {
    pub x: &'a PrimitiveArray<f64>,
    pub y: &'a PrimitiveArray<f64>,
    pub geom_offsets: &'a OffsetsBuffer<i64>,
    pub validity: Option<&'a Bitmap>,
}

impl<'a> LineStringArrayParts<'a> {
    /// Number of geometries in this container
    pub fn len(&self) -> usize {
        self.geom_offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of null slots on this [`Array`].
    /// # Implementation
    /// This is `O(1)` since the number of null elements is pre-computed.
    #[inline]
    pub fn null_count(&self) -> usize {
        self.validity.as_ref().map(|x| x.unset_bits()).unwrap_or(0)
    }

    /// Returns whether slot `i` is null.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.validity
            .as_ref()
            .map(|x| !x.get_bit(i))
            .unwrap_or(false)
    }

    /// Returns whether slot `i` is valid.
    /// # Panic
    /// Panics iff `i >= self.len()`.
    #[inline]
    pub fn is_valid(&self, i: usize) -> bool {
        !self.is_null(i)
    }

    /// Iterate over values as coordinates without taking into account validity bitmap
    pub fn values_iter_coords(&self) -> impl Iterator<Item = Coord> + '_ {
        self.x
            .values_iter()
            .zip(self.y.values_iter())
            .map(|(x, y)| Coord { x: *x, y: *y })
    }

    /// Iterate over coordinates
    pub fn iter_coords(&self) -> ZipValidity<Coord, impl Iterator<Item = Coord> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.values_iter_coords(), self.validity)
    }

    /// Iterate over values as geo objects without taking into account validity bitmap
    pub fn values_iter_geo(&self) -> impl Iterator<Item = LineString> + '_ {
        (0..self.len()).into_iter().map(|i| self.value_as_geo(i))
    }

    /// Iterate over geo geometries
    pub fn iter_geo(
        &self,
    ) -> ZipValidity<LineString, impl Iterator<Item = LineString> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.values_iter_geo(), self.validity)
    }

    /// Returns the value at slot `i` as a geo object.
    ///
    /// The value of a null slot is undetermined (it can be anything).
    pub fn value_as_geo(&self, i: usize) -> LineString {
        let (start_idx, end_idx) = self.geom_offsets.start_end(i);
        let mut coords: Vec<Coord> = Vec::with_capacity(end_idx - start_idx);

        for i in start_idx..end_idx {
            coords.push(Coord {
                x: self.x.value(i),
                y: self.y.value(i),
            })
        }

        LineString::new(coords)
    }

    /// Gets the value at slot `i` as a geo object, additionally checking the validity bitmap
    pub fn get_as_geo(&self, i: usize) -> Option<LineString> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    #[cfg(feature = "geos")]
    pub fn get_as_geos(&self, i: usize) -> Option<geos::Geometry> {
        // TODO: handle this error
        self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
    }
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct LineStringArray<'a>(pub &'a ListArray<i64>);

impl<'a> LineStringArray<'a> {
    pub fn get(&self, i: usize) -> Option<LineStringScalar> {
        if self.0.is_null(i) {
            return None;
        }

        let value = self.0.value(i);
        let line_string_item = value.as_any().downcast_ref::<StructArray>().unwrap();
        Some(LineStringScalar(line_string_item.clone()))
    }

    pub fn get_as_geo(&self, i: usize) -> Option<LineString> {
        let line_string_item = self.get(i);
        line_string_item.map(|ls| ls.into_geo())
    }

    pub fn parts(&self) -> LineStringArrayParts<'a> {
        let struct_dyn_array = self.0.values();
        let struct_array = struct_dyn_array
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let geom_offsets = self.0.offsets();

        let validity = self.0.validity();

        let x_array_values = struct_array.values()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = struct_array.values()[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        LineStringArrayParts {
            x: x_array_values,
            y: y_array_values,
            geom_offsets,
            validity,
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct LineStringSeries<'a>(pub &'a Series);

impl LineStringSeries<'_> {
    pub fn get(&self, i: usize) -> Option<LineStringScalar> {
        let (chunk_idx, local_idx) = index_to_chunked_index(self.0, i);
        let chunk = &self.0.chunks()[chunk_idx];

        let linestring_array =
            LineStringArray(chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap());
        linestring_array.get(local_idx)
    }

    pub fn get_as_geo(&self, i: usize) -> Option<LineString> {
        let line_string_item = self.get(i);
        line_string_item.map(|ls| ls.into_geo())
    }

    pub fn chunks(&self) -> Vec<LineStringArray> {
        self.0
            .chunks()
            .iter()
            .map(|chunk| LineStringArray(chunk.as_any().downcast_ref::<ListArray<i64>>().unwrap()))
            .collect()
    }
}
