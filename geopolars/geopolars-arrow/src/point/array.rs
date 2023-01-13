use geo::{Coord, Point};
use polars::export::arrow::array::{Array, PrimitiveArray, StructArray};
use polars::export::arrow::bitmap::utils::{BitmapIter, ZipValidity};
use polars::export::arrow::bitmap::Bitmap;
use polars::prelude::Series;

use crate::util::index_to_chunked_index;

/// Deconstructed PointArray
/// We define this as a separate struct so that we don't have to downcast on every row
#[derive(Debug, Clone)]
pub struct PointArrayParts<'a> {
    pub x: &'a PrimitiveArray<f64>,
    pub y: &'a PrimitiveArray<f64>,
    validity: Option<&'a Bitmap>,
}

impl PointArrayParts<'_> {
    pub fn len(&self) -> usize {
        self.x.len()
    }

    pub fn is_empty(&self) -> bool {
        self.x.len() == 0
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
    pub fn values_iter_geo(&self) -> impl Iterator<Item = Point> + '_ {
        self.x
            .values_iter()
            .zip(self.y.values_iter())
            .map(|(x, y)| Point::new(*x, *y))
    }

    /// Iterate over geo geometries
    pub fn iter_geo(&self) -> ZipValidity<Point, impl Iterator<Item = Point> + '_, BitmapIter> {
        ZipValidity::new_with_validity(self.values_iter_geo(), self.validity)
    }

    /// Returns the value at slot `i` as a geo object.
    ///
    /// The value of a null slot is undetermined (it can be anything).
    pub fn value_as_geo(&self, i: usize) -> Point {
        Point::new(self.x.value(i), self.y.value(i))
    }

    /// Gets the value at slot `i` as a geo object, additionally checking the validity bitmap
    pub fn get_as_geo(&self, i: usize) -> Option<Point> {
        if self.is_null(i) {
            return None;
        }

        Some(self.value_as_geo(i))
    }

    /// Returns the value at slot `i` as a GEOS geometry.
    ///
    /// The value of a null slot is undetermined (it can be anything).
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

        // TODO: handle this error
        self.get_as_geo(i).as_ref().map(|g| g.try_into().unwrap())
    }
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PointArray<'a>(pub &'a StructArray);

impl<'a> PointArray<'a> {
    pub fn get_as_geo(&self, i: usize) -> Option<Point> {
        if self.0.is_null(i) {
            return None;
        }

        let struct_array_values = self.0.values();
        let x_array_values = struct_array_values[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = struct_array_values[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        Some(Point::new(x_array_values.value(i), y_array_values.value(i)))
    }

    pub fn parts(&self) -> PointArrayParts<'a> {
        let arrays = self.0.values();
        let validity = self.0.validity();

        let x_array_values = arrays[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();
        let y_array_values = arrays[1]
            .as_any()
            .downcast_ref::<PrimitiveArray<f64>>()
            .unwrap();

        PointArrayParts {
            x: x_array_values,
            y: y_array_values,
            validity,
        }
    }

    // //  -> impl Iterator<Item = Option<Point>> + '_
    // pub fn iter(&self) -> impl Iterator<Item = Point> + '_ {
    //     let struct_array_values = self.0.values();
    //     let x_array_values = struct_array_values[0]
    //         .as_any()
    //         .downcast_ref::<PrimitiveArray<f64>>()
    //         .unwrap();
    //     let y_array_values = struct_array_values[1]
    //         .as_any()
    //         .downcast_ref::<PrimitiveArray<f64>>()
    //         .unwrap();
    //     let validity_array = self.0.validity();

    //     let tmp = if let Some(validity) = validity_array {
    //         // Note: rust-analyzer incorrectly thinks valid is an f64, but it's actually a bool
    //         // https://github.com/rust-lang/rust-analyzer/issues/11681
    //         return izip!(
    //             x_array_values.values_iter(),
    //             y_array_values.values_iter(),
    //             validity.iter()
    //         )
    //         .map(|(x, y, valid)| {
    //             if valid {
    //                 return Some(Point::new(x.clone(), y.clone()));
    //             } else {
    //                 return None;
    //             }
    //         });
    //     } else {
    //         // std::iter::zip(x_array_values.values_iter(), y_array_values.values_iter())
    //         //     .map(|(x, y)| return Some(Point::new(x.clone(), y.clone())))
    //         return izip!(x_array_values.values_iter(), y_array_values.values_iter(),)
    //             .map(|(x, y)| return Some(Point::new(x.clone(), y.clone())));
    //     };

    // todo!();
    // }
}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct PointSeries<'a>(pub &'a Series);

impl PointSeries<'_> {
    pub fn get_as_geo(&self, i: usize) -> Option<Point> {
        let (chunk_idx, local_idx) = index_to_chunked_index(self.0, i);
        let chunk = &self.0.chunks()[chunk_idx];

        let pa = PointArray(chunk.as_any().downcast_ref::<StructArray>().unwrap());
        pa.get_as_geo(local_idx)
    }

    pub fn chunks(&self) -> Vec<PointArray> {
        self.0
            .chunks()
            .iter()
            .map(|chunk| PointArray(chunk.as_any().downcast_ref::<StructArray>().unwrap()))
            .collect()
    }
}
