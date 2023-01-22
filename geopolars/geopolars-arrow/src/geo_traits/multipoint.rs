use std::iter::Cloned;
use std::slice::Iter;

use super::point::PointTrait;

pub trait MultiPointTrait<'a>: Send + Sync {
    type ItemType: 'a + PointTrait;
    type Iter: Iterator<Item = Self::ItemType>;

    /// An iterator over the points in this MultiPoint
    fn points(&'a self) -> Self::Iter;

    /// The number of points in this MultiPoint
    fn num_points(&'a self) -> usize;

    /// Access to a specified point in this MultiPoint
    /// Will return None if the provided index is out of bounds
    fn point(&'a self, i: usize) -> Option<Self::ItemType>;
}

impl<'a> MultiPointTrait<'a> for geo::MultiPoint<f64> {
    type ItemType = geo::Point;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn points(&'a self) -> Self::Iter {
        self.0.iter().cloned()
    }

    fn num_points(&'a self) -> usize {
        self.0.len()
    }

    fn point(&'a self, i: usize) -> Option<Self::ItemType> {
        self.0.get(i).cloned()
    }
}
