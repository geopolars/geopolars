use super::point::PointTrait;
use std::iter::Cloned;
use std::slice::Iter;

pub trait LineStringTrait<'a>: Send + Sync {
    type ItemType: 'a + PointTrait;
    type Iter: Iterator<Item = Self::ItemType>;

    /// An iterator over the points in this LineString
    fn points(&'a self) -> Self::Iter;

    /// The number of points in this LineString
    fn num_points(&'a self) -> usize;

    /// Access to a specified point in this LineString
    /// Will return None if the provided index is out of bounds
    fn point(&'a self, i: usize) -> Option<Self::ItemType>;
}

impl<'a> LineStringTrait<'a> for geo::LineString<f64> {
    type ItemType = geo::Coord;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn points(&'a self) -> Self::Iter {
        self.0.iter().cloned()
    }

    fn num_points(&self) -> usize {
        self.0.len()
    }

    fn point(&'a self, i: usize) -> Option<Self::ItemType> {
        self.0.get(i).cloned()
    }
}

impl<'a> LineStringTrait<'a> for &geo::LineString<f64> {
    type ItemType = geo::Coord;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn points(&'a self) -> Self::Iter {
        self.0.iter().cloned()
    }

    fn num_points(&self) -> usize {
        self.0.len()
    }

    fn point(&'a self, i: usize) -> Option<Self::ItemType> {
        self.0.get(i).cloned()
    }
}
