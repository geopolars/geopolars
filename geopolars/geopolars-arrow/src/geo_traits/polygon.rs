use std::iter::Cloned;
use std::slice::Iter;

use super::LineStringTrait;
use geo::{LineString, Polygon};

pub trait PolygonTrait<'a>: Send + Sync {
    type ItemType: 'a + LineStringTrait<'a>;
    type Iter: Iterator<Item = Self::ItemType>;

    /// The exterior ring of the polygon
    fn exterior(&'a self) -> Self::ItemType;

    /// An iterator of the interior rings of this Polygon
    fn interiors(&'a self) -> Self::Iter;

    /// The number of interior rings in this Polygon
    fn num_interiors(&'a self) -> usize;

    /// Access to a specified interior ring in this Polygon
    /// Will return None if the provided index is out of bounds
    fn interior(&'a self, i: usize) -> Option<Self::ItemType>;
}

impl<'a> PolygonTrait<'a> for Polygon<f64> {
    type ItemType = LineString;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn exterior(&'a self) -> Self::ItemType {
        self.exterior().clone()
    }

    fn interiors(&'a self) -> Self::Iter {
        self.interiors().iter().cloned()
    }

    fn num_interiors(&'a self) -> usize {
        self.interiors().len()
    }

    fn interior(&'a self, i: usize) -> Option<Self::ItemType> {
        self.interiors().get(i).cloned()
    }
}

impl<'a> PolygonTrait<'a> for &Polygon<f64> {
    type ItemType = LineString;
    type Iter = Cloned<Iter<'a, Self::ItemType>>;

    fn exterior(&'a self) -> Self::ItemType {
        (*self).exterior().clone()
    }

    fn interiors(&'a self) -> Self::Iter {
        (*self).interiors().iter().cloned()
    }

    fn num_interiors(&'a self) -> usize {
        self.interiors().len()
    }

    fn interior(&'a self, i: usize) -> Option<Self::ItemType> {
        (*self).interiors().get(i).cloned()
    }
}
