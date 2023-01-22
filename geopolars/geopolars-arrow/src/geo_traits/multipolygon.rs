use super::PolygonTrait;
use geo::{MultiPolygon, Polygon};

pub trait MultiPolygonTrait<'a>: Send + Sync {
    type ItemType: 'a + PolygonTrait<'a>;
    // type Iter: Iterator<Item = &'a Self::ItemType>;

    // /// An iterator over the Polygons in this MultiPolygon
    // fn polygons(&'a self) -> Self::Iter;

    /// The number of polygons in this MultiPolygon
    fn num_polygons(&'a self) -> usize;

    /// Access to a specified polygon in this MultiPolygon
    /// Will return None if the provided index is out of bounds
    fn polygon(&'a self, i: usize) -> Option<Self::ItemType>;
}

impl<'a> MultiPolygonTrait<'a> for MultiPolygon<f64> {
    type ItemType = &'a Polygon;
    // type Iter = Iter<'a, Self::ItemType>;

    // fn polygons(&'a self) -> Self::Iter {
    //     self.0.iter()
    // }

    fn num_polygons(&'a self) -> usize {
        self.0.len()
    }

    fn polygon(&'a self, i: usize) -> Option<Self::ItemType> {
        self.0.get(i)
    }
}
