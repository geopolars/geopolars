use super::point::PointTrait;

pub trait MultiPointTrait<'a>: Send + Sync {
    type ItemType: 'a + PointTrait;
    type Iter: Iterator<Item = &'a Self::ItemType>;

    // /// An iterator over the points in this MultiPoint
    // fn points(&'a self) -> Self::Iter;

    /// The number of points in this MultiPoint
    fn num_points(&'a self) -> usize;

    /// Access to a specified point in this MultiPoint
    /// Will return None if the provided index is out of bounds
    fn point(&'a self, i: usize) -> Option<Self::ItemType>;
}
