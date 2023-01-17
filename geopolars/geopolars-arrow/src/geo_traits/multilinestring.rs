use super::LineStringTrait;
use geo::{LineString, MultiLineString};

pub trait MultiLineStringTrait<'a>: Send + Sync {
    type ItemType: 'a + LineStringTrait<'a>;
    // type Iter: Iterator<Item = &'a Self::ItemType>;

    // /// An iterator over the LineStrings in this MultiLineString
    // fn lines(&'a self) -> Self::Iter;

    /// The number of lines in this MultiLineString
    fn num_lines(&'a self) -> usize;

    /// Access to a specified line in this MultiLineString
    /// Will return None if the provided index is out of bounds
    fn line(&'a self, i: usize) -> Option<Self::ItemType>;
}

impl<'a> MultiLineStringTrait<'a> for MultiLineString<f64> {
    type ItemType = LineString;
    // type Iter = Iter<'a, Self::ItemType>;

    // fn lines(&'a self) -> Self::Iter {
    //     self.0.iter()
    // }

    fn num_lines(&'a self) -> usize {
        self.0.len()
    }

    fn line(&'a self, i: usize) -> Option<Self::ItemType> {
        self.0.get(i).cloned()
    }
}
