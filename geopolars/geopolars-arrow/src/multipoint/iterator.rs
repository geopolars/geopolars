use crate::geo_traits::MultiPointTrait;
use crate::{GeometryArrayTrait, Point};
use crate::{MultiPoint, MultiPointArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;

/// Iterator of values of a [`MultiPointArray`]
#[derive(Clone, Debug)]
pub struct MultiPointArrayValuesIter<'a> {
    array: &'a MultiPointArray,
    index: usize,
    end: usize,
}

impl<'a> MultiPointArrayValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a MultiPointArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for MultiPointArrayValuesIter<'a> {
    type Item = MultiPoint<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(self.array.value(old))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a> TrustedLen for MultiPointArrayValuesIter<'a> {}

impl<'a> DoubleEndedIterator for MultiPointArrayValuesIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(self.array.value(self.end))
        }
    }
}

impl<'a> IntoIterator for &'a MultiPointArray {
    type Item = Option<MultiPoint<'a>>;
    type IntoIter = ZipValidity<MultiPoint<'a>, MultiPointArrayValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MultiPointArray {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<MultiPoint<'a>, MultiPointArrayValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(MultiPointArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> MultiPointArrayValuesIter<'a> {
        MultiPointArrayValuesIter::new(self)
    }
}

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct MultiPointIterator<'a> {
    geom: &'a MultiPoint<'a>,
    index: usize,
    end: usize,
}

impl<'a> MultiPointIterator<'a> {
    #[inline]
    pub fn new(geom: &'a MultiPoint<'a>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_points(),
        }
    }
}

impl<'a> Iterator for MultiPointIterator<'a> {
    type Item = crate::Point<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.point(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a> TrustedLen for MultiPointIterator<'a> {}

impl<'a> DoubleEndedIterator for MultiPointIterator<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.point(self.end)
        }
    }
}

impl<'a> IntoIterator for &'a MultiPoint<'a> {
    type Item = Point<'a>;
    type IntoIter = MultiPointIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MultiPoint<'a> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> MultiPointIterator<'a> {
        MultiPointIterator::new(self)
    }
}
