use crate::geo_traits::LineStringTrait;
use crate::{GeometryArrayTrait, Point};
use crate::{LineString, LineStringArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;

/// Iterator of values of a [`LineStringArray`]
#[derive(Clone, Debug)]
pub struct LineStringArrayValuesIter<'a> {
    array: &'a LineStringArray,
    index: usize,
    end: usize,
}

impl<'a> LineStringArrayValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a LineStringArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for LineStringArrayValuesIter<'a> {
    type Item = LineString<'a>;

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

unsafe impl<'a> TrustedLen for LineStringArrayValuesIter<'a> {}

impl<'a> DoubleEndedIterator for LineStringArrayValuesIter<'a> {
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

impl<'a> IntoIterator for &'a LineStringArray {
    type Item = Option<LineString<'a>>;
    type IntoIter = ZipValidity<LineString<'a>, LineStringArrayValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> LineStringArray {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<LineString<'a>, LineStringArrayValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(LineStringArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> LineStringArrayValuesIter<'a> {
        LineStringArrayValuesIter::new(self)
    }
}

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct LineStringIterator<'a> {
    geom: &'a LineString<'a>,
    index: usize,
    end: usize,
}

impl<'a> LineStringIterator<'a> {
    #[inline]
    pub fn new(geom: &'a LineString<'a>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_points(),
        }
    }
}

impl<'a> Iterator for LineStringIterator<'a> {
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

unsafe impl<'a> TrustedLen for LineStringIterator<'a> {}

impl<'a> DoubleEndedIterator for LineStringIterator<'a> {
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

impl<'a> IntoIterator for &'a LineString<'a> {
    type Item = Point<'a>;
    type IntoIter = LineStringIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> LineString<'a> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> LineStringIterator<'a> {
        LineStringIterator::new(self)
    }
}
