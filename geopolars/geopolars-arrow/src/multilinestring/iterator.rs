use crate::geo_traits::MultiLineStringTrait;
use crate::{GeometryArrayTrait, LineString};
use crate::{MultiLineString, MultiLineStringArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;

/// Iterator of values of a [`MultiLineStringArray`]
#[derive(Clone, Debug)]
pub struct MultiLineStringArrayValuesIter<'a> {
    array: &'a MultiLineStringArray,
    index: usize,
    end: usize,
}

impl<'a> MultiLineStringArrayValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a MultiLineStringArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for MultiLineStringArrayValuesIter<'a> {
    type Item = MultiLineString<'a>;

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

unsafe impl<'a> TrustedLen for MultiLineStringArrayValuesIter<'a> {}

impl<'a> DoubleEndedIterator for MultiLineStringArrayValuesIter<'a> {
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

impl<'a> IntoIterator for &'a MultiLineStringArray {
    type Item = Option<MultiLineString<'a>>;
    type IntoIter =
        ZipValidity<MultiLineString<'a>, MultiLineStringArrayValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MultiLineStringArray {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<MultiLineString<'a>, MultiLineStringArrayValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(MultiLineStringArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> MultiLineStringArrayValuesIter<'a> {
        MultiLineStringArrayValuesIter::new(self)
    }
}

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct MultiLineStringIterator<'a> {
    geom: &'a MultiLineString<'a>,
    index: usize,
    end: usize,
}

impl<'a> MultiLineStringIterator<'a> {
    #[inline]
    pub fn new(geom: &'a MultiLineString<'a>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_lines(),
        }
    }
}

impl<'a> Iterator for MultiLineStringIterator<'a> {
    type Item = crate::LineString<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.line(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a> TrustedLen for MultiLineStringIterator<'a> {}

impl<'a> DoubleEndedIterator for MultiLineStringIterator<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.line(self.end)
        }
    }
}

impl<'a> IntoIterator for &'a MultiLineString<'a> {
    type Item = LineString<'a>;
    type IntoIter = MultiLineStringIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MultiLineString<'a> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> MultiLineStringIterator<'a> {
        MultiLineStringIterator::new(self)
    }
}
