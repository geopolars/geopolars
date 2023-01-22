use crate::{GeometryArrayTrait, WKBArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct WKBArrayValuesIter<'a> {
    array: &'a WKBArray,
    index: usize,
    end: usize,
}

impl<'a> WKBArrayValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a WKBArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for WKBArrayValuesIter<'a> {
    type Item = crate::WKB<'a>;

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

unsafe impl<'a> TrustedLen for WKBArrayValuesIter<'a> {}

impl<'a> DoubleEndedIterator for WKBArrayValuesIter<'a> {
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

impl<'a> IntoIterator for &'a WKBArray {
    type Item = Option<crate::WKB<'a>>;
    type IntoIter = ZipValidity<crate::WKB<'a>, WKBArrayValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> WKBArray {
    /// Returns an iterator of `Option<WKB>`
    pub fn iter(&'a self) -> ZipValidity<crate::WKB<'a>, WKBArrayValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(WKBArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `WKB`
    pub fn values_iter(&'a self) -> WKBArrayValuesIter<'a> {
        WKBArrayValuesIter::new(self)
    }
}
