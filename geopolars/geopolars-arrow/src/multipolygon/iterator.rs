use crate::geo_traits::MultiPolygonTrait;
use crate::{GeometryArrayTrait, Polygon};
use crate::{MultiPolygon, MultiPolygonArray};
use arrow2::bitmap::utils::{BitmapIter, ZipValidity};
use arrow2::trusted_len::TrustedLen;

/// Iterator of values of a [`MultiPolygonArray`]
#[derive(Clone, Debug)]
pub struct MultiPolygonArrayValuesIter<'a> {
    array: &'a MultiPolygonArray,
    index: usize,
    end: usize,
}

impl<'a> MultiPolygonArrayValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a MultiPolygonArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for MultiPolygonArrayValuesIter<'a> {
    type Item = MultiPolygon<'a>;

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

unsafe impl<'a> TrustedLen for MultiPolygonArrayValuesIter<'a> {}

impl<'a> DoubleEndedIterator for MultiPolygonArrayValuesIter<'a> {
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

impl<'a> IntoIterator for &'a MultiPolygonArray {
    type Item = Option<MultiPolygon<'a>>;
    type IntoIter = ZipValidity<MultiPolygon<'a>, MultiPolygonArrayValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MultiPolygonArray {
    /// Returns an iterator of `Option<Point>`
    pub fn iter(
        &'a self,
    ) -> ZipValidity<MultiPolygon<'a>, MultiPolygonArrayValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(MultiPolygonArrayValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Point`
    pub fn values_iter(&'a self) -> MultiPolygonArrayValuesIter<'a> {
        MultiPolygonArrayValuesIter::new(self)
    }
}

/// Iterator of values of a [`PointArray`]
#[derive(Clone, Debug)]
pub struct MultiPolygonIterator<'a> {
    geom: &'a MultiPolygon<'a>,
    index: usize,
    end: usize,
}

impl<'a> MultiPolygonIterator<'a> {
    #[inline]
    pub fn new(geom: &'a MultiPolygon<'a>) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_polygons(),
        }
    }
}

impl<'a> Iterator for MultiPolygonIterator<'a> {
    type Item = crate::Polygon<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        self.geom.polygon(old)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a> TrustedLen for MultiPolygonIterator<'a> {}

impl<'a> DoubleEndedIterator for MultiPolygonIterator<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            self.geom.polygon(self.end)
        }
    }
}

impl<'a> IntoIterator for &'a MultiPolygon<'a> {
    type Item = Polygon<'a>;
    type IntoIter = MultiPolygonIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MultiPolygon<'a> {
    /// Returns an iterator of `Point`
    pub fn iter(&'a self) -> MultiPolygonIterator<'a> {
        MultiPolygonIterator::new(self)
    }
}
